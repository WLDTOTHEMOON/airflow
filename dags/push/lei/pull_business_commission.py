import logging

import pendulum
from airflow.decorators import dag, task
from include.database.mysql import engine
from airflow.models import Variable
from include.service.message import task_failure_callback
from include.feishu.feishu_sheet import FeishuSheet
import pandas as pd
from typing import Dict
import xlrd

logger = logging.getLogger(__name__)


@dag(
    dag_id='pull_business_commission',
    schedule=None,
    default_args=
    {
        'owner': 'Lei Jiangling',
        'retries': 3
        # 'on_failure_callback': task_failure_callback
    },
    tags=['pull', 'business_commission'],
    max_active_tasks=8,

)
def business_commission():
    feishu_sheet = FeishuSheet(**Variable.get('feishu', deserialize_json=True))

    def slice_name(name, name_dict):
        if name:
            name = name.split(',')
            id_list = []
            for i in name:
                if i == '孙腾':
                    i = '101280767'
                if i == '李志浩':
                    i = '3192488'
                id_list.append(name_dict[i])
            joined_string = ','.join(id_list)
            return joined_string
        else:
            return None

    def excel_time_convert(timestamp):
        if pd.isna(timestamp):
            return None
        else:
            return xlrd.xldate_as_datetime(timestamp, 0)

    def format_date(date):
        return pd.to_datetime(date).strftime('%Y-%m')

    def convert_roi(x):
        if x < 6:
            return 0
        elif 6 <= x < 8:
            return 0.8
        elif 8 <= x < 10:
            return 1
        elif 10 <= x < 11:
            return 1.1
        else:
            return 1.2

    def convert_rate(x, min=0.2, max=0.6, rate=1.5):
        k1 = 1 / (max - 0.3) ** 2
        k2 = (rate - 1) / (min - 0.3) ** 2
        if x < min:
            return rate
        elif x > max:
            return 0
        elif x > 0.3:
            return round(-k1 * (x - 0.3) ** 2 + 1, 6)
        elif x <= 0.3:
            return round(k2 * (x - 0.3) ** 2 + 1, 6)
        else:
            return None

    def percent_convert(num):
        return str(round(num * 100, 2)) + '%'

    @task
    def create_feishu_file(**kwargs):
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        begin_time = start_time.subtract(days=30).strftime('%Y-%m-01')
        end_time = start_time.subtract(days=30).end_of('month').strftime('%Y-%m-%d')
        month = start_time.subtract(days=30).strftime('%Y-%m')

        Variable.set('business_commission_start_date', begin_time)
        Variable.set('business_commission_end_date', end_time)
        Variable.set('business_commission_month', month)

        title = (month.replace('-', '') + '提成数据_' + begin_time.replace('-', '') + '_' + end_time.replace('-', '') +
                 '_' + start_time.strftime('%Y%m%d%H%M%S'))

        result = feishu_sheet.create_spreadsheet(
            title=title, folder_token='ZviUfSf0GlKLecdcXkgccGd9nUh'
        )
        spreadsheet_token = result['spreadsheet']['spreadsheet_token']
        url = result['spreadsheet']['url']
        return {
            'spreadsheet_token': spreadsheet_token,
            'url': url
        }

    @task
    def get_operation_gmv(file_info_dict: Dict):
        sql = """
            select
                live_name
                ,start
                ,end
                ,live_anchor
                ,live_account
                ,plan_admin
                ,live_admin
                ,live_staff
                ,live_asst
                ,live_operate
                ,sum(order_trade_amount) final_gmv
                ,sum(estimated_income) estimated_income
                ,remark
            from(
                select
                    live_name
                    ,start
                    ,end
                    ,live_anchor
                    ,live_account
                    ,plan_admin
                    ,live_admin
                    ,live_staff
                    ,live_asst
                    ,live_operate
                    ,remark
                from xlsd.plan p
                where date(live_date) between %(start_date)s and %(end_date)s
                    and status = 1)live
            inner join(
                select
                    o_id
                    ,account_id
                    ,order_create_time
                    ,dc.item_id
                    ,cps_order_status
                    ,order_trade_amount
                    ,estimated_income
                from(
                    select
                        o_id
                        ,account_id
                        ,order_create_time
                        ,item_id
                        ,cps_order_status
                        ,order_trade_amount
                        ,estimated_income + coalesce(estimated_service_income,0) estimated_income
                    from dwd.dwd_ks_cps_order dco
                    where order_create_time between concat(%(start_date)s, ' 04:00:00') and concat(%(end_date)s, ' 04:00:00') + interval 1 day
                        and cps_order_status != '已失效') dc
                inner join(
                    select
                        item_id
                    from dws.dws_ks_big_tbl dkeh
                    where order_date between %(start_date)s and %(end_date)s
                        and (bd_name is not null or product_id is not null)
                    group by
                        item_id
                )dw on dc.item_id = dw.item_id
            ) cps on live.live_account = cps.account_id and cps.order_create_time between live.start and live.end
            group by
                live_name
                ,start
                ,end
                ,live_anchor
                ,live_account
                ,plan_admin
                ,live_admin
                ,live_staff
                ,live_asst
                ,live_operate
                ,remark
        """
        df = pd.read_sql(sql, engine, params={'start_date': Variable.get('business_commission_start_date'),
                                              'end_date': Variable.get('business_commission_end_date')})

        user_info_sql = '''
            select
                id
                ,name
            from xlsd.users
        '''
        user_info_df = pd.read_sql(user_info_sql, engine)
        user_info_dict = dict(zip(user_info_df['id'], user_info_df['name']))

        columns_to_process = ['plan_admin', 'live_admin', 'live_staff', 'live_asst', 'live_anchor', 'live_operate']
        df[columns_to_process] = df[columns_to_process].applymap(lambda x: slice_name(x, user_info_dict))

        df.start = df.start.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        df.end = df.end.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

        df = df.rename(columns={
            'live_name': '直播场次',
            'start': '开始时间',
            'end': '结束时间',
            'live_anchor': '直播主播',
            'live_account': '直播账号',
            'plan_admin': '统筹',
            'live_admin': '中控统筹',
            'live_staff': '中控',
            'live_asst': '助播',
            'live_operate': '运营',
            'final_gmv': '结算GMV',
            'estimated_income': '佣金',
            'remark': '备注'
        })

        cps_sheet_id = feishu_sheet.create_sheet(spreadsheet_token=file_info_dict['spreadsheet_token'],
                                                 title='运营参与场次GMV')
        sheet_id = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        feishu_sheet.write_df_replace(df, file_info_dict['spreadsheet_token'], sheet_id)

    @task
    def get_association_success_rate(file_info_dict: Dict):
        df = feishu_sheet.fetch_dat('I6tXsdexFh5KGKtxSylcc4khnkb', '5bbd31')
        df = df['valueRanges'][0]['values']
        df = pd.DataFrame(df[1:], columns=['账户名称', '账户ID', '时间', '账户类型', '快手昵称', '快手ID', '总花费(元)',
                                           '当日累计GMV(元)', '当日累计ROI'])
        df = df[['时间', '总花费(元)', '当日累计GMV(元)']]
        df = df.rename(columns={
            '时间': 'date',
            '总花费(元)': 'expenditure',
            '当日累计GMV(元)': 'gmv'
        })
        df['date'] = df['date'].apply(lambda x: excel_time_convert(x).strftime('%Y-%m-%d'))

        df['date'] = df['date'].apply(format_date)
        df = df.groupby('date')[['expenditure', 'gmv']].sum().reset_index()

        award = feishu_sheet.fetch_dat('I6tXsdexFh5KGKtxSylcc4khnkb', 'Uhmnui')
        award = award['valueRanges'][0]['values']
        award = pd.DataFrame(award[1:], columns=['月份', '账户', '金额'])
        award = award[['月份', '金额']]
        award = award.rename(columns={
            '月份': 'date',
            '金额': 'money'
        })
        award = award.groupby('date')[['money']].sum().reset_index()

        result = pd.merge(df, award, on='date')

        origin_gmv_sql = '''
            select
                DATE_FORMAT(order_date, '%%Y-%%m') date
                ,sum(origin_gmv) origin_gmv
            from dws.dws_ks_big_tbl dkeh
            where order_date between %(start_date)s and %(end_date)s
                and anchor_name = '乐总'
            group by
                DATE_FORMAT(order_date, '%%Y-%%m')
        '''
        origin_gmv = pd.read_sql(origin_gmv_sql, engine,
                                 params={'start_date': Variable.get('business_commission_start_date'),
                                         'end_date': Variable.get('business_commission_end_date')})

        kpi_success_two_sql = '''
            select
                order_month date
                ,final_gmv
                ,target_final
                ,final_gmv/target_final kpi_success_2
            from(
                select
                    DATE_FORMAT(order_date, '%%Y-%%m') order_month
                    ,sum(final_gmv) final_gmv
                from dws.dws_ks_big_tbl dkeh
                where order_date between %(start_date)s and %(end_date)s
                group by
                    DATE_FORMAT(order_date, '%%Y-%%m'))bd_gmv
            left join(
                select
                    month
                    ,sum(target_final) * 10000 target_final
                from ods.ods_fs_gmv_target ogt
                where anchor != '刘海州'
                group by
                    month
            )target on bd_gmv.order_month = target.month
        '''
        kpi_success_two = pd.read_sql(kpi_success_two_sql, engine, params={
            'start_date': Variable.get('business_commission_start_date'),
            'end_date': Variable.get('business_commission_end_date')
        })

        merge_df = pd.merge(result, kpi_success_two, on='date')
        merge_df = pd.merge(merge_df, origin_gmv, on='date')

        merge_df['kpi_success_1'] = merge_df['gmv'] / (merge_df['expenditure'] - merge_df['money'])
        merge_df['kpi_success_1'] = merge_df['kpi_success_1'].apply(lambda x: convert_roi(x=x))

        merge_df['kpi_success_3'] = merge_df['gmv'] / merge_df['origin_gmv']
        merge_df['kpi_success_3'] = merge_df['kpi_success_3'].apply(lambda x: convert_rate(x=x))
        merge_df['kpi_success_comprehensive'] = merge_df['kpi_success_1'] * 0.6 + merge_df['kpi_success_2'] * 0.15 + \
                                                merge_df['kpi_success_3'] * 0.25

        merge_df = merge_df[['date', 'expenditure', 'money', 'gmv', 'origin_gmv', 'final_gmv',
                             'target_final', 'kpi_success_1', 'kpi_success_2', 'kpi_success_3',
                             'kpi_success_comprehensive']]
        merge_df = merge_df.rename(columns={
            'date': '月份',
            'expenditure': '总花费',
            'money': '奖励金使用',
            'gmv': '投流GMV',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'target_final': '结算目标',
            'kpi_success_1': 'kpi1完成率',
            'kpi_success_2': 'kpi2完成率',
            'kpi_success_3': 'kpi3完成率',
            'kpi_success_comprehensive': '综合完成率'
        })

        cps_sheet_id = feishu_sheet.create_sheet(spreadsheet_token=file_info_dict['spreadsheet_token'],
                                                 title='公会投流完成率')
        sheet_id = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        feishu_sheet.write_df_replace(merge_df, file_info_dict['spreadsheet_token'], sheet_id)

    @task
    def get_external_rate(file_info_dict: Dict):
        external_sql = '''
            select
                date_format(order_date, '%%Y-%%m') order_month
                ,sum(final_gmv) final_gmv
                ,sum(case when bd_name is null then 0 else final_gmv end) bd_gmv
                ,sum(case when bd_name is null then final_gmv else 0 end) external_gmv
                ,sum(case when bd_name is null then final_gmv else 0 end)/sum(final_gmv) rate
            from dws.dws_ks_big_tbl dkeh
            where order_date between %(start_date)s and %(end_date)s
            group by
                date_format(order_date, '%%Y-%%m')

        '''
        external = pd.read_sql(external_sql, engine, params={
            'start_date': Variable.get('business_commission_start_date'),
            'end_date': Variable.get('business_commission_end_date')
        })
        external.rate = external.rate.apply(percent_convert)
        external = external.rename(columns={
            'order_month': '月份',
            'final_gmv': '结算GMV',
            'bd_gmv': '商务选品GMV',
            'external_gmv': '外部选品GMV',
            'rate': '外部选品率'
        })
        cps_sheet_id = feishu_sheet.create_sheet(spreadsheet_token=file_info_dict['spreadsheet_token'],
                                                 title='商务外部选品率')
        sheet_id = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        feishu_sheet.write_df_replace(external, file_info_dict['spreadsheet_token'], sheet_id)

    @task
    def get_target_success_rate(file_info_dict: Dict):
        sql = '''
            select
                order_month 
                ,anchor_name
                ,sum(final_gmv) final_gmv
                ,sum(target_final) target_final
                ,if(sum(target_final) = 0, 0, sum(final_gmv) / sum(target_final)) target_success_rate
            from (
                select
                    date_format(order_date, '%%Y-%%m') order_month
                    ,anchor_name
                    ,sum(final_gmv) final_gmv
                    ,0 target_final
                from dws.dws_ks_big_tbl dkbt
                where order_date between %(start_date)s and %(end_date)s
                group by
                      1,2,4
                union all
                select
                    month order_month
                    ,anchor anchor_name
                    ,0 final_gmv
                    ,sum(target_final * 10000) target_final
                from ods.ods_fs_gmv_target ofgt 
                where month = %(month)s
                    and anchor != '刘海州'
                group by 
                    1,2,3
            )src
            group by 
                1,2
        '''
        target_success = pd.read_sql(sql, engine, params={
            'start_date': Variable.get('business_commission_start_date'),
            'end_date': Variable.get('business_commission_end_date'),
            'month': Variable.get('business_commission_month')
        })
        target_success.target_success_rate = target_success.target_success_rate.apply(percent_convert)
        target_success = target_success.rename(columns={
            'order_month': '月份',
            'anchor_name': '主播名',
            'final_gmv': '结算GMV',
            'target_final': '结算目标',
            'target_success_rate': '目标完成率'
        })
        cps_sheet_id = feishu_sheet.create_sheet(spreadsheet_token=file_info_dict['spreadsheet_token'],
                                                 title='商务整体GMV达成率')
        sheet_id = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        feishu_sheet.write_df_replace(target_success, file_info_dict['spreadsheet_token'], sheet_id)

    @task
    def get_bd_final_gmv(file_info_dict: Dict):
        sql = '''
            select
                order_date
                ,anchor_name
                ,bd_name
                ,sum(origin_gmv) origin_gmv
                ,sum(final_gmv) final_gmv
                ,sum(estimated_income + coalesce(estimated_service_income,0)) estimated_income
            from dws.dws_ks_big_tbl dkeh
            where order_date between %(start_date)s and %(end_date)s
                and bd_name is not null
                and item_id not in (
                    select
                        item_id
                    from tmp.tmp_offline_item
                )
            group by
                    order_date
                    ,anchor_name
                    ,bd_name
            order by order_date asc,origin_gmv desc
        '''
        bd_final = pd.read_sql(sql, engine, params={
            'start_date': Variable.get('business_commission_start_date'),
            'end_date': Variable.get('business_commission_end_date')
        })
        bd_final.order_date = bd_final.order_date.apply(lambda x: x.strftime('%Y-%m-%d'))
        bd_final = bd_final.rename(columns={
            'order_date': '日期',
            'anchor_name': '主播',
            'bd_name': '商务',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'estimated_income': '佣金'
        })
        cps_sheet_id = feishu_sheet.create_sheet(spreadsheet_token=file_info_dict['spreadsheet_token'],
                                                 title='商务结算GMV')
        sheet_id = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        feishu_sheet.write_df_replace(bd_final, file_info_dict['spreadsheet_token'], sheet_id)

    @task
    def get_private_commission(file_info_dict: Dict):
        sql = '''
            select
                date_format(order_create_time, '%%Y-%%m') order_month
                ,item_id
                ,item_title
                ,sum(order_trade_amount) origin_gmv
                ,sum(case when cps_order_status = '已失效' then 0 else order_trade_amount end) final_gmv
                ,sum(coalesce(estimated_income, 0) + coalesce(estimated_service_income, 0)) estimated_income
            from (
                select
                    src.account_id anchor_id
                    ,item_id
                    ,item_title
                    ,order_trade_amount
                    ,commission_rate
                    ,service_rate
                    ,estimated_income
                    ,estimated_service_income
                    ,order_create_time
                    ,cps_order_status
                from (
                    select
                        account_id
                        ,o_id
                        ,item_id
                        ,item_title
                        ,commission_rate
                        ,service_rate
                        ,order_trade_amount
                        ,estimated_income
                        ,estimated_service_income
                        ,order_create_time
                        ,cps_order_status
                    from dwd.dwd_ks_cps_order
                    where account_id in ('18541124')
                        and date(order_create_time) between %(start_date)s and %(end_date)s
                        and o_id not in (
                            select o_id
                            from ods.ods_crawler_recreation ocor
                        )
                ) src
                left join (
                    select
                        anchor_id
                        ,start_time
                        ,end_time
                    from ods.ods_crawler_anchor_live_record
                    where anchor_id in ('18541124')
                        and date(start_time) between date_sub(%(start_date)s, interval 1 day) and %(end_date)s
                ) live on src.account_id = live.anchor_id and src.order_create_time between live.start_time and live.end_time
                where start_time is null
            ) tmp
            group by
                DATE_FORMAT(order_create_time, '%%Y-%%m')
                ,item_id
                ,item_title
        '''
        private_commission_income = pd.read_sql(sql, engine, params={
            'start_date': Variable.get('business_commission_start_date'),
            'end_date': Variable.get('business_commission_end_date')
        })
        private_commission_income = private_commission_income.rename(columns={
            'order_month': '月份',
            'item_id': '商品ID',
            'item_title': '商品名称',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'estimated_income': '佣金'
        })
        cps_sheet_id = feishu_sheet.create_sheet(spreadsheet_token=file_info_dict['spreadsheet_token'],
                                                 title='私域佣金')
        sheet_id = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        feishu_sheet.write_df_replace(private_commission_income, file_info_dict['spreadsheet_token'], sheet_id)

    @task
    def get_anchor_target_rate(file_info_dict: Dict):
        sql = '''
            select 
                 project
                ,sum(final_gmv) final_gmv
                ,sum(target_final) target_final
                ,if(sum(target_final) = 0, 0, sum(final_gmv) / sum(target_final)) target_success_rate
                ,sum(estimated_income) estimated_income
            from (
                select
                     anchor_name project
                    ,sum(final_gmv) final_gmv
                    ,0 target_final
                    ,sum(estimated_income + coalesce(estimated_service_income,0)) estimated_income 
                from dws.dws_ks_big_tbl dkbt
                where order_date between %(start_date)s and %(end_date)s
                group by
                      1,3
                union all
                select 
                     anchor project
                    ,0 final_gmv
                    ,sum(target_final * 10000) target_final
                    ,0 estimated_income
                from ods.ods_fs_gmv_target ofgt 
                where month = %(month)s
                    and anchor != '刘海州'
                group by 1,2
            )src
            group by 1
        '''

        anchor_target = pd.read_sql(sql, engine, params={
            'month': Variable.get('business_commission_month'),
            'start_date': Variable.get('business_commission_start_date'),
            'end_date': Variable.get('business_commission_end_date')
        })
        anchor_target.target_success_rate = anchor_target.target_success_rate.apply(percent_convert)
        anchor_target = anchor_target.rename(columns={
            'project': '主播',
            'target_final': '结算目标',
            'final_gmv': '结算GMV',
            'target_success_rate': '目标完成率',
            'estimated_income': '佣金'
        })
        cps_sheet_id = feishu_sheet.create_sheet(spreadsheet_token=file_info_dict['spreadsheet_token'],
                                                 title='主播目标完成率')
        sheet_id = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        feishu_sheet.write_df_replace(anchor_target, file_info_dict['spreadsheet_token'], sheet_id)

    @task
    def get_activity_session(file_info_dict: Dict):
        # 获取活动场数据
        df = feishu_sheet.read_cells(spreadsheet_token='WouosDwmIhAmAxtwDPQcsmkMnbM', sheet_id='9f34bb',
                                     ranges='A1:V2000')
        df = df['valueRanges'][0]['values']
        df = pd.DataFrame(df[2:], columns=['时间', '主播账号', '主播', ' 场次类型', '活动名称', '活动策划参与人1',
                                           '活动策划参与人2', '活动策划负责人', '刘凯', '邓波', '肖鹏', '何海诺', '白玉婷',
                                           '陈沛', '陈洋', '唐豪', '陈鹏', '侯悦', '王鑫', '周佳鑫', '流量运营参与人1',
                                           '流量运营参与人2'])
        df = df.dropna(axis=0, how='all')
        df['时间'] = df['时间'].apply(lambda x: excel_time_convert(x).strftime('%Y-%m-%d'))
        table_df = df[['时间', '主播账号']]
        table_df.rename(columns={'时间': 'live_date', '主播账号': 'live_account'}, inplace=True)
        clear_sql = '''
            truncate table tmp.tmp_acticity_session
        '''
        engine.execute(clear_sql)
        table_df.to_sql('tmp_acticity_session', engine, if_exists='append', index=False, schema='tmp')

        sql = '''
            select
                live_account
                ,live_date
                ,final_gmv
                ,estimated_income
            from tmp.tmp_acticity_session ta
            left join(
                select
                    order_date
                    ,account_id
                    ,sum(final_gmv) final_gmv
                    ,sum(estimated_income + coalesce(estimated_service_income,0)) estimated_income
                from dws.dws_ks_big_tbl dkeh
                where order_date between %(start_date)s and %(end_date)s
                group by
                    1,2
            )src on ta.live_account = src.account_id and ta.live_date = src.order_date
            where live_date between %(start_date)s and %(end_date)s
            order by
                live_account asc
                ,live_date asc
        '''
        activity_session_df = pd.read_sql(sql, engine, params={
            'start_date': Variable.get('business_commission_start_date'),
            'end_date': Variable.get('business_commission_end_date')
        })
        activity_session_df.live_date = activity_session_df.live_date.apply(lambda x: x.strftime('%Y-%m-%d'))
        activity_session_df = activity_session_df.rename(columns={
            'live_account': '主播账号',
            'live_date': '时间',
            'final_gmv': '结算GMV',
            'estimated_income': '佣金'
        })

        filtered_df = df[(df['时间'] >= Variable.get('business_commission_start_date')) & (
                          df['时间'] <= Variable.get('business_commission_end_date'))].reset_index(drop=True)
        logger.info(filtered_df)
        logger.info(activity_session_df)
        filtered_df['主播账号'] = filtered_df['主播账号'].astype(int).astype(str)
        activity_session_df['主播账号'] = activity_session_df['主播账号'].astype(str)
        # merge_df = pd.merge(filtered_df, activity_session_df, how='left', on=['时间', '主播账号'])
        filtered_df = filtered_df.sort_values(by=['时间', '主播账号']).reset_index(drop=True)
        activity_session_df = activity_session_df.sort_values(by=['时间', '主播账号']).reset_index(drop=True)
        activity_session_df = activity_session_df.drop(columns=['时间', '主播账号'])

        merge_df = pd.concat([filtered_df, activity_session_df], axis=1)
        cps_sheet_id = feishu_sheet.create_sheet(spreadsheet_token=file_info_dict['spreadsheet_token'],
                                                 title='运营活动场数据')
        sheet_id = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']
        logger.info(merge_df)

        feishu_sheet.write_df_replace(merge_df, file_info_dict['spreadsheet_token'], sheet_id)

    @task
    def get_slice_a_port_data(file_info_dict):
        a_port_sql = '''
            select
                month
                ,'A端' port
                ,anchor_id
                ,sum(estimated_service_income) estimated_service_income
                ,sum(estimated_income) estimated_income
                ,sum(estimated_service_income + estimated_income) tol_income
            from (
            -- A端剪手
             select
                 A.anchor_id
                 ,month(settlement_success_time) month
                 ,sum(estimated_service_income) estimated_service_income
                 ,sum(estimated_income) estimated_income
             from (
                 select
                     anchor_id
                     ,start_time
                     ,end_time
                 from ods.ods_fs_slice_account
                 where anchor_id not in ('173119688') -- 这个账号只是作为二创作者存在的
             )A
             inner join (
                 select
                     account_id
                     ,order_create_time
                     ,order_trade_amount
                     ,settlement_success_time
                     ,coalesce(estimated_income,0) estimated_income
                     ,coalesce(estimated_service_income,0) estimated_service_income
                 from dwd.dwd_ks_cps_order
                 where o_id not in (select o_id from dwd.dwd_ks_recreation)
                     -- 自有剪手'3054930335'账号自2024-06-11后成为二创达人IP，
                    -- 为保留这个账号历史数据以及不重复计算，故需在这块抛除掉它以后产生的二创订单
                         and settlement_success_time between concat(%(start_date)s, ' 00:00:00') and concat(%(end_date)s, ' 23:59:59')
                         and cps_order_status = '已结算'
             )src on A.anchor_id = src.account_id
                 and src.order_create_time between A.start_time and A.end_time
            group by
                    1,2
            union all
            -- A端二创
            select
                 A.anchor_id
                 ,month(settlement_success_time ) month
                 ,sum(leader_origin_estimated_service_income + leader_keep_estimated_service_income) estimated_service_income
                 ,sum(anchor_keep_estimated_income + leader_share_estimated_income) estimated_income
             from (
                 select
                     anchor_id
                     ,start_time
                     ,end_time
                 from ods.ods_fs_slice_account
             )A
             inner join (
                 select
                     author_id
                     ,order_create_time
                     ,settlement_success_time
                     ,coalesce(anchor_keep_estimated_income,0) anchor_keep_estimated_income
                     ,coalesce(leader_origin_estimated_service_income,0) leader_origin_estimated_service_income
                     ,coalesce(leader_share_estimated_income,0) leader_share_estimated_income
                     ,coalesce(leader_keep_estimated_service_income,0) leader_keep_estimated_service_income
                 from dwd.dwd_ks_leader_commission_income
                 where settlement_success_time between concat(%(start_date)s, ' 00:00:00') and concat(%(end_date)s, ' 23:59:59')
                         and cps_order_status = '已结算'
             )com on A.anchor_id = com.author_id
                 and com.order_create_time between A.start_time and A.end_time
            group by
                    1,2
            ) tol
            group by
                    1,2,3
        '''
        params = {
            'start_date': Variable.get('business_commission_start_date'),
            'end_date': Variable.get('business_commission_end_date')
        }
        a_port = pd.read_sql(a_port_sql, engine, params=params)
        return a_port

    @task
    def get_slice_b_port_data(file_info_dict):
        b_port_sql = '''
                    select
                        month
                        ,'B' port
                        ,'B' anchor_id
                        ,sum(estimated_service_income) estimated_service_income
                        ,sum(estimated_income) estimated_income
                        ,sum(estimated_service_income + estimated_income) tol_income
                    from (
                    -- B端剪手
                    select
                        month(settlement_success_time) month
                        ,sum(coalesce(estimated_service_income,0)) estimated_service_income
                        ,0 estimated_income
                    from dwd.dwd_ks_leader_order
                    where activity_id in ('5084323902','5142199902','4920930902','6701551902','6244252902', '7469588902')
                        and promotion_id not in (select anchor_id from ods.ods_fs_slice_account)
                        and settlement_success_time between concat(%(start_date)s, ' 00:00:00') and concat(%(end_date)s, ' 23:59:59')
                        and cps_order_status = '已结算'
                    group by
                            1
                    union all
                    -- B端二创
                    select
                        month(settlement_success_time) month
                        ,sum(coalesce(estimated_service_income, 0)) estimated_service_income
                        ,sum(coalesce(estimated_income, 0)) estimated_income
                    from dwd.dwd_ks_recreation
                    where o_id not in (select o_id from dwd.dwd_ks_leader_commission_income)
                            and settlement_success_time between concat(%(start_date)s, ' 00:00:00') and concat(%(end_date)s, ' 23:59:59')
                      and cps_order_status = '已结算'
                    group by
                            1
                    )tol
                    group by
                            1,2,3
                '''
        params = {
            'start_date': Variable.get('business_commission_start_date'),
            'end_date': Variable.get('business_commission_end_date')
        }
        b_port = pd.read_sql(b_port_sql, engine, params=params)
        return b_port

    @task
    def get_slice_c_port_data(file_info_dict):
        c_port_sql = '''
                        select
                            month
                            ,'C' port
                            ,'C' anchor_id
                            ,sum(estimated_service_income) estimated_service_income
                            ,sum(estimated_income) estimated_income
                            ,sum(estimated_service_income + estimated_income) tol_income
                        from (
                        -- C端剪手
                        select
                            month(settlement_success_time) month
                            ,'C端剪手'
                            ,sum(coalesce(estimated_service_income,0)) estimated_service_income
                            ,0 estimated_income
                        from dwd.dwd_ks_leader_order
                        where activity_id = '5084317902'
                            and promotion_id not in (select anchor_id from ods.ods_fs_slice_account)
                            and settlement_success_time between concat(%(start_date)s, ' 00:00:00') and concat(%(end_date)s, ' 23:59:59')
                            and cps_order_status = '已结算'
                        group by
                                1,2
                        union all
                        -- C端二创
                        select
                             month(settlement_success_time) month
                             ,'C端二创'
                             ,sum(leader_origin_estimated_service_income + leader_keep_estimated_service_income) estimated_service_income
                             ,sum(anchor_keep_estimated_income + leader_share_estimated_income) estimated_income
                         from (
                             select
                                 author_id
                                 ,settlement_success_time
                                 ,coalesce(anchor_keep_estimated_income,0) anchor_keep_estimated_income
                                 ,coalesce(leader_origin_estimated_service_income,0) leader_origin_estimated_service_income
                                 ,0 leader_share_estimated_income
                                 ,coalesce(leader_keep_estimated_service_income,0) leader_keep_estimated_service_income
                             from dwd.dwd_ks_leader_commission_income
                             where settlement_success_time between concat(%(start_date)s, ' 00:00:00') and concat(%(end_date)s, ' 23:59:59')
                                     and cps_order_status = '已结算'
                        ) com
                         left join (
                             select
                                 anchor_id
                             from ods.ods_fs_slice_account
                         )A on com.author_id = A.anchor_id
                         where A.anchor_id is null
                        group by
                                1,2
                        )tol
                        group by
                                1,2,3
                    '''
        params = {
            'start_date': Variable.get('business_commission_start_date'),
            'end_date': Variable.get('business_commission_end_date')
        }
        c_port = pd.read_sql(c_port_sql, engine, params=params)
        return c_port

    @task
    def get_slice_mcn_port_data(file_info_dict):
        mcn_port_sql = '''
                       select
                            month(settlement_success_time) month
                            ,'MCN' port
                            ,'MCN' anchor_id
                            ,sum(coalesce(mcn.settlement_amount,0)) estimated_service_income
                            ,0 estimated_income
                            ,sum(coalesce(mcn.settlement_amount,0)) tol_income
                        from dwd.dwd_ks_cps_order dkco
                        left join(
                            select
                                o_id
                                ,settlement_amount
                            from ods.ods_crawler_mcn_order ocmo
                        ) mcn on dkco.o_id = mcn.o_id
                        where account_id in(
                            select
                                account_id
                            from dim.dim_ks_account_info dkai
                            where anchor_name = '樊欣羽'
                        )
                            and settlement_success_time between concat(%(start_date)s, ' 00:00:00') and concat(%(end_date)s, ' 23:59:59')
                            and cps_order_status = '已结算'
                        group by
                            1,2,3
                        '''
        params = {
            'start_date': Variable.get('business_commission_start_date'),
            'end_date': Variable.get('business_commission_end_date')
        }
        mcn_port = pd.read_sql(mcn_port_sql, engine, params=params)
        return mcn_port

    @task
    def merge_slice_data(file_info_dict: Dict, a_port, b_port, c_port, mcn_port):
        all_slice = pd.concat([a_port, b_port, c_port, mcn_port], axis=0, ignore_index=True)
        all_slice = all_slice.rename(columns={
            'month': '月份',
            'port': '端口',
            'anchor_id': '账号ID',
            'estimated_service_income': '团长端佣金',
            'estimated_income': '主播端佣金',
            'tol_income': '总佣金'
        })
        cps_sheet_id = feishu_sheet.create_sheet(spreadsheet_token=file_info_dict['spreadsheet_token'],
                                                 title='切片端口数据')
        sheet_id = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        feishu_sheet.write_df_replace(all_slice, file_info_dict['spreadsheet_token'], sheet_id)

    file_info = create_feishu_file()
    get_operation_gmv(file_info)
    get_association_success_rate(file_info)
    get_external_rate(file_info)
    get_target_success_rate(file_info)
    get_bd_final_gmv(file_info)
    get_private_commission(file_info)
    get_anchor_target_rate(file_info)
    get_activity_session(file_info)
    a_port_df = get_slice_a_port_data(file_info)
    b_port_df = get_slice_b_port_data(file_info)
    c_port_df = get_slice_c_port_data(file_info)
    mcn_port_df = get_slice_mcn_port_data(file_info)
    merge_slice_data(file_info, a_port_df, b_port_df, c_port_df, mcn_port_df)


business_commission()
