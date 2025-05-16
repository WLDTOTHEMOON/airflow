import logging
from typing import Dict, Any

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.zhaoyifan.data_push_zhao.base_dag import BaseDag
from dags.push.zhaoyifan.data_push_zhao.format_utils import *

logger = logging.getLogger(__name__)


class WyPerformance(BaseDag):
    def __init__(self):
        super().__init__(
            dag_id='push_wy_performance',
            robot_url=Variable.get('Sesshoumaru'),
            tags=['push', 'wy_performance'],
            schedule='0 5 * * *'
        )
        self.card_id = ['AAq4P9q346tpL', 'AAq4u3UPfmZNs', 'AAq4u7W1Ehi8v', 'AAq4uDwOlobJU']

    def fetch_data_logic(self, date_interval: Dict[str, Any]) -> Dict[str, Any]:
        yes_tol_sql = f'''
            select 
                sum(origin_gmv) origin_gmv 
                ,sum(final_gmv) final_gmv 
                ,sum(final_gmv) / sum(origin_gmv) final_rate
                ,sum(coalesce(estimated_income,0)) + sum(coalesce(estimated_service_income,0)) commission_income
            from dws.dws_ks_big_tbl dkbt  
            where order_date = '{date_interval['yes_ds']}'
                and anchor_name in (
                        select 
                            distinct anchor_name  
                        from dim.dim_ks_account_info dkai
                        where group_leader = '吴月'
                            and anchor_name not in ('夜郎','梦洋','金秀妍','小鲁班','李芷墨','葛珊')
                    )
        '''
        yes_tol_df = pd.read_sql(yes_tol_sql, self.engine)

        month_tol_sql = f'''
           select 
               origin_gmv
               ,final_gmv
               ,final_rate
               ,target_final
               ,final_gmv / target_final target_success_rate
               ,commission_income
           from (
               select 
                   sum(origin_gmv) origin_gmv 
                   ,sum(final_gmv) final_gmv 
                   ,sum(final_gmv) / sum(origin_gmv) final_rate
                   ,sum(coalesce(estimated_income,0)) + sum(coalesce(estimated_service_income,0)) commission_income
               from dws.dws_ks_big_tbl dkbt
               where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                   and anchor_name  in (
                           select 
                           distinct anchor_name  
                       from dim.dim_ks_account_info dkai
                       where group_leader = '吴月'
                        and anchor_name not in ('夜郎','梦洋','金秀妍','小鲁班','李芷墨','葛珊')
                   )
           ) gmv,
           (
               select 
                   sum(target_final * 10000) target_final
               from ods.ods_fs_gmv_target ofgt 
               where month = '{date_interval['month']}'
                   and anchor in (
                           select 
                               distinct anchor_name 
                           from dim.dim_ks_account_info dkai 
                           where group_leader = '吴月'
                       )	
           ) target
       '''
        month_tol_df = pd.read_sql(month_tol_sql, self.engine)

        anchor_sql = f'''
            select 
                order_date
                ,case 
                    when anchor_name = '阿凯' then '小凯'
                    else anchor_name
                end anchor_name 
                ,sum(origin_gmv) origin_gmv 
                ,sum(final_gmv) final_gmv 
                ,sum(final_gmv) / sum(origin_gmv) final_rate
                ,sum(coalesce(estimated_income,0)) + sum(coalesce(estimated_service_income,0)) commission_income
            from dws.dws_ks_big_tbl dkbt
            where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                and anchor_name in (
                    select 
                        distinct anchor_name 
                    from dim.dim_ks_account_info dkai
                    where group_leader = '吴月'
                )
            group by 1,2
            order by 
                order_date
                ,origin_gmv desc
        '''
        anchor_df = pd.read_sql(anchor_sql, self.engine)
        yes_anchor_df = anchor_df[anchor_df.order_date.astype(str) >= date_interval['yes_ds']]
        # yes_anchor_df['order_date'] = yes_anchor_df['order_date'].astype('str')
        # yes_anchor_df['origin_gmv'] = (yes_anchor_df['origin_gmv'] / 10000).round(1)
        # yes_anchor_df['final_gmv'] = (yes_anchor_df['final_gmv'] / 10000).round(1)
        # yes_anchor_df['commission_income'] = (yes_anchor_df['commission_income'] / 10000).round(1)

        month_anchor_df = anchor_df[['anchor_name', 'origin_gmv', 'final_gmv', 'commission_income']]
        month_anchor_df = month_anchor_df.groupby('anchor_name').sum().reset_index()
        month_anchor_df = month_anchor_df.sort_values('origin_gmv', ascending=False)
        month_anchor_df['final_rate'] = month_anchor_df.final_gmv / month_anchor_df.origin_gmv
        # month_anchor_df = anchor_df
        # month_anchor_df['order_date'] = pd.to_datetime(month_anchor_df['order_date'])
        # month_anchor_df.insert(0, 'month', month_anchor_df['order_date'].dt.strftime('%Y-%m'))
        # month_anchor_df = month_anchor_df.groupby(['month', 'anchor_name']).sum(numeric_only=True).reset_index()
        # month_anchor_df = month_anchor_df.sort_values('final_gmv', ascending=False)
        # month_anchor_df['final_rate'] = month_anchor_df.final_gmv / month_anchor_df.origin_gmv
        # month_anchor_df['origin_gmv'] = (month_anchor_df['origin_gmv'] / 10000).round(1)
        # month_anchor_df['final_gmv'] = (month_anchor_df['final_gmv'] / 10000).round(1)
        # month_anchor_df['commission_income'] = (month_anchor_df['commission_income'] / 10000).round(1)

        anchor_target_sql = f'''
            select 
                anchor_name
                ,sum(final_gmv) final_gmv
                ,sum(target_final) target_final
                ,case
                    when sum(target_final) = 0 then 0
                    else sum(final_gmv) / sum(target_final) 
                end target_success_rate
            from (
                select 
                    case 
                        when anchor_name = '阿凯' then '小凯'
                        else anchor_name
                    end anchor_name
                    ,sum(final_gmv) final_gmv
                    ,0 target_final
                from dws.dws_ks_big_tbl dkbt
                where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                    and anchor_name  in (
                        select 
                            distinct anchor_name  
                        from dim.dim_ks_account_info dkai
                        where group_leader = '吴月'
                    )
                group by 1,3
                union all
                select 
                   case 
                        when anchor = '阿凯' then '小凯'
                        else anchor
                   end anchor_name
                   ,0 final_gmv
                   ,sum(target_final * 10000) target_final
                from ods.ods_fs_gmv_target ofgt 
                where month = '{date_interval['month']}'
                      and anchor in (
                          select 
                              distinct anchor_name 
                          from dim.dim_ks_account_info dkai
                          where group_leader = '吴月'
                      )	
                group by 1,2
            ) gmv
            group by 1
            order by final_gmv desc
        '''
        month_anchor_target_df = pd.read_sql(anchor_target_sql, self.engine)
        # month_anchor_target_df['final_gmv'] = (month_anchor_target_df['final_gmv'] / 10000).round(1)
        # month_anchor_target_df['target_final'] = (month_anchor_target_df['target_final'] / 10000).round(1)

        return {
            'yes_tol_df': yes_tol_df,
            'month_tol_df': month_tol_df,
            'anchor_df': anchor_df,
            'yes_anchor_df': yes_anchor_df,
            'month_anchor_df': month_anchor_df,
            'month_anchor_target_df': month_anchor_target_df,
            'date_interval': date_interval
        }

    def prepare_card_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        yes_tol_df = data_dict['yes_tol_df']
        if len(yes_tol_df) == 0:
            yes_origin_gmv = '0'
            yes_final_gmv = '0'
            yes_final_rate = '-'
            yes_commission_income = '0'
        else:
            yes_origin_gmv = str(round(yes_tol_df['origin_gmv'].iloc[0] / 10000, 1))
            yes_final_gmv = str(round(yes_tol_df['final_gmv'].iloc[0] / 10000, 1))
            yes_final_rate = str(
                '{:.2f}%'.format(yes_tol_df.final_rate.iloc[0] * 100))
            yes_commission_income = str(round(yes_tol_df['commission_income'].iloc[0] / 10000, 1))

        month_tol_df = data_dict['month_tol_df']
        if pd.isnull(month_tol_df.origin_gmv.iloc[0]):
            month_origin_gmv = '0'
            month_final_gmv = '0'
            month_final_rate = '\\-'
            month_commission_income = '0'
        else:
            month_origin_gmv = str(round(month_tol_df['origin_gmv'].iloc[0] / 10000, 1))
            month_final_gmv = str(round(month_tol_df['final_gmv'].iloc[0] / 10000, 1))
            month_commission_income = str(round(month_tol_df['commission_income'].iloc[0] / 10000, 1))
            month_final_rate = str(
                '{:.2f}%'.format(month_tol_df.final_rate.iloc[0] * 100))
        if pd.isnull(month_tol_df.target_final.iloc[0]):
            month_target = '\\-'
            month_target_success_rate = '\\-'
        else:
            month_target = str(round(month_tol_df['target_final'].iloc[0] / 10000, 1))
            month_target_success_rate = str(
                '{:.2f}%'.format(month_tol_df.target_success_rate.iloc[0] * 100))

        yes_anchor_df = data_dict['yes_anchor_df']
        yes_anchor_data = []
        for i in range(yes_anchor_df.shape[0]):
            yes_anchor_data.append({
                'anchor_leader': str(yes_anchor_df.anchor_name.iloc[i]),
                'leader_yes_origin_gmv': str(round(yes_anchor_df.origin_gmv.iloc[i] / 10000, 1)),
                'leader_yes_residue_gmv': str(round(yes_anchor_df.final_gmv.iloc[i] / 10000, 1)),
                'leader_yes_commission_income': str(round(yes_anchor_df.commission_income.iloc[i] / 10000, 1)),
                'leader_yes_retrun_rate': str('{:.2f}%'.format(yes_anchor_df.final_rate.iloc[i] * 100))
            })

        month_anchor_df = data_dict['month_anchor_df']
        month_anchor_data = []
        for i in range(month_anchor_df.shape[0]):
            month_anchor_data.append({
                'anchor_leader': str(month_anchor_df.anchor_name.iloc[i]),
                'leader_month_origin_gmv': str(round(month_anchor_df.origin_gmv.iloc[i] / 10000, 1)),
                'leader_month_residue_gmv': str(round(month_anchor_df.final_gmv.iloc[i] / 10000, 1)),
                'leader_month_commission_income': str(round(month_anchor_df.commission_income.iloc[i] / 10000, 1)),
                'leader_success_rate': str('{:.2f}%'.format(month_anchor_df.final_rate.iloc[i] * 100))
            })

        month_anchor_target_df = data_dict['month_anchor_target_df']
        month_target_data = []
        for i in range(month_anchor_target_df.shape[0]):
            anchor_target_formatted = month_anchor_target_df.target_final.iloc[i]
            if anchor_target_formatted == 0:
                anchor_target_formatted = '\\-'
                target_success_formatted = '\\-'
            else:
                anchor_target_formatted = str(round(month_anchor_target_df.target_final.iloc[i] / 10000, 1))
                target_success_formatted = str(
                    '{:.2f}%'.format(month_anchor_target_df.target_success_rate.iloc[i] * 100))
            month_target_data.append({
                'anchor_leader': str(month_anchor_target_df.anchor_name.iloc[i]),
                'leader_month_origin_gmv': str(round(month_anchor_target_df.final_gmv.iloc[i] / 10000, 1)),
                'leader_month_target': anchor_target_formatted,
                'leader_month_target_success_rate': target_success_formatted

            })

        return {
            'card_1': {
                'time_score': data_dict['date_interval']['month_start_ds'] + ' to ' + data_dict['date_interval']['yes_ds'],
                'yes_day': data_dict['date_interval']['yes_ds'],
                'leader_yes_origin_gmv': str(yes_origin_gmv),
                'leader_yes_residue_gmv': str(yes_final_gmv),
                'leader_yes_commission_income': str(yes_commission_income),
                'leader_yes_retrun_rate': str(yes_final_rate),
                'gmv_sum': str(month_origin_gmv),
                'residue_gmv_sum': str(month_final_gmv),
                'commission_sum': str(month_commission_income),
                'anchor_return_date': str(month_final_rate),
                'anchor_target': str(month_target),
                'anchor_target_success_rate': str(month_target_success_rate)
            },
            'card_2': {
                'yes_day': data_dict['date_interval']['yes_ds'],
                'leader_yes_data': yes_anchor_data
            },
            'card_3': {
                'time_score': data_dict['date_interval']['month_start_ds'] + ' to ' + data_dict['date_interval'][
                    'yes_ds'],
                'leader_month_data': month_anchor_data
            },
            'card_4': {
                'time_score': data_dict['date_interval']['month_start_ds'] + ' to ' + data_dict['date_interval'][
                    'yes_ds'],
                'anchor_data': month_target_data
            }
        }

    def write_to_sheet_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        # yes_anchor_df = data_dict['yes_anchor_df']
        # yes_anchor_df.rename(columns={
        #     'order_date': '卖货日期',
        #     'anchor_name': '主播',
        #     'origin_gmv': '支付GMV(万)',
        #     'final_gmv': '结算GMV(万)',
        #     'final_rate': '结算率',
        #     'commission_income': '佣金(万)'
        # }, inplace=True)
        # yes_style_dict = {
        #     'A1:' + col_convert(yes_anchor_df.shape[1]) + '1': {
        #         'font': {
        #             'bold': True
        #         },
        #         # 'backColor': '#FFFF00'
        #     },
        #     'E2:E' + str(yes_anchor_df.shape[0] + 1): {
        #         'formatter': "0.00%"
        #     }
        # }
        #
        # month_anchor_df = data_dict['month_anchor_df']
        # month_anchor_df.rename(columns={
        #     'month': '月份',
        #     'anchor_name': '主播',
        #     'origin_gmv': '支付GMV(万)',
        #     'final_gmv': '结算GMV(万)',
        #     'final_rate': '结算率',
        #     'commission_income': '佣金(万)'
        # }, inplace=True)
        # month_style_dict = {
        #     'A1:' + col_convert(month_anchor_df.shape[1]) + '1': {
        #         'font': {
        #             'bold': True
        #         },
        #         # 'backColor': '#FFFF00'
        #     },
        #     'E2:E' + str(month_anchor_df.shape[0] + 1): {
        #         'formatter': "0.00%"
        #     }
        # }
        #
        # month_anchor_target_df = data_dict['month_anchor_target_df']
        # month_anchor_target_df.rename(columns={
        #     'month': '月份',
        #     'anchor_name': '主播',
        #     'final_gmv': '结算GMV(万)',
        #     'target_final': '结算目标(万)',
        #     'target_success_rate': '完成率'
        # }, inplace=True)
        # target_style_dict = {
        #     'A1:' + col_convert(month_anchor_target_df.shape[1]) + '1': {
        #         'font': {
        #             'bold': True
        #         },
        #         # 'backColor': '#FFFF00'
        #     },
        #     'E2:E' + str(month_anchor_target_df.shape[0] + 1): {
        #         'formatter': "0.00%"
        #     }
        # }
        #
        # sheet_title = f"各主播业绩数据_{data_dict['date_interval']['month_start_time']}_{data_dict['date_interval']['yes_time']}_{data_dict['date_interval']['now_time']}"
        # url, spreadsheet_token = self.feishu_sheet_supply.get_workbook_params(
        #     workbook_name=sheet_title, folder_token='Fn9ZfxSxylvMSsdwzGwcZPEGn9j'
        # )
        # yes_sheet_id = self.feishu_sheet_supply.get_sheet_params(
        #     spreadsheet_token=spreadsheet_token, sheet_name='昨日各主播业绩'
        # )
        # month_sheet_id = self.feishu_sheet_supply.get_sheet_params(
        #     spreadsheet_token=spreadsheet_token, sheet_name='各主播月度业绩汇总'
        # )
        # target_sheet_id = self.feishu_sheet_supply.get_sheet_params(
        #     spreadsheet_token=spreadsheet_token, sheet_name='各主播月度目标完成率'
        # )
        #
        # self.feishu_sheet.write_df_replace(
        #     dat=yes_anchor_df, spreadsheet_token=spreadsheet_token, sheet_id=yes_sheet_id, to_char=False)
        # self.feishu_sheet.write_df_replace(
        #     dat=month_anchor_df, spreadsheet_token=spreadsheet_token, sheet_id=month_sheet_id, to_char=False)
        # self.feishu_sheet.write_df_replace(
        #     dat=month_anchor_target_df, spreadsheet_token=spreadsheet_token, sheet_id=target_sheet_id, to_char=False)
        #
        # for key, value in yes_style_dict.items():
        #     self.feishu_sheet.style_cells(
        #         spreadsheet_token=spreadsheet_token, sheet_id=yes_sheet_id, ranges=key, styles=value
        #     )
        # for key, value in month_style_dict.items():
        #     self.feishu_sheet.style_cells(
        #         spreadsheet_token=spreadsheet_token, sheet_id=month_sheet_id, ranges=key, styles=value
        #     )
        # for key, value in target_style_dict.items():
        #     self.feishu_sheet.style_cells(
        #         spreadsheet_token=spreadsheet_token, sheet_id=target_sheet_id, ranges=key, styles=value
        #     )
        #
        # return {
        #     'sheet_params': {
        #         'sheet_title': sheet_title,
        #         'url': url
        #     },
        #     'date_interval': data_dict['date_interval']
        # }
        return {}

    def send_card_logic(self, card: Dict[str, Any], sheet):
        res1 = {**card['card_1']}
        res2 = {**card['card_2']}
        res3 = {**card['card_3']}
        res4 = {**card['card_4']}
        self.feishu_robot.send_msg_card(data=res1, card_id=self.card_id[0], version_name='1.0.0')
        self.feishu_robot.send_msg_card(data=res2, card_id=self.card_id[1], version_name='1.0.1')
        self.feishu_robot.send_msg_card(data=res3, card_id=self.card_id[2], version_name='1.0.0')
        self.feishu_robot.send_msg_card(data=res4, card_id=self.card_id[3], version_name='1.0.1')
        # self.feishu_robot.send_msg_card(data=res, card_id=self.card_id, version_name='1.0.3')


dag = WyPerformance().create_dag()
