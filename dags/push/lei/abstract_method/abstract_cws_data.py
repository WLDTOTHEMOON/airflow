import logging
from typing import Any, Dict

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.lei.abstract_method.abstract_base import AbstractDagTask

logger = logging.getLogger(__name__)


class AbstractCwsData(AbstractDagTask):
    def __init__(self):
        super().__init__(
            dag_id='push_cw_data',
            schedule='0 7 * * *',
            default_args=
            {
                'owner': 'Lei Jiangling',
                'start_date': pendulum.datetime(2023, 1, 1)
            },
            tags=['push', 'cw_data'],
            robot_url=Variable.get('TEST')
        )
        self.card_id: str = 'AAqRPrHrP2wKb'

    def fetch_data(self, **kwargs) -> Dict:
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        begin_time = start_time.subtract(days=1).strftime('%Y-%m-01')
        end_time = start_time.subtract(days=1).strftime('%Y-%m-%d')
        sql = """
            select
                order_date
                ,account_id
                ,anchor_name
                ,bd_name
                ,product_id
                ,gmv.item_id
                ,item_title
                ,item_price
                ,commission_rate
                ,service_rate
                ,all_rate
                ,comRatio/100 comratio
                ,settle_way
                ,origin_gmv
                ,origin_order_num
                ,final_gmv
                ,final_order_num
                ,estimated_income
                ,settlement_gmv
                ,settlement_amount
            from(
                select
                    date(order_create_time - interval 4 hour) order_date
                    ,dkco.account_id
                    ,anchor_name
                    ,item_id
                    ,item_title
                    ,min(order_trade_amount) item_price
                    ,commission_rate
                    ,coalesce(service_rate,0) service_rate
                    ,commission_rate + coalesce(service_rate,0) all_rate
                    ,sum(order_trade_amount) origin_gmv
                    ,count(*) origin_order_num
                    ,sum(case when cps_order_status = '已失效' then 0 else order_trade_amount end) final_gmv
                    ,count(case when cps_order_status = '已失效' then null else 1 end) final_order_num
                    ,sum(case when cps_order_status = '已失效' then 0 else estimated_income + coalesce(estimated_service_income,0) end) estimated_income
                    ,sum(case when cps_order_status = '已结算' then order_trade_amount else 0 end) settlement_gmv
                    ,sum(case when cps_order_status = '已结算' then settlement_amount + coalesce(leader_settlement_amount,0) else 0 end) settlement_amount
                from dwd.dwd_ks_cps_order dkco
                left join(
                    select
                        account_id
                        ,anchor_name
                        ,line
                    from dim.dim_ks_account_info dkai
                ) ai on dkco.account_id = ai.account_id
                where date(order_create_time - interval 4 hour) between %(begin_time)s and %(end_time)s
                    and o_id not in ( select o_id from dwd.dwd_ks_recreation )
                    and o_id not in ( select o_id from dwd.dwd_ks_leader_commission_income )
                    and line in ('直播电商','其它')
                group by
                    date(order_create_time - interval 4 hour)
                    ,dkco.account_id
                    ,anchor_name
                    ,item_id
                    ,item_title
                    ,commission_rate + coalesce(service_rate,0)
                    ,commission_rate
                    ,coalesce(service_rate,0)
                union all
                select
                    '合计' as order_date
                    ,null as account_id
                    ,null as anchor_name
                    ,null as item_id
                    ,null as item_title
                    ,null as item_price
                    ,null as commission_rate
                    ,null as service_rate
                    ,null as all_rate
                    ,sum(order_trade_amount) origin_gmv
                    ,count(*) origin_order_num
                    ,sum(case when cps_order_status = '已失效' then 0 else order_trade_amount end) final_gmv
                    ,count(case when cps_order_status = '已失效' then null else 1 end) final_order_num
                    ,sum(case when cps_order_status = '已失效' then 0 else estimated_income + coalesce(estimated_service_income,0) end) estimated_income
                    ,sum(case when cps_order_status = '已结算' then order_trade_amount else 0 end) settlement_gmv
                    ,sum(case when cps_order_status = '已结算' then settlement_amount + coalesce(leader_settlement_amount,0) else 0 end) settlement_amount
                from dwd.dwd_ks_cps_order dkco
                inner join(
                    select
                        account_id,
                        anchor_name
                    from dim.dim_ks_account_info dkai
                    where line in ('其它', '直播电商')
                ) ai on dkco.account_id = ai.account_id
                where date(order_create_time - interval 4 hour) between %(begin_time)s and %(end_time)s
                    and o_id not in ( select o_id from dwd.dwd_ks_recreation )
                    and o_id not in ( select o_id from dwd.dwd_ks_leader_commission_income ))gmv
            left join(
                select
                    item_id
                    ,bd_name
                    ,settle_way
                    ,dws.product_id
                    ,comRatio
                from(
                    select
                        distinct
                        item_id
                        ,bd_name
                        ,product_id
                    from dws.dws_ks_big_tbl dkeh 
                    where product_id is not null
                        and order_date between %(begin_time)s and %(end_time)s) dws
                    left join(
                        select
                            select_product_ids
                            ,itemId
                            ,case 
                                when settle is null then settle_way
                                else settle
                            end settle_way
                            ,select_product_id
                            ,product_id
                        from(
                            select
                                select_product_ids
                                ,itemId
                                ,case
                                    when settle = 0 then '线上'
                                    when settle = 1 then '线下'
                                end settle
                                ,case
                                    when settle_way = 0 then '线上'
                                    when settle_way = 1 then '线下'
                                    when settle_way = 2 then '现结'
                                end settle_way
                                ,src.select_product_id
                                ,src.product_id
                                ,row_number() over (partition by itemId order by created_at desc) rn
                            from(
                                select
                                    case 
                                        when select_product_id = '' then product_id
                                        else select_product_id
                                    end select_product_ids
                                    ,product_id
                                    ,select_product_id
                                    ,itemId
                                    ,created_at
                                from xlsd.link l
                                where status = 1) src
                            left join(
                                select
                                    asp.id
                                    ,select_id
                                    ,product_id
                                    ,settle
                                from xlsd.anchor_select_product asp
                                left join(
                                    select
                                        *
                                    from xlsd.anchor_select 
                                )ass on asp.select_id = ass.id
                            )as2 on src.select_product_ids = as2.id
                            left join(
                                select
                                    id
                                    ,by_anchor
                                    ,settle_way
                                from xlsd.products p
                            )pd on src.select_product_ids = pd.id)st
                        where rn = 1) stl on dws.item_id = stl.itemId
                    left join(
                        select
                            id
                            ,comRatio
                        from xlsd.products p 
                    )pd on dws.product_id = pd.id
            )pdi on gmv.item_id = pdi.item_id
        """
        cw_df = pd.read_sql(sql, self.engine, params={'begin_time': begin_time, 'end_time': end_time})
        return {
            'cw_data': cw_df
        }

    def process_data(self, data: Dict, **kwargs) -> Dict:
        cw_process_data = data['cw_data']
        data_one = cw_process_data[cw_process_data['anchor_name'] != '合计']
        data_one = data_one.groupby('anchor_name')[['origin_gmv', 'final_gmv', 'estimated_income']].sum().reset_index()
        data_one['origin_estimated_income'] = data_one['origin_gmv'] * (
                data_one['estimated_income'] / data_one['final_gmv'])
        data_one = data_one[['anchor_name', 'origin_gmv', 'origin_estimated_income', 'final_gmv', 'estimated_income']]
        totals = data_one[['origin_gmv', 'origin_estimated_income', 'final_gmv', 'estimated_income']].sum()
        totals['anchor_name'] = '合计'
        # 将 totals 转换为 DataFrame 并附加到 data_one
        totals_df = pd.DataFrame(totals).T
        totals_df = totals_df[data_one.columns]
        data_one = pd.concat([data_one, totals_df], ignore_index=True)
        data_one = data_one.fillna(0)
        data_one = data_one.rename(columns={
            'anchor_name': '主播名', 'origin_gmv': '支付GMV', 'origin_estimated_income': '支付预估佣金',
            'final_gmv': '结算GMV', 'estimated_income': '结算预估佣金'
        })
        dat_three = cw_process_data.rename(columns={
            'order_date': '订单日期', 'account_id': '主播ID', 'anchor_name': '主播名称',
            'item_id': '商品ID', 'item_title': '商品名称',
            'item_price': '商品单价', 'all_rate': '总佣金率', 'commission_rate': '主播佣金率',
            'service_rate': '团长佣金率', 'origin_gmv': '支付gmv',
            'origin_order_num': '支付订单数量', 'final_gmv': '结算GMV',
            'final_order_num': '结算订单数量', 'estimated_income': '预估佣金收入', 'settlement_gmv': '已结算GMV',
            'settlement_amount': '已结算佣金', 'bd_name': '商务', 'product_id': '系统内ID', 'comratio': '提报佣金',
            'settle_way': '提报结算方式'}
        )
        dat_three.loc[:, ["订单日期"]] = dat_three.loc[:, ["订单日期"]].astype(str)

        return {
            'data_one': data_one,
            'data_three': dat_three
        }

    def create_feishu_file(self, data_dic: Dict, **kwargs) -> Dict:
        logger.info(f'创建飞书文件')
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        title = ('财务数据_' + start_time.subtract(days=1).strftime('%Y%m01') + '_' +
                 start_time.subtract(days=1).strftime('%Y%m%d') + '_' + start_time.strftime('%Y%m%d%H%M'))

        result = self.feishu_sheet.create_spreadsheet(
            title=title, folder_token='NHiCfVfyJlIGoMd0wTacOhiinyY'
        )
        spreadsheet_token = result['spreadsheet']['spreadsheet_token']
        url = result['spreadsheet']['url']
        cps_sheet_id_three = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='三级表')
        cps_sheet_id_three = cps_sheet_id_three['replies'][0]['addSheet']['properties']['sheetId']

        cps_sheet_id_one = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='一级表')
        cps_sheet_id_one = cps_sheet_id_one['replies'][0]['addSheet']['properties']['sheetId']

        return {
            'spreadsheet_token': spreadsheet_token,
            'cps_sheet_id': [cps_sheet_id_one, cps_sheet_id_three],
            'url': url,
            'title': title
        }

    def render_feishu_format(
            self,
            processed_data: Any,
            spreadsheet_token: str,
            sheet_id: str
    ):
        data_one = processed_data['data_one']
        data_three = processed_data['data_three']

        self.feishu_sheet.write_df_replace(data_three, spreadsheet_token, sheet_id[1], to_char=False)
        self.feishu_sheet.write_df_replace(data_one, spreadsheet_token, sheet_id[0], to_char=False)
        return processed_data

    def send_card(self, url: str, title: str, data_dict: Dict):
        logger.info(f'发送卡片')

        data = {
            'title': '财务数据',
            'file_name': title,
            'url': url,
            'description': '数据请见下方链接附件'
        }

        robot = self.feishu_robot
        robot.send_msg_card(data=data, card_id=self.card_id, version_name='1.0.1')


cw_data_dag = AbstractCwsData()
cw_data_dag.create_dag()


