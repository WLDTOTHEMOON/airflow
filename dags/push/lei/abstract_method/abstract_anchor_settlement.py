import logging
from typing import Dict, Any

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.lei.abstract_method.abstract_base import AbstractDagTask

logger = logging.getLogger(__name__)


class AbstractAnchorSettlement(AbstractDagTask):
    def __init__(self):
        super().__init__(
            dag_id='push_anchor_settlement',
            schedule='0 21 * * *',
            default_args=
            {
                'owner': 'Lei Jiangling',
                'start_date': pendulum.datetime(2023, 1, 1),
                'provide_context': True
            },
            tags=['push', 'anchor_settlement'],
            robot_url=Variable.get('Pain'),
        )
        self.card_id: str = 'AAqRPrHrP2wKb'

    def fetch_data(self, **kwargs) -> Dict:
        logger.info(f'获取数据')
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        begin_time = start_time.subtract(days=1).strftime('%Y-%m-01 00:00:00')
        end_time = start_time.subtract(days=1).strftime('%Y-%m-%d 23:59:59')
        sql = """
             select
                date(settlement_success_time) order_date
                ,dkco.account_id
                ,anchor_name
                ,sum(settlement_amount) settlement_amount
                ,sum(coalesce(leader_settlement_amount,0)) leader_settlement_amount
            from dwd.dwd_ks_cps_order dkco 
            inner join(
                select
                    account_id
                    ,anchor_name
                from dim.dim_ks_anchor_info dkai 
                where line in ('直播电商','其它')
            ) ai on dkco.account_id = ai.account_id
            where settlement_success_time between %(begin_time)s and %(end_time)s
            group by
                1,2,3
            order by
                order_date asc
                ,settlement_amount desc
        """
        anchor_settlement_df = pd.read_sql(sql, self.engine, params={'begin_time': begin_time, 'end_time': end_time})

        slice_sql = """
            select 
                date
                ,sum(estimated_service_income) estimated_service_income
                ,sum(estimated_income) estimated_income
                ,sum(estimated_service_income + estimated_income) tol_income
            from (
            -- A端剪手
             select 
                 date(settlement_success_time) date
                 ,'A端剪手'
                 ,sum(estimated_service_income) estimated_service_income 
                 ,sum(estimated_income) estimated_income 
             from (
                 select 
                     anchor_id
                     ,start_time
                     ,end_time
                 from ods.ods_slice_account
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
                         and settlement_success_time between %(begin_time)s and %(end_time)s
                         and cps_order_status = '已结算'
             )src on A.anchor_id = src.account_id
                 and src.order_create_time between A.start_time and A.end_time
            group by 
                    1,2        
            union all
            -- A端二创
            select 
                 date(settlement_success_time ) date
                 ,'A端二创'
                 ,sum(leader_origin_estimated_service_income + leader_keep_estimated_service_income) estimated_service_income
                 ,sum(anchor_keep_estimated_income + leader_share_estimated_income) estimated_income
             from (
                 select 
                     anchor_id
                     ,start_time
                     ,end_time
                 from ods.ods_slice_account
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
                 where settlement_success_time between %(begin_time)s and %(end_time)s
                         and cps_order_status = '已结算'
             )com on A.anchor_id = com.author_id
                 and com.order_create_time between A.start_time and A.end_time
            group by 
                    1,2
            ) tol
            group by 
                    1
            union all
            select 
                    date 
                    ,sum(estimated_service_income) estimated_service_income
                    ,sum(estimated_income) estimated_income
                    ,sum(estimated_service_income + estimated_income) tol_income
            from (
            -- B端剪手  
            select 
                date(settlement_success_time) date
                ,'B端剪手' 
                ,sum(coalesce(estimated_service_income,0)) estimated_service_income
                ,0 estimated_income
            from dwd.dwd_ks_leader_order 
            where activity_id in ('5084323902','5142199902','4920930902','6701551902','6244252902', '7469588902')
                and promotion_id not in (select anchor_id from ods.ods_slice_account)
                and settlement_success_time between %(begin_time)s and %(end_time)s
                and cps_order_status = '已结算'
            group by 
                    1,2
            union all
            -- B端二创
            select 
                date(settlement_success_time) date
                ,'B端二创'
                ,sum(coalesce(estimated_service_income, 0)) estimated_service_income
                ,sum(coalesce(estimated_income, 0)) estimated_income    
            from dwd.dwd_ks_recreation
            where o_id not in (select o_id from dwd.dwd_ks_leader_commission_income)
                    and settlement_success_time between %(begin_time)s and %(end_time)s
              and cps_order_status = '已结算'
            group by 
                    1,2
            )tol
            group by 
                    1
            union all
            select 
                    date
                    ,sum(estimated_service_income) estimated_service_income
                    ,sum(estimated_income) estimated_income
                    ,sum(estimated_service_income + estimated_income) tol_income
            from (
            -- C端剪手
            select 
                date(settlement_success_time) date
                ,'C端剪手' 
                ,sum(coalesce(estimated_service_income,0)) estimated_service_income
                ,0 estimated_income
            from dwd.dwd_ks_leader_order 
            where activity_id = '5084317902'
                and promotion_id not in (select anchor_id from ods.ods_slice_account)
                and settlement_success_time between %(begin_time)s and %(end_time)s
                and cps_order_status = '已结算'
            group by 
                    1,2
            union all 
            -- C端二创
            select 
                 date(settlement_success_time ) date
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
                 where settlement_success_time between %(begin_time)s and %(end_time)s
                         and cps_order_status = '已结算'
            ) com
             left join (
                 select 
                     anchor_id
                 from ods.ods_slice_account
             )A on com.author_id = A.anchor_id  
             where A.anchor_id is null
            group by 
                    1,2
            )tol
            group by
                    1
        """
        slice_df = pd.read_sql(slice_sql, self.engine, params={'begin_time': begin_time, 'end_time': end_time})
        return {
            'anchor_settlement_data': anchor_settlement_df,
            'slice_df': slice_df
        }

    def process_data(self, data: Dict, **kwargs) -> Dict:
        logger.info(f'处理数据')
        anchor_settlement_data = data['anchor_settlement_data']
        slice_df = data['slice_df']

        slice_data = slice_df.groupby('date')[['estimated_income', 'estimated_service_income']].sum().reset_index()
        slice_data['account_id'] = '切片'
        slice_data['anchor_name'] = '切片'
        slice_data = slice_data.rename(columns={
            'date': 'order_date',
            'estimated_income': 'settlement_amount',
            'estimated_service_income': 'leader_settlement_amount'
        })
        slice_data = slice_data[['order_date', 'account_id', 'anchor_name', 'settlement_amount',
                                 'leader_settlement_amount']]
        merged_df = pd.concat([anchor_settlement_data, slice_data], ignore_index=True)

        merged_df.order_date = merged_df.order_date.astype('str')

        anchor_settlement_data = merged_df.rename(columns={
            'order_date': '结算日期',
            'account_id': '账号ID',
            'anchor_name': '主播名称',
            'settlement_amount': '主播端结算金额',
            'leader_settlement_amount': '团长端结算金额',
        })

        return {
            'process_data': anchor_settlement_data
        }

    def create_feishu_file(self, data_dic: Dict, **kwargs) -> Dict:
        logger.info(f'创建飞书文件')
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        title = ('主播佣金结算_' + start_time.subtract(days=1).strftime('%Y%m%d') + '_' + start_time.strftime(
            '%Y%m%d%H%M'))

        result = self.feishu_sheet.create_spreadsheet(
            title=title, folder_token='TIxUfix3jlM7TFdwr3gcletOnkh'
        )
        spreadsheet_token = result['spreadsheet']['spreadsheet_token']
        url = result['spreadsheet']['url']
        cps_sheet_id = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='CPS')
        cps_sheet_id = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        return {
            'spreadsheet_token': spreadsheet_token,
            'cps_sheet_id': cps_sheet_id,
            'url': url,
            'title': title
        }

    def render_feishu_format(
            self,
            process_data_dict: Dict,
            spreadsheet_token: str,
            cps_sheet_id: str
    ) -> Dict:
        logger.info(f'渲染飞书格式')
        process_data = process_data_dict['process_data']

        cps_style_dict = {
            'A1:' + self.col_convert(process_data.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                'backColor': '#FFFF00'
            },
            'A1:' + self.col_convert(process_data.shape[1]) + str(process_data.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER'
            }
        }

        self.feishu_sheet.write_df_replace(dat=process_data, spreadsheet_token=spreadsheet_token,
                                           sheet_id=cps_sheet_id)

        for key, value in cps_style_dict.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=cps_sheet_id, ranges=key, styles=value
            )
        return process_data_dict

    def send_card(self, url: str, title: str, data_dict: Dict):
        logger.info(f'发送卡片')

        data = {
            'title': '主播佣金结算',
            'file_name': title,
            'url': url,
            'description': '数据请见下方链接附件'
        }

        robot = self.feishu_robot
        robot.send_msg_card(data=data, card_id=self.card_id, version_name='1.0.1')


anchor_settlement_dag = AbstractAnchorSettlement()
anchor_settlement_dag.create_dag()
