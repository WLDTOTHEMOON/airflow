import logging
from typing import Dict

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.lei.abstract_method.abstract_base import AbstractDagTask

logger = logging.getLogger(__name__)


class AbstractItemCommission(AbstractDagTask):
    def __init__(self):
        super().__init__(
            dag_id='push_item_commission',
            schedule='0 5 * * *',
            default_args=
            {
                'owner': 'Lei Jiangling',
                'start_date': pendulum.datetime(2023, 1, 1),
            },
            tags=['push', 'item_commission'],
            robot_url=Variable.get('TEST')
        )
        self.card_id = 'AAqRPrHrP2wKb'

    def fetch_data(self, **kwargs):
        begin_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai').subtract(days=1).strftime('%Y-%m-%d 04:00:00')
        end_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai').strftime('%Y-%m-%d 04:00:00')
        begin_date = kwargs['data_interval_end'].in_tz('Asia/Shanghai').subtract(days=1).strftime('%Y-%m-%d')
        logger.info(f'获取数据, 数据开始日期:{begin_time}')

        sql = f"""
            select
                order_date
                ,dkeh.account_id
                ,anchor_name
                ,dkeh.item_id
                ,item_title
                ,final_gmv
                ,coalesce(service_rate,0) leader_commission
                ,coalesce(commission_rate,0) anchor_commission
                ,by_anchor
                ,status
            from dws.dws_ks_big_tbl dkeh 
            left join(
                select
                    by_anchor
                    ,itemId
                    ,status
                from(
                    select
                        case 
                            when by_anchor = 0 then '招商'
                            when by_anchor = 1 then '自采'
                        end by_anchor
                        ,itemId
                        ,case
                            when status = 0 then '待审核'
                            when status = 1 then '链接审核通过'
                            when status = 2 then '链接审核不通过'
                        end status
                        ,row_number() over (partition by itemId order by updated_at desc) rn
                    from xlsd.link l ) s
                where rn = 1
            ) src on dkeh.item_id = src.itemId
            left join(
                select
                    distinct
                    item_id
                    ,account_id
                    ,commission_rate
                    ,service_rate
                from(
                    select
                        item_id
                        ,account_id
                        ,commission_rate
                        ,service_rate
                        ,sum(order_trade_amount) origin_gmv
                        ,row_number() over (partition by item_id order by sum(order_trade_amount) desc) rn
                    from dwd.dwd_ks_cps_order dkco 
                    where order_create_time between %(begin_time)s and %(end_time)s
                    group by
                        1,2,3,4) r
                where rn = 1
            ) info on dkeh.item_id = info.item_id and dkeh.account_id = info.account_id
            where order_date = %(begin_date)s
            and dkeh.account_id not in (
                select
                    account_id 
                from dim.dim_ks_account_info dkai 
                where anchor_status = '已解约'
                )
            having
                final_gmv > 1000
        """
        item_commission_df = pd.read_sql(sql, con=self.engine, params={'begin_time': begin_time, 'end_time': end_time, 'begin_date': begin_date})
        return {
            'item_commission_data': item_commission_df
        }

    def process_data(self, data: Dict, **kwargs) -> Dict:
        logger.info(f'处理数据')

        process_data = data['item_commission_data']
        process_data.order_date = process_data.order_date.astype('str')
        process_data.leader_commission = process_data.leader_commission.apply(self.percent_convert)
        process_data.anchor_commission = process_data.anchor_commission.apply(self.percent_convert)
        process_data = process_data.rename(columns={
            'order_date': '日期', 'account_id': '账号ID', 'anchor_name': '主播名称', 'item_id': '商品ID',
            'item_title': '商品名称', 'final_gmv': '结算GMV', 'leader_commission': '团长端佣金率',
            'anchor_commission': '主播端佣金率', 'by_anchor': '商品来源', 'status': '链接审核状态'
        })
        return {
            'process_data': process_data
        }

    def create_feishu_file(self, process_data_dict: Dict, **kwargs) -> Dict:
        logger.info(f'创建飞书文件')
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        title = '商品佣金率_' + start_time.subtract(days=1).strftime('%Y%m%d') + '_' + start_time.strftime('%Y%m%d%H%M%S')

        result = self.feishu_sheet.create_spreadsheet(
            title=title, folder_token='VdJkfZGe3lS2uGdK4ORclqLsnPg'
        )
        spreadsheet_token = result['spreadsheet']['spreadsheet_token']
        url = result['spreadsheet']['url']
        cps_sheet_id = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='Result')
        cps_sheet_id = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        return {
            'spreadsheet_token': spreadsheet_token,
            'cps_sheet_id': cps_sheet_id,
            'url': url,
            'title': title
        }

    def render_feishu_format(self, processed_data_dict: Dict, file_info: Dict) -> Dict:
        logger.info(f'渲染飞书格式')
        process_data = processed_data_dict['process_data']

        spreadsheet_token = file_info['spreadsheet_token']
        cps_sheet_id = file_info['cps_sheet_id']

        cps_style_dict = {
            'A1:' + self.col_convert(process_data.shape[1]) + '1': {
                'font': {
                    'bold': True
                }
            }
        }
        self.feishu_sheet.write_df_replace(dat=process_data, spreadsheet_token=spreadsheet_token,
                                           sheet_id=cps_sheet_id)
        for key, value in cps_style_dict.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=cps_sheet_id, ranges=key, styles=value
            )
        return processed_data_dict

    def send_card(self, file_info: Dict, data_dict: Dict):
        logger.info(f'发送卡片')
        title = file_info['title']
        url = file_info['url']

        res = {
            'title': '商品佣金率',
            'file_name': title,
            'url': url,
            'description': '数据请见下方链接附件'
        }

        robot = self.feishu_robot
        robot.send_msg_card(data=res, card_id=self.card_id, version_name='1.0.1')


# 创建DAG实例
item_commission_dag = AbstractItemCommission()
item_commission_dag.create_dag()
