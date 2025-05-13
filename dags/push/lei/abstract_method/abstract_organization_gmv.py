import logging
from typing import Dict

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.lei.abstract_method.abstract_base import AbstractDagTask

logger = logging.getLogger(__name__)


class AbstractOrganizationGMV(AbstractDagTask):
    def __init__(self):
        super().__init__(
            dag_id='push_organization_gmv',
            schedule='0 21 * * *',
            default_args=
            {
                'owner': 'Lei Jiangling',
                'start_date': pendulum.datetime(2023, 1, 1),
            },
            tags=['push', 'organization_gmv'],
            robot_url=Variable.get('KUCHIKI')
        )
        self.card_id = 'AAqRPrHrP2wKb'

    def fetch_data(self, **kwargs):
        begin_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai').subtract(days=1).strftime('%Y-%m-%d')
        logger.info(f'获取数据, 数据开始日期:{begin_time}')

        sql = f"""
            select
                order_date
                ,account_id
                ,anchor_name
                ,item_id
                ,item_title
                ,origin_gmv
                ,null update_at
                ,null protection_until
            from
                dws.dws_ks_big_tbl dkeh 
            where
                order_date = %(begin_time)s
            and origin_gmv >=30000
            order by  origin_gmv DESC
        """
        organization_gmv = pd.read_sql(sql, con=self.engine, params={'begin_time': begin_time})
        return {
            'organization_gmv': organization_gmv
        }

    def process_data(self, data: Dict, **kwargs) -> Dict:
        logger.info(f'处理数据')
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')

        processed_data = data['organization_gmv']
        processed_data['update_at'] = start_time.strftime('%Y-%m-%d %H:00:00')
        processed_data['protection_until'] = start_time.subtract(days=3).strftime('%Y-%m-%d %H:00:00')
        processed_data['order_date'] = processed_data['order_date'].astype(str)
        processed_data = processed_data.rename(columns={
            'order_date': '卖货日期', 'account_id': '主播ID', 'anchor_name': '主播名', 'item_id': '商品ID',
            'item_title': '商品名称', 'origin_gmv': 'GMV', 'update_at': '数据更新时间', 'protection_until': '保护截止时间'
        })
        return {
            'processed_data': processed_data
        }

    def create_feishu_file(self, process_data_dict: Dict, **kwargs) -> Dict:
        logger.info(f'创建飞书文件')
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        title = '保护期监控_' + start_time.strftime('%Y%m%d') + '_' + start_time.strftime('%Y%m%d%H%M%S')

        result = self.feishu_sheet.create_spreadsheet(
            title=title, folder_token='GIygfK0b5lndXCdIZqUcBygan4c'
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

    def render_feishu_format(self, processed_data_dict: Dict, spreadsheet_token: str, cps_sheet_id: str) -> Dict:
        logger.info(f'渲染飞书格式')
        process_data = processed_data_dict['processed_data']
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

    def send_card(self, url: str, title: str, processed_data_dict: Dict):
        logger.info(f'发送卡片')

        data = {
            'title': '保护期监控',
            'file_name': title,
            'url': url,
            'description': '数据请见下方链接附件'
        }

        robot = self.feishu_robot
        robot.send_msg_card(data=data, card_id=self.card_id, version_name='1.0.1')


# 创建DAG实例
organization_gmv_task = AbstractOrganizationGMV()
organization_gmv_dag = organization_gmv_task.create_dag()
