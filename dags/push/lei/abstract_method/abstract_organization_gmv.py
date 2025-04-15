# abstract_organization_gmv.py
import logging
import string

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.lei.abstract_method.abstract_base import AbstractDagTask

logger = logging.getLogger(__name__)


class AbstractOrganizationGMV(AbstractDagTask):
    def __init__(self):
        super().__init__(
            dag_id='push_organization_gmv',
            schedule=None,
            default_args={'owner': 'Lei Jiangling'},
            tags=['push', 'organization_gmv'],
            robot_url=Variable.get('TEST'),
        )
        self.start_time = pendulum.now('Asia/Shanghai').subtract(days=1).strftime('%Y-%m-%d')
        self.card_id = 'AAqRPrHrP2wKb'
        self.title = ('保护期监控_' + pendulum.now('Asia/Shanghai').strftime('%Y%m%d') + '_' +
                      pendulum.now('Asia/Shanghai').strftime('%Y%m%d%H%M'))

    def fetch_data(self):
        logger.info(f'获取数据')
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
                dws.dws_ks_ec_2hourly dkeh 
            where
                order_date = %(start_time)s
            and origin_gmv >=30000
            order by  origin_gmv DESC
        """
        organization_gmv = pd.read_sql(sql, con=self.engine, params={'start_time': self.start_time})
        return organization_gmv

    def process_data(self, data):
        logger.info(f'处理数据')
        processed_data = data
        processed_data['update_at'] = pendulum.now('Asia/Shanghai').strftime('%Y-%m-%d %H:00:00')
        processed_data['protection_until'] = pendulum.now('Asia/Shanghai').subtract(days=3).strftime(
            '%Y-%m-%d %H:00:00')
        processed_data['order_date'] = processed_data['order_date'].astype(str)
        processed_data = processed_data.rename(columns={
            'order_date': '卖货日期', 'account_id': '主播ID', 'anchor_name': '主播名', 'item_id': '商品ID',
            'item_title': '商品名称', 'origin_gmv': 'GMV', 'update_at': '数据更新时间', 'protection_until': '保护截止时间'
        })
        return processed_data

    def create_feishu_file(self):
        logger.info(f'创建飞书文件')

        result = self.feishu_sheet.create_spreadsheet(
            title=self.title, folder_token='AolgfOvb7lzF2KdWj6hclit2nvb'
        )
        spreadsheet_token = result['spreadsheet']['spreadsheet_token']
        url = result['spreadsheet']['url']
        cps_sheet_id = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='CPS')
        cps_sheet_id = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        return {
            'spreadsheet_token': spreadsheet_token,
            'cps_sheet_id': cps_sheet_id,
            'url': url
        }

    def render_feishu_format(self, processed_data, spreadsheet_token, cps_sheet_id):
        def col_convert(col_num):
            alphabeta = string.ascii_uppercase[:26]
            alphabeta = [i for i in alphabeta] + [i + j for i in alphabeta for j in alphabeta]
            return alphabeta[col_num - 1]

        logger.info(f'渲染飞书格式')
        # 示例渲染
        logger.info(spreadsheet_token)
        logger.info(cps_sheet_id)

        cps_style_dict = {
            'A1:' + col_convert(processed_data.shape[1]) + '1': {
                'font': {
                    'bold': True
                }
            }
        }

        self.feishu_sheet.write_df_replace(dat=processed_data, spreadsheet_token=spreadsheet_token,
                                           sheet_id=cps_sheet_id)

        for key, value in cps_style_dict.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=cps_sheet_id, ranges=key, styles=value
            )

    def send_card(self, url):
        logger.info(f'发送卡片')

        data = {
            'title': '保护期监控',
            'file_name': self.title,
            'url': url,
            'description': '数据请见下方链接附件'
        }

        robot = self.feishu_robot
        robot.send_msg_card(data=data, card_id=self.card_id, version_name='1.0.1')


# 创建DAG实例
organization_gmv_task = AbstractOrganizationGMV()
organization_gmv_dag = organization_gmv_task.create_dag()
