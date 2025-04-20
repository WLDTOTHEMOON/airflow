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
        begin_time = start_time.subtract(days=1).strftime('%Y-%m-%d 00:00:00')
        end_time = start_time.subtract(days=1).strftime('%Y-%m-%d 23:59:59')
        sql = """
             select
                dkco.account_id
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
                1,2
            order by
                settlement_amount desc
        """
        anchor_settlement_df = pd.read_sql(sql, self.engine, params={'begin_time': begin_time, 'end_time': end_time})
        return {
            'anchor_settlement_data': anchor_settlement_df
        }

    def process_data(self, data: Dict, **kwargs) -> Dict:
        logger.info(f'处理数据')
        anchor_settlement_data = data['anchor_settlement_data']
        anchor_settlement_data = anchor_settlement_data.rename(columns={
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
        title = ('主播佣金结算_' + start_time.subtract(days=1).strftime('%Y%m%d') + '_' + start_time.strftime('%Y%m%d%H%M'))

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
