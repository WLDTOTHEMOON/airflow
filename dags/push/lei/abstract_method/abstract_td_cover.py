import logging
from typing import Dict, Any

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.lei.abstract_method.abstract_base import AbstractDagTask

logger = logging.getLogger(__name__)


class AbstractTdCover(AbstractDagTask):
    def __init__(self):
        super().__init__(
            dag_id='push_td_cover',
            schedule='0 7 * * *',
            default_args=
            {
                'owner': 'Lei Jiangling',
                'start_date': pendulum.datetime(2023, 1, 1),
                'provide_context': True
            },
            tags=['push', 'td_cover'],
            robot_url=Variable.get('TEST'),
        )
        self.card_id: str = 'AAqRPrHrP2wKb'

    def fetch_data(self, **kwargs) -> Dict:
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        begin_time = start_time.subtract(days=1).strftime('%Y-%m-01')
        end_time = start_time.subtract(days=1).strftime('%Y-%m-%d')
        cover_rate_sql = """
            select
                bd_name
                ,13 all_num
                ,count(distinct anchor_name) cover_num
                ,case when count(distinct anchor_name) = 0 then 0 else count(distinct anchor_name) / 13 end cover_rate
            from(
                select
                        DISTINCT
                        bd_name
                        ,order_date
                        ,account_id
                        ,anchor_name
                        ,product_id
                from dws.dws_ks_ec_2hourly dkeh
                where order_date between %(begin_time)s and %(end_time)s
                        and bd_name is not null
                        and anchor_name != '乐总'
                ) src
            group by
                bd_name
        """
        cover_rate_df = pd.read_sql(cover_rate_sql, con=self.engine,
                                    params={'begin_time': begin_time, 'end_time': end_time})
        cover_num_sql = """
            select
                bd_name
                ,anchor_name
                ,count(*) pd_num
            from(
                select
                        DISTINCT
                        bd_name
                        ,anchor_name
                        ,product_id
                from dws.dws_ks_ec_2hourly dkeh
                where order_date between %(begin_time)s and %(end_time)s
                        and bd_name is not null
                        and anchor_name != '乐总'
                        ) src
            group by
                bd_name
                ,anchor_name
            order by
                bd_name asc
                ,pd_num desc
        """
        cover_num_df = pd.read_sql(cover_num_sql, con=self.engine,
                                   params={'begin_time': begin_time, 'end_time': end_time})
        return {
            'cover_rate_data': cover_rate_df,
            'cover_num_data': cover_num_df
        }

    def process_data(self, data: Dict, **kwargs) -> Dict:
        cover_rate_data = data['cover_rate_data']
        cover_num_data = data['cover_num_data']

        # 处理cover_rate_data
        cover_rate_data.cover_rate = cover_rate_data.cover_rate.apply(self.percent_convert)
        cover_rate_data = cover_rate_data.rename(columns={
            'bd_name': '商务',
            'all_num': '徒弟主播数量',
            'cover_num': '覆盖主播数量',
            'cover_rate': '覆盖率'
        })

        # 处理cover_num_data
        cover_num_data = cover_num_data.rename(columns={
            'bd_name': '商务名',
            'anchor_name': '主播名',
            'pd_num': '上品数'
        })

        return {
            'cover_rate_data': cover_rate_data,
            'cover_num_data': cover_num_data
        }

    def create_feishu_file(self, data_dic: Dict, **kwargs) -> Dict:
        logger.info(f'创建飞书文件')
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        title = ('徒弟主播推品覆盖率_' + start_time.subtract(days=1).strftime('%Y%m01') + '_' +
                 start_time.subtract(days=1).strftime('%Y%m%d') + '_' + start_time.strftime('%Y%m%d%H%M'))

        result = self.feishu_sheet.create_spreadsheet(
            title=title, folder_token='N1FFfIQuylfPBFdrMQ0cKL5qnzf'
        )
        spreadsheet_token = result['spreadsheet']['spreadsheet_token']
        url = result['spreadsheet']['url']
        cps_sheet_id = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='徒弟主播推品覆盖率')
        cps_sheet_id_1 = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        cps_sheet_id = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='徒弟主播上品数量')
        cps_sheet_id_2 = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        return {
            'spreadsheet_token': spreadsheet_token,
            'cps_sheet_id': [cps_sheet_id_1, cps_sheet_id_2],
            'url': url,
            'title': title
        }

    def render_feishu_format(
            self,
            processed_data_dict: Dict,
            spreadsheet_token: str,
            cps_sheet_id: str
    ) -> Dict:
        logger.info(f'渲染飞书格式')
        cover_rate_data = processed_data_dict['cover_rate_data']
        cover_num_data = processed_data_dict['cover_num_data']

        style_dict = {
            'A1:' + self.col_convert(cover_rate_data.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                'backColor': '#FFFF00'
            },
            'A1:' + self.col_convert(cover_rate_data.shape[1]) + str(cover_rate_data.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER'
            }
        }

        style_dict_2 = {
            'A1:' + self.col_convert(cover_num_data.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                'backColor': '#FFFF00'
            },
            'A1:' + self.col_convert(cover_num_data.shape[1]) + str(cover_num_data.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER'
            }
        }

        self.feishu_sheet.write_df_replace(cover_rate_data, spreadsheet_token, cps_sheet_id[0])

        self.feishu_sheet.write_df_replace(cover_num_data, spreadsheet_token, cps_sheet_id[1])

        for key, value in style_dict.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=cps_sheet_id[0], ranges=key, styles=value
            )

        for key, value in style_dict_2.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=cps_sheet_id[1], ranges=key, styles=value
            )

        return processed_data_dict

    def send_card(self, url: str, title: str, data_dic: Dict):
        logger.info(f'发送卡片')

        data = {
            'title': '徒弟主播推品覆盖率',
            'file_name': title,
            'url': url,
            'description': '数据请见下方链接附件'
        }

        robot = self.feishu_robot
        robot.send_msg_card(data=data, card_id=self.card_id, version_name='1.0.1')


td_cover_dag = AbstractTdCover()
td_cover_dag.create_dag()

