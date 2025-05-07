import logging
from typing import Dict

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.lei.abstract_method.abstract_base import AbstractDagTask

logger = logging.getLogger(__name__)


class AbstractLeDelivery(AbstractDagTask):
    def __init__(self):
        super().__init__(
            dag_id='push_le_delivery',
            schedule='0 3,9 * * *',
            default_args={
                'owner': 'Lei Jiangling',
                'start_date': pendulum.datetime(2023, 1, 1)
            },
            tags=['push', 'le_delivery'],
            robot_url=Variable.get('TEST'),
        )
        self.card_id: str = 'AAqRLrhJaJ8IV'

    def fetch_data(self, **kwargs) -> Dict:
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        end_time = start_time.subtract(days=1).strftime('%Y-%m-%d')
        sql = f"""
            SELECT
                    order_date
                    ,account_id
                    ,anchor_name
                    ,item_id
                    ,item_title
                    ,bd_name
                    ,origin_gmv
                    ,final_gmv
                    ,origin_order_number
                    ,final_order_number
                    ,send_order_number
                    ,final_order_number-send_order_number wait_send_order_number
                    ,send_order_number_yesterday
                    ,COALESCE(round(send_order_number/final_order_number,4),0) send_rate
                    ,origin_order_number-final_order_number invalid_order_num
                    ,round(1-final_order_number/origin_order_number,4) invalid_rate
                FROM
                    dws.dws_ks_big_tbl dkeh	
                WHERE
                    anchor_name = '乐总'
                    and COALESCE(round(send_order_number/final_order_number,4),0)!=1
                    and origin_order_number >= 50
                    and final_order_number > 0
                    and order_date < %(end_time)s
                ORDER BY
                    wait_send_order_number DESC
        """
        le_delivery_df = pd.read_sql(sql, self.engine, params={'end_time': end_time})

        return {
            'le_delivery_df': le_delivery_df
        }

    def process_data(self, data: Dict, **kwargs) -> Dict:
        processed_data = data['le_delivery_df']
        processed_data = processed_data.astype(
            {'origin_order_number': int,
             'final_order_number': int,
             'send_order_number': int,
             'wait_send_order_number': int,
             'invalid_order_num': int
             }
        )
        processed_data['order_date'] = processed_data['order_date'].astype(str)

        # 筛选出小于60的数据
        group_table_data = processed_data[processed_data.send_rate < 0.6]
        res = []
        for i in range(group_table_data.shape[0]):
            res.append({
                'item_title': group_table_data.item_title.iloc[i],
                'valid_order_number': str(group_table_data.final_order_number.iloc[i]),
                'undelivered_number': str(group_table_data.wait_send_order_number.iloc[i]),
                'delivery_rate': self.percent_convert(group_table_data.send_rate.iloc[i]),
            })

        processed_data.send_rate = processed_data.send_rate.apply(self.percent_convert)
        processed_data.invalid_rate = processed_data.invalid_rate.apply(self.percent_convert)
        processed_data = processed_data.astype({
            'send_rate': str,
            'invalid_rate': str,
        })
        processed_data.rename(columns={
            'order_date': '卖货时间',
            'account_id': '账号ID',
            'anchor_name': '主播名称',
            'item_id': '商品ID',
            'item_title': '商品名称',
            'bd_name': '对应商务',
            'origin_gmv': '总销售额',
            'final_gmv': '剩余销售额',
            'origin_order_number': '总下单量',
            'final_order_number': '剩余单量',
            'send_order_number': '剩余已发货单量',
            'wait_send_order_number': '剩余未发货单量(降序)',
            'send_order_number_yesterday': '昨日发货量(昨日10点~今日10点)',
            'send_rate': '订单发货率(订单量)',
            'invalid_order_num': '失效单量(订单量)',
            'invalid_rate': '订单退货率(订单量)',
        }, inplace=True)
        return {
            'file_data': processed_data,
            'unprocess_data': data['le_delivery_df'],
            'group_data': res
        }

    def create_feishu_file(self, process_data_dict: Dict, **kwargs) -> Dict:
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        title = '发货进度监控_' + start_time.strftime('%Y%m%d') + '_' + start_time.strftime('%Y%m%d%H%M%S')

        result = self.feishu_sheet.create_spreadsheet(
            title=title, folder_token='UPOLfqZ7AlxDZIdwFyycqauQnGb'
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

    def render_feishu_format(
            self,
            process_data_dict: Dict,
            spreadsheet_token: str,
            cps_sheet_id: str
    ) -> Dict:
        logger.info(f'渲染飞书格式')
        processed_data = process_data_dict['file_data']
        un_process_data = process_data_dict['unprocess_data']

        style_dict = {
            'A1:' + self.col_convert(processed_data.shape[1]) + '1': {
                'font': {
                    'bold': True
                }
            }
        }

        col_num = self.col_convert(un_process_data.columns.get_loc('send_rate') + 1)
        for i in range(un_process_data.shape[0]):
            ranges = col_num + str(i + 2) + ':' + col_num + str(i + 2)
            if un_process_data.send_rate.iloc[i] >= 0.9:
                color = '#B3D600'
            elif 0.9 > un_process_data.send_rate.iloc[i] >= 0.6:
                color = '#FFF258'
            elif un_process_data.send_rate.iloc[i] >= 0.3:
                color = '#FF8800'
            else:
                color = '#F64A46'
            style_dict[ranges] = {'backColor': color}

        self.feishu_sheet.write_df_replace(dat=processed_data, spreadsheet_token=spreadsheet_token,
                                           sheet_id=cps_sheet_id)

        for key, value in style_dict.items():
            self.feishu_sheet.style_cells(spreadsheet_token=spreadsheet_token, sheet_id=cps_sheet_id, ranges=key,
                                          styles=value)
        return process_data_dict

    def send_card(self, url: str, title: str, process_data_dict: Dict):
        logger.info(f'发送卡片')

        group_data = process_data_dict['group_data']
        res = {
            'title': '发货进度监控（乐总）',
            'file_name': title,
            'url': url,
            'group_table': group_data
        }

        robot = self.feishu_robot
        robot.send_msg_card(data=res, card_id=self.card_id, version_name='1.0.0')


le_delivery = AbstractLeDelivery()
le_delivery.create_dag()
