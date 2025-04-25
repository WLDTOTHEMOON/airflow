from abc import ABC
from typing import Dict, Any
from dags.push.zhaoyifan.data_push_zhao.format_utils import *
import pendulum
import pandas as pd
from airflow.models import Variable
from dags.push.zhaoyifan.data_push_zhao.base_dag import BaseDag


class LeMoItem(BaseDag):
    def __init__(self):
        super().__init__(
            dag_id='push_le_mo_item',
            default_args={'owner': 'zhaoyifan'},
            tags=['push', 'item'],
            robot_url=Variable.get('TEST'),
            schedule='0 7 * * *'
        )
        self.card_id = 'AAq4uuRPgclE1'

    def fetch_data_logic(self, date_interval: Dict[str, Any]) -> Dict[str, Any]:
        le_sql = f'''
            select 
                    order_date 
                    ,item_id 
                    ,item_title 
                    ,first_sell_at
                    ,product_name
                    ,bd_name
                    ,product_submit_at
                    ,origin_gmv 
                    ,final_gmv 
            from dws.dws_ks_ec_2hourly dkeh 
            where order_date = '{date_interval['yes_ds']}'
                    and anchor_name = '乐总'
--                     and (first_sell_at < product_submit_at or bd_name is null)
            order by origin_gmv desc
        '''
        le_df = pd.read_sql(le_sql, self.engine)
        le_df['order_date'] = le_df['order_date'].astype(str)
        le_df['first_sell_at'] = le_df['first_sell_at'].apply(convert_timestamp_to_str)
        le_df['product_submit_at'] = le_df['product_submit_at'].apply(convert_timestamp_to_str)

        mo_sql = f'''
                    select 
                            order_date 
                            ,item_id 
                            ,item_title 
                            ,first_sell_at
                            ,product_name
                            ,bd_name
                            ,product_submit_at
                            ,origin_gmv 
                            ,final_gmv 
                    from dws.dws_ks_ec_2hourly dkeh 
                    where order_date = '{date_interval['yes_ds']}'
                            and anchor_name = '墨晨夏'
                            and account_id = '2884165591'
        --                     and (first_sell_at < product_submit_at or bd_name is null)
                    order by origin_gmv desc
                '''
        mo_df = pd.read_sql(mo_sql, self.engine)
        mo_df['order_date'] = mo_df['order_date'].astype(str)
        mo_df['first_sell_at'] = mo_df['first_sell_at'].apply(convert_timestamp_to_str)
        mo_df['product_submit_at'] = mo_df['product_submit_at'].apply(convert_timestamp_to_str)

        return {
            'df': {
                'le_df': le_df,
                'mo_df': mo_df
            },
            'date_interval': date_interval
        }

    def prepare_card_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        return {}

    def write_to_sheet_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        le_df = data_dict['df']['le_df']
        mo_df = data_dict['df']['mo_df']
        style_dict = {
            'A1:' + col_convert(le_df.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                # 'backColor': '#FFFF00'
            },
            'A1:' + col_convert(le_df.shape[1]) + '1': {
                'hAlign': 1,
                'vAlign': 1,
                # 'borderType': 'FULL_BORDER'
            },
        }
        mo_style_dict = {
            'A1:' + col_convert(mo_df.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                # 'backColor': '#FFFF00'
            },
            'A1:' + col_convert(mo_df.shape[1]) + '1': {
                'hAlign': 1,
                'vAlign': 1,
                # 'borderType': 'FULL_BORDER'
            },
        }

        le_df.rename(columns={
            'order_date': '日期',
            'item_id': '商品ID',
            'item_title': '商品名称',
            'first_sell_at': '售卖时间',
            'product_name': '系统内商品名称',
            'bd_name': '商务',
            'product_submit_at': '商品提报时间',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
        }, inplace=True)
        mo_df.rename(columns={
            'order_date': '日期',
            'item_id': '商品ID',
            'item_title': '商品名称',
            'first_sell_at': '售卖时间',
            'product_name': '系统内商品名称',
            'bd_name': '商务',
            'product_submit_at': '商品提报时间',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
        }, inplace=True)

        date_interval = data_dict['date_interval']
        sheet_title = f"乐总和墨晨夏售卖商品数据列表_{date_interval['now_time']}"
        url, spreadsheet_token = self.feishu_sheet_supply.get_workbook_params(
            workbook_name=sheet_title, folder_token='Of8uf5hVnliyKqddb5ocDdNmnf0'
        )
        sheet_id = self.feishu_sheet_supply.get_sheet_params(spreadsheet_token, '乐总')
        mo_sheet_id = self.feishu_sheet_supply.get_sheet_params(spreadsheet_token, '墨晨夏')
        self.feishu_sheet.write_df_replace(le_df, spreadsheet_token, sheet_id, to_char=False)
        self.feishu_sheet.write_df_replace(mo_df, spreadsheet_token, mo_sheet_id, to_char=False)

        for key, value in style_dict.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=sheet_id, ranges=key, styles=value
            )
        for key, value in mo_style_dict.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=mo_sheet_id, ranges=key, styles=value
            )

        return {
            'file_name': sheet_title,
            'url': url
        }

    def send_card_logic(self, card: Dict[str, Any], sheet):
        res = {
            'title': '乐总和墨晨夏售卖商品数据列表',
            **sheet,
            'description': '数据请见下方链接附件'
        }
        self.feishu_robot.send_msg_card(data=res, card_id=self.card_id, version_name='1.0.1')


dag = LeMoItem().create_dag()
