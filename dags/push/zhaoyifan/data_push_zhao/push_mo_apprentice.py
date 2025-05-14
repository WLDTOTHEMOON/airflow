import logging
from typing import Dict, Any

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.zhaoyifan.data_push_zhao.base_dag import BaseDag
from dags.push.zhaoyifan.data_push_zhao.format_utils import *

logger = logging.getLogger(__name__)


class MoApprentice(BaseDag):
    def __init__(self):
        super().__init__(
            dag_id='push_mo_apprentice',
            robot_url=Variable.get('TEST'),
            tags=['push', 'mo_apprentice'],
            schedule='0 5 * * *'
        )
        self.card_id = 'AAq4gmGf4Wu8I'

    def fetch_data_logic(self, date_interval: Dict[str, Any]) -> Dict[str, Any]:
        sql = f'''
            select 
                order_date 
                ,anchor_name 
                ,account_id 
                ,item_id 
                ,item_title 
                ,origin_gmv
                ,final_gmv
                ,origin_order_number
                ,final_order_number
                ,coalesce(estimated_income,0) estimated_income 
                ,coalesce(estimated_service_income,0) estimated_service_income 
                ,coalesce(estimated_income,0) + coalesce(estimated_service_income,0) commission_income
            from dws.dws_ks_big_tbl dkbt 
            where anchor_name in (
                select 
                    anchor_name 
                from dim.dim_ks_account_info dkai 
                where other_commission_belong = '墨晨夏'
            ) and order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
        '''
        df = pd.read_sql(sql, self.engine)
        df['order_date'] = df['order_date'].astype('str')

        target_sql = f'''
            select 
                sum(target_final * 10000) target_final 
            from ods.ods_fs_gmv_target ofgt
            where anchor in (
                select 
                    anchor_name 
                from dim.dim_ks_account_info dkai
                where other_commission_belong = '墨晨夏'
            ) and month = '{date_interval['month']}'
        '''
        target_df = pd.read_sql(target_sql, self.engine)

        # 每日汇总-卡片
        day_tol_df = df[df['order_date'] >= date_interval['yes_ds']]
        day_tol_df = day_tol_df[['origin_gmv', 'final_gmv', 'commission_income']]
        day_tol_df = day_tol_df.sum().to_frame().T
        day_tol_df['final_rate'] = day_tol_df['final_gmv'] / day_tol_df['origin_gmv']

        # 月度汇总-卡片
        month_tol_df = df[['origin_gmv', 'final_gmv', 'commission_income']]
        month_tol_df = month_tol_df.sum().to_frame().T
        month_tol_df['final_rate'] = month_tol_df['final_gmv'] / month_tol_df['origin_gmv']
        month_tol_df['target_final'] = target_df['target_final']
        month_tol_df['target_rate'] = month_tol_df['final_gmv'] / month_tol_df['target_final']

        # 每日单品数据-详情数据
        day_df = df[df['order_date'] >= date_interval['yes_ds']]
        day_df = day_df.groupby(
            ['order_date', 'anchor_name', 'account_id', 'item_id', 'item_title']).sum().reset_index()
        day_df['final_rate'] = day_df['final_gmv'] / day_df['origin_gmv']
        day_df['commission_rate'] = day_df['commission_income'] / day_df['final_gmv']
        day_df['return_order_number'] = day_df['origin_order_number'] - day_df['final_order_number']
        day_df['return_gmv'] = day_df['origin_gmv'] - day_df['final_gmv']
        day_df['return_rate'] = 1 - day_df['final_order_number'] / day_df['origin_order_number']
        day_df = day_df[[
            'order_date', 'anchor_name', 'account_id', 'item_id', 'item_title',
            'origin_gmv', 'final_gmv', 'final_rate', 'commission_income', 'estimated_income',
            'estimated_service_income', 'commission_rate', 'return_order_number', 'return_gmv', 'return_rate'
        ]]

        # 每日汇总数据 - 详情数据
        day_sum_df = df[df['order_date'] >= date_interval['yes_ds']]
        day_sum_df = day_sum_df[[
            'order_date', 'anchor_name', 'origin_gmv', 'final_gmv', 'origin_order_number',
            'final_order_number', 'commission_income', 'estimated_income',
            'estimated_service_income'
        ]]
        day_sum_df = day_sum_df.groupby(['order_date', 'anchor_name']).sum().reset_index()
        day_sum_df['final_rate'] = day_sum_df['final_gmv'] / day_sum_df['origin_gmv']
        day_sum_df['commission_rate'] = day_sum_df['commission_income'] / day_sum_df['final_gmv']
        day_sum_df['price'] = day_sum_df['final_gmv'] / day_sum_df['final_order_number']
        day_sum_df['return_order_number'] = day_sum_df['origin_order_number'] - day_sum_df['final_order_number']
        day_sum_df['return_gmv'] = day_sum_df['origin_gmv'] - day_sum_df['final_gmv']
        day_sum_df['return_rate'] = 1 - day_sum_df['final_order_number'] / day_sum_df['origin_order_number']
        day_sum_df = day_sum_df[[
            'order_date', 'anchor_name', 'origin_gmv', 'final_gmv', 'final_rate', 'commission_income',
            'estimated_income',
            'estimated_service_income', 'commission_rate', 'price', 'return_order_number', 'return_gmv', 'return_rate'
        ]]

        # 月度单品数据 - 详情数据
        month_df = df
        month_df['order_date'] = pd.to_datetime(month_df['order_date']).dt.strftime('%Y-%m')
        month_df = month_df.groupby(
            ['order_date', 'anchor_name', 'account_id', 'item_id', 'item_title']).sum().reset_index()
        month_df['final_rate'] = month_df['final_gmv'] / month_df['origin_gmv']
        month_df['commission_rate'] = month_df['commission_income'] / month_df['final_gmv']
        month_df['return_order_number'] = month_df['origin_order_number'] - month_df['final_order_number']
        month_df['return_gmv'] = month_df['origin_gmv'] - month_df['final_gmv']
        month_df['return_rate'] = 1 - month_df['final_order_number'] / month_df['origin_order_number']
        month_df = month_df[[
            'order_date', 'anchor_name', 'account_id', 'item_id', 'item_title',
            'origin_gmv', 'final_gmv', 'final_rate', 'commission_income', 'estimated_income',
            'estimated_service_income', 'commission_rate', 'return_order_number', 'return_gmv', 'return_rate'
        ]]

        # 每月汇总数据 - 详情数据
        month_sum_df = df[[
            'order_date', 'anchor_name', 'origin_gmv', 'final_gmv', 'origin_order_number',
            'final_order_number', 'commission_income', 'estimated_income',
            'estimated_service_income'
        ]]
        month_sum_df['order_date'] = pd.to_datetime(month_sum_df['order_date']).dt.strftime('%Y-%m')
        month_sum_df = month_sum_df.groupby(['order_date', 'anchor_name']).sum().reset_index()
        month_sum_df['final_rate'] = month_sum_df['final_gmv'] / month_sum_df['origin_gmv']
        month_sum_df['commission_rate'] = month_sum_df['commission_income'] / month_sum_df['final_gmv']
        month_sum_df['price'] = month_sum_df['final_gmv'] / month_sum_df['final_order_number']
        month_sum_df['return_order_number'] = month_sum_df['origin_order_number'] - month_sum_df['final_order_number']
        month_sum_df['return_gmv'] = month_sum_df['origin_gmv'] - month_sum_df['final_gmv']
        month_sum_df['return_rate'] = 1 - month_sum_df['final_order_number'] / month_sum_df['origin_order_number']
        month_sum_df = month_sum_df[[
            'order_date', 'anchor_name', 'origin_gmv', 'final_gmv', 'final_rate', 'commission_income',
            'estimated_income',
            'estimated_service_income', 'commission_rate', 'price', 'return_order_number', 'return_gmv', 'return_rate'
        ]]

        return {
            'df': df,
            'target_df': target_df,
            'day_tol_df': day_tol_df,
            'month_tol_df': month_tol_df,
            'day_df': day_df,
            'day_sum_df': day_sum_df,
            'month_df': month_df,
            'month_sum_df': month_sum_df,
            'date_interval': date_interval
        }

    def prepare_card_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        day_tol_df = data_dict['day_tol_df']
        if len(day_tol_df) == 0:
            yes_origin_gmv = '0'
            yes_final_gmv = '0'
            yes_final_rate = '-'
            yes_commission_income = '0'
        else:
            yes_origin_gmv = str(round(day_tol_df['origin_gmv'].iloc[0] / 10000, 1))
            yes_final_gmv = str(round(day_tol_df['final_gmv'].iloc[0] / 10000, 1))
            yes_final_rate = str(
                '{:.2f}%'.format(day_tol_df.final_rate.iloc[0] * 100))
            yes_commission_income = str(round(day_tol_df['commission_income'].iloc[0] / 10000, 1))

        month_tol_df = data_dict['month_tol_df']
        if pd.isnull(month_tol_df.origin_gmv.iloc[0]):
            month_origin_gmv = '0'
            month_final_gmv = '0'
            month_final_rate = '0'
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
                '{:.2f}%'.format(month_tol_df.target_rate.iloc[0] * 100))

        return {
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
        }

    def write_to_sheet_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        month_sum_df = data_dict['month_sum_df']
        tol_style_dict = {
            'A1:' + col_convert(month_sum_df.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                # 'backColor': '#FFFF00'
            },
            'A1:' + col_convert(month_sum_df.shape[1]) + str(month_sum_df.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                # 'borderType': 'FULL_BORDER'
            },
            'E2:E' + str(month_sum_df.shape[0] + 1): {
                'formatter': "0.00%"
            },
            'I2:I' + str(month_sum_df.shape[0] + 1): {
                'formatter': "0.00%"
            },
            'M2:M' + str(month_sum_df.shape[0] + 1): {
                'formatter': "0.00%"
            }
        }
        month_df = data_dict['month_df']
        detail_style_dict = {
            'A1:' + col_convert(month_df.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                # 'backColor': '#FFFF00'
            },
            'A1:' + col_convert(month_df.shape[1]) + str(month_df.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                # 'borderType': 'FULL_BORDER'
            },
            'H2:H' + str(month_df.shape[0] + 1): {
                'formatter': "0.00%"
            },
            'L2:L' + str(month_df.shape[0] + 1): {
                'formatter': "0.00%"
            },
            'O2:O' + str(month_df.shape[0] + 1): {
                'formatter': "0.00%"
            }
        }
        day_df = data_dict['day_df']
        day_detail_style_dict = {
            'A1:' + col_convert(day_df.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                # 'backColor': '#FFFF00'
            },
            'A1:' + col_convert(day_df.shape[1]) + str(day_df.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                # 'borderType': 'FULL_BORDER'
            },
            'H2:H' + str(day_df.shape[0] + 1): {
                'formatter': "0.00%"
            },
            'L2:L' + str(day_df.shape[0] + 1): {
                'formatter': "0.00%"
            },
            'O2:O' + str(day_df.shape[0] + 1): {
                'formatter': "0.00%"
            }
        }
        month_sum_df.rename(columns={
            'order_date': '月份',
            'anchor_name': '主播',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'final_rate': '结算率',
            'commission_income': '佣金收入',
            'estimated_income': '主播端佣金',
            'estimated_service_income': '团长端佣金',
            'commission_rate': '综合佣金率',
            'price': '客单价',
            'return_order_number': '退货订单数',
            'return_gmv': '退款',
            'return_rate': '退货率'
        }, inplace=True)
        month_df.rename(columns={
            'order_date': '月份',
            'anchor_name': '主播',
            'account_id': '账号ID',
            'item_id': '商品ID',
            'item_title': '商品名称',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'final_rate': '结算率',
            'commission_income': '佣金收入',
            'estimated_income': '主播端佣金',
            'estimated_service_income': '团长端佣金',
            'commission_rate': '综合佣金率',
            'return_order_number': '退货订单数',
            'return_gmv': '退款',
            'return_rate': '退货率'
        }, inplace=True)
        day_sum_df = data_dict['day_sum_df']
        day_sum_df.rename(columns={
            'order_date': '日期',
            'anchor_name': '主播',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'final_rate': '结算率',
            'commission_income': '佣金收入',
            'estimated_income': '主播端佣金',
            'estimated_service_income': '团长端佣金',
            'commission_rate': '综合佣金率',
            'price': '客单价',
            'return_order_number': '退货订单数',
            'return_gmv': '退款',
            'return_rate': '退货率'
        }, inplace=True)
        day_df.rename(columns={
            'order_date': '日期',
            'anchor_name': '主播',
            'account_id': '账号ID',
            'item_id': '商品ID',
            'item_title': '商品名称',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'final_rate': '结算率',
            'commission_income': '佣金收入',
            'estimated_income': '主播端佣金',
            'estimated_service_income': '团长端佣金',
            'commission_rate': '综合佣金率',
            'return_order_number': '退货订单数',
            'return_gmv': '退款',
            'return_rate': '退货率'
        }, inplace=True)

        sheet_title = f"墨晨夏徒弟业绩数据_{data_dict['date_interval']['month_start_time']}_{data_dict['date_interval']['yes_time']}_{data_dict['date_interval']['now_time']}"
        url, spreadsheet_token = self.feishu_sheet_supply.get_workbook_params(
            workbook_name=sheet_title, folder_token='OUbsfyEKelHz23dSDelcgwf2nee'
        )
        tol_sheet_id = self.feishu_sheet_supply.get_sheet_params(
            spreadsheet_token=spreadsheet_token, sheet_name='月度汇总数据'
        )
        detail_sheet_id = self.feishu_sheet_supply.get_sheet_params(
            spreadsheet_token=spreadsheet_token, sheet_name='月度单品数据'
        )
        day_tol_sheet_id = self.feishu_sheet_supply.get_sheet_params(
            spreadsheet_token=spreadsheet_token, sheet_name='昨日汇总数据'
        )
        day_detail_sheet_id = self.feishu_sheet_supply.get_sheet_params(
            spreadsheet_token=spreadsheet_token, sheet_name='昨日单品数据'
        )

        self.feishu_sheet.write_df_replace(
            dat=month_df, spreadsheet_token=spreadsheet_token, sheet_id=detail_sheet_id, to_char=False)
        self.feishu_sheet.write_df_replace(
            dat=month_sum_df, spreadsheet_token=spreadsheet_token, sheet_id=tol_sheet_id, to_char=False)
        self.feishu_sheet.write_df_replace(
            dat=day_df, spreadsheet_token=spreadsheet_token, sheet_id=day_detail_sheet_id, to_char=False)
        self.feishu_sheet.write_df_replace(
            dat=day_sum_df, spreadsheet_token=spreadsheet_token, sheet_id=day_tol_sheet_id, to_char=False)

        for key, value in tol_style_dict.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=tol_sheet_id, ranges=key, styles=value
            )
        for key, value in tol_style_dict.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=day_tol_sheet_id, ranges=key, styles=value
            )
        for key, value in detail_style_dict.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=detail_sheet_id, ranges=key, styles=value
            )
        for key, value in day_detail_style_dict.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=day_detail_sheet_id, ranges=key, styles=value
            )

        return {
            'sheet_params': {
                'sheet_title': sheet_title,
                'url': url
            },
            'date_interval': data_dict['date_interval']
        }

    def send_card_logic(self, card: Dict[str, Any], sheet):
        res = {
            **card,
            **sheet['sheet_params']
        }
        self.feishu_robot.send_msg_card(data=res, card_id=self.card_id, version_name='1.0.1')


dag = MoApprentice().create_dag()
