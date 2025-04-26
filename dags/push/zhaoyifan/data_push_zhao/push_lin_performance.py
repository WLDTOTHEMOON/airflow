import logging
from typing import Dict, Any

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.zhaoyifan.data_push_zhao.base_dag import BaseDag
from dags.push.zhaoyifan.data_push_zhao.format_utils import *

logger = logging.getLogger(__name__)


class LinPerformance(BaseDag):
    def __init__(self):
        super().__init__(
            dag_id='push_lin_performance',
            default_args={'owner': 'zhaoyifan'},
            robot_url=Variable.get('TEST'),
            tags=['push', 'lin_performance'],
            schedule='0 5 * * *'
        )
        self.card_id = 'AAq4gyC5p5YSt'

    def fetch_data_logic(self, date_interval: Dict[str, Any]) -> Dict[str, Any]:
        yes_sql = f'''
            select 
                    sum(origin_gmv) origin_gmv 
                    ,sum(final_gmv) final_gmv 
                    ,sum(final_gmv) /  sum(origin_gmv) final_rate
                    ,sum(coalesce(estimated_income,0)) + sum(coalesce(estimated_service_income,0)) commission_income
                from dws.dws_ks_ec_2hourly dkeh   
                where order_date  = '{date_interval['yes_ds']}'
                    and anchor_name = '林一泽'
       '''
        yes_df = pd.read_sql(yes_sql, self.engine)

        month_sql = f'''
            select
                origin_gmv
                ,final_gmv
                ,final_gmv / origin_gmv final_rate
                ,commission_income
                ,target_final
                ,final_gmv / target_final target_success_rate
            from (
                select 
                    sum(origin_gmv) origin_gmv 
                    ,sum(final_gmv) final_gmv 
                    ,sum(coalesce(estimated_income,0)) + sum(coalesce(estimated_service_income,0)) commission_income
                from dws.dws_ks_ec_2hourly dkeh  
                where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                    and anchor_name = '林一泽'
            ) gmv,
            (
                select 
                    sum(target_final * 10000) target_final
                from ods.ods_gmv_target ogt 
                where month= '{date_interval['month']}'
                    and anchor = '林一泽'
            ) tar 
        '''
        month_df = pd.read_sql(month_sql, self.engine)

        # 明细数据 - 月度数据
        tol_sql = f'''
            select 
                date_format(order_date,'%%Y-%%m') month
                ,anchor_name
                ,account_id
                ,item_id 
                ,item_title 
                ,sum(origin_gmv) origin_gmv 
                ,sum(final_gmv) final_gmv 
                ,sum(final_order_number) final_order_number
                ,sum(final_gmv) / sum(origin_gmv) final_rate
                ,sum(coalesce(estimated_income,0) + coalesce(estimated_service_income,0)) commission_income
                ,sum(coalesce(estimated_income,0) + coalesce(estimated_service_income,0)) / sum(final_gmv) commission_rate
            from dws.dws_ks_ec_2hourly dkeh 
            where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                and anchor_name = '林一泽'
            group by 
                date_format(order_date,'%%Y-%%m')
                ,anchor_name
                ,account_id
                ,item_id 
                ,item_title 
            order by 
                sum(origin_gmv) desc
        '''
        df = pd.read_sql(tol_sql, self.engine)
        detail_df = df[
            ['month', 'anchor_name', 'account_id', 'item_id', 'item_title', 'origin_gmv', 'final_gmv', 'final_rate',
             'commission_income', 'commission_rate']]
        tol_df = df[['month', 'anchor_name', 'origin_gmv', 'final_gmv', 'final_order_number', 'commission_income']]
        tol_df = tol_df.groupby(['month', 'anchor_name']).sum().reset_index()
        tol_df['final_rate'] = tol_df.final_gmv / tol_df.origin_gmv
        tol_df['commission_rate'] = tol_df.commission_income / tol_df.final_gmv
        tol_df['price'] = round(tol_df.final_gmv / tol_df.final_order_number, 2)
        tol_df = tol_df[
            ['month', 'anchor_name', 'origin_gmv', 'final_gmv', 'final_rate', 'commission_income', 'commission_rate',
             'price']]

        # 明细数据 - 昨日数据
        day_sql = f'''
            select 
                order_date
                ,anchor_name
                ,account_id
                ,item_id 
                ,item_title 
                ,sum(origin_gmv) origin_gmv 
                ,sum(final_gmv) final_gmv 
                ,sum(final_order_number) final_order_number
                ,sum(final_gmv) / sum(origin_gmv) final_rate
                ,sum(coalesce(estimated_income,0) + coalesce(estimated_service_income,0)) commission_income
                ,sum(coalesce(estimated_income,0) + coalesce(estimated_service_income,0)) / sum(final_gmv) commission_rate
            from dws.dws_ks_ec_2hourly dkeh 
            where  order_date  = '{date_interval['yes_ds']}'
                and anchor_name = '林一泽'
            group by 
                order_date
                ,anchor_name
                ,account_id
                ,item_id 
                ,item_title 
            order by 
                sum(origin_gmv) desc
        '''
        day_df = pd.read_sql(day_sql, self.engine)
        day_detail_df = day_df[
            ['order_date', 'anchor_name', 'account_id', 'item_id', 'item_title', 'origin_gmv', 'final_gmv',
             'final_rate', 'commission_income', 'commission_rate']]
        day_detail_df['order_date'] = day_detail_df['order_date'].astype('str')
        day_tol_df = day_df[
            ['order_date', 'anchor_name', 'origin_gmv', 'final_gmv', 'final_order_number', 'commission_income']]
        day_tol_df = day_tol_df.groupby(['order_date', 'anchor_name']).sum().reset_index()
        day_tol_df['final_rate'] = day_tol_df.final_gmv / day_tol_df.origin_gmv
        day_tol_df['commission_rate'] = day_tol_df.commission_income / day_tol_df.final_gmv
        day_tol_df['price'] = round(day_tol_df.final_gmv / day_tol_df.final_order_number, 2)
        day_tol_df = day_tol_df[
            ['order_date', 'anchor_name', 'origin_gmv', 'final_gmv', 'final_rate', 'commission_income',
             'commission_rate', 'price']]
        day_tol_df['order_date'] = day_tol_df['order_date'].astype('str')

        return {
            'yes_df': yes_df,
            'month_df': month_df,
            'df': df,
            'detail_df': detail_df,
            'tol_df': tol_df,
            'day_df': day_df,
            'day_detail_df': day_detail_df,
            'day_tol_df': day_tol_df,
            'date_interval': date_interval
        }

    def prepare_card_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        yes_df = data_dict['yes_df']
        if len(yes_df) == 0:
            yes_origin_gmv = '0'
            yes_final_gmv = '0'
            yes_final_rate = '-'
            yes_commission_income = '0'
        else:
            yes_origin_gmv = str(round(yes_df['origin_gmv'].iloc[0] / 10000, 1))
            yes_final_gmv = str(round(yes_df['final_gmv'].iloc[0] / 10000, 1))
            yes_final_rate = str(
                '{:.2f}%'.format(yes_df.final_rate.iloc[0] * 100))
            yes_commission_income = str(round(yes_df['commission_income'].iloc[0] / 10000, 1))

        month_df = data_dict['month_df']
        if pd.isnull(month_df.origin_gmv.iloc[0]):
            month_origin_gmv = '0'
            month_final_gmv = '0'
            month_final_rate = '0'
            month_commission_income = '0'
        else:
            month_origin_gmv = str(round(month_df['origin_gmv'].iloc[0] / 10000, 1))
            month_final_gmv = str(round(month_df['final_gmv'].iloc[0] / 10000, 1))
            month_commission_income = str(round(month_df['commission_income'].iloc[0] / 10000, 1))
            month_final_rate = str(
                '{:.2f}%'.format(month_df.final_rate.iloc[0] * 100))

        if pd.isnull(month_df.target_final.iloc[0]):
            month_target = '\\-'
            month_target_success_rate = '\\-'
        else:
            month_target = str(round(month_df['target_final'].iloc[0] / 10000, 1))
            month_target_success_rate = str(
                '{:.2f}%'.format(month_df.target_success_rate.iloc[0] * 100))

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
        tol_df = data_dict['tol_df']
        tol_style_dict = {
            'A1:' + col_convert(tol_df.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                # 'backColor': '#FFFF00'
            },
            'A1:' + col_convert(tol_df.shape[1]) + str(tol_df.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                # 'borderType': 'FULL_BORDER'
            },
            'E2:E' + str(tol_df.shape[0] + 1): {
                'formatter': "0.00%"
            },
            'G2:G' + str(tol_df.shape[0] + 1): {
                'formatter': "0.00%"
            }
        }
        detail_df = data_dict['detail_df']
        detail_style_dict = {
            'A1:' + col_convert(detail_df.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                # 'backColor': '#FFFF00'
            },
            'A1:' + col_convert(detail_df.shape[1]) + str(detail_df.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                # 'borderType': 'FULL_BORDER'
            },
            'H2:H' + str(detail_df.shape[0] + 1): {
                'formatter': "0.00%"
            },
            'J2:J' + str(detail_df.shape[0] + 1): {
                'formatter': "0.00%"
            }
        }
        day_detail_df = data_dict['day_detail_df']
        day_detail_style_dict = {
            'A1:' + col_convert(day_detail_df.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                # 'backColor': '#FFFF00'
            },
            'A1:' + col_convert(day_detail_df.shape[1]) + str(day_detail_df.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                # 'borderType': 'FULL_BORDER'
            },
            'H2:H' + str(day_detail_df.shape[0] + 1): {
                'formatter': "0.00%"
            },
            'J2:J' + str(day_detail_df.shape[0] + 1): {
                'formatter': "0.00%"
            }
        }

        tol_df.rename(columns={
            'month': '月份',
            'anchor_name': '主播',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'final_rate': '结算率',
            'commission_income': '佣金收入',
            'commission_rate': '综合佣金率',
            'price': '客单价'
        }, inplace=True)
        detail_df.rename(columns={
            'month': '月份',
            'anchor_name': '主播',
            'account_id': '账号ID',
            'item_id': '商品ID',
            'item_title': '商品名称',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'final_rate': '结算率',
            'commission_income': '佣金收入',
            'commission_rate': '综合佣金率',
        }, inplace=True)
        day_tol_df = data_dict['day_tol_df']
        day_tol_df.rename(columns={
            'order_date': '日期',
            'anchor_name': '主播',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'final_rate': '结算率',
            'commission_income': '佣金收入',
            'commission_rate': '综合佣金率',
            'price': '客单价'
        }, inplace=True)
        day_detail_df.rename(columns={
            'order_date': '日期',
            'anchor_name': '主播',
            'account_id': '账号ID',
            'item_id': '商品ID',
            'item_title': '商品名称',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'final_rate': '结算率',
            'commission_income': '佣金收入',
            'commission_rate': '综合佣金率',
        }, inplace=True)

        sheet_title = f"林一泽业绩数据_{data_dict['date_interval']['month_start_time']}_{data_dict['date_interval']['yes_time']}_{data_dict['date_interval']['now_time']}"
        url, spreadsheet_token = self.feishu_sheet_supply.get_workbook_params(
            workbook_name=sheet_title, folder_token='QU4Of1BedlgFUHdEPZjcWNu8n28'
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
            dat=detail_df, spreadsheet_token=spreadsheet_token, sheet_id=detail_sheet_id, to_char=False)
        self.feishu_sheet.write_df_replace(
            dat=tol_df, spreadsheet_token=spreadsheet_token, sheet_id=tol_sheet_id, to_char=False)
        self.feishu_sheet.write_df_replace(
            dat=day_detail_df, spreadsheet_token=spreadsheet_token, sheet_id=day_detail_sheet_id, to_char=False)
        self.feishu_sheet.write_df_replace(
            dat=day_tol_df, spreadsheet_token=spreadsheet_token, sheet_id=day_tol_sheet_id, to_char=False)

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
        self.feishu_robot.send_msg_card(data=res, card_id=self.card_id, version_name='1.0.0')


dag = LinPerformance().create_dag()
