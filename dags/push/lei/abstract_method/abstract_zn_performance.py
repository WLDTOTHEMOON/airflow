import logging
from typing import Any, Dict

import pandas as pd
import pendulum
from copy import deepcopy
from airflow.models import Variable
from dags.push.lei.abstract_method.abstract_base import AbstractDagTask

logger = logging.getLogger(__name__)


class AbstractZnPerformance(AbstractDagTask):
    def __init__(self):
        super().__init__(
            dag_id='push_zn_performance',
            schedule='0 5 * * *',
            default_args=
            {
                'owner': 'Lei Jiangling',
                'start_date': pendulum.datetime(2023, 1, 1),
                'provide_context': True
            },
            tags=['push', 'zn_performance'],
            robot_url=Variable.get('NoharaRin')
        )
        self.card_id: str = 'AAq4NniWFT389'

    def fetch_data(self, **kwargs) -> Dict:
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        begin_time = start_time.subtract(days=1).strftime('%Y-%m-01')
        end_time = start_time.subtract(days=1).strftime('%Y-%m-%d')
        month = start_time.subtract(days=1).strftime('%Y-%m')
        yesterday_performance_sql = """
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
                ,if(sum(final_gmv) = 0,0,sum(coalesce(estimated_income,0) + coalesce(estimated_service_income,0)) / sum(final_gmv)) commission_rate
            from dws.dws_ks_big_tbl dkbt
            where  order_date  = %(end_time)s
                and anchor_name = '曾楠'
            group by 
                order_date
                ,anchor_name
                ,account_id
                ,item_id 
                ,item_title 
            order by 
                sum(origin_gmv) desc
        """
        yesterday_performance_df = pd.read_sql(yesterday_performance_sql, con=self.engine, params={'end_time': end_time})
        month_performance_sql = """
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
            from dws.dws_ks_big_tbl dkbt 
            where order_date between %(begin_time)s and %(end_time)s
                and anchor_name = '曾楠'
            group by 
                date_format(order_date,'%%Y-%%m')
                ,anchor_name
                ,account_id
                ,item_id 
                ,item_title 
            order by 
                sum(origin_gmv) desc
        """
        month_performance_df = pd.read_sql(month_performance_sql, con=self.engine,
                                           params={'begin_time': begin_time, 'end_time': end_time})
        month_target_sql = """
            select
                *
            from ods.ods_fs_gmv_target ofgt 
            where anchor = '曾楠'
                and month = %(month)s
        """
        month_target_df = pd.read_sql(month_target_sql, con=self.engine, params={'month': month})
        return {
            'yesterday_performance_data': yesterday_performance_df,
            'month_performance_data': month_performance_df,
            'month_target_data': month_target_df
        }

    def process_data(self, data: Dict, **kwargs) -> Dict:
        yesterday_performance_data = data['yesterday_performance_data']
        month_performance_data = data['month_performance_data']
        month_target_data = data['month_target_data']

        # 昨日明细数据
        yesterday_item_data = yesterday_performance_data.copy()
        yesterday_item_data.drop(columns=['final_order_number'], inplace=True)
        yesterday_item_data['final_rate'] = yesterday_item_data['final_rate'].apply(self.percent_convert)
        yesterday_item_data['commission_rate'] = yesterday_item_data['commission_rate'].apply(self.percent_convert)
        yesterday_item_data.order_date = yesterday_item_data.order_date.apply(lambda x: x.strftime('%Y-%m-%d'))
        yesterday_item_data = yesterday_item_data.rename(columns={
            'order_date': '月份',
            'anchor_name': '主播',
            'account_id': '账号ID',
            'item_id': '商品ID',
            'item_title': '商品名称',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'final_rate': '结算率',
            'commission_income': '佣金收入',
            'commission_rate': '综合佣金率',
        })

        # 昨日汇总数据
        yesterday_total_data = yesterday_performance_data.copy()
        yesterday_total_data = yesterday_total_data.groupby(['order_date', 'anchor_name']).agg(
            {
                'origin_gmv': 'sum',
                'final_gmv': 'sum',
                'final_order_number': 'sum',
                'commission_income': 'sum',
            }
        ).reset_index()
        yesterday_total_data['final_rate'] = yesterday_total_data['final_gmv'] / yesterday_total_data['origin_gmv']
        yesterday_total_data['commission_rate'] = yesterday_total_data['commission_income'] / yesterday_total_data[
                                                  'final_gmv']
        yesterday_total_data['avg_price'] = round(yesterday_total_data['final_gmv'] / yesterday_total_data['final_order_number'], 2)
        yesterday_total_data = yesterday_total_data[[
            'order_date', 'anchor_name', 'origin_gmv', 'final_gmv', 'final_rate', 'commission_income',
            'commission_rate', 'avg_price',
        ]]
        yesterday_total_data['final_rate'] = yesterday_total_data['final_rate'].apply(self.percent_convert)
        yesterday_total_data['commission_rate'] = yesterday_total_data['commission_rate'].apply(self.percent_convert)
        yesterday_total_data.order_date = yesterday_total_data.order_date.apply(lambda x: x.strftime('%Y-%m-%d'))
        yesterday_total_data = yesterday_total_data.rename(columns={
            'order_date': '日期',
            'anchor_name': '主播',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'final_rate': '结算率',
            'commission_income': '佣金',
            'commission_rate': '综合佣金率',
            'avg_price': '客单价'
        })

        # 月度明细数据
        month_item_data = month_performance_data.copy()
        month_item_data.drop(columns=['final_order_number'], inplace=True)
        month_item_data['final_rate'] = month_item_data['final_rate'].apply(self.percent_convert)
        month_item_data['commission_rate'] = month_item_data['commission_rate'].apply(self.percent_convert)
        month_item_data = month_item_data.rename(columns={
            'month': '日期',
            'anchor_name': '主播',
            'account_id': '账号ID',
            'item_id': '商品ID',
            'item_title': '商品名称',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'final_rate': '结算率',
            'commission_income': '佣金收入',
            'commission_rate': '综合佣金率',
        })

        # 月度汇总数据
        month_total_data = month_performance_data.copy()
        month_total_data = month_total_data.groupby(['month', 'anchor_name']).agg(
            {
                'origin_gmv': 'sum',
                'final_gmv': 'sum',
                'final_order_number': 'sum',
                'commission_income': 'sum',
            }
        ).reset_index()
        month_total_data['final_rate'] = month_total_data['final_gmv'] / month_total_data['origin_gmv']
        month_total_data['commission_rate'] = month_total_data['commission_income'] / month_total_data['final_gmv']
        month_total_data['avg_price'] = round(month_total_data['final_gmv'] / month_total_data['final_order_number'], 2)
        month_total_data['target'] = month_target_data['target_final'].values[0] if not month_target_data.empty else 0
        month_total_data['target_rate'] = month_total_data['final_gmv'] / month_total_data['target'].values[0] if month_total_data['target'].values[0] > 0 else 0
        month_total_data = month_total_data[[
            'month', 'anchor_name', 'origin_gmv', 'final_gmv', 'final_rate', 'commission_income',
            'commission_rate', 'avg_price', 'target', 'target_rate'
        ]]
        month_total_data['final_rate'] = month_total_data['final_rate'].apply(self.percent_convert)
        month_total_data['commission_rate'] = month_total_data['commission_rate'].apply(self.percent_convert)
        month_total_data['target_rate'] = month_total_data['target_rate'].apply(self.percent_convert)
        month_total_data = month_total_data.rename(columns={
            'month': '月份',
            'anchor_name': '主播',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'final_rate': '结算率',
            'commission_income': '佣金',
            'commission_rate': '综合佣金率',
            'avg_price': '客单价'
        })

        return {
            'month_total_data': month_total_data,
            'month_item_data': month_item_data,
            'yesterday_total_data': yesterday_total_data,
            'yesterday_item_data': yesterday_item_data
        }

    def create_feishu_file(self, data_dic: Dict, **kwargs) -> Dict:
        logger.info(f'创建飞书文件')
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        title = ('曾楠业绩数据_' + start_time.subtract(days=1).strftime('%Y%m01') + '_' +
                 start_time.subtract(days=1).strftime('%Y%m%d') + '_' + start_time.strftime('%Y%m%d%H%M'))
        yes_day = start_time.subtract(days=1).strftime('%Y-%m-%d')
        time_score = start_time.subtract(days=1).strftime('%Y-%m-01') + ' to ' + start_time.subtract(days=1).strftime('%Y-%m-%d')

        result = self.feishu_sheet.create_spreadsheet(
            title=title, folder_token='OcUEfDYHElyZ5kdJ7D2cW96kncd'
        )
        spreadsheet_token = result['spreadsheet']['spreadsheet_token']
        url = result['spreadsheet']['url']
        cps_sheet_id = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='月度汇总数据')
        month_total_sheet_id = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        cps_sheet_id = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='月度单品数据')
        month_item_sheet_id = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        cps_sheet_id = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='昨日汇总数据')
        yesterday_total_sheet_id = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        cps_sheet_id = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='昨日单品数据')
        yesterday_item_sheet_id = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        return {
            'spreadsheet_token': spreadsheet_token,
            'cps_sheet_id': [month_total_sheet_id, month_item_sheet_id, yesterday_total_sheet_id, yesterday_item_sheet_id],
            'url': url,
            'title': [title, yes_day, time_score]
        }

    def render_feishu_format(
            self,
            processed_data_dict: Dict,
            file_info: Dict
    ) -> Dict:
        logger.info(f'渲染飞书格式')
        process_data = deepcopy(processed_data_dict)
        process_data['month_total_data'].drop(columns=['target', 'target_rate'], inplace=True)
        spreadsheet_token = file_info['spreadsheet_token']
        cps_sheet_id = file_info['cps_sheet_id']

        sheet_id_num = 0
        for i in process_data.values():
            style_dict = {
                'A1:' + self.col_convert(i.shape[1]) + '1': {
                    'font': {
                        'bold': True
                    },
                    'backColor': '#FFFF00'
                },
                'A1:' + self.col_convert(i.shape[1]) + str(i.shape[0] + 1): {
                    'hAlign': 1,
                    'vAlign': 1,
                    'borderType': 'FULL_BORDER'
                }
            }
            self.feishu_sheet.write_df_replace(i, spreadsheet_token, cps_sheet_id[sheet_id_num])
            for key, value in style_dict.items():
                self.feishu_sheet.style_cells(
                    spreadsheet_token=spreadsheet_token, sheet_id=cps_sheet_id[sheet_id_num], ranges=key, styles=value
                )
            sheet_id_num += 1

        return processed_data_dict

    def send_card(self, file_info: Dict, data_dic: Dict):
        logger.info(f'发送卡片')
        title = file_info['title']
        url = file_info['url']
        yesterday_total_data = data_dic['yesterday_total_data']
        month_total_data = data_dic['month_total_data']
        logger.info(month_total_data.columns.tolist())

        data = {
            'title': '曾楠业绩数据',
            'sheet_title': title[0],
            'sheet_url': url,
            'yes_origin_gmv': str(round(yesterday_total_data['支付GMV'][0]/10000, 2)),
            'yes_final_gmv': str(round(yesterday_total_data['结算GMV'][0]/10000, 2)),
            'yes_return_rate': str(yesterday_total_data['结算率'][0]),
            'yes_commission_income': str(round(yesterday_total_data['佣金'][0]/10000, 2)),
            'month_origin_gmv': str(round(month_total_data['支付GMV'][0]/10000, 2)),
            'month_final_gmv': str(round(month_total_data['结算GMV'][0]/10000, 2)),
            'month_return_rate': str(month_total_data['结算率'][0]),
            'month_target': str(round(month_total_data['target'][0]/10000, 2)),
            'month_target_rate': str(month_total_data['target_rate'][0]),
            'month_commission_income': str(round(month_total_data['佣金'][0]/10000, 2)),
            'yes_day': str(title[1]),
            'time_score': str(title[2])
        }

        logger.info(data)
        robot = self.feishu_robot
        robot.send_msg_card(data=data, card_id=self.card_id, version_name='1.0.2')


zn_performance_dag = AbstractZnPerformance()
zn_performance_dag.create_dag()

