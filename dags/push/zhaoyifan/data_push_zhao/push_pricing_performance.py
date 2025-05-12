import logging
from typing import Dict, Any

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.zhaoyifan.data_push_zhao.base_dag import BaseDag
from dags.push.zhaoyifan.data_push_zhao.format_utils import *

logger = logging.getLogger(__name__)


class PricingPerformance(BaseDag):
    def __init__(self):
        super().__init__(
            dag_id='push_pricing_performance',
            robot_url=Variable.get('TEST'),
            tags=['push', 'pricing_performance'],
            schedule='0 3 * * *'
        )
        self.card_id = 'AAq4ui4QeXoeB'

    def fetch_data_logic(self, date_interval: Dict[str, Any]) -> Dict[str, Any]:
        detail_sql = f'''
                    with src as (
                        select
                            pro.id product_id
                            ,pro.name product_name
                            ,submit_time
                            ,case 
                                when pre_review_time is null then submit_time 
                                else pre_review_time
                            end to_cost_review_time
                            ,audit_time
                            ,audit_user.name audit_user_name
                        from (
                            select 
                                id
                                ,name
                                ,created_at submit_time
                            from xlsd.products p 
                        )pro
                        right join (
                            select
                                target_id 
                                ,audit_time
                                ,user_id 
                                ,lag(audit_time) over(partition by target_id order by audit_time) pre_review_time
                            from (
                                select 
                                    target 
                                    ,target_id 
                                    ,created_at audit_time 
                                    ,user_id 
                                from xlsd.review r 
                                where target = 'product'
                                    and result != 20
                                    and act != 15
                                union all 
                                select 
                                    null target
                                    ,product_id
                                    ,updated_at
                                    ,user_id
                                from xlsd.products_log pl
                            ) all_act
                        )review on pro.id = review.target_id
                        left join (
                          select 
                            id,name 
                          from xlsd.users u 
                        )audit_user on review.user_id = audit_user.id 
                        where audit_user.name in ('黄程程','羊丽','施汶君')
                    )
                    select 
                        product_id
                        ,product_name
                        ,to_cost_review_time
                        ,audit_time
                        ,audit_user_name
                        ,timestampdiff(second,to_cost_review_time,audit_time) / 3600 time_diff
                    from src
                    where date(audit_time) between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                    order by audit_time
                '''
        detail_df = pd.read_sql(detail_sql, self.engine)

        return {
            'detail_df': detail_df,
            'date_interval': date_interval
        }

    def prepare_card_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        detail_df = data_dict['detail_df']
        result = detail_df.groupby('audit_user_name').agg({
            'audit_time': 'count',
            'time_diff': lambda x: x[x <= 24].count()
        }).reset_index()
        result.rename(columns={result.columns[1]: 'tol_num', result.columns[-1]: 'two_hours_num'}, inplace=True)
        for i in range(result.shape[0]):
            if result.tol_num.iloc[i] == 0:
                result['rate'] = 0
            else:
                result['rate'] = result.two_hours_num / result.tol_num

        data = []
        for i in range(result.shape[0]):
            result_rate = result.rate.iloc[i]
            if result_rate == 0:
                result_rate = '\\-'
            else:
                result_rate = str(
                    '{:.2f}%'.format(result.rate.iloc[i] * 100))
            data.append({
                'audit_user_name': str(result.audit_user_name.iloc[i]),
                'tol_num': str(result.tol_num.iloc[i]),
                'two_hours_num': str(result.two_hours_num.iloc[i]),
                'rate': str(result_rate)
            })

        return {
            'time_scope': data_dict['date_interval']['month_start_ds'] + ' to ' + data_dict['date_interval']['yes_ds'],
            'group_table': data
        }

    def write_to_sheet_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        detail_df = data_dict['detail_df']
        style_dict = {
            'A1:' + col_convert(detail_df.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                'backColor': '#FFFF00'
            },
            'A1:' + col_convert(detail_df.shape[1]) + str(detail_df.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                # 'borderType': 'FULL_BORDER'
            }
        }
        detail_df['to_cost_review_time'] = detail_df['to_cost_review_time'].astype(str)
        detail_df['audit_time'] = detail_df['audit_time'].astype(str)
        dat = detail_df.rename(columns={
            'product_id': '产品ID', 'product_name': '产品名称', 'to_cost_review_time': '到成本审核时间',
            'audit_time': '审核时间', 'audit_user_name': '审核人', 'time_diff': '审核时长（小时）'
        })
        sheet_title = f"核价部绩效数据_{data_dict['date_interval']['month_start_time']}_{data_dict['date_interval']['yes_time']}_{data_dict['date_interval']['now_time']}"
        url, spreadsheet_token = self.feishu_sheet_supply.get_workbook_params(
            workbook_name=sheet_title, folder_token='NvjTf41btl1EYcdPzHIc8KWpnMc'
        )
        sheet_id = self.feishu_sheet_supply.get_sheet_params(spreadsheet_token=spreadsheet_token, sheet_name='Result')
        self.feishu_sheet.write_df_replace(dat=dat, spreadsheet_token=spreadsheet_token, sheet_id=sheet_id,
                                           to_char=False)
        for key, value in style_dict.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=sheet_id, ranges=key, styles=value
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


dag = PricingPerformance().create_dag()
