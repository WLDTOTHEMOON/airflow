from dags.push.zhaoyifan.base_utils.base_dag import FeishuNotificationDAG
import pandas as pd
from typing import Dict, Any
from dags.push.zhaoyifan.base_utils.feishu_provider import FeishuCardSender


class TestDAG(FeishuNotificationDAG):

    def __init__(self):
        super().__init__(
            conn=self.conn,
            feishu_sheet_supply=self.feishu_sheet_supply,
            feishu_sheet=self.feishu_sheet,
            dag_id='test_data',
            schedule=None,
            card_id='AAqRW5F1mUPZD'
        )

    def _create_dag(self):
        dag = super()._create_dag()

        # 覆盖任务组中的方法
        def fetch_data(date_interval: Dict[str, Any]) -> Dict[str, Any]:
            sql_template = f'''
                select 
                    sum(origin_gmv) origin_gmv
                    ,sum(final_gmv) final_gmv
                    ,sum(origin_order_number) origin_order_number
                    ,sum(coalesce(estimated_income,0) + coalesce(estimated_service_income,0)) commission_income
                from dws.dws_ks_slice_daily dksd
                where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
            '''
            df = pd.read_sql(sql_template, self.conn)
            return {
                'data': df,
                'date_interval': date_interval
            }

        def prepare_card(data_params: Dict[str, Any]) -> Dict[str, Any]:
            df = data_params['data']
            return {
                'origin_gmv': str(round(df.origin_gmv.iloc[0], 2)),
                'final_gmv': str(round(df.final_gmv.iloc[0], 2)),
                'origin_order_number': str(int(df.origin_order_number.iloc[0])),
                'commission_income': str(round(df.commission_income.iloc[0], 2))
            }

        def write_to_sheet(data_params: Dict[str, Any]) -> Dict[str, Any]:
            df = data_params['data'].rename(columns={
                'origin_gmv': '支付GMV',
                'final_gmv': '结算GMV',
                'origin_order_number': '支付订单数',
                'commission_income': '佣金收入'
            })

            date_interval = data_params['date_interval']
            workbook_name = f"测试数据_{date_interval['month_start_time']}_{date_interval['yes_time']}_{date_interval['now_time']}"

            workbook_url, spreadsheet_token = self.feishu_sheet_supply.get_workbook_params(workbook_name=workbook_name,
                                                                                           folder_token='Fn9ZfxSxylvMSsdwzGwcZPEGn9j')
            sheet_id = self.feishu_sheet_supply.get_sheet_params('测试数据', spreadsheet_token)
            self.feishu_sheet.write_df_replace(df, spreadsheet_token, sheet_id, to_char=False)
            return {
                'sheet_params': {
                    'workbook_name': workbook_name,
                    'url': workbook_url
                },
                'date_interval': date_interval
            }

        def send_card(results: Dict[str, Any]) -> None:
            res = {
                **results['card'],
                **results['sheet']['sheet_params'],
                'yes_date': results['sheet']['date_interval']['yes_ds'],
                'month_start_date': results['sheet']['date_interval']['month_start_ds']
            }
            sender = FeishuCardSender('SELFTEST')
            sender.send_card(res, self.card_id, self.card_version)

        # 动态替换任务组中的函数
        dag.task_dict['fetch_data'].python_callable = fetch_data
        dag.task_group_dict['process_data'].task_dict['prepare_card'].python_callable = prepare_card
        dag.task_group_dict['process_data'].task_dict['write_to_sheet'].python_callable = write_to_sheet
        dag.task_dict['send_card'].python_callable = send_card

        return dag


ly_sales_dag = TestDAG().register()
