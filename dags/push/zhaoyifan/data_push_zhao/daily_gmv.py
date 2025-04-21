from abc import ABC

from airflow.decorators import dag, task
import pendulum
from dags.push.zhaoyifan.data_push_zhao.base_dag import FeishuNotificationDAG
import pandas as pd


class GmvDag(FeishuNotificationDAG):
    def __init__(self):
        super().__init__(
            dag_id='gmv',
            default_args={'owner': 'zhaoyifan'},
            robot_url='SELFTEST',
            tags=['example'],
            schedule=None
        )
        self.card_id = 'AAqRW5F1mUPZD'

    def fetch_data_logic(self, date_interval: dict):
        sql = f'''
                    select 
                        sum(origin_gmv) origin_gmv
                        ,sum(final_gmv) final_gmv
                        ,sum(origin_order_number) origin_order_number
                        ,sum(coalesce(estimated_income,0) + coalesce(estimated_service_income,0)) commission_income
                    from dws.dws_ks_ec_2hourly dkeh 
                    where order_date = '{date_interval['yes_ds']}'
                '''
        yes_df = pd.read_sql(sql, self.conn)
        return {
            'data': yes_df,
            'date_interval': date_interval
        }

    def prepare_card_logic(self, data_params):
        data = data_params['data']
        origin_gmv = str(round(data.origin_gmv.iloc[0], 2))
        final_gmv = str(round(data.final_gmv.iloc[0], 2))
        origin_order_number = str(int(data.origin_order_number.iloc[0]))
        commission_income = str(round(data.commission_income.iloc[0], 2))
        return {
            'origin_gmv': origin_gmv,
            'final_gmv': final_gmv,
            'origin_order_number': origin_order_number,
            'commission_income': commission_income
        }

    def write_to_sheet_logic(self, data_params):
        df = data_params['data'].rename(columns={
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'origin_order_number': '支付订单数',
            'commission_income': '佣金收入'
        })

        date_interval = data_params['date_interval']
        workbook_name = f"测试数据_{date_interval['month_start_time']}_{date_interval['yes_time']}_{date_interval['now_time']}"

        workbook_url, spreadsheet_token = self.feishu_sheet_supply.get_workbook_params(
            workbook_name,folder_token='Fn9ZfxSxylvMSsdwzGwcZPEGn9j'
        )

        sheet_id = self.feishu_sheet_supply.get_sheet_params('测试数据', spreadsheet_token)
        print(f"Type of sheet: {type(sheet_id)}")  # 关键调试信息

        self.feishu_sheet.write_df_replace(df, spreadsheet_token, sheet_id, to_char=False)
        return {
            'sheet_params': {
                'workbook_name': workbook_name,
                'url': workbook_url
            },
            'date_interval': date_interval
        }

    def send_card_logic(self, card, sheet) -> None:
        res = {
            **card,
            **sheet['sheet_params'],
            'yes_date': sheet['date_interval']['yes_ds'],
            'month_start_date': sheet['date_interval']['month_start_ds']
        }
        self.feishu_robot.send_msg_card(data=res, card_id=self.card_id, version_name='1.0.0')


gmv_dag = GmvDag().create_dag()
