from airflow.decorators import dag, task, task_group
import pendulum


CARD_ID = 'AAqRW5F1mUPZD'


@dag(schedule=None, start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'zhaoyifan'}, tags=['example'])
def test_dag():
    @task
    def get_date_interval(**context):
        logical_datetime = context['logical_date'].in_timezone('Asia/Shanghai')
        yes_date = logical_datetime.subtract(days=1).format('YYYY-MM-DD')
        month_date_start = logical_datetime.subtract(days=1).start_of('month').format('YYYY-MM-DD')
        yes_timestamp = logical_datetime.subtract(days=1).format('YYYYMMDD')
        month_start_timestamp = logical_datetime.subtract(days=1).start_of('month').format('YYYYMMDD')
        now_timestamp = logical_datetime.format('YYYYMMDDHHMM')
        date_interval = {
            "logical": logical_datetime,
            "yesterday": yes_date,
            "month_start": month_date_start,
            "yes_timestamp": yes_timestamp,
            "month_start_timestamp": month_start_timestamp,
            "now_timestamp": now_timestamp
        }
        return date_interval


    @task
    def fetch_data(date_interval: dict):
        from include.database.mysql import engine
        import pandas as pd
        sql = f'''
            select 
                sum(origin_gmv) origin_gmv
                ,sum(final_gmv) final_gmv
                ,sum(origin_order_number) origin_order_number
                ,sum(coalesce(estimated_income,0) + coalesce(estimated_service_income,0)) commission_income
            from dws.dws_ks_slice_daily dksd
            where order_date = '{date_interval['yesterday']}'
        '''
        yes_df = pd.read_sql(sql, engine)
        return {
            'data': yes_df,
            'date_interval': date_interval
        }

    @task_group
    def write_operations_group(data_params: dict):

        @task
        def add_data_to_card(data_params):
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

        @task
        def write_data_to_sheet(data_params: dict):
            data = data_params['data']
            data.rename(columns={
                'origin_gmv': '支付GMV',
                'final_gmv': '结算GMV',
                'origin_order_number': '支付订单数',
                'commission_income': '佣金收入'
            }, inplace=True)
            from include.feishu.feishu_sheet import FeishuSheet
            from airflow.models import Variable
            date_interval = data_params['date_interval']
            sheet_name = f"测试数据_{date_interval['month_start_timestamp']}_{date_interval['yes_timestamp']}_{date_interval['now_timestamp']}"
            feishu_sheet = FeishuSheet(app_id=Variable.get('feishu', deserialize_json=True).get('app_id'),
                                       app_secret=Variable.get("feishu", deserialize_json=True).get("app_secret"))
            dict1 = feishu_sheet.create_spreadsheet(sheet_name, folder_token='Fn9ZfxSxylvMSsdwzGwcZPEGn9j')
            spreadsheet_token = dict1['spreadsheet']['spreadsheet_token']
            sheet_url = dict1['spreadsheet']['url']
            dict2 = feishu_sheet.create_sheet(spreadsheet_token, '测试数据')
            sheet_token = dict2['replies'][0]['addSheet']['properties']['sheetId']
            feishu_sheet.write_df_replace(data, spreadsheet_token, sheet_token, to_char=False)
            return {
                'sheet_params': {
                    'sheet_title': sheet_name,
                    'url': sheet_url
                },
                'date_interval': date_interval
            }

        card_status = add_data_to_card(data_params)
        sheet_status = write_data_to_sheet(data_params)
        return {
            "card": card_status,
            "sheet": sheet_status
        }

    @task
    def send_card(dat: dict):
        from include.feishu.feishu_robot import FeishuRobot
        from airflow.models import Variable
        res = {
            **dat['card'],
            **dat['sheet']['sheet_params'],
            'yes_date': dat['sheet']['date_interval']['yesterday']
        }
        robot = FeishuRobot(robot_url=Variable.get('SELFTEST'))
        robot.send_msg_card(data=res, card_id=CARD_ID, version_name='1.0.0')

    interval = get_date_interval()
    data = fetch_data(interval)
    full_data_add = write_operations_group(data)
    send_card(full_data_add)


test_dag()
