from airflow.decorators import dag, task
import pendulum


CARD_ID = 'AAqRW5F1mUPZD'


@dag(schedule=None, start_date=pendulum.datetime(2023, 1, 1),
     default_args={'owner': 'zhaoyifan'}, tags=['example'])
def test_dag():
    def get_interval():
        from airflow.operators.python import get_current_context
        context = get_current_context()
        logical_datetime = context['logical_date'].in_timezone('Asia/Shanghai')
        yes_date = logical_datetime.subtract(days=1).format('YYYY-MM-DD')
        month_date_start = logical_datetime.subtract(days=1).start_of('month').format('YYYY-MM-DD')
        return {
            "logical": logical_datetime,
            "yesterday": yes_date,
            "month_start": month_date_start
        }

    @task
    def fetch_data(interval:dict):
        from include.database.mysql import engine
        import pandas as pd
        yes_date = interval['yesterday']
        sql = f'''
            select 
                sum(origin_gmv) origin_gmv
                ,sum(final_gmv) final_gmv
                ,sum(origin_order_number) origin_order_number
                ,sum(coalesce(estimated_income,0) + coalesce(estimated_service_income,0)) commission_income
            from dws.dws_ks_slice_daily dksd
            where order_date = '{yes_date}'
        '''
        yes_df = pd.read_sql(sql, engine)
        return yes_df

    @task
    def add_data_to_card(data):
        origin_gmv = str(round(data.origin_gmv.iloc[0], 2))
        final_gmv = str(round(data.final_gmv.iloc[0], 2))
        origin_order_number = str(int(data.origin_order_number.iloc[0]))
        commission_income = str(round(data.commission_income.iloc[0], 2))
        res = {
            'origin_gmv': origin_gmv,
            'final_gmv': final_gmv,
            'origin_order_number': origin_order_number,
            'commission_income': commission_income
        }
        return res

    @task
    def write_data_to_sheet(interval:dict,data):
        data.rename(columns={
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'origin_order_number': '支付订单数',
            'commission_income': '佣金收入'
        }, inplace=True)
        from include.feishu.feishu_sheet import FeishuSheet
        from airflow.models import Variable
        logical_datetime = interval['logical']
        sheet_name = '测试数据_' + logical_datetime.subtract(days=1).start_of('month').strftime("%Y%m%d") + '_' + \
                     logical_datetime.subtract(days=1).strftime("%Y%m%d") + '_' + logical_datetime.strftime(
            "%Y%m%d%H%M")
        feishu_sheet = FeishuSheet(app_id=Variable.get('feishu', deserialize_json=True).get('app_id'),
                                   app_secret=Variable.get("feishu", deserialize_json=True).get("app_secret"))
        dict1 = feishu_sheet.create_spreadsheet(sheet_name, folder_token='Dylgf6mQvl7nCWdmhZVc6bI5nZX')
        spreadsheet_token = dict1['spreadsheet']['spreadsheet_token']
        sheet_url = dict1['spreadsheet']['url']
        dict2 = feishu_sheet.create_sheet(spreadsheet_token, '测试数据')
        sheet_token = dict2['replies'][0]['addSheet']['properties']['sheetId']
        feishu_sheet.write_df_replace(data, spreadsheet_token, sheet_token, to_char=False)
        return sheet_name, sheet_url

    @task
    def send_card(dat, file: tuple,interval:dict):
        from include.feishu.feishu_robot import FeishuRobot
        from airflow.models import Variable
        sheet_name, sheet_url = file
        dat['yes_date'] = interval['yesterday']
        dat['sheet_title'] = sheet_name
        dat['url'] = sheet_url
        robot = FeishuRobot(robot_url=Variable.get('SELFTEST'))
        robot.send_msg_card(data=dat, card_id=CARD_ID, version_name='1.0.0')

    interval = get_interval()
    data = fetch_data(interval)
    dat = add_data_to_card(data)
    file = write_data_to_sheet(interval, data)
    send_card(dat, file, interval)

test_dag()
