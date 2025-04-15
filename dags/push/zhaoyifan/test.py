from airflow.decorators import dag, task
import pendulum

yes_date = pendulum.yesterday().to_date_string()
filename = '测试数据_' + pendulum.yesterday().start_of('month').strftime("%Y%m%d") + '_' + \
        pendulum.yesterday().strftime("%Y%m%d") + '_' + pendulum.now().strftime("%Y%m%d%H%M")
CARD_ID = 'AAqRW5F1mUPZD'

@dag(schedule=None,default_args={'owner': 'zhaoyifan'},tags=['example'])
def test_dag():
    @task
    def fetch_data():
        from include.database.mysql import engine
        import pandas as pd
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
    def write_data_to_sheet(data):
        data.rename(columns={
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'origin_order_number': '支付订单数',
            'commission_income': '佣金收入'
        }, inplace=True)
        from include.feishu.feishu_sheet import FeishuSheet
        from airflow.models import Variable
        feishu_sheet = FeishuSheet(app_id=Variable.get('feishu', deserialize_json=True).get('app_id'),
                                   app_secret=Variable.get("feishu", deserialize_json=True).get("app_secret"))
        dict1 = feishu_sheet.create_spreadsheet(filename, folder_token='Dylgf6mQvl7nCWdmhZVc6bI5nZX')
        spreadsheet_token = dict1['spreadsheet']['spreadsheet_token']
        url = dict1['spreadsheet']['url']
        dict2 = feishu_sheet.create_sheet(spreadsheet_token, '测试数据')
        sheet_token = dict2['replies'][0]['addSheet']['properties']['sheetId']
        feishu_sheet.write_df_replace(data, spreadsheet_token, sheet_token, to_char=False)
        return url

    @task
    def send_card(dat, url):
        from include.feishu.feishu_robot import FeishuRobot
        from airflow.models import Variable
        dat['yes_date'] = yes_date
        dat['sheet_title'] = filename
        dat['url'] = url
        robot = FeishuRobot(robot_url=Variable.get('SELFTEST'))
        robot.send_msg_card(data=dat, card_id=CARD_ID, version_name='1.0.0')

    data = fetch_data()
    dat = add_data_to_card(data)
    url = write_data_to_sheet(data)
    send_card(dat, url)

test_dag()


