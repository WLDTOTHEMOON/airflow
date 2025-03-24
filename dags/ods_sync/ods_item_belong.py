from airflow.decorators import dag, task


@dag(schedule=None,
# @dag(schedule_interval='*/30 * * * *', start_date=datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['ods', 'sync'])
def ods_item_belong():
    @task
    def fetch_data_from_feishu():
        from include.feishu.feishu_sheet import FeishuSheet
        from airflow.models import Variable
        raw_data = FeishuSheet(**Variable.get('feishu', deserialize_json=True)).fetch_dat(
            spreadsheet_token='NlrjsZjiEhEXantYY07cAIOcnth', sheet_id='QI8uTK'
        )
        return raw_data['valueRanges'][0]['values'][0:3]
    

    raw_data = fetch_data_from_feishu()

ods_item_belong()