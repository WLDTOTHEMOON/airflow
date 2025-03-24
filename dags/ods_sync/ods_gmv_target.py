from airflow.decorators import dag, task
from datetime import datetime


# @dag(schedule=None,
@dag(schedule_interval='*/30 * * * *', start_date=datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['ods', 'sync'])
def ods_gmv_target():
    @task
    def fetch_data_from_feishu():
        from include.feishu.feishu_sheet import FeishuSheet
        from airflow.models import Variable
        raw_data = FeishuSheet(**Variable.get('feishu', deserialize_json=True)).fetch_dat(
            spreadsheet_token='L6WqsavOphqKcDt4l26cZpVGnBi', sheet_id='d7f9ae'
        )
        return raw_data

    @task
    def process_data(raw_data, **kwargs):
        import pandas as pd
        raw_data = raw_data['valueRanges'][0]['values']
        raw_data = pd.DataFrame(raw_data[1:], columns=['month', 'anchor', 'target', 'target_final'])
        raw_data['update_at'] = kwargs['ts']
        raw_data = raw_data.fillna(0)
        return raw_data.to_dict(orient='records')

    @task
    def sync_data(processed_data):
        from include.service.sync import write_to_mysql
        from include.models.ods_gmv_target import OdsGmvTarget
        from include.database.mysql import db_session
        write_to_mysql(data=processed_data, table=OdsGmvTarget, session=db_session, type='full')
        

    raw_data = fetch_data_from_feishu()
    processed_data = process_data(raw_data)
    sync_data(processed_data)

ods_gmv_target()
    