from airflow.decorators import task, dag
from airflow import Dataset
import logging
import pendulum

logger = logging.getLogger(__name__)
ods_crawler_dataset = Dataset('ods_crawler_dataset')


@dag(schedule=None,
     default_args={'owner': 'Fang Yongchao'}, tags=['ods', 'sync', 'crawler'])
def ods_crawler():
    def get_flag(path):
        from qcloud_cos import CosConfig, CosS3Client
        from airflow.models import Variable
        import json
        cos_config = Variable.get('cos', deserialize_json=True)
        client=CosS3Client(CosConfig(SecretId=cos_config['secret_id'], SecretKey=cos_config['secret_key'], Region=cos_config['region']))
        path = path + 'updated.json'
        raw_data = client.get_object(Bucket=cos_config['bucket'], Key=path)
        raw_data = raw_data['Body'].get_raw_stream().read()
        raw_data = json.loads(raw_data)
        return raw_data['updated']
    
    def update_flag(path, flag):
        from qcloud_cos import CosConfig, CosS3Client
        from airflow.models import Variable
        import json
        cos_config = Variable.get('cos', deserialize_json=True)
        client=CosS3Client(CosConfig(SecretId=cos_config['secret_id'], SecretKey=cos_config['secret_key'], Region=cos_config['region']))
        path = path + 'updated.json'
        client.put_object(
            Bucket=cos_config['bucket'],
            Body=json.dumps({'updated': flag}, indent=2),
            Key=path
        )
        return flag

    def read_data(path, suffix):
        from qcloud_cos import CosConfig, CosS3Client
        from airflow.models import Variable
        from include.database.mysql import engine
        from sqlalchemy import text
        from io import BytesIO
        import pandas as pd
        import json
        cos_config = Variable.get('cos', deserialize_json=True)
        client=CosS3Client(CosConfig(SecretId=cos_config['secret_id'], SecretKey=cos_config['secret_key'], Region=cos_config['region']))
        logger.info(f'获取数据 {path}')

        current_time = pendulum.now('Asia/Shanghai').format('YYYYMMDD')
        path = path + current_time + suffix
        response = client.get_object(Bucket=cos_config['bucket'], Key=path)
        raw_data = response['Body'].get_raw_stream().read()
        if suffix == '.csv':
            raw_data = pd.read_csv(BytesIO(raw_data), na_filter=None)
        elif suffix == '.json':
            raw_data = json.loads(raw_data)
        elif suffix == '.xlsx':
            raw_data = pd.read_excel(BytesIO(raw_data))
        return raw_data

    @task
    def ods_crawler_leader_commission_income():
        pass
    # 佣金收入订单
    
    @task
    def ods_crawler_recreation():
        pass
    # 二创

    @task
    def ods_crawler_mcn_order():
        pass
    # mcn

    @task(outlets=[ods_crawler_dataset])
    def task_finished():
        logger.info(f'platform 相关数据ods更新完成')

    ods_crawler_leader_commission_income() >> ods_crawler_recreation() >> ods_crawler_mcn_order() >> \
    task_finished()

ods_crawler()