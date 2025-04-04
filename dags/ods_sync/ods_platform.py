from airflow.decorators import dag, task
from airflow import Dataset
import logging
import pendulum

logger = logging.getLogger(__name__)
MYSQL_KEYWORDS = ['group']
ods_platform_dataset = Dataset('ods_platform_dataset')


@dag(schedule_interval='0 */2 * * *', start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['ods', 'sync', 'platform'],
     max_active_tasks=4, max_active_runs=1)
def ods_platform():
    def generate_upsert_template(schema, table):
        import pandas as pd
        from include.database.mysql import engine
        primary_key = pd.read_sql(
            f"select column_name from information_schema.columns where table_schema = '{schema}' and table_name = '{table}' and column_key = 'PRI'", engine
        )
        primary_key = primary_key['COLUMN_NAME'].to_list()
        other_columns = pd.read_sql(
            f"select column_name from information_schema.columns where table_schema = '{schema}' and table_name = '{table}' and column_key != 'PRI'", engine
        )
        other_columns = other_columns['COLUMN_NAME'].to_list()

        
        primary_key = [f'{each}_s' if each in MYSQL_KEYWORDS else each for each in primary_key]
        other_columns = [f'`{each}`' if each in MYSQL_KEYWORDS else each for each in other_columns]

        sql = f'''
        insert into {schema}.{table} ({','.join(primary_key + other_columns)})
        values ({','.join([f':{each}' for each in primary_key + other_columns])})
        on duplicate key update
        {',\n'.join([f'{each} = values({each})' for each in other_columns])}
        '''
        return sql

    def fetch_from_source(table, start_time, end_time, key_word='updated_at'):
        import pandas as pd
        from include.database.mysql import engine
        file_name = table + '-' + start_time + '-' + end_time + '.csv'
        data = pd.read_sql(f"select * from {table} where {key_word} between '{start_time}' and '{end_time}'", engine)
        data.to_csv(file_name, index=False)
        logger.info(f'完成数据 {file_name} 获取, {len(data)} items')
        return file_name

    def write_to_cos(path, file_name):
        from qcloud_cos import CosConfig, CosS3Client
        from airflow.models import Variable
        import os
        cos_config = Variable.get('cos', deserialize_json=True)
        client=CosS3Client(CosConfig(SecretId=cos_config['secret_id'], SecretKey=cos_config['secret_key'], Region=cos_config['region']))
        path = path + file_name
        client.upload_file(
            Bucket=cos_config['bucket'],
            Key=path,
            LocalFilePath=file_name,
        )
        os.remove(file_name)
        logger.info(f'完成数据写入 {path} ')
        return path
    
    def read_and_sync(path, sql):
        from qcloud_cos import CosConfig, CosS3Client
        from airflow.models import Variable
        from io import BytesIO
        from include.database.mysql import engine
        from sqlalchemy import text
        import pandas as pd
        cos_config = Variable.get('cos', deserialize_json=True)
        client=CosS3Client(CosConfig(SecretId=cos_config['secret_id'], SecretKey=cos_config['secret_key'], Region=cos_config['region']))
        response = client.get_object(Bucket=cos_config['bucket'], Key=path)
        raw_data = response['Body'].get_raw_stream().read()
        data = pd.read_csv(BytesIO(raw_data), na_filter=None)
        data = data.rename(columns={each: f'{each}_s' for each in MYSQL_KEYWORDS})
        data = data.replace('', None)
        data = data.where(pd.notna(data), None).to_dict(orient='records')
        
        if len(data) > 0:
            with engine.connect() as conn:
                conn.execute(text(sql), data)
        logger.info(f'完成数据同步 {len(data)} items')
        return 1
    
    @task(retries=5, retry_delay=10)
    def ods_pf_links(**kwargs):
        begin_time = kwargs['data_interval_start']
        end_time = kwargs['data_interval_end']
        begin_time_fmt = begin_time.in_tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
        end_time_fmt = end_time.in_tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
        file_name = fetch_from_source(table='xlsd.link', start_time=begin_time_fmt, end_time=end_time_fmt)
        path = write_to_cos(file_name=file_name, path='platform/links/')
        sql = generate_upsert_template('ods', 'ods_pf_links')
        read_and_sync(path=path, sql=sql)

    @task(retries=5, retry_delay=10)
    def ods_pf_suppliers(**kwargs):
        begin_time = kwargs['data_interval_start']
        end_time = kwargs['data_interval_end']
        begin_time_fmt = begin_time.in_tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
        end_time_fmt = end_time.in_tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
        file_name = fetch_from_source(table='xlsd.suppliers', start_time=begin_time_fmt, end_time=end_time_fmt)
        path = write_to_cos(file_name=file_name, path='platform/suppliers/')
        sql = generate_upsert_template('ods', 'ods_pf_suppliers')
        read_and_sync(path=path, sql=sql)

    @task(retries=5, retry_delay=10)
    def ods_pf_products(**kwargs):
        begin_time = kwargs['data_interval_start']
        end_time = kwargs['data_interval_end']
        begin_time_fmt = begin_time.in_tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
        end_time_fmt = end_time.in_tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
        file_name = fetch_from_source(table='xlsd.products', start_time=begin_time_fmt, end_time=end_time_fmt)
        path = write_to_cos(file_name=file_name, path='platform/products/')
        sql = generate_upsert_template('ods', 'ods_pf_products')
        read_and_sync(path=path, sql=sql)

    @task(retries=5, retry_delay=10)
    def ods_pf_reviews(**kwargs):
        begin_time = kwargs['data_interval_start']
        end_time = kwargs['data_interval_end']
        begin_time_fmt = begin_time.in_tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
        end_time_fmt = end_time.in_tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
        file_name = fetch_from_source(table='xlsd.review', start_time=begin_time_fmt, end_time=end_time_fmt)
        path = write_to_cos(file_name=file_name, path='platform/reviews/')
        sql = generate_upsert_template('ods', 'ods_pf_reviews')
        read_and_sync(path=path, sql=sql)

    @task(retries=5, retry_delay=10)
    def ods_pf_anchor_select_products(**kwargs):
        begin_time = kwargs['data_interval_start']
        end_time = kwargs['data_interval_end']
        begin_time_fmt = begin_time.in_tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
        end_time_fmt = end_time.in_tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
        file_name = fetch_from_source(table='xlsd.anchor_select_product', start_time=begin_time_fmt, end_time=end_time_fmt)
        path = write_to_cos(file_name=file_name, path='platform/anchor_select_products/')
        sql = generate_upsert_template('ods', 'ods_pf_anchor_select_products')
        read_and_sync(path=path, sql=sql)

    @task(retries=5, retry_delay=10)
    def ods_pf_anchor_info(**kwargs):
        begin_time = kwargs['data_interval_start']
        end_time = kwargs['data_interval_end']
        begin_time_fmt = begin_time.in_tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
        end_time_fmt = end_time.in_tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
        file_name = fetch_from_source(table='xlsd.anchor_info', start_time=begin_time_fmt, end_time=end_time_fmt)
        path = write_to_cos(file_name=file_name, path='platform/anchor_info/')
        sql = generate_upsert_template('ods', 'ods_pf_anchor_info')
        read_and_sync(path=path, sql=sql)

    @task(retries=5, retry_delay=10)
    def ods_pf_account_info(**kwargs):
        begin_time = kwargs['data_interval_start']
        end_time = kwargs['data_interval_end']
        begin_time_fmt = begin_time.in_tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
        end_time_fmt = end_time.in_tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
        file_name = fetch_from_source(table='xlsd.account_info', start_time=begin_time_fmt, end_time=end_time_fmt, key_word='update_at')
        path = write_to_cos(file_name=file_name, path='platform/account_info/')
        sql = generate_upsert_template('ods', 'ods_pf_account_info')
        read_and_sync(path=path, sql=sql)

    @task(retries=5, retry_delay=10)
    def ods_pf_users(**kwargs):
        begin_time = kwargs['data_interval_start']
        end_time = kwargs['data_interval_end']
        begin_time_fmt = begin_time.in_tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
        end_time_fmt = end_time.in_tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
        file_name = fetch_from_source(table='xlsd.users', start_time=begin_time_fmt, end_time=end_time_fmt)
        path = write_to_cos(file_name=file_name, path='platform/users/')
        sql = generate_upsert_template('ods', 'ods_pf_users')
        read_and_sync(path=path, sql=sql)


    @task(retries=5, retry_delay=10)
    def ods_pf_handover(**kwargs):
        begin_time = kwargs['data_interval_start']
        end_time = kwargs['data_interval_end']
        begin_time_fmt = begin_time.in_tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
        end_time_fmt = end_time.in_tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
        file_name = fetch_from_source(table='xlsd.handover', start_time=begin_time_fmt, end_time=end_time_fmt)
        path = write_to_cos(file_name=file_name, path='platform/handover/')
        sql = generate_upsert_template('ods', 'ods_pf_handover')
        read_and_sync(path=path, sql=sql)



    @task(outlets=[ods_platform_dataset])
    def task_finished():
        logger.info(f'platform 相关数据ods更新完成')


    ods_pf_links() >> ods_pf_suppliers() >> ods_pf_users() >> \
    ods_pf_products() >> ods_pf_reviews() >> ods_pf_anchor_select_products() >> \
    ods_pf_anchor_info() >> ods_pf_account_info() >> ods_pf_handover() >> \
    task_finished()


ods_platform()