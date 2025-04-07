from airflow.decorators import dag, task
from airflow.models import Variable
from airflow import Dataset
import logging
import pendulum

logger = logging.getLogger(__name__)
MYSQL_KEYWORDS = ['group']
LEADER_OPEN_ID = Variable.get('leader_open_id')
SCHEMA = 'ods'
TABLE = 'ods_ks_activity_info'

@dag(schedule_interval='0 */2 * * *', start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['ods', 'sync', 'kuaishou'],
     max_active_tasks=3, max_active_runs=1)
def ods_ks_activity_info():
    def timestamp2datetime(timestamp):
        try:
            timestamp = int(timestamp)
            if timestamp == 0:
                return None
            else:
                return pendulum.from_timestamp(timestamp / 1000, tz='Asia/Shanghai')
        except BaseException as e:
            return None

    def generate_upsert_template(schema=SCHEMA, table=TABLE):
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

    @task(retries=5, retry_delay=10)
    def get_token():
        from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
        sql = f'''
        select access_token, refresh_token, updated_at
        from xlsd.ks_token
        where open_id = %(leader_open_id)s
        '''
        tokens = SQLExecuteQueryOperator(
            task_id='tokens', 
            conn_id='mysql', 
            sql=sql, 
            parameters={'leader_open_id': LEADER_OPEN_ID}
        ).execute({})
        return {
            'access_token': tokens[0][0],
            'refresh_token': tokens[0][1],
            'updated_at': tokens[0][2]
        }
    
    @task(retries=5, retry_delay=10)
    def update_token(tokens):
        from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
        from airflow.models import Variable
        from include.kuaishou.ks_client import KsClient
        ks_client = KsClient(**Variable.get('kuaishou', deserialize_json=True))
        current_time = pendulum.now('Asia/Shanghai').naive()
        LEADER_OPEN_ID = Variable.get('leader_open_id')
        if (current_time - tokens['updated_at']).total_seconds() / 3600 < 999:
            logger.info('TOKEN有效期内，不需要更新')
            return tokens
        else:
            new_tokens = ks_client.get_access_token(refresh_token=tokens['refresh_token'])
            new_tokens['updated_at'] = current_time
            sql = f'''
            update xlsd.ks_token
            set
                refresh_token = %(refresh_token)s,
                access_token = %(access_token)s,
                updated_at = %(updated_at)s
            where open_id = %(leader_open_id)s
            '''
            update_tokens = SQLExecuteQueryOperator(
                task_id='update_tokens',
                conn_id='mysql',
                sql=sql,
                parameters={
                    'refresh_token': new_tokens['refresh_token'],
                    'access_token': new_tokens['access_token'],
                    'updated_at': new_tokens['updated_at'],
                    'leader_open_id': LEADER_OPEN_ID
                }
            ).execute({})
            return new_tokens

    @task(retries=5, retry_delay=10)
    def fetch_write_data(tokens, **kwargs):
        from qcloud_cos import CosConfig, CosS3Client
        from airflow.models import Variable
        from include.kuaishou.ks_client import KsClient
        import json

        begin_time = kwargs['data_interval_start']
        end_time = kwargs['data_interval_end']
        date_fmt = begin_time.in_tz('Asia/Shanghai').format('YYYYMMDD')
        begin_time_fmt = begin_time.in_tz('Asia/Shanghai').format('YYYYMMDDHHmmss')
        end_time_fmt = end_time.in_tz('Asia/Shanghai').format('YYYYMMDDHHmmss')

        ks_client = KsClient(**Variable.get('kuaishou', deserialize_json=True))
        cos_config = Variable.get('cos', deserialize_json=True)
        client=CosS3Client(CosConfig(SecretId=cos_config['secret_id'], SecretKey=cos_config['secret_key'], Region=cos_config['region']))
        raw_data = ks_client.get_activity_info(access_token=tokens['access_token'], limit=100)

        path = f'activity_info/{date_fmt}/{begin_time_fmt}_{end_time_fmt}.json'
        client.put_object(
            Bucket=cos_config['bucket'],
            Body=json.dumps(raw_data, indent=2),
            Key=path
        )
        return path

    @task(retries=5, retry_delay=10, outlets=[Dataset('mysql://ods.ods_ks_activity_info')])
    def read_sync_data(path):
        from qcloud_cos import CosConfig, CosS3Client
        from airflow.models import Variable
        from include.database.mysql import engine
        from sqlalchemy import text
        import json

        cos_config = Variable.get('cos', deserialize_json=True)
        client=CosS3Client(CosConfig(
            SecretId=cos_config['secret_id'], SecretKey=cos_config['secret_key'], Region=cos_config['region']
        ))
        raw_data = client.get_object(Bucket=cos_config['bucket'], Key=path)
        raw_data = raw_data['Body'].get_raw_stream().read()
        raw_data = json.loads(raw_data)

        processed_data = [
            {
                'activity_id': each.get('activityId'),
                'activity_user_id': each.get('activityUserId'),
                'activity_user_nick_name': each.get('activityUserNickName'),
                'activity_title': each.get('activityTitle'),
                'activity_type': each.get('activityType'),
                'activity_begin_time': timestamp2datetime(each.get('activityBeginTime')),
                'activity_end_time': timestamp2datetime(each.get('activityEndTime')),
                'activity_status': each.get('activityStatus'),
                'apply_item_count': each.get('activityItemDataView', {}).get('applyItemCount'),
                'wait_audit_item_count': each.get('activityItemDataView', {}).get('waitAuditItemCount'),
                'audit_pass_item_count': each.get('activityItemDataView', {}).get('auditPassItemCount'),
                'min_investment_promotion_rate': each.get('minInvestmentPromotionRate'),
                'min_item_commission_rate': each.get('minItemCommissionRate'),
                'contact': ','.join(map(str, each.get('contact'))),
                'pre_exclusive_activity_sign_type': each.get('preExclusiveActivitySignType'),
                'seller_apply_url': each.get('sellerApplyUrl'),
                'leader_apply_url': each.get('leaderApplyUrl'),
                'exists_activity_id': each.get('existsActivityId'),
                'promoter_id': ','.join(map(str, each.get('promoterId', []))),
                'tips': each.get('tips'),
                'base_order_amount': each.get('baseOrderAmount'),
                'base_order_status': each.get('baseOrderStatus'),
                'cooperate_begin_time': timestamp2datetime(each.get('cooperateBeginTime')),
                'cooperate_end_time': timestamp2datetime(each.get('cooperateEndTime'))
            } for each in raw_data
        ]

        sql = generate_upsert_template(schema=SCHEMA, table=TABLE)
        logger.info(sql)
        with engine.connect() as conn:
            conn.execute(text(sql), processed_data)
        logger.info(f'完成数据同步 {len(processed_data)} items')

    tokens = get_token()
    new_tokens = update_token(tokens=tokens)
    path = fetch_write_data(tokens=new_tokens)
    read_sync_data(path)

    

ods_ks_activity_info()