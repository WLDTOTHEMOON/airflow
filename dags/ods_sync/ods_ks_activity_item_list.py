from airflow.decorators import dag, task
from airflow.models import Variable
from airflow import Dataset
from include.service.message import task_failure_callback
import logging
import pendulum

logger = logging.getLogger(__name__)
MYSQL_KEYWORDS = ['group']
LEADER_OPEN_ID = Variable.get('leader_open_id')
SCHEMA = 'ods'
TABLE = 'ods_ks_activity_item_list'
default_args = {
    'owner': 'Fang Yongchao',
    'on_failure_callback': task_failure_callback,
    'retries': 10,
    'retry_delay': 10
}

@dag(schedule_interval=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/ods/ods_ks_activity_info')],
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['ods', 'kuaishou'], max_active_tasks=3, max_active_runs=1)
def ods_ks_activity_item_list():
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

    @task()
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
    
    @task()
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

    @task()
    def get_activity_list(**kwargs):
        from include.database.mysql import engine
        import pandas as pd
        begin_time = kwargs['data_interval_start']
        begin_time_fmt = begin_time.in_tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
        if begin_time.in_tz('Asia/Shanghai').hour == 2:
            sql = f'''
            select activity_id
            from ods.ods_ks_activity_info
            where activity_status != 4
                or date(activity_end_time) >= '{begin_time_fmt}'
            order by activity_id desc
            '''
            acticity_id_list = pd.read_sql(sql, engine).activity_id.to_list()
            return acticity_id_list
        else:
            return []

    @task(trigger_rule='all_done')
    def fetch_write_data(tokens, activity_id, **kwargs):
        from qcloud_cos import CosConfig, CosS3Client
        from airflow.models import Variable
        from include.kuaishou.ks_client import KsClient
        import json
        logger.info(f'获取数据 activity_id: {activity_id} ')
        begin_time = kwargs['data_interval_start']
        end_time = kwargs['data_interval_end']
        date_fmt = begin_time.in_tz('Asia/Shanghai').format('YYYYMMDD')
        begin_time_fmt = begin_time.in_tz('Asia/Shanghai').format('YYYYMMDDHHmmss')
        end_time_fmt = end_time.in_tz('Asia/Shanghai').format('YYYYMMDDHHmmss')

        ks_client = KsClient(**Variable.get('kuaishou', deserialize_json=True))
        cos_config = Variable.get('cos', deserialize_json=True)
        client=CosS3Client(CosConfig(SecretId=cos_config['secret_id'], SecretKey=cos_config['secret_key'], Region=cos_config['region']))
        raw_data = ks_client.get_activity_item_list(access_token=tokens['access_token'], activity_id=activity_id)

        path = f'activity_item_list/{date_fmt}/{activity_id}_{begin_time_fmt}_{end_time_fmt}.json'
        client.put_object(
            Bucket=cos_config['bucket'],
            Body=json.dumps(raw_data, indent=2),
            Key=path
        )
        return {
            'flag': True if raw_data else False,
            'path': path
        }

    @task(trigger_rule='all_done')
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

        if path['flag']:
            raw_data = client.get_object(Bucket=cos_config['bucket'], Key=path['path'])
            raw_data = raw_data['Body'].get_raw_stream().read()
            raw_data = json.loads(raw_data)

            processed_data = [
                {
                    'item_title': each.get('itemTitle'),
                    'reason': each.get('reason'),
                    'item_volume': each.get('itemVolume'),
                    'base_order_status': each.get('baseOrderStatus'),
                    'item_audit_status': each.get('itemAuditStatus'),
                    'base_order_amount': each.get('baseOrderAmount'),
                    'cooperate_begin_time': timestamp2datetime(each.get('cooperateBeginTime')),
                    'activity_id': each.get('activityId'),
                    'seller_id': each.get('sellerId'),
                    'disabled_msg': each.get('disabledMsg'),
                    'identity': each.get('identity'),
                    'item_status': each.get('itemStatus'),
                    'seller_nick_name': each.get('sellerNickName'),
                    'disabled': each.get('disabled'),
                    'pre_activity_id': each.get('preActivityId'),
                    'shop_id': each.get('shopId'),
                    'item_img_url': each.get('itemImgUrl'),
                    'cooperate_end_time': timestamp2datetime(each.get('cooperateEndTime')),
                    'investment_promotion_rate': each.get('investmentPromotionRate'),
                    'investment_promotion_remise_rate': each.get('investmentPromotionRemiseRate'),
                    'item_category_name': each.get('itemCategoryName'),
                    'item_commission_rate': each.get('itemCommissionRate'),
                    'item_id': each.get('itemId'),
                    'item_leaf_category_id': each.get('itemLeafCategoryId'),
                    'item_stock': each.get('itemStock'),
                    'item_price': each.get('itemPrice'),
                    'contact_user_type': ','.join(map(str, each.get('contactUserType', []))),
                    'item_apply_time': timestamp2datetime(each.get('itemApplyTime')),
                    'contact': ','.join(map(str, each.get('contact', [])))
                } for each in raw_data
            ]

            sql = generate_upsert_template(schema=SCHEMA, table=TABLE)
            logger.info(sql)
            with engine.connect() as conn:
                conn.execute(text(sql), processed_data)
            logger.info(f'完成数据同步 {len(processed_data)} items')
            return len(processed_data)
        else:
            logger.info('数据为空，跳过同步')
            return 0

    @task(trigger_rule='all_done', outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/ods/ods_ks_activity_item_list')])
    def summary(num):
        if num:
            total = sum(num)
        else:
            total = 0
        logger.info(f'完成数据同步 {total} items')

    tokens = get_token()
    new_tokens = update_token(tokens=tokens)
    activity_id_list = get_activity_list()
    path = fetch_write_data.partial(tokens=new_tokens).expand(activity_id=activity_id_list)
    num = read_sync_data.expand(path=path)
    summary(num)

ods_ks_activity_item_list()