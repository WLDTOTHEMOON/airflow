from airflow.decorators import dag, task
from airflow.models import Variable
import logging
import pendulum

logger = logging.getLogger(__name__)
MYSQL_KEYWORDS = ['group']
LEADER_OPEN_ID = Variable.get('leader_open_id')
SCHEMA = 'ods'
TABLE = 'ods_ks_leader_order'


# @dag(schedule=None,
@dag(schedule_interval='0 * * * *', start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['ods', 'sync'],
     max_active_tasks=3, max_active_runs=1)
def ods_leader_order():
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
        {',\n'.join([f'{each} = if(values(update_time) >= update_time or update_time is null, values({each}), {each})' for each in other_columns])}
        '''
        return sql

    @task
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
    
    @task
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

    @task
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
        raw_data = ks_client.get_leader_orders(access_token=tokens['access_token'], begin_time=begin_time, end_time=end_time)

        path = f'leader_order/{date_fmt}/{begin_time_fmt}_{end_time_fmt}.json'
        client.put_object(
            Bucket=cos_config['bucket'],
            Body=json.dumps(raw_data, indent=2),
            Key=path
        )
        return path

    @task
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
                'kwaimoney_user_nickname': each.get('kwaimoneyUserNickName'),
                'regimental_promotion_rate': each.get('regimentalPromotionRate'),
                'pay_time': timestamp2datetime(each.get('payTime')),
                'o_id': str(each.get('oid')),
                'expend_estimate_settle_amount': each.get('expendEstimateSettleAmount'),
                'promotion_id': each.get('promotionId'),
                'expend_regimental_settle_amount': each.get('expendRegimentalSettleAmount'),
                'activity_id': each.get('activityId'),
                'pay_amount': each.get('payAmount'),
                'order_trade_amount': each.get('orderTradeAmount'),
                'settlement_success_time': timestamp2datetime(each.get('settlementSuccessTime')),
                'expend_regimental_promotion_rate': each.get('expendRegimentalPromotionRate'),
                'activity_user_id': each.get('activityUserId'),
                'promotion_type': each.get('promotionType'),
                'recv_time': timestamp2datetime(each.get('recvTime')),
                'share_rate_str': each.get('shareRateStr'),
                'buyer_open_id': each.get('buyerOpenId'),
                'order_create_time': timestamp2datetime(each.get('orderCreateTime')),
                'promotion_nickname': each.get('promotionNickName'),
                'update_time': timestamp2datetime(each.get('updateTime')),
                'cps_order_status': each.get('cpsOrderStatus'),
                'base_amount': each.get('baseAmount'),
                'promotion_kwai_id': each.get('promotionKwaiId'),
                'send_time': timestamp2datetime(each.get('sendTime')),
                'create_time': timestamp2datetime(each.get('createTime')),
                'send_status': each.get('sendStatus'),
                'excitation_income': each.get('excitationInCome'),
                'kwaimoney_user_id': each.get('kwaimoneyUserId'),
                'settlement_amount': each.get('settlementAmount'),
                'item_id': each.get('cpsOrderProductView')[0].get('itemId') if each.get('cpsOrderProductView') else None,
                'item_title': each.get('cpsOrderProductView')[0].get('itemTitle') if each.get('cpsOrderProductView') else None,
                'item_price': each.get('cpsOrderProductView')[0].get('itemPrice') if each.get('cpsOrderProductView') else None,
                'item_url': each.get('cpsOrderProductView')[0].get('itemUrl') if each.get('cpsOrderProductView') else None,
                'seller_id': each.get('cpsOrderProductView')[0].get('sellerId') if each.get('cpsOrderProductView') else None,
                'seller_nickname': each.get('cpsOrderProductView')[0].get('sellerNickName') if each.get('cpsOrderProductView') else None,
                'sku_id': each.get('cpsOrderProductView')[0].get('skuId') if each.get('cpsOrderProductView') else None,
                'regimental_promotion_amount': each.get('regimentalPromotionAmount'),
                'fund_type': each.get('fundType'),
                'settlement_biz_type': each.get('settlementBizType'),
                'service_income': each.get('serviceIncome')
            }
            for each in raw_data
        ]
        
        sql = generate_upsert_template(schema=SCHEMA, table=TABLE)
        logger.info(sql)
        with engine.connect() as conn:
            conn.execute(text(sql), processed_data)
        logger.info(f'完成数据同步 {len(processed_data)} items')

    tokens = get_token()
    new_tokens = update_token(tokens)
    path = fetch_write_data(new_tokens)
    read_sync_data(path)

ods_leader_order()
