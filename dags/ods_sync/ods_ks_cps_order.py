from airflow.decorators import dag, task
from airflow.models import Variable
from airflow import Dataset
import logging
import pendulum

logger = logging.getLogger(__name__)
MYSQL_KEYWORDS = ['group']
LEADER_OPEN_ID = Variable.get('leader_open_id')
SCHEMA = 'ods'
TABLE = 'ods_ks_cps_order'
ods_ks_cps_order_dataset = Dataset('ods_ks_cps_order_dataset')


@dag(schedule_interval='0 */2 * * *', start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['ods', 'sync', 'kuaishou'],
     max_active_tasks=3, max_active_runs=1)
def ods_ks_cps_order():
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
    def get_period(**kwargs):
        begin_time = kwargs['data_interval_start']
        end_time = kwargs['data_interval_end']
        
        if end_time.in_tz('Asia/Shanghai').hour in [22, 8]:  
            begin_time = begin_time.subtract(hours=20)
        else:
            begin_time = begin_time.subtract(hours=2)
        return {
            'begin_time': begin_time.in_tz('Asia/Shanghai'),
            'end_time': end_time.in_tz('Asia/Shanghai')
        }


    @task(retries=10, retry_delay=10)
    def get_open_id():
        from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
        sql = f'''
        select open_id
        from xlsd.account_info
        where status = 1
            and open_id != %(leader_open_id)s
        order by open_id desc
        '''
        open_ids = SQLExecuteQueryOperator(
            task_id='open_ids',
            conn_id='mysql',
            sql=sql,
            parameters={'leader_open_id': LEADER_OPEN_ID}
        ).execute({})
        return [each[0] for each in open_ids]

    @task(trigger_rule='all_done', retries=10, retry_delay=10)
    def get_token(open_id):  
        from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
        sql = f'''
        select access_token, refresh_token, updated_at
        from xlsd.ks_token
        where open_id = %(open_id)s
        '''
        tokens = SQLExecuteQueryOperator(
            task_id='tokens', 
            conn_id='mysql', 
            sql=sql, 
            parameters={'open_id': open_id}
        ).execute({})
        return {
            'open_id': open_id,
            'access_token': tokens[0][0],
            'refresh_token': tokens[0][1],
            'updated_at': tokens[0][2]
        }
    
    @task(trigger_rule='all_done', retries=10, retry_delay=10)
    def update_token(tokens):
        from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
        from airflow.models import Variable
        from include.kuaishou.ks_client import KsClient
        ks_client = KsClient(**Variable.get('kuaishou', deserialize_json=True))
        current_time = pendulum.now('Asia/Shanghai').naive()
        if (current_time - tokens['updated_at']).total_seconds() / 3600 < 999:
            logger.info('TOKEN有效期内，不需要更新')
            return tokens
        else:
            logger.info(f"更新TOKEN {tokens['open_id']}")
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
                    'open_id': new_tokens['open_id']
                }
            ).execute({})
            return new_tokens
    
    @task(trigger_rule='all_done', retries=10, retry_delay=10)
    def fetch_write_data(tokens, period):
        from qcloud_cos import CosConfig, CosS3Client
        from airflow.models import Variable
        from include.kuaishou.ks_client import KsClient
        import json
        
        begin_time = period['begin_time']
        end_time = period['end_time']

        date_fmt = begin_time.in_tz('Asia/Shanghai').format('YYYYMMDD')
        begin_time_fmt = begin_time.in_tz('Asia/Shanghai').format('YYYYMMDDHHmmss')
        end_time_fmt = end_time.in_tz('Asia/Shanghai').format('YYYYMMDDHHmmss')

        ks_client = KsClient(**Variable.get('kuaishou', deserialize_json=True))
        raw_data = ks_client.get_cps_orders(access_token=tokens['access_token'], begin_time=begin_time, end_time=end_time)
        
        path = f'cps_order/{date_fmt}/{tokens['open_id']}/{begin_time_fmt}_{end_time_fmt}.json'
        cos_config = Variable.get('cos', deserialize_json=True)
        client=CosS3Client(CosConfig(SecretId=cos_config['secret_id'], SecretKey=cos_config['secret_key'], Region=cos_config['region']))
        client.put_object(
            Bucket=cos_config['bucket'],
            Body=json.dumps(raw_data, indent=2),
            Key=path
        )
        return {
            'flag': True if raw_data else False,
            'path': path
        }
    
    @task(trigger_rule='all_done', retries=10, retry_delay=10)
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
        logger.info(f'获取数据 {path}')
        if path['flag']:
            raw_data = client.get_object(Bucket=cos_config['bucket'], Key=path['path'])
            raw_data = raw_data['Body'].get_raw_stream().read()
            raw_data = json.loads(raw_data)

            processed_data = [
                {
                    'update_time': timestamp2datetime(each.get('updateTime')),
                    'anchor_id': each.get('distributorId'),
                    'order_create_time': timestamp2datetime(each.get('orderCreateTime')),
                    'o_id': str(each.get('oid')),
                    'item_id': each.get('cpsOrderProductView')[0].get('itemId') if each.get('cpsOrderProductView') else None,
                    'item_title': each.get('cpsOrderProductView')[0].get('itemTitle') if each.get('cpsOrderProductView') else None,
                    'order_trade_amount': each.get('orderTradeAmount'),
                    'recv_time': timestamp2datetime(each.get('recvTime')),
                    # 'share_rate_str': each.get('shareRateStr'),
                    'share_rate_str': 850,
                    'buyer_open_id': each.get('buyerOpenId'),
                    'pay_time': timestamp2datetime(each.get('payTime')),
                    'cps_order_status': each.get('cpsOrderStatus'),
                    'base_amount': each.get('baseAmount'),
                    'send_time': timestamp2datetime(each.get('sendTime')),
                    'settlement_biz_type': each.get('settlementBizType'),
                    'create_time': timestamp2datetime(each.get('createTime')),
                    'settlement_success_time': timestamp2datetime(each.get('settlementSuccessTime')),
                    'send_status': each.get('sendStatus'),
                    'settlement_amount': each.get('settlementAmount'),
                    'service_income': each.get('cpsOrderProductView')[0].get('serviceInCome') if each.get('cpsOrderProductView') else None,
                    'commission_rate': each.get('cpsOrderProductView')[0].get('commissionRate') if each.get('cpsOrderProductView') else None,
                    'cps_type': each.get('cpsOrderProductView')[0].get('cpsType') if each.get('cpsOrderProductView') else None,
                    'num': each.get('cpsOrderProductView')[0].get('num') if each.get('cpsOrderProductView') else None,
                    'step_commission_amount': each.get('cpsOrderProductView')[0].get('stepCommissionAmount') if each.get('cpsOrderProductView') else None,
                    'cps_pid': each.get('cpsOrderProductView')[0].get('cpsPid') if each.get('cpsOrderProductView') else None,
                    'step_commission_rate': each.get('cpsOrderProductView')[0].get('stepCommissionRate') if each.get('cpsOrderProductView') else None,
                    'service_amount': each.get('cpsOrderProductView')[0].get('serviceAmount') if each.get('cpsOrderProductView') else None,
                    'seller_id': each.get('cpsOrderProductView')[0].get('sellerId') if each.get('cpsOrderProductView') else None,
                    'remise_commission_rate': each.get('cpsOrderProductView')[0].get('remiseCommissionRate') if each.get('cpsOrderProductView') else None,
                    'seller_nick_name': each.get('cpsOrderProductView')[0].get('sellerNickName') if each.get('cpsOrderProductView') else None,
                    'remise_commission_amount': each.get('cpsOrderProductView')[0].get('remiseCommissionAmount') if each.get('cpsOrderProductView') else None,
                    'item_price': each.get('cpsOrderProductView')[0].get('itemPrice') if each.get('cpsOrderProductView') else None,
                    'excitation_income': each.get('cpsOrderProductView')[0].get('excitationInCome') if each.get('cpsOrderProductView') else None,
                    'service_rate': each.get('cpsOrderProductView')[0].get('serviceRate') if each.get('cpsOrderProductView') else None,
                    'kwaimoney_user_id': each.get('cpsOrderProductView')[0].get('kwaimoneyUserId') if each.get('cpsOrderProductView') else None,
                    'estimated_income': each.get('cpsOrderProductView')[0].get('estimatedIncome') if each.get('cpsOrderProductView') else None,
                    'step_condition': each.get('cpsOrderProductView')[0].get('stepCondition') if each.get('cpsOrderProductView') else None
                }
                for each in raw_data
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

    @task(trigger_rule='all_done', outlets=[ods_ks_cps_order_dataset])
    def summary(num, period):
        from airflow.models import Variable
        total = sum(num)
        begin_time = period['begin_time'].in_tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
        end_time = period['end_time'].in_tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
        Variable.set('ods_ks_cps_order_begin_time', begin_time)
        Variable.set('ods_ks_cps_order_end_time', end_time)
        logger.info(f'写入数据范围 From {begin_time} to {end_time}')
        logger.info(f'完成数据同步 {total} items')

    period = get_period()
    open_ids = get_open_id()
    tokens = get_token.expand(open_id=open_ids)
    new_tokens = update_token.expand(tokens=tokens)
    path = fetch_write_data.partial(period=period).expand(tokens=new_tokens)
    num = read_sync_data.expand(path=path)
    summary(num, period)


ods_ks_cps_order()