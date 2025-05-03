from airflow.decorators import dag, task
from airflow.models import Variable
import logging
import pendulum

logger = logging.getLogger(__name__)
SCHEMA = 'tmp'
TABLE = 'tmp_ks_cps_order0501'
default_args = {
    'owner': 'Fang Yongchao',
    # 'on_failure_callback': task_failure_callback,
    # 'retries': 10,
    # 'retry_delay': pendulum.duration(seconds=30)
}


@dag(schedule='* * * * *',
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['tmp', 'kuaishou'],
     max_active_tasks=3, max_active_runs=1)
def tmp_ks_cps_order0501():
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

        sql = f'''
        insert into {schema}.{table} ({','.join(primary_key + other_columns)})
        values ({','.join([f':{each}' for each in primary_key + other_columns])})
        on duplicate key update
        {',\n'.join([f'{each} = if(values(update_time) >= update_time or update_time is null, values({each}), {each})' for each in other_columns])}
        '''
        return sql

    @task(trigger_rule='all_done')
    def get_token():  
        from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
        sql = f'''
        select access_token, refresh_token, updated_at
        from xlsd.ks_token
        where open_id = 'f18d5ec29982990414bf5724d8cf2a06'
        '''
        tokens = SQLExecuteQueryOperator(
            task_id='tokens', 
            conn_id='mysql', 
            sql=sql, 
        ).execute({})
        return {
            'access_token': tokens[0][0],
            'refresh_token': tokens[0][1],
            'updated_at': tokens[0][2]
        }
    
    @task(trigger_rule='all_done')
    def fetch_write_data(tokens, **kwargs):
        from airflow.models import Variable
        from include.kuaishou.ks_client import KsClient
        
        end_time = kwargs['data_interval_end']
        begin_time = end_time.subtract(minutes=5)

        # begin_time_fmt = begin_time.in_tz('Asia/Shanghai').format('YYYYMMDDHHmmss')
        # end_time_fmt = end_time.in_tz('Asia/Shanghai').format('YYYYMMDDHHmmss')

        ks_client = KsClient(**Variable.get('kuaishou', deserialize_json=True))
        raw_data = ks_client.get_cps_orders(access_token=tokens['access_token'], begin_time=begin_time, end_time=end_time)
        
        return raw_data
    
    @task(trigger_rule='all_done')
    def read_sync_data(raw_data):
        from include.database.mysql import engine
        from sqlalchemy import text
        import json
        if raw_data:
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

    tokens = get_token()
    raw_data = fetch_write_data(tokens=tokens)
    read_sync_data(raw_data)

tmp_ks_cps_order0501()