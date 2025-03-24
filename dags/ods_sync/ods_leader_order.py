from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from include.utils.utils import timestamp2datetime
from include.kuaishou.ks_client import KsClient
from include.service.sync import write_to_mysql
from include.models.ods_leader_order import OdsLeaderOrder
from include.database.mysql import engine, db_session
from qcloud_cos import CosConfig, CosS3Client
import pendulum
import logging
import json

logger = logging.getLogger(__name__)


LEADER_OPEN_ID = Variable.get('leader_open_id')
ks_client = KsClient(**Variable.get('kuaishou', deserialize_json=True))
cos_config = Variable.get('cos', deserialize_json=True)
client=CosS3Client(CosConfig(
    SecretId=cos_config['secret_id'], SecretKey=cos_config['secret_key'], Region=cos_config['region']
))


# @dag(schedule=None,
@dag(schedule_interval='*/10 * * * *', start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['ods', 'sync'])
def ods_leader_order():
    @task
    def get_token():  
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
        current_time = pendulum.now('Asia/Shanghai').naive()
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
        begin_time = kwargs['data_interval_start']
        end_time = kwargs['data_interval_end']
        raw_data = ks_client.get_leader_orders(access_token=tokens['access_token'], begin_time=begin_time, end_time=end_time)
        
        date_fmt = begin_time.in_tz('Asia/Shanghai').format('YYYYMMDD')
        begin_time_fmt = begin_time.in_tz('Asia/Shanghai').format('YYYYMMDDHHmmss')
        end_time_fmt = end_time.in_tz('Asia/Shanghai').format('YYYYMMDDHHmmss')

        path = f'leader_order/{date_fmt}/{begin_time_fmt}_{end_time_fmt}.json'
        client.put_object(
            Bucket=cos_config['bucket'],
            Body=json.dumps(raw_data, indent=2),
            Key=path
        )
        return path

    @task
    def read_sync_data(path):
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
        
        write_to_mysql(data=processed_data, table=OdsLeaderOrder, session=db_session, type='increment')


    tokens = get_token()
    new_tokens = update_token(tokens)
    path = fetch_write_data(new_tokens)
    read_sync_data(path)



ods_leader_order()
