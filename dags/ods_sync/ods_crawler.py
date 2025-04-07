from airflow.decorators import task, dag
from airflow import Dataset
import logging
import pendulum


logger = logging.getLogger(__name__)
MYSQL_KEYWORDS = ['group']
ods_crawler_dataset = Dataset('ods_crawler_dataset')


@dag(schedule_interval='0 */2 * * *', start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['ods', 'sync', 'crawler'],
     max_active_tasks=3, max_active_runs=1)
def ods_crawler():
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
        logger.info(f'更新flag 为 {flag}')
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
    
        current_time = pendulum.now('Asia/Shanghai').format('YYYY-MM-DD')
        path = path + current_time + suffix
        logger.info(f'获取数据 {path}')
        response = client.get_object(Bucket=cos_config['bucket'], Key=path)
        raw_data = response['Body'].get_raw_stream().read()
        if suffix == '.csv':
            raw_data = pd.read_csv(BytesIO(raw_data), na_filter=None)
        elif suffix == '.json':
            raw_data = json.loads(raw_data)
        elif suffix == '.xlsx':
            raw_data = pd.read_excel(BytesIO(raw_data))
        return raw_data

    def get_child_folder(path):
        from qcloud_cos import CosConfig, CosS3Client
        from airflow.models import Variable
        cos_config = Variable.get('cos', deserialize_json=True)
        client=CosS3Client(CosConfig(SecretId=cos_config['secret_id'], SecretKey=cos_config['secret_key'], Region=cos_config['region']))
        response = client.list_objects(Bucket=cos_config['bucket'], Prefix=path, Delimiter='/')
        return [each['Prefix'] for each in response['CommonPrefixes']]

    @task(retries=5, retry_delay=10, outlets=[Dataset('mysql://ods.ods_crawler_leader_commission_income')])
    def ods_crawler_leader_commission_income():
        from include.database.mysql import engine
        from sqlalchemy import text
        import pandas as pd
        path = 'leader_commission_income/'
        suffix = '.xlsx'
        flag = get_flag(path)
        if flag:
            logger.info(f'数据更新，开始同步')
            raw_data = read_data(path=path, suffix=suffix) 
            data = raw_data.rename(columns={
                '达人ID': 'anchor_id',
                '达人昵称': 'anchor_name',
                '创作者ID': 'author_id',
                '创作者昵称': 'author_name',
                '订单ID': 'o_id',
                '订单状态': 'order_status',
                '订单类型': 'order_type',
                '商品ID': 'item_id',
                '商品名称': 'item_title',
                '商品图片': 'item_picture',
                '商品价格': 'item_price',
                '赠品信息': 'gift_info',
                '下单时间': 'order_create_time',
                '商家ID': 'merchant_id',
                '推广者身份': 'spreader_identity',
                '订单金额': 'order_trade_amount',
                '付款金额': 'payment_amount',
                '货款基数': 'payment_amount_2',
                '推广佣金率': 'service_rate',
                '分佣比例': 'share_commission_rate',
                '出让比例': 'share_rate_str',
                '技术服务费': 'service_income',
                '预估收入': 'estimated_income',
                '结算金额(元)': 'service_amount',
                '结算时间': 'settlement_success_time',
                '发货时间': 'send_time',
                '是否发货': 'send_status',
                '收货时间': 'recv_time'
            })
            data = data.replace('', None)
            data = data.replace('-', None)
            data = data.where(pd.notna(data), None).to_dict(orient='records')
            sql = generate_upsert_template('ods', 'ods_crawler_leader_commission_income')
            if len(data) > 0:
                with engine.connect() as conn:
                    conn.execute(text(sql), data)
            logger.info(f'完成数据同步 {len(data)} items')
            update_flag(path=path, flag=0)
        else:
            logger.info(f'数据未更新，结束任务')
    
    @task(retries=5, retry_delay=10, outlets=[Dataset('mysql://ods.ods_crawler_recreation')])
    def ods_crawler_recreation():
        from include.database.mysql import engine
        from sqlalchemy import text
        import pandas as pd
        path = 'recreation/'
        suffix = '.xlsx'
        
        for each in get_child_folder(path=path):
            flag = get_flag(each)
            logger.info(f'处理目录 {each}')
            if flag:
                logger.info(f'数据更新，开始同步')
                raw_data = read_data(path=each, suffix=suffix)
                data = raw_data.rename(columns={
                    '下单时间': 'order_create_time',
                    '发货时间': 'send_time', 
                    '是否发货': 'send_status', 
                    '收货时间': 'recv_time', 
                    '订单编号': 'o_id', 
                    '商品ID': 'item_id', 
                    '商品名称': 'item_title', 
                    '赠品信息': 'gift_info', 
                    '订单状态': 'order_status',
                    '订单类型': 'order_type', 
                    '订单金额(元)': 'order_trade_amount', 
                    '佣金比率': 'service_rate', 
                    'MCN 机构金额': 'mcn_amount', 
                    '预估收入(元)': 'estimated_income', 
                    '计佣金额(元)': 'order_trade_amount_payment', 
                    '分成比例': 'proportion',
                    '技术服务费(元)': 'service_income', 
                    '出让比率': 'share_rate_str', 
                    '出让金额': 'share_rate_amount', 
                    '推广位': 'service_position', 
                    '商家ID': 'merchant_id',
                    '商家昵称': 'merchant_name',
                    '付款时间': 'pay_time',
                    '结算时间': 'settlement_success_time',
                    '结算金额(元)': 'settlement_amount', 
                    '商品数量': 'number'
                })
                data = data.astype(object)
                data = data.replace('', None)
                data = data.replace('-', None)
                data['account_id'] = each.split('/')[1]
                data = data.where(pd.notna(data), None).to_dict(orient='records')
                sql = generate_upsert_template('ods', 'ods_crawler_recreation')
                if len(data) > 0:
                    with engine.connect() as conn:
                        conn.execute(text(sql), data)
                logger.info(f'完成数据同步 {len(data)} items')
                update_flag(path=each, flag=0)
            else:
                logger.info(f'数据未更新，结束任务')

    @task(retries=5, retry_delay=10, outlets=[Dataset('mysql://ods.ods_crawler_mcn_order')])
    def ods_crawler_mcn_order():
        from include.database.mysql import engine
        from sqlalchemy import text
        import pandas as pd
        path = 'mcn_order/'
        suffix = '.json'
        flag = get_flag(path)
        if flag:
            logger.info(f'数据更新，开始同步')
            raw_data = read_data(path=path, suffix=suffix)
            sql = generate_upsert_template('ods', 'ods_crawler_mcn_order')
            if len(raw_data) > 0:
                with engine.connect() as conn:
                    conn.execute(text(sql), raw_data)
            logger.info(f'完成数据同步 {len(raw_data)} items')
            update_flag(path=path, flag=0)
        else:
            logger.info(f'数据未更新，结束任务')

    ods_crawler_leader_commission_income() 
    ods_crawler_recreation()
    ods_crawler_mcn_order()

ods_crawler()