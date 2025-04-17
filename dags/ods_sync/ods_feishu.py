from airflow.decorators import dag, task
from airflow import Dataset
from include.service.message import task_failure_callback
import logging
import pendulum

logger = logging.getLogger(__name__)
MYSQL_KEYWORDS = ['group']
default_args = {
    'owner': 'Fang Yongchao',
    'on_failure_callback': task_failure_callback,
    'retries': 10,
    'retry_delay': pendulum.duration(seconds=10)
}


def excel_time_convert(timestamp):
    import pandas as pd
    import xlrd
    if pd.isna(timestamp):
        return None
    else:
        return xlrd.xldate_as_datetime(timestamp, 0)

@dag(schedule_interval='0 */2 * * *', start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['ods', 'feishu', 'src'])
def src_fs_start():
    @task(outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/src/feishu_start')])
    def src_fs_start():
        pass
    src_fs_start()
src_fs_start()


@dag(schedule=Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/src/feishu_start'),
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['ods', 'feishu'])
def ods_fs_gmv_target():
    @task(outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/ods/ods_fs_gmv_target')])
    def ods_fs_gmv_target(**kwargs):
        from include.feishu.feishu_sheet import FeishuSheet
        from airflow.models import Variable
        from include.database.mysql import engine
        from sqlalchemy import text
        import pandas as pd
        raw_data = FeishuSheet(**Variable.get('feishu', deserialize_json=True)).fetch_dat(
            spreadsheet_token='L6WqsavOphqKcDt4l26cZpVGnBi', sheet_id='d7f9ae'
        )
        raw_data = raw_data['valueRanges'][0]['values']
        raw_data = pd.DataFrame(raw_data[1:], columns=['month', 'anchor', 'target', 'target_final'])
        raw_data['update_at'] = kwargs['ts']
        raw_data = raw_data.fillna(0)
        with engine.connect() as conn:
            conn.execute(text('delete from ods.ods_fs_gmv_target'))
        raw_data.to_sql('ods_fs_gmv_target', engine, if_exists='append', index=False, schema='ods')
        logger.info(f'数据更新完成 {len(raw_data)} items')
    ods_fs_gmv_target()
ods_fs_gmv_target()

@dag(schedule=Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/ods/ods_fs_gmv_target'),
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['ods', 'feishu'])
def ods_fs_links():
    @task(outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/ods/ods_fs_links')])
    def ods_fs_links(**kwargs):
        from include.feishu.feishu_sheet import FeishuSheet
        from airflow.models import Variable
        from sqlalchemy import text
        from include.database.mysql import engine
        import pandas as pd
        raw_data = FeishuSheet(**Variable.get('feishu', deserialize_json=True)).fetch_dat(
            spreadsheet_token='NlrjsZjiEhEXantYY07cAIOcnth', sheet_id='QI8uTK'
        )
        raw_data = raw_data['valueRanges'][0]['values']
        raw_data = pd.DataFrame(raw_data[1:], columns=raw_data[0])
        raw_data = raw_data.rename(columns={
            '卖货日期': 'order_date', '对应商务': 'bd_name', '商品ID': 'item_id', '商品名称': 'item_title'
        })
        raw_data = raw_data[['order_date', 'item_id', 'item_title', 'bd_name']]
        raw_data['update_at'] = kwargs['ts']

        su = FeishuSheet(**Variable.get('feishu', deserialize_json=True)).fetch_dat(
            spreadsheet_token='AG8usnExOhgNQmtlmIOcwIUinNd', sheet_id='70e53e'
        )
        su = su['valueRanges'][0]['values']
        su = pd.DataFrame(su[1:], columns=su[0])
        su = su.rename(columns={'#商品ID': 'item_id'})
        su['item_id'] = su['item_id'].apply(lambda x: x[1:])

        raw_data.loc[(raw_data.order_date >= '2023-11-01') & (raw_data.order_date <= '2023-11-30') & (raw_data.bd_name == '苏志雄'), 'bd_name'] = None
        raw_data.loc[(raw_data.order_date >= '2023-11-01') & (raw_data.order_date <= '2023-11-30') & (raw_data.item_id.isin(su.item_id)), 'bd_name'] = '苏志雄'

        raw_data = raw_data.drop_duplicates(['order_date', 'item_id'])

        with engine.connect() as conn:
            conn.execute(text('delete from ods.ods_fs_links'))
        raw_data.to_sql('ods_fs_links', engine, if_exists='append', index=False, schema='ods')
        logger.info(f'数据更新完成 {len(raw_data)} items')
    ods_fs_links()
ods_fs_links()


@dag(schedule=Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/ods/ods_fs_links'),
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['ods', 'feishu'])
def ods_fs_slice_account():
    @task(outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/ods/ods_fs_slice_account')])
    def ods_fs_slice_account(**kwargs):
        from include.feishu.feishu_sheet import FeishuSheet
        from airflow.models import Variable
        from sqlalchemy import text
        from include.database.mysql import engine
        import pandas as pd
        raw_data = FeishuSheet(**Variable.get('feishu', deserialize_json=True)).fetch_dat(
            spreadsheet_token='QolKsWZiMhhGhxtxkOycIXJ4nOg', sheet_id='c226e2'
        )
        raw_data = raw_data['valueRanges'][0]['values']
        raw_data = pd.DataFrame(raw_data[1:], columns=raw_data[0])
        raw_data = raw_data.rename(columns={
            '账号ID': 'anchor_id', '开始时间': 'start_time', '结束时间': 'end_time'
        })
        raw_data['start_time'] = raw_data.start_time.apply(excel_time_convert)
        raw_data['end_time'] = raw_data.end_time.apply(excel_time_convert)
        raw_data['update_at'] = kwargs['ts']

        with engine.connect() as conn:
            conn.execute(text('delete from ods.ods_fs_slice_account'), conn)
        raw_data.to_sql('ods_fs_slice_account', engine, if_exists='append', index=False, schema='ods')
        logger.info(f'数据更新完成 {len(raw_data)} items')
    ods_fs_slice_account()
ods_fs_slice_account()


@dag(schedule=Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/ods/ods_fs_slice_account'),
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['ods', 'feishu', 'src'])
def src_fs_finish():
    @task(outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/src/feishu_finish')])
    def src_fs_finish():
        pass
    src_fs_finish()
src_fs_finish()
