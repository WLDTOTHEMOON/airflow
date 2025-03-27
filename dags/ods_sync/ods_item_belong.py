from airflow.decorators import dag, task


@dag(schedule=None,
# @dag(schedule_interval='*/30 * * * *', start_date=datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['ods', 'sync'])
def ods_item_belong():
    @task
    def fetch_process_sync_data(**kwargs):
        from include.feishu.feishu_sheet import FeishuSheet
        from include.service.sync import write_to_mysql
        from airflow.models import Variable
        from include.models.ods_item_belong import OdsItemBelong
        from include.database.mysql import engine, db_session
        import pandas as pd
        raw_data = FeishuSheet(**Variable.get('feishu', deserialize_json=True)).fetch_dat(
            spreadsheet_token='NlrjsZjiEhEXantYY07cAIOcnth', sheet_id='QI8uTK'
        )
        raw_data = raw_data['valueRanges'][0]['values']
        raw_data_df = pd.DataFrame(raw_data[1:], columns=raw_data[0])
        raw_data_df = raw_data_df.rename(columns={
            '卖货日期': 'order_date', '对应商务': 'bd_name', '商品名称': 'item_title', '商品ID': 'item_id'
        })
        raw_data_df = raw_data_df[['order_date', 'item_id', 'item_title', 'bd_name']]
        raw_data_df['bd_name_vice'] = None

        su = FeishuSheet(**Variable.get('feishu', deserialize_json=True)).fetch_dat(
            spreadsheet_token='AG8usnExOhgNQmtlmIOcwIUinNd', sheet_id='70e53e'
        )
        su = su['valueRanges'][0]['values']
        su_df = pd.DataFrame(su[1:], columns=su[0])
        su_df = su_df.rename(columns={'#商品ID': 'item_id'})
        su_df['item_id'] = su_df['item_id'].apply(lambda x: x[1:])

        raw_data_df.loc[(raw_data_df.order_date >= '2023-11-01') & (raw_data_df.order_date <= '2023-11-30') & (raw_data_df.bd_name == '苏志雄'), 'bd_name'] = None
        raw_data_df.loc[(raw_data_df.order_date >= '2023-11-01') & (raw_data_df.order_date <= '2023-11-30') & (raw_data_df.item_id.isin(su_df.item_id)), 'bd_name'] = '苏志雄'

        processed_data = raw_data_df.to_dict(orient='records')

        write_to_mysql(data=processed_data, table=OdsItemBelong, session=db_session, type='full')

    fetch_process_sync_data()

ods_item_belong()