from airflow.decorators import task, dag
from airflow import Dataset
import pendulum
import logging

logger = logging.getLogger(__name__)

ods_activity_dataset = Dataset('ods_activity_dataset')

@dag(schedule=[ods_activity_dataset], start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang yongchao'}, tags=['dwd', 'etl'])
def dwd_ks_activity_info():
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
    dwd_ks_activity_info = SQLExecuteQueryOperator(
        task_id='dwd_ks_activity_info',
        conn_id='mysql',
        sql='sql/dwd_ks_activity_info.sql',
    )
    
    dwd_ks_activity_info


dwd_ks_activity_info()