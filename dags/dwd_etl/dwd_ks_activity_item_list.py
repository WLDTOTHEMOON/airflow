from airflow.decorators import task, dag
from airflow import Dataset
import pendulum
import logging

logger = logging.getLogger(__name__)
default_args = {
    'retries': 5, 
    'retry_delay': 10
}


@dag(schedule=[Dataset('mysql://ods.ods_ks_activity_item_list')],
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang yongchao'}, tags=['dwd', 'etl'], max_active_runs=1)
def dwd_ks_activity_item_list():
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
    dwd_ks_activity_item_list = SQLExecuteQueryOperator(
        task_id='dwd_ks_activity_item_list',
        conn_id='mysql',
        sql='sql/dwd_ks_activity_item_list.sql',
        default_args=default_args
    )
    
    dwd_ks_activity_item_list

dwd_ks_activity_item_list()