from airflow.decorators import task, dag
from airflow import Dataset
from include.service.message import task_failure_callback
import pendulum
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'Fang Yongchao',
    'on_failure_callback': task_failure_callback,
    'retries': 5, 
    'retry_delay': pendulum.duration(seconds=10)
}


@dag(schedule=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/src/kuaishou_finish')],
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['dwd', 'etl'], max_active_runs=1)
def dwd_ks_activity_item_list():
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
    dwd_ks_activity_item_list = SQLExecuteQueryOperator(
        task_id='dwd_ks_activity_item_list',
        conn_id='mysql',
        sql='sql/dwd_ks_activity_item_list.sql',
        outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/dwd/dwd_ks_activity_item_list')]
    )
    
    dwd_ks_activity_item_list

dwd_ks_activity_item_list()