from airflow.decorators import task, dag
from airflow import Dataset
import pendulum
import logging

logger = logging.getLogger(__name__)
from include.service.message import task_failure_callback
dag_default_args = {
    'owner': 'Fang Yongchao',
    'on_failure_callback': task_failure_callback
}
task_default_args = {
    'retries': 5, 
    'retry_delay': 10
}

@dag(schedule=[Dataset('mysql://ods.ods_ks_activity_item_list')],
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=dag_default_args, tags=['dim', 'etl'], max_active_runs=1)
def dim_ks_activity_info():
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
    dim_ks_activity_info = SQLExecuteQueryOperator(
        task_id='dim_ks_activity_info',
        conn_id='mysql',
        sql='sql/dim_ks_activity_info.sql',
        default_args=task_default_args
    )
    
    dim_ks_activity_info


dim_ks_activity_info()