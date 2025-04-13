from airflow.decorators import dag, task
from airflow import Dataset
import logging
import pendulum

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

@dag(schedule='0 1 * * *', 
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=dag_default_args, tags=['ads', 'etl'], max_active_runs=1)
def ads_sale_target():
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    ads_sale_target = SQLExecuteQueryOperator(
        task_id='ads_sale_target',
        conn_id='mysql',
        sql='sql/ads_sale_target.sql',
        default_args = task_default_args
    )
    
    ads_sale_target

ads_sale_target()