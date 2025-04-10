from airflow.decorators import dag, task
from airflow import Dataset
import logging
import pendulum

logger = logging.getLogger(__name__)
default_args = {
    'retries': 5, 
    'retry_delay': 10
}

@dag(schedule='0 1 * * * *', 
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['ads', 'etl'], max_active_runs=1)
def ads_sale_target_2hourly():
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    ads_sale_target_2hourly = SQLExecuteQueryOperator(
        task_id='ads_sale_target_2hourly',
        conn_id='mysql',
        sql='sql/ads_sale_target_2hourly.sql',
        default_args = default_args
    )
    
    ads_sale_target_2hourly

ads_sale_target_2hourly()