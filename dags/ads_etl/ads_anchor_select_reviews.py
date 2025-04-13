from airflow.decorators import dag, task
from airflow import Dataset
from include.service.message import task_failure_callback
import logging
import pendulum

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'Fang Yongchao',
    'on_failure_callback': task_failure_callback,
    'retries': 5, 
    'retry_delay': 10
}


@dag(schedule='0 1 * * *', 
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['ads', 'etl'], max_active_runs=1)
def ads_anchor_select_reviews():
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    ads_anchor_select_reviews = SQLExecuteQueryOperator(
        task_id='ads_anchor_select_reviews',
        conn_id='mysql',
        sql='sql/ads_anchor_select_reviews.sql'
    )
    
    ads_anchor_select_reviews

ads_anchor_select_reviews()