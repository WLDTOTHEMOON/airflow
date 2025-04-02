from airflow.decorators import task, dag
from airflow import Dataset
import pendulum
import logging

logger = logging.getLogger(__name__)

ods_activity_dataset = Dataset('ods_activity_dataset')

@dag(schedule=[ods_activity_dataset], start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang yongchao'}, tags=['dim', 'etl'])
def dim_ks_activity_info():
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
    dim_ks_activity_info = SQLExecuteQueryOperator(
        task_id='dim_ks_activity_info',
        conn_id='mysql',
        sql='sql/dim_ks_activity_info.sql',
    )
    
    dim_ks_activity_info


dim_ks_activity_info()