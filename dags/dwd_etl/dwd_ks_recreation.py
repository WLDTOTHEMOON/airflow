from airflow.decorators import dag, task
from airflow import Dataset
import logging
import pendulum

logger = logging.getLogger(__name__)
ods_crawler_dataset = Dataset('ods_crawler_dataset')


@dag(schedule=[ods_crawler_dataset], start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['dwd', 'etl'],
     max_active_runs=1)
def dwd_ks_recreation():
    from airflow.models import Variable
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    begin_time = Variable.get('ods_ks_cps_order_begin_time')
    end_time = Variable.get('ods_ks_cps_order_end_time')
    
    dwd_ks_recreation = SQLExecuteQueryOperator(
        task_id='dwd_ks_recreation',
        conn_id='mysql',
        sql='sql/dwd_ks_recreation.sql',
        parameters={'begin_time': begin_time, 'end_time': end_time}
    )
    
    dwd_ks_recreation

dwd_ks_recreation()