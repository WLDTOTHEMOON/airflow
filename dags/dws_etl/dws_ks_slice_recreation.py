from airflow.decorators import dag, task
from airflow import Dataset
import logging
import pendulum

logger = logging.getLogger(__name__)
default_args = {
    'retries': 5, 
    'retry_delay': 10
}

@dag(schedule=[Dataset('mysql://dwd.dwd_ks_leader_commission_income'), Dataset('mysql://dwd.dwd_ks_recreation'), Dataset('mysql://ods.ods_fs_slice_account')], 
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['dws', 'etl'], max_active_runs=1)
def dws_ks_slice_recreation():
    from airflow.models import Variable
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    order_create_time = Variable.get('ods_ks_cps_order_order_create_time')
    
    dws_ks_slice_recreation = SQLExecuteQueryOperator(
        task_id='dws_ks_slice_recreation',
        conn_id='mysql',
        sql='sql/dws_ks_slice_recreation.sql',
        default_args = default_args,
        parameters={'order_create_time': order_create_time}
    )
    
    dws_ks_slice_recreation

dws_ks_slice_recreation()