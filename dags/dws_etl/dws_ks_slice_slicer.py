from airflow.decorators import dag, task
from airflow import Dataset
import logging
import pendulum

logger = logging.getLogger(__name__)
default_args = {
    'retries': 5, 
    'retry_delay': 10
}

@dag(schedule=[Dataset('mysql://dim.dim_ks_account_info'), Dataset('mysql://dwd.dwd_ks_cps_order'), Dataset('mysql://ods.ods_fs_slice_account'),
               Dataset('mysql://dwd.dwd_ks_leader_order')], 
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['dws', 'etl'], max_active_runs=1)
def dws_ks_slice_slicer():
    from airflow.models import Variable
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    begin_time = Variable.get('ods_ks_cps_order_begin_time')
    end_time = Variable.get('ods_ks_cps_order_end_time')
    
    dws_ks_slice_slicer = SQLExecuteQueryOperator(
        task_id='dws_ks_slice_slicer',
        conn_id='mysql',
        sql='sql/dws_ks_slice_slicer.sql',
        default_args = default_args,
        parameters={'begin_time': begin_time, 'end_time': end_time}
    )
    
    dws_ks_slice_slicer

dws_ks_slice_slicer()