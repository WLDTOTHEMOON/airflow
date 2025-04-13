from airflow.decorators import dag, task
from airflow import Dataset
import logging
import pendulum

logger = logging.getLogger(__name__)
from include.service.message import task_failure_callback
default_args = {
    'owner': 'Fang Yongchao',
    'on_failure_callback': task_failure_callback,
    'retries': 5, 
    'retry_delay': 10
}


@dag(schedule=[Dataset('mysql://dwd.dwd_ks_cps_order'), Dataset('mysql://dwd.dwd_ks_leader_commission_income'), Dataset('mysql://dwd.dwd_ks_recreation'),
               Dataset('mysql://dim.dim_ks_account_info'), Dataset('mysql://dwd.dwd_ks_item_belong'), Dataset('mysql://ods.ods_special_allocation')], 
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['dws', 'etl'], max_active_runs=1)
def dws_ks_big_tbl():
    from airflow.models import Variable
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    order_create_time = Variable.get('ods_ks_cps_order_order_create_time')
    
    dws_ks_big_tbl = SQLExecuteQueryOperator(
        task_id='dws_ks_big_tbl',
        conn_id='mysql',
        sql='sql/dws_ks_big_tbl.sql',
        parameters={'order_create_time': order_create_time},
        outlets=[Dataset('mysql://dws.dws_ks_big_tbl')]
    )
    
    dws_ks_big_tbl

dws_ks_big_tbl()