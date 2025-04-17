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
    'retry_delay': pendulum.duration(seconds=10)
}


@dag(schedule=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/dwd/dwd_ks_leader_commission_income'),
               Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/dwd/dwd_ks_recreation'),
               Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/src/feishu_finish')], 
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['dws', 'etl'], max_active_runs=1)
def dws_ks_slice_recreation():
    from airflow.models import Variable
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    order_create_time = Variable.get('ods_ks_cps_order_order_create_time')
    
    dws_ks_slice_recreation = SQLExecuteQueryOperator(
        task_id='dws_ks_slice_recreation',
        conn_id='mysql',
        sql='sql/dws_ks_slice_recreation.sql',
        parameters={'order_create_time': order_create_time}
    )
    
    dws_ks_slice_recreation

dws_ks_slice_recreation()