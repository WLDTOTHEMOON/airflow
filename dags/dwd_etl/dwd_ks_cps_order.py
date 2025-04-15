from airflow.decorators import task, dag
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


@dag(schedule=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/ods/ods_ks_cps_order'), Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/ods/ods_ks_leader_order')],
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['dwd', 'etl'], max_active_runs=1)
def dwd_ks_cps_order():
    from airflow.models import Variable
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    begin_time = Variable.get('ods_ks_cps_order_begin_time')
    end_time = Variable.get('ods_ks_cps_order_end_time')
    
    dwd_ks_cps_order = SQLExecuteQueryOperator(
        task_id='dwd_ks_cps_order',
        conn_id='mysql',
        sql='sql/dwd_ks_cps_order.sql',
        parameters={'begin_time': begin_time, 'end_time': end_time},
        outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/dwd/dwd_ks_cps_order')]
    )
    
    dwd_ks_cps_order

dwd_ks_cps_order()