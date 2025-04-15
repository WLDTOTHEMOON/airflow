from airflow.decorators import task, dag
from airflow import Dataset
from include.service.message import task_failure_callback
import pendulum
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'Fang Yongchao',
    'on_failure_callback': task_failure_callback,
    'retries': 5, 
    'retry_delay': 10
}


@dag(schedule=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/ods/ods_pf_account_info'), Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/ods/ods_pf_anchor_info'), Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/ods/ods_pf_users')], 
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['dim', 'etl'], max_active_runs=1)
def dim_ks_account_info():
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
    dim_ks_account_info = SQLExecuteQueryOperator(
        task_id='dim_ks_account_info',
        conn_id='mysql',
        sql='sql/dim_ks_account_info.sql',
        outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/dim/dim_ks_account_info')]
    )
    
    dim_ks_account_info


dim_ks_account_info()