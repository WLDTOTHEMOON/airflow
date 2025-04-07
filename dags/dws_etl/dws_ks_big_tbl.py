from airflow.decorators import dag, task
from airflow import Dataset
import logging
import pendulum

logger = logging.getLogger(__name__)


@dag(schedule=[Dataset('mysql://dwd.dwd_ks_cps_order'), Dataset('mysql://dwd.dwd_ks_leader_commission_income'), Dataset('mysql://dwd.dwd_ks_recreation'),
               Dataset('mysql://dim.dim_ks_account_info'), Dataset('mysql://dwd.dwd_ks_item_belong')], 
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['dws', 'etl'], max_active_runs=1)
def dws_ks_big_tbl():
    from airflow.models import Variable
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    begin_time = Variable.get('ods_ks_cps_order_begin_time')
    end_time = Variable.get('ods_ks_cps_order_end_time')
    
    dws_ks_big_tbl = SQLExecuteQueryOperator(
        task_id='dws_ks_big_tbl',
        conn_id='mysql',
        sql='sql/dws_ks_big_tbl.sql',
        parameters={'begin_time': begin_time, 'end_time': end_time}
    )
    
    dws_ks_big_tbl

dws_ks_big_tbl()