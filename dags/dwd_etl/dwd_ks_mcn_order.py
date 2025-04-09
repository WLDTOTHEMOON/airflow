from airflow.decorators import dag, task
from airflow import Dataset
import logging
import pendulum

logger = logging.getLogger(__name__)


@dag(schedule=[Dataset('mysql://ods.ods_crawler_mcn_order'), Dataset('mysql://ods.ods_ks_cps_order')],
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['dwd', 'etl'], max_active_runs=1)
def dwd_ks_mcn_order():
    from airflow.models import Variable
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    order_create_time = Variable.get('ods_ks_cps_order_order_create_time')
    
    dwd_ks_mcn_order = SQLExecuteQueryOperator(
        task_id='dwd_ks_mcn_order',
        conn_id='mysql',
        sql='sql/dwd_ks_mcn_order.sql',
        parameters={'order_create_time': order_create_time},
        outlets=[Dataset('mysql://dwd.dwd_ks_mcn_order')]
    )
    
    dwd_ks_mcn_order

dwd_ks_mcn_order()