from airflow.decorators import task, dag
from airflow import Dataset
import logging
import pendulum

logger = logging.getLogger(__name__)


@dag(schedule=[Dataset('mysql://ods.ods_ks_cps_order'), Dataset('mysql://ods.ods_ks_leader_order')],
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['dwd', 'etl'], max_active_runs=1)
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
        outlets=[Dataset('mysql://dwd.dwd_ks_cps_order')]
    )
    
    dwd_ks_cps_order

dwd_ks_cps_order()