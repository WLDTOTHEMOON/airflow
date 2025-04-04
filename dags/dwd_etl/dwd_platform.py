from airflow.decorators import dag, task
from airflow import Dataset
import logging
import pendulum

logger = logging.getLogger(__name__)
ods_platform_dataset = Dataset('ods_platform_dataset')

@dag(schedule=[ods_platform_dataset], start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['dwd', 'etl'],
     max_active_tasks=4, max_active_runs=1)
def dwd_platform():
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
    dwd_pf_links = SQLExecuteQueryOperator(
        task_id='dwd_pf_links',
        conn_id='mysql',
        sql='sql/dwd_pf_links.sql'
    )

    dwd_pf_users = SQLExecuteQueryOperator(
        task_id='dwd_pf_users',
        conn_id='mysql',
        sql='sql/dwd_pf_users.sql'
    )

    dwd_pf_users >> dwd_pf_links

dwd_platform()