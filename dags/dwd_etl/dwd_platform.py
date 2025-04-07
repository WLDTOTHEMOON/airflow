from airflow.decorators import dag, task
from airflow import Dataset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
import logging
import pendulum

logger = logging.getLogger(__name__)
default_args = {
    'retries': 5, 
    'retry_delay': 10
}


@dag(schedule=[Dataset('mysql://ods.ods_pf_links'), Dataset('mysql://ods.ods_pf_anchor_select_products')],
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['dwd', 'etl'], max_active_runs=1)
def dwd_pf_links():
    dwd_pf_links = SQLExecuteQueryOperator(
        task_id='dwd_pf_links',
        conn_id='mysql',
        sql='sql/dwd_pf_links.sql',
        parameters={'begin_time': Variable.get('ods_platform_begin_time'), 'end_time': Variable.get('ods_platform_end_time')},
        default_args = default_args
    )
    dwd_pf_links
dwd_pf_links()


@dag(schedule=[Dataset('mysql://ods.ods_pf_users')], start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['dwd', 'etl'], max_active_runs=1)
def dwd_pf_users():
    dwd_pf_users = SQLExecuteQueryOperator(
        task_id='dwd_pf_users',
        conn_id='mysql',
        sql='sql/dwd_pf_users.sql',
        parameters={'begin_time': Variable.get('ods_platform_begin_time'), 'end_time': Variable.get('ods_platform_end_time')},
        default_args = default_args
    )
    dwd_pf_users
dwd_pf_users()


@dag(schedule=[Dataset('mysql://ods.ods_pf_suppliers'), Dataset('mysql://ods.ods_pf_users')], 
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['dwd', 'etl'], max_active_runs=1)
def dwd_pf_suppliers():
    dwd_pf_suppliers = SQLExecuteQueryOperator(
        task_id='dwd_pf_suppliers',
        conn_id='mysql',
        sql='sql/dwd_pf_suppliers.sql',
        parameters={'begin_time': Variable.get('ods_platform_begin_time'), 'end_time': Variable.get('ods_platform_end_time')},
        default_args = default_args
    )
    dwd_pf_suppliers
dwd_pf_suppliers()


@dag(schedule=[Dataset('mysql://ods.ods_pf_reviews'), Dataset('mysql://ods.ods_pf_users')], start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['dwd', 'etl'], max_active_runs=1)
def dwd_pf_reviews():
    dwd_pf_reviews = SQLExecuteQueryOperator(
        task_id='dwd_pf_reviews',
        conn_id='mysql',
        sql='sql/dwd_pf_reviews.sql',
        parameters={'begin_time': Variable.get('ods_platform_begin_time'), 'end_time': Variable.get('ods_platform_end_time')},
        default_args = default_args
    )
    dwd_pf_reviews
dwd_pf_reviews()


@dag(schedule=[Dataset('mysql://ods.ods_pf_products')], start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['dwd', 'etl'],
     max_active_tasks=4, max_active_runs=1)
def dwd_pf_products_anchor():
    dwd_pf_products_anchor = SQLExecuteQueryOperator(
        task_id='dwd_pf_products_anchor',
        conn_id='mysql',
        sql='sql/dwd_pf_products_anchor.sql',
        parameters={'begin_time': Variable.get('ods_platform_begin_time'), 'end_time': Variable.get('ods_platform_end_time')},
        default_args = default_args
    )
    dwd_pf_products_anchor
dwd_pf_products_anchor()


@dag(schedule=[Dataset('mysql://ods.ods_pf_products')], start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['dwd', 'etl'],
     max_active_tasks=4, max_active_runs=1)
def dwd_pf_products_bd():
    dwd_pf_products_bd = SQLExecuteQueryOperator(
        task_id='dwd_pf_products_bd',
        conn_id='mysql',
        sql='sql/dwd_pf_products_bd.sql',
        parameters={'begin_time': Variable.get('ods_platform_begin_time'), 'end_time': Variable.get('ods_platform_end_time')},
        default_args = default_args
    )
    dwd_pf_products_bd
dwd_pf_products_bd()


@dag(schedule=[Dataset('mysql://ods.ods_pf_reviews')], start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['dwd', 'etl'],
     max_active_tasks=4, max_active_runs=1)
def dwd_ks_item_belong():
    dwd_ks_item_belong = SQLExecuteQueryOperator(
        task_id='dwd_ks_item_belong',
        conn_id='mysql',
        sql='sql/dwd_ks_item_belong.sql',
        # default_args = default_args
    )
    dwd_ks_item_belong
dwd_ks_item_belong()


@dag(schedule=[Dataset('mysql://ods.ods_pf_suppliers'), Dataset('mysql://ods.ods_pf_handover'), Dataset('mysql://ods.ods_pf_users')], 
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args={'owner': 'Fang Yongchao'}, tags=['dwd', 'etl'],
     max_active_tasks=4, max_active_runs=1)
def dwd_pf_suppliers_belong():
    dwd_pf_suppliers_belong = SQLExecuteQueryOperator(
        task_id='dwd_pf_suppliers_belong',
        conn_id='mysql',
        sql='sql/dwd_pf_suppliers_belong.sql',
        default_args = default_args
    )
    dwd_pf_suppliers_belong
dwd_pf_suppliers_belong()

