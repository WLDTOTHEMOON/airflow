from airflow.decorators import dag, task
from airflow import Dataset
from include.service.message import task_failure_callback
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
import logging
import pendulum

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'Fang Yongchao',
    'on_failure_callback': task_failure_callback,
    'retries': 5, 
    'retry_delay': pendulum.duration(seconds=10)
}


@dag(schedule=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/src/platform_finish')],
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['dwd', 'etl'], max_active_runs=1)
def dwd_pf_links():
    dwd_pf_links = SQLExecuteQueryOperator(
        task_id='dwd_pf_links',
        conn_id='mysql',
        sql='sql/dwd_pf_links.sql',
        parameters={'begin_time': Variable.get('ods_platform_begin_time'), 'end_time': Variable.get('ods_platform_end_time')},
        outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/dwd/dwd_pf_links')]
    )
    dwd_pf_links
dwd_pf_links()


@dag(schedule=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/src/platform_finish')],
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['dwd', 'etl'], max_active_runs=1)
def dwd_pf_users():
    dwd_pf_users = SQLExecuteQueryOperator(
        task_id='dwd_pf_users',
        conn_id='mysql',
        sql='sql/dwd_pf_users.sql',
        parameters={'begin_time': Variable.get('ods_platform_begin_time'), 'end_time': Variable.get('ods_platform_end_time')},
        outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/dwd/dwd_pf_users')]
    )
    dwd_pf_users
dwd_pf_users()


@dag(schedule=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/src/platform_finish')], 
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['dwd', 'etl'], max_active_runs=1)
def dwd_pf_suppliers():
    dwd_pf_suppliers = SQLExecuteQueryOperator(
        task_id='dwd_pf_suppliers',
        conn_id='mysql',
        sql='sql/dwd_pf_suppliers.sql',
        parameters={'begin_time': Variable.get('ods_platform_begin_time'), 'end_time': Variable.get('ods_platform_end_time')},
        outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/dwd/dwd_pf_suppliers')]
    )
    dwd_pf_suppliers
dwd_pf_suppliers()


@dag(schedule=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/src/platform_finish')],
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['dwd', 'etl'], max_active_runs=1)
def dwd_pf_reviews():
    dwd_pf_reviews = SQLExecuteQueryOperator(
        task_id='dwd_pf_reviews',
        conn_id='mysql',
        sql='sql/dwd_pf_reviews.sql',
        parameters={'begin_time': Variable.get('ods_platform_begin_time'), 'end_time': Variable.get('ods_platform_end_time')},
        outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/dwd/dwd_pf_reviews')]
    )
    dwd_pf_reviews
dwd_pf_reviews()


@dag(schedule=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/src/platform_finish')],
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['dwd', 'etl'], max_active_runs=1)
def dwd_pf_products_anchor():
    dwd_pf_products_anchor = SQLExecuteQueryOperator(
        task_id='dwd_pf_products_anchor',
        conn_id='mysql',
        sql='sql/dwd_pf_products_anchor.sql',
        parameters={'begin_time': Variable.get('ods_platform_begin_time'), 'end_time': Variable.get('ods_platform_end_time')},
        outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/dwd/dwd_pf_products_anchor')]
    )
    dwd_pf_products_anchor
dwd_pf_products_anchor()


@dag(schedule=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/src/platform_finish')],
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['dwd', 'etl'], max_active_runs=1)
def dwd_pf_products_bd():
    dwd_pf_products_bd = SQLExecuteQueryOperator(
        task_id='dwd_pf_products_bd',
        conn_id='mysql',
        sql='sql/dwd_pf_products_bd.sql',
        parameters={'begin_time': Variable.get('ods_platform_begin_time'), 'end_time': Variable.get('ods_platform_end_time')},
        outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/dwd/dwd_pf_products_bd')]
    )
    dwd_pf_products_bd
dwd_pf_products_bd()


@dag(schedule=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/dwd/dwd_pf_links'), 
               Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/dwd/dwd_pf_products_bd'),
               Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/dwd/dwd_pf_products_anchor'),
               Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/dwd/dwd_pf_suppliers_belong'),
               Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/dwd/dwd_ks_activity_item_list'),
               Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/src/feishu_finish')], 
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['dwd', 'etl'], max_active_runs=1)
def dwd_ks_item_belong():
    dwd_ks_item_belong = SQLExecuteQueryOperator(
        task_id='dwd_ks_item_belong',
        conn_id='mysql',
        sql='sql/dwd_ks_item_belong.sql',
        outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/dwd/dwd_ks_item_belong')]
    )
    dwd_ks_item_belong
dwd_ks_item_belong()


@dag(schedule=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/src/platform_finish')], 
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['dwd', 'etl'], max_active_runs=1)
def dwd_pf_suppliers_belong():
    dwd_pf_suppliers_belong = SQLExecuteQueryOperator(
        task_id='dwd_pf_suppliers_belong',
        conn_id='mysql',
        sql='sql/dwd_pf_suppliers_belong.sql',
        outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/dwd/dwd_pf_suppliers_belong')]
    )
    dwd_pf_suppliers_belong
dwd_pf_suppliers_belong()

