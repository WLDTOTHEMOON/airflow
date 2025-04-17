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


@dag(schedule_interval='0 */2 * * *', start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['ods', 'src'], max_active_runs=1)
def src_stable_start():
    @task(outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/src/stable_start')])
    def src_stable_start(**kwargs):
        pass
    src_stable_start()
src_stable_start()


@dag(schedule=Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/src/stable_start'),
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['ods', 'platform'], max_active_runs=1)
def ods_item_category():
    @task(outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/ods/ods_item_category')])
    def ods_item_category(**kwargs):
        logger.info(f'类目数据')
    ods_item_category()
ods_item_category()


@dag(schedule=Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/ods/ods_item_category'),
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['ods', 'platform'], max_active_runs=1)
def ods_special_allocation():
    @task(outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/ods/ods_special_allocation')])
    def ods_special_allocation(**kwargs):
        logger.info(f'特殊分配比例数据')
    ods_special_allocation()
ods_special_allocation()


@dag(schedule=Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/ods/ods_special_allocation'),
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['ods', 'platform'], max_active_runs=1)
def ods_cps_order_fake():
    @task(outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/ods/ods_cps_order_fake')])
    def ods_cps_order_fake(**kwargs):
        logger.info(f'不可明说的订单')
    ods_cps_order_fake()
ods_cps_order_fake()

@dag(schedule=Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/ods/ods_cps_order_fake'),
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['ods', 'src'], max_active_runs=1)
def src_stable_finish():
    @task(outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/src/stable_finish')])
    def src_stable_finish(**kwargs):
        pass
    src_stable_finish()
src_stable_finish()
