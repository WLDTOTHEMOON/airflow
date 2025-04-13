from airflow.decorators import dag, task
from airflow import Dataset
import logging
import pendulum


logger = logging.getLogger(__name__)
from include.service.message import task_failure_callback
default_args = {
    'owner': 'Fang Yongchao',
    'on_failure_callback': task_failure_callback
}

@dag(schedule_interval='0 */2 * * *', start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['ods', 'sync', 'platform'], max_active_runs=1)
def ods_item_category():
    @task(retries=5, retry_delay=10, outlets=[Dataset('mysql://ods.ods_item_category')])
    def ods_item_category(**kwargs):
        logger.info(f'类目数据')
    ods_item_category()
ods_item_category()


@dag(schedule_interval='0 */2 * * *', start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['ods', 'sync', 'platform'], max_active_runs=1)
def ods_special_allocation():
    @task(retries=5, retry_delay=10, outlets=[Dataset('mysql://ods.ods_special_allocation')])
    def ods_special_allocation(**kwargs):
        logger.info(f'特殊分配比例数据')
    ods_special_allocation()
ods_special_allocation()


@dag(schedule_interval='0 */2 * * *', start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['ods', 'sync', 'platform'], max_active_runs=1)
def ods_cps_order_fake():
    @task(retries=5, retry_delay=10, outlets=[Dataset('mysql://ods.ods_cps_order_fake')])
    def ods_cps_order_fake(**kwargs):
        logger.info(f'不可明说的订单')
    ods_cps_order_fake()
ods_cps_order_fake()