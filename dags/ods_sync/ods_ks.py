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
     default_args=default_args, tags=['src', 'kuaishou'], max_active_runs=1)
def src_kuaishou_start():
    @task(outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/src/kuaishou_start')])
    def src_kuaishou_start(**kwargs):
        pass
    src_kuaishou_start()
src_kuaishou_start()




@dag(schedule=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/ods/ods_ks_leader_order'),
               Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/ods/ods_ks_activity_item_list')],
     start_date=pendulum.datetime(2023, 1, 1), catchup=False,
     default_args=default_args, tags=['src', 'kuaishou'], max_active_runs=1)
def src_kuaishou_finish():
    @task(outlets=[Dataset('mysql://cd-cynosdbmysql-grp-lya2inq0.sql.tencentcdb.com:21775/src/kuaishou_finish')])
    def src_kuaishou_finish(**kwargs):
        pass
    src_kuaishou_finish()
src_kuaishou_finish()