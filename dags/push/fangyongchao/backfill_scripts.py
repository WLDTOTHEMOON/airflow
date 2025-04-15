from airflow import task, dag
from airflow.models import Param
import logging

logger = logging.getLogger(__name__)
params = {
    'Dag id': Param(type='array',enum=[])
}


@dag(schedule=None)
def backfill_scripts():
    @task
    def tmp():
        pass
    tmp()
backfill_scripts()