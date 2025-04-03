from airflow.decorators import dag, task
import logging

logger = logging.getLogger(__name__)

@dag(schedule=None,
     default_args={'owner': 'Fang Yongchao'}, tags=['push', 'example'])
def example_dag():
    @task()
    def example_task():
        logger.info('Hello, Airflow')
    
    example_task()

example_dag()