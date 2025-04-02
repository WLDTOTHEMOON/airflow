from airflow.decorators import task, dag


@dag(schedule=None,
     default_args={'owner': 'Fang Yongchao'}, tags=['ods', 'sync', 'crawler'])
def ods_crawler():
    @task
    def ods_crawler_leader_commission_income():
        pass

    ods_crawler_leader_commission_income()

ods_crawler()