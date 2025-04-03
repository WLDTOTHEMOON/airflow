from airflow.decorators import task, dag


@dag(schedule=None,
     default_args={'owner': 'Fang Yongchao'}, tags=['ods', 'sync', 'crawler'])
def ods_crawler():
    @task
    def ods_crawler_leader_commission_income():
        pass
    # 佣金收入订单
    
    @task
    def ods_crawler_recreation():
        pass
    # 二创

    ods_crawler_leader_commission_income()
    ods_crawler_recreation()

ods_crawler()