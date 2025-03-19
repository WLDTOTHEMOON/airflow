from airflow.decorators import dag, task



@dag(schedule=None)
def test_dag():
    @task
    def test_task():
        print('Hello World')
    
    test_task()

test_dag()