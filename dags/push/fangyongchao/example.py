from airflow.decorators import dag, task
from include.service.message import task_failure_callback
import logging

logger = logging.getLogger(__name__)
CARD_ID = 'ctp_AAwXZHaGec4d'


@dag(schedule=None,
     default_args={'owner': 'Fang Yongchao', 'on_failure_callback': task_failure_callback}, tags=['push', 'example'])
def example_dag():
    @task()
    def fetch_data():
        logger.info(f'获取数据')
        data = {
            'title': '测试title',
            'file_name': '测试file_name',
            'url': 'www.baidu.com',
            'description': '测试description'
        }
        return data
        
    @task()
    def send_card(data):
        from include.feishu.feishu_robot import FeishuRobot
        from airflow.models import Variable
        logger.info(f'发送卡片')
        robot = FeishuRobot(robot_url=Variable.get('TEST'))
        robot.send_msg_card(data=data, card_id=CARD_ID)
    
    data = fetch_data()
    send_card(data)

example_dag()