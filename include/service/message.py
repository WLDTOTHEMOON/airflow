from airflow.models import Variable
from include.feishu.feishu_robot import FeishuRobot
import pytz

def task_failure_callback(context):
    ti = context['task_instance']
    task = context['task']
    dag = context['dag']
    run_id = context['run_id']
    execution_date = context['execution_date']
    log_url = context['task_instance'].log_url
    
    local_tz = pytz.timezone("Asia/Shanghai")

    local_dt = execution_date.astimezone(local_tz)
    local_str = local_dt.strftime("%Y-%m-%d %H:%M:%S")

    CARD_ID = 'AAqRtahrLKwa0'
    VERSION = '1.0.0'
    title = f'DAG调度失败通知'

    description = f'''DAG运行失败：
    **DAG:** {dag.dag_id}
    **TASK:** {task.task_id}
    **Run ID:** {run_id}
    **Execution Date:** {local_str}
    '''
    data = {
        'title': title,
        'file_name': '点击查看日志',
        'url': log_url.replace('localhost', Variable.get('webserver_url')),
        'description': description,
        'color': 'red'
    }
    robot = FeishuRobot(robot_url=Variable.get('ZORO'))
    robot.send_msg_card(data=data, card_id=CARD_ID, version_name=VERSION)
