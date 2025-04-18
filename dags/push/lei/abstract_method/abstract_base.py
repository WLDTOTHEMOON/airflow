# abstract_base.py
import logging
import string
from abc import ABC, abstractmethod
from typing import Any, Dict

from airflow.decorators import dag, task
from airflow.models import Variable
from include.database.mysql import engine
from include.feishu.feishu_app_robot import FeishuAppRobot
from include.feishu.feishu_robot import FeishuRobot
from include.feishu.feishu_sheet import FeishuSheet

logger = logging.getLogger(__name__)


class AbstractDagTask(ABC):
    def __init__(
            self,
            dag_id: str,
            schedule: Any,
            default_args: dict,
            tags: list[str],
            robot_url: str,
            feishu_sheet: FeishuSheet = None,
            feishu_robot: FeishuRobot = None,
            feishu_app_robot: FeishuAppRobot = None,
            db_engine=None
    ):
        self.dag_id = dag_id
        self.schedule = schedule
        self.default_args = default_args
        self.tags = tags
        self.feishu_sheet = feishu_sheet or FeishuSheet(**Variable.get('feishu', deserialize_json=True))
        self.feishu_robot = feishu_robot or FeishuRobot(robot_url)
        self.feishu_app_robot = feishu_app_robot or FeishuAppRobot(**Variable.get('feishu', deserialize_json=True))
        self.engine = db_engine or engine

    @staticmethod
    def col_convert(col_num: int):
        alphabeta = string.ascii_uppercase[:26]
        alphabeta = [k for k in alphabeta] + [k + j for k in alphabeta for j in alphabeta]
        return alphabeta[col_num - 1]

    @staticmethod
    def percent_convert(num):
        return str(round(num * 100, 2)) + '%'

    @abstractmethod
    def fetch_data(self, start_time) -> Any:
        """Fetch raw data from source"""
        raise NotImplementedError

    @abstractmethod
    def process_data(self, data: Any, **kwargs) -> Any:
        """Process raw data into structured format"""
        raise NotImplementedError

    @abstractmethod
    def render_feishu_format(
            self,
            processed_data: Any,
            spreadsheet_token: str,
            sheet_id: str
    ):
        """Render data to Feishu sheet format"""
        raise NotImplementedError

    @abstractmethod
    def create_feishu_file(self, data_dic: Dict, **kwargs) -> Dict:
        """Create Feishu file and return access info"""
        raise {}

    @abstractmethod
    def send_card(self, file_key: str, title: str, flag):
        """Send notification card with file link"""
        raise NotImplementedError

    def create_dag(self):
        @dag(
            dag_id=self.dag_id,
            schedule=self.schedule,
            default_args=self.default_args,
            tags=self.tags,
            catchup=False
        )
        def generated_dag():
            @task(task_id='fetch_data_task', retries=2)
            def fetch_data_task(**kwargs):
                return self.fetch_data(**kwargs)

            @task(task_id='process_data_task')
            def process_data_task(data, **kwargs):
                return self.process_data(data, **kwargs)

            @task(task_id='create_feishu_file_task', multiple_outputs=True)
            def create_feishu_file_task(data_dic: Dict, **kwargs):
                return self.create_feishu_file(data_dic, **kwargs)

            @task(task_id='render_feishu_format_task')
            def render_feishu_format_task(process_data, spreadsheet_token, sheet_id):
                return self.render_feishu_format(
                    process_data,
                    spreadsheet_token,
                    sheet_id
                )

            @task(task_id='send_card_task')
            def send_card_task(url, title, data_dic):
                return self.send_card(url, title, data_dic)

            data_dict = fetch_data_task()
            processed_data_dict = process_data_task(data_dict)
            file_info = create_feishu_file_task(data_dict)
            processed_data_dict = render_feishu_format_task(processed_data_dict, file_info['spreadsheet_token'],
                                                            file_info['cps_sheet_id'])
            send_card_task(file_info['url'], file_info['title'], processed_data_dict)

        return generated_dag()
