from abc import ABC, abstractmethod
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
import pendulum
import logging
from typing import Dict, Any
from include.database.mysql import engine
from include.feishu.feishu_sheet import FeishuSheet
from include.feishu.feishu_robot import FeishuRobot
from dags.push.zhaoyifan.data_push_zhao.feishu_provider import FeishuSheetManager
# logger = logging.getLogger(__name__)


class BaseDag(ABC):
    """飞书通知DAG基类"""

    def __init__(
            self,
            dag_id: str,
            default_args: dict,
            tags: list[str],
            card_id: str,
            robot_url: str,
            schedule: Any
    ):
        self.conn = engine
        self.feishu_sheet_supply = FeishuSheetManager()
        self.feishu_sheet = FeishuSheet(**Variable.get('feishu', deserialize_json=True))
        self.feishu_robot = FeishuRobot(Variable.get(robot_url))
        self.dag_id = dag_id
        self.default_args = default_args
        self.tags = tags
        self.card_id = card_id
        self.schedule = schedule

    @abstractmethod
    def fetch_data_logic(self, date_interval: Dict[str, Any]) -> Dict[str, Any]:
        """获取业务数据的抽象方法"""
        raise NotImplementedError

    @abstractmethod
    def prepare_card_logic(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """准备卡片数据的抽象方法"""
        raise NotImplementedError

    @abstractmethod
    def write_to_sheet_logic(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """写入飞书表格的抽象方法"""
        raise NotImplementedError

    @abstractmethod
    def send_card_logic(self, card: Dict[str, Any], sheet):
        """发送通知的抽象方法"""
        raise NotImplementedError

    def create_dag(self):
        """生成DAG的模板方法（无需子类修改）"""
        @dag(
            dag_id=self.dag_id,
            schedule=self.schedule,
            start_date=pendulum.datetime(2023, 1, 1),
            catchup=False,
            default_args=self.default_args,
            tags=self.tags
        )
        def generated_dag():
            @task
            def get_date_params(**context) -> Dict[str, Any]:
                """时间参数获取（固定逻辑）"""
                logical_datetime = context['logical_date'].in_timezone('Asia/Shanghai')
                yes_date = logical_datetime.subtract(days=1).format('YYYY-MM-DD')
                month_date_start = logical_datetime.subtract(days=1).start_of('month').format('YYYY-MM-DD')
                yes_timestamp = logical_datetime.subtract(days=1).format('YYYYMMDD')
                month_start_timestamp = logical_datetime.subtract(days=1).start_of('month').format('YYYYMMDD')
                now_timestamp = logical_datetime.format('YYYYMMDDHHMM')
                return {
                    "logical": logical_datetime,
                    "yes_ds": yes_date,
                    "month_start_ds": month_date_start,
                    "yes_time": yes_timestamp,
                    "month_start_time": month_start_timestamp,
                    "now_time": now_timestamp
                }

            @task
            def fetch_data(date_interval: Dict[str, Any]) -> Dict[str, Any]:
                return self.fetch_data_logic(date_interval)

            # @task_group
            # def process_data(data_params: Dict[str, Any]) -> Dict[str, Any]:

            @task
            def prepare_card(data: Dict[str, Any]) -> Dict[str, Any]:
                return self.prepare_card_logic(data)

            @task
            def write_to_sheet(data: Dict[str, Any]) -> Dict[str, Any]:
                return self.write_to_sheet_logic(data)

            #
            # # card_data = prepare_card(data_params)
            # # sheet_data = write_to_sheet(data_params)
            # # return {
            # #     'card': card_data,
            # #     'sheet': sheet_data
            # # }

            @task
            def send_card(card: Dict[str, Any], sheet):
                self.send_card_logic(card, sheet)

            # 任务流定义
            dates = get_date_params()
            data = fetch_data(dates)
            card_data = prepare_card(data)
            sheet_data = write_to_sheet(data)
            # # # processed = process_data(data)
            send_card(card_data, sheet_data)

        return generated_dag()

    # def register(self):
    #     """注册DAG"""
    #     return self.create_dag()
