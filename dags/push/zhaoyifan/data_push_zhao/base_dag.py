from abc import ABC, abstractmethod
from airflow.decorators import dag, task
from airflow.models import Variable
import pendulum
from datetime import  datetime
import logging
from typing import Dict, Any
from include.database.mysql import engine
from include.feishu.feishu_sheet import FeishuSheet
from include.feishu.feishu_robot import FeishuRobot
from dags.push.zhaoyifan.data_push_zhao.feishu_provider import FeishuSheetManager
from include.service.message import task_failure_callback
# logger = logging.getLogger(__name__)


class BaseDag(ABC):
    """Feishu informs the DAG base class"""

    def __init__(
            self,
            dag_id: str,
            schedule: Any,
            tags: list[str],
            robot_url: str,
            feishu_sheet: FeishuSheet = None,
            feishu_robot: FeishuRobot = None,
            db_engine=None
    ):
        self.dag_id = dag_id
        self.schedule = schedule
        self.tags = tags
        self.feishu_sheet_supply = FeishuSheetManager()
        self.feishu_sheet = feishu_sheet or FeishuSheet(**Variable.get('feishu', deserialize_json=True))
        self.feishu_robot = feishu_robot or FeishuRobot(robot_url)
        self.engine = db_engine or engine

    @abstractmethod
    def fetch_data_logic(self, date_interval: Dict[str, Any]) -> Dict[str, Any]:
        """Abstract methods for obtaining business data"""
        raise NotImplementedError

    @abstractmethod
    def prepare_card_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """An abstract method for preparing card data"""
        raise NotImplementedError

    @abstractmethod
    def write_to_sheet_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """The abstract method for writing to the Feishu table"""
        raise NotImplementedError

    @abstractmethod
    def send_card_logic(self, card: Dict[str, Any], sheet):
        """An abstract method for sending notifications"""
        raise NotImplementedError

    def create_dag(self):
        """Template method for generating DAG (no subclass modification required)"""
        @dag(
            dag_id=self.dag_id,
            schedule=self.schedule,
            start_date=pendulum.datetime(2023, 1, 1),
            catchup=False,
            default_args={
                'owner': 'zhaoyifan',
                'on_failure_callback': task_failure_callback,
                'retries': 5,
                'retry_delay': pendulum.duration(seconds=10)
            },
            tags=self.tags,
            max_active_tasks=1
        )
        def generated_dag():
            @task(multiple_outputs=False)
            def get_date_params(**context) -> Dict[str, Any]:
                """Time parameter acquisition (fixed logic)"""
                end_datetime = context['data_interval_end'].in_tz('Asia/Shanghai')
                yes_date = end_datetime.subtract(days=1).format('YYYY-MM-DD')
                month_date_start = end_datetime.subtract(days=1).start_of('month').format('YYYY-MM-DD')
                yes_timestamp = end_datetime.subtract(days=1).format('YYYYMMDD')
                month_start_timestamp = end_datetime.subtract(days=1).start_of('month').format('YYYYMMDD')
                now_timestamp = end_datetime.format('YYYYMMDDHHmm')
                month = end_datetime.subtract(days=1).start_of('month').format('YYYY-MM')
                return {
                    "end_datetime": end_datetime,
                    "yes_ds": yes_date,
                    "month_start_ds": month_date_start,
                    "yes_time": yes_timestamp,
                    "month_start_time": month_start_timestamp,
                    "now_time": now_timestamp,
                    'month': month
                }

            @task(multiple_outputs=False)
            def fetch_data(date_interval: Dict[str, Any]) -> Dict[str, Any]:
                return self.fetch_data_logic(date_interval)

            @task(multiple_outputs=False)
            def prepare_card(data_dict: Dict[str, Any]) -> Dict[str, Any]:
                return self.prepare_card_logic(data_dict)

            @task(multiple_outputs=False)
            def write_to_sheet(data_dict: Dict[str, Any]) -> Dict[str, Any]:
                return self.write_to_sheet_logic(data_dict)

            @task(multiple_outputs=False)
            def send_card(card: Dict[str, Any], sheet):
                self.send_card_logic(card, sheet)

            dates = get_date_params()
            data = fetch_data(dates)
            card_data = prepare_card(data)
            sheet_data = write_to_sheet(data)
            send_card(card_data, sheet_data)

        return generated_dag()

