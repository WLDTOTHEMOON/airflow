from airflow.decorators import dag, task, task_group
import pendulum
from typing import Dict, Any
from include.database.mysql import engine
from include.feishu.feishu_sheet import FeishuSheet
from dags.push.zhaoyifan.base_utils.feishu_provider import FeishuSheetManager


class FeishuNotificationDAG:
    """飞书通知DAG基类"""

    def __init__(
            self,
            conn: engine,
            feishu_sheet_supply: FeishuSheetManager,
            feishu_sheet: FeishuSheet,
            dag_id: str,
            schedule: None,
            card_id: str,
            card_version: str = '1.0.0',
            start_date: pendulum.datetime = pendulum.datetime(2023, 1, 1),
    ):
        self.conn = conn
        self.feishu_sheet_supply = feishu_sheet_supply
        self.feishu_sheet = feishu_sheet
        self.dag_id = dag_id
        self.schedule = schedule
        self.card_id = card_id
        self.card_version = card_version
        self.start_date = start_date

    def _create_dag(self):
        """创建DAG的模板方法"""

        @dag(
            dag_id=self.dag_id,
            schedule=self.schedule,
            start_date=self.start_date,
            catchup=False,
            default_args={'owner': 'zhaoyifan'},
            tags=['example']
        )
        def generated_dag():
            @task
            def get_date_params(**context) -> Dict[str, Any]:
                """获取时间参数"""
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
                """获取业务数据"""
                # 由子类实现具体逻辑
                raise NotImplementedError

            @task_group
            def process_data(data_params: Dict[str, Any]) -> Dict[str, Any]:
                """数据处理任务组"""

                @task
                def prepare_card(data: Dict[str, Any]) -> Dict[str, Any]:
                    """准备卡片数据"""
                    # 由子类实现具体逻辑
                    raise NotImplementedError

                @task
                def write_to_sheet(data: Dict[str, Any]) -> Dict[str, Any]:
                    """写入飞书表格"""
                    # 由子类实现具体逻辑
                    raise NotImplementedError

                card_data = prepare_card(data_params)
                sheet_data = write_to_sheet(data_params)
                return {
                    'card': card_data,
                    'sheet': sheet_data
                }

            @task
            def send_card(results: Dict[str, Any]) -> None:
                """发送通知"""

                # 由子类实现具体逻辑
                raise NotImplementedError

            # 任务流定义
            dates = get_date_params()
            data = fetch_data(dates)
            processed = process_data(data)
            send_card(processed)

        return generated_dag()

    def register(self):
        """注册DAG"""
        return self._create_dag()
