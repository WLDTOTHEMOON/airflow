from abc import ABC, abstractmethod

import pandas as pd
from airflow.decorators import dag, task,task_group
from airflow.models import Variable
import pendulum
from typing import Dict, Any, List, Callable, Union, Iterator

from pandas import DataFrame

from include.database.mysql import engine
from include.feishu.feishu_sheet import FeishuSheet
from include.feishu.feishu_robot import FeishuRobot
from dags.push.zhaoyifan.data_push_zhao.feishu_provider import FeishuSheetManager


class BaseParallelDag(ABC):
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
    def get_sql_queries(self, date_interval: dict) -> Dict[str, str]:
        """返回需要执行的SQL字典 {task_id: sql}"""
        raise NotImplementedError

    # @abstractmethod
    # def deal_with_data_logic(self, data: Dict, date_interval: dict) -> Any:
    #     """数据处理逻辑抽象方法"""
    #     raise NotImplementedError
    #
    # @abstractmethod
    # def prepare_card_logic(self, data: Dict[str, Any], date_interval: dict) -> Dict[str, Any]:
    #     """准备卡片数据的抽象方法"""
    #     raise NotImplementedError
    #
    # @abstractmethod
    # def write_to_sheet_logic(self, data: Dict[str, Any], date_interval: dict) -> Dict[str, Any]:
    #     """写入飞书表格的抽象方法"""
    #     raise NotImplementedError
    #
    # @abstractmethod
    # def send_card_logic(self, card: Dict[str, Any], sheet):
    #     """发送通知的抽象方法"""
    #     raise NotImplementedError

    # def get_query_info(self, date_interval) -> List[Dict]:
    #     """生成动态任务参数"""
    #     queries = self.get_sql_queries(date_interval)
    #     return [
    #         {
    #             "task_id": task_id,
    #             "sql": sql,
    #             "date_interval": queries['date_interval']
    #         } for task_id, sql in queries['sql'].items()
    #     ]

    def create_dag(self):
        """构建DAG结构"""

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

            @task_group
            def get_data_df(date_interval):
                sql_dict = {}
                sql_group = self.get_sql_queries(date_interval)
                for task_id, sql in sql_group.items():
                    @task(task_id=f'fetch_data_task_{task_id}')
                    def execute_sql(sql):
                        return pd.read_sql(sql, self.conn)

                    sql_dict[task_id] = execute_sql

            # @task
            # def execute_sql_queries(date_interval) -> dict[Dict]:
            #     """动态展开执行所有SQL"""

                # return _execute_sql.expand(query_info=self.get_query_info(date_interval))

            # @task
            # def deal_with_data(data, date_interval) -> Dict:
            #     """合并多个SQL结果"""
            #     return self.deal_with_data_logic(data, date_interval)
            #
            # @task
            # def prepare_card(data: Dict) -> Dict:
            #     """准备卡片数据"""
            #     return self.prepare_card_logic(data)
            #
            # @task
            # def write_to_sheet(data, date_interval) -> Dict:
            #     """写入存储系统"""
            #     return self.write_to_sheet_logic(data,date_interval)
            #
            # @task
            # def send_notification(card: dict, sheet: dict):
            #     """发送通知"""
            #     self.send_card_logic(card, sheet)

            # # 动态生成SQL查询任务
            date = get_date_params()
            get_data_df(date)
            # sql_results = execute_sql_queries()
            #
            # # 处理数据
            # deal_with_data = deal_with_data(sql_results)
            #
            # # 准备卡片数据
            # prepare_card = prepare_card(deal_with_data)
            #
            # # 写入表格
            # write_result = write_to_sheet(deal_with_data)
            #
            # # 发送通知
            # send_notification(prepare_card, write_result)

        return generated_dag()


