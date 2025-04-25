import logging
from typing import Any, Dict

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.lei.abstract_method.abstract_base import AbstractDagTask

logger = logging.getLogger(__name__)


class AbstractCrawlerLog(AbstractDagTask):
    def __init__(self):
        super().__init__(
            dag_id='push_crawler_log',
            schedule='30 3 * * *',
            default_args=
            {
                'owner': 'Lei Jiangling',
                'start_date': pendulum.datetime(2023, 1, 1),
            },
            tags=['push', 'crawler_log'],
            robot_url=Variable.get('TEST')
        )
        self.card_id = 'AAqRQE2J9JM0g'

    def fetch_data(self, **kwargs) -> Dict:
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        last_date = start_time.subtract(days=1).strftime('%Y-%m-%d')
        sql = f'''
                select
                    'ods' schema_name
                    ,'ods_anchor_live_record' table_name
                    ,count(*) number_yesterday
                from ods.ods_anchor_live_record oalr
                where date(start_time) >= %(last_date)s
                union
                select
                    'ods' schema_name
                    ,'ods_leader_commission_income' table_name
                    ,count(*) number_yesterday
                from ods.ods_leader_commission_income olci 
                where date(order_create_time) >= %(last_date)s
                union
                select
                    'ods' schema_name
                    ,'ods_cps_order_recreation' table_name
                    ,count(*) number_yesterday
                from ods.ods_cps_order_recreation ocor 
                where date(order_create_time) >= %(last_date)s
                union
                select
                    'tmp' schema_name
                    ,'tmp_mcn_order' table_name
                    ,count(*) number_yesterday
                from tmp.tmp_mcn_order tmo 
                where date(order_create_time) >= %(last_date)s
            '''

        crawler_df = pd.read_sql(sql, self.engine, params={'last_date': last_date})
        return {
            'crawler_df': crawler_df
        }

    def process_data(self, data: Dict, **kwargs) -> Dict:
        crawler_data = data['crawler_df']
        res = []
        for i in range(crawler_data.shape[0]):
            res.append({
                'schema_name': str(crawler_data.schema_name.iloc[i]),
                'table_name': str(crawler_data.table_name.iloc[i]),
                'update_time': str(crawler_data.number_yesterday.iloc[i])
            })

        return {
            'group_data': res
        }

    def create_feishu_file(self, data_dic: Dict, **kwargs) -> Dict:
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        title = start_time.strftime('%Y-%m-%d')
        return {
            'spreadsheet_token': '',
            'cps_sheet_id': '',
            'url': '',
            'title': title
        }

    def render_feishu_format(
            self,
            processed_data: Dict,
            spreadsheet_token: str,
            sheet_id: str
    ):
        return processed_data

    def send_card(self, url: str, title: str, data_dic: Dict):
        logger.info(f'发送卡片')
        group_data = data_dic['group_data']

        res = {
            'now_date': title,
            'group_table': group_data,
            'title': 'number_yesterday'
        }

        robot = self.feishu_robot
        robot.send_msg_card(data=res, card_id=self.card_id, version_name='1.0.3')


crawler_log_dag = AbstractCrawlerLog()
crawler_log_dag.create_dag()
