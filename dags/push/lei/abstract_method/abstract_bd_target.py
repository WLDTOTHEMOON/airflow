import logging
from typing import Any, Dict

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.lei.abstract_method.abstract_base import AbstractDagTask

logger = logging.getLogger(__name__)


class AbstractBdTarget(AbstractDagTask):
    def __init__(self):
        super().__init__(
            dag_id='push_bd_target',
            schedule='0 7 * * *',
            default_args=
            {
                'owner': 'Lei Jiangling',
                'start_date': pendulum.datetime(2023, 1, 1),
                'provide_context': True
            },
            tags=['push', 'bd_target'],
            robot_url=Variable.get('TEST'),
        )
        self.card_id: str = 'AAq4cDfHHntNt'

    def fetch_data(self, **kwargs) -> Dict:
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        begin_time = start_time.subtract(days=1).strftime('%Y-%m-01')
        end_time = start_time.subtract(days=1).strftime('%Y-%m-%d')
        logger.info(f'获取数据, 数据开始日期:{begin_time}')
        sql = """
            select 
                bd_name bd
                ,sum(origin_gmv) origin_gmv 
                ,sum(final_gmv) final_gmv 
            from dws.dws_ks_big_tbl dbth
            where order_date between %(begin_time)s and %(end_time)s
                and bd_name is not null
            group by bd_name 
        """
        actual = pd.read_sql(sql, self.engine, params={'begin_time': begin_time, 'end_time': end_time})
        return {
            'actual': actual
        }

    def process_data(self, data: Dict, **kwargs) -> Dict:
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        architecture = {
            '商务一组': {'leader': '陈奇俊', 'member': ['董晨佼', '高天骄', '龚维', '胡典', '蒋露华', '李陈莲', '李海艳', '刘媛媛', '郑昕']},
            '商务二组': {'leader': '吴建兵', 'member': ['邓远鹏', '刘晓玲', '苏志雄', '肖瑶']}
        }

        # 目标完成相关计算
        print('TARGET DATA FETCH')
        target = self.feishu_sheet.fetch_dat(
            spreadsheet_token='M2emsUX95hIoxat7SNxcN8Tfnlh', sheet_id='ca25fb'
        )
        logger.info(target)
        target = target['valueRanges'][0]['values']
        target = pd.DataFrame(target[1:], columns=target[0])

        target = target.rename(columns={
            '目标月份': 'month', '商务': 'bd', '目标': 'target'
        })
        target = target[['month', 'bd', 'target']]
        target = target[target.month == start_time.subtract(days=1).strftime('%Y-%m')]

        actual = data['actual']
        target = pd.merge(target[['bd', 'target']], actual, how='outer').fillna(0)

        # send message card
        group_table_summary = []
        for key, value in architecture.items():
            all = target['target'][target.bd.isin(value['member'])].sum()
            passed = target['final_gmv'][target.bd.isin(value['member'])].sum()
            tmp = {
                'group': key,
                # 'all': str(round(all / 10000, 2)),
                'pass': str(round(passed / 10000, 2)),
                'pass_rate': '\\-' if all == 0 else self.percent_convert(passed / all)
            }
            group_table_summary.append(tmp)

        group_table_detail = []
        for key, value in architecture.items():
            for each in value['member']:
                all = target['target'][target['bd'] == each].iloc[0] if not target['target'][
                    target['bd'] == each].empty else 0
                passed = target['final_gmv'][target['bd'] == each].iloc[0] if not target['final_gmv'][
                    target['bd'] == each].empty else 0
                group_d = key if value['member'].index(each) == 0 else ''
                tmp = {
                    'group_d': group_d,
                    'bd_d': each,
                    # 'all_d': str(round(all / 10000, 2)),
                    'pass_d': str(round(passed / 10000, 2)),
                    'pass_rate_d': '\\-' if all == 0 else self.percent_convert(passed / all)
                }
                group_table_detail.append(tmp)

        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        begin_time = start_time.subtract(days=1).strftime('%Y-%m-01')
        end_time = start_time.subtract(days=1).strftime('%Y-%m-%d')
        return {
            'group_table_summary': group_table_summary,
            'group_table_detail': group_table_detail,
            'start_date': begin_time,
            'end_date': end_time
        }

    def create_feishu_file(self, data_dic: Dict, **kwargs) -> Dict:
        return {
            'spreadsheet_token': '',
            'cps_sheet_id': '',
            'url': '',
            'title': ''
        }

    def render_feishu_format(
            self,
            process_data_dict: Dict,
            spreadsheet_token: str,
            cps_sheet_id: str
    ) -> Dict:
        return process_data_dict

    def send_card(self, url: str, title: str, data_dict: Dict):
        logger.info(f'发送卡片')
        group_table_detail = data_dict['group_table_detail']
        group_table_summary = data_dict['group_table_summary']
        month_start_date = data_dict['start_date']
        month_end_date = data_dict['end_date']

        data = {
            'start_date': month_start_date,
            'end_date': month_end_date,
            'group_table_detail': group_table_detail,
            'group_table_summary': group_table_summary
        }

        robot = self.feishu_robot
        robot.send_msg_card(data=data, card_id=self.card_id, version_name='1.0.0')


bd_target_dag = AbstractBdTarget()
bd_target_dag.create_dag()
