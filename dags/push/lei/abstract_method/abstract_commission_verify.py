import logging
from typing import Any, Dict

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.lei.abstract_method.abstract_base import AbstractDagTask

logger = logging.getLogger(__name__)


class AbstractCommissionVerify(AbstractDagTask):
    def __init__(self):
        super().__init__(
            dag_id='push_commission_verify',
            schedule='0 7 * * *',
            default_args=
            {
                'owner': 'Lei Jiangling',
                'start_date': pendulum.datetime(2023, 1, 1),
                'provide_context': True
            },
            tags=['push', 'commission_verify'],
            robot_url=Variable.get('TEST'),
        )
        self.card_id: str = 'AAq4cjW2TgIBl'

    def fetch_data(self, **kwargs) -> Dict:
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        begin_time = start_time.subtract(days=1).strftime('%Y-%m-01')
        end_time = start_time.subtract(days=1).strftime('%Y-%m-%d')
        logger.info(f'获取数据, 数据开始日期:{begin_time}')
        sql = """
            select
                us.name bd
                ,count(case when src.status in ('成本-不通过') then 1 else null end) pass_num
                ,count(case when src.status in ('成本-不通过','成本-通过 | 待入库','入库','不允许入库') then 1 else null end) all_num
                ,case
                    when count(*) = 0 then 0 
                    else count(case when src.status in ('成本-不通过') then 1 else null end) / count(*)
                end rate
            from(
            select
                id
                ,case status
                    when 0 then '待审核 | 初审中 | 特批-初审中'
                    when 1 then '初审不通过'
                    when 2 then '初审通过 | 复审中 | 特批-复审中'
                    when 3 then '复审不通过'
                    when 4 then '复审不通过（不可再次提交）'
                    when 5 then '复审通过 | 成本审核中'
                    when 6 then '待成本审核 - 在成本审核不通过后，供应商可以选择只修改机制，从而无需再次【初审】和【复审】，直接到达【成本终审】（6是个意外）'
                    when 7 then '特批-复审不通过'
                    when 8 then '特批-审核通过 | 成本审核中'
                    when 9 then '成本-不通过'
                    when 10 then '成本-通过 | 待入库'
                    when 11 then '入库'
                    when 12 then '不允许入库'
                    when 13 then '复审不通过 - 退回初审，包括招商商品和主播自采'
                    when 14 then '退回 - 把已通过的退回，编辑后重新审核'
                    when -1 then '编辑中，这是老平台的数据'
                    else '未知状态'
                end status  
                ,supplier_id
                ,reviewed_at
            from xlsd.products p 
            where by_anchor = 0
                and date(reviewed_at) between %(begin_time)s and %(end_time)s) src
            left join(
                select
                    *
                from xlsd.suppliers s
            ) sp on src.supplier_id = sp.id
            left join(
                select
                    id
                    ,name
                from xlsd.users u 
            )us on sp.commerce = us.id
            group by
                us.name
        """
        entered = pd.read_sql(sql, self.engine, params={'begin_time': begin_time, 'end_time': end_time})
        return {
            'entered': entered
        }

    def process_data(self, data: Dict, **kwargs) -> Dict:
        architecture = {
            '商务一组': {'leader': '陈奇俊', 'member': ['董晨佼', '高天骄', '龚维', '胡典', '蒋露华', '李陈莲', '李海艳', '刘媛媛', '郑昕']},
            '商务二组': {'leader': '吴建兵', 'member': ['邓远鹏', '刘晓玲', '苏志雄', '肖瑶']}
        }

        entered = data['entered']

        entered['rate'] = (entered['rate'] * 100).apply(lambda x: "{:.2f}%".format(x))
        # send message card
        group_table_summary = []
        for key, value in architecture.items():
            all = entered['all_num'][entered.bd.isin(value['member'])].sum() if not entered.empty else 0
            passed = entered['pass_num'][entered.bd.isin(value['member'])].sum() if not entered.empty else 0
            tmp = {
                'group': key,
                'all': str(all),
                'pass': str(passed),
                'pass_rate': '\\-' if all == 0 else self.percent_convert(passed / all)
            }
            group_table_summary.append(tmp)

        group_table_detail = []
        for key, value in architecture.items():
            for each in value['member']:
                if not entered.empty:
                    all = entered['all_num'][entered['bd'] == each].iloc[0] if not entered['all_num'][
                        entered['bd'] == each].empty else 0
                    passed = entered['pass_num'][entered['bd'] == each].iloc[0] if not entered['pass_num'][
                        entered['bd'] == each].empty else 0
                else:
                    all = 0
                    passed = 0
                group_d = key if value['member'].index(each) == 0 else ''
                tmp = {
                    'group_d': group_d,
                    'bd_d': each,
                    'all_d': str(all),
                    'pass_d': str(passed),
                    'pass_rate_d': '\\-' if all == 0 else self.percent_convert(passed / all)
                }
                group_table_detail.append(tmp)

        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        begin_time = start_time.subtract(days=1).strftime('%Y-%m-01')
        end_time = start_time.subtract(days=1).strftime('%Y-%m-%d')

        title = ' From ' + begin_time + ' to ' + end_time
        return {
            'order_date': title,
            'group_table_detail': group_table_detail,
            'group_table_summary': group_table_summary
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
            file_info: Dict
    ) -> Dict:
        return process_data_dict

    def send_card(self, file_info: Dict, data_dict: Dict):
        logger.info(f'发送卡片')
        group_table_detail = data_dict['group_table_detail']
        group_table_summary = data_dict['group_table_summary']
        order_date = data_dict['order_date']

        data = {
            'order_date': order_date,
            'group_table_detail': group_table_detail,
            'group_table_summary': group_table_summary
        }

        robot = self.feishu_robot
        robot.send_msg_card(data=data, card_id=self.card_id, version_name='1.0.0')


commission_verify_dag = AbstractCommissionVerify()
commission_verify_dag.create_dag()
