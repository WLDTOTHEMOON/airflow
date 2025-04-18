import logging
from typing import Dict, Any

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.lei.abstract_method.abstract_base import AbstractDagTask

logger = logging.getLogger(__name__)


class AbstractReturnProduct(AbstractDagTask):

    def __init__(self):
        super().__init__(
            dag_id='push_return_product',
            schedule='0 21 * * *',
            default_args=
            {
                'owner': 'Lei Jiangling',
                'start_date': pendulum.datetime(2023, 1, 1),
                'provide_context': True
            },
            tags=['push', 'return_product'],
            robot_url=Variable.get('TEST'),
        )
        self.card_id: str = 'AAqRPrHrP2wKb'

    def fetch_data(self, **kwargs) -> Dict:
        sql = """
            select
                number
                ,inner_number
                ,pd.name pd_name
                ,wh_count
                ,spec
                ,sp.name sp_name
                ,us.name bd_name
                ,case 
                    when pd.status = 14 then '品控退回'
                    when pd.status = 20 then '成本退回'
                end pd_status
                ,updated_at
            from(
                select
                    user_id
                    ,supplier_id
                    ,id
                    ,name
                    ,status
                    ,number
                    ,inner_number
                    ,wh_count
                    ,spec
                    ,created_at
                    ,updated_at
                from xlsd.products p
                where status in (14,20)
                    and is_sale = 1
                    and by_anchor = 0) pd
            left join(
                select
                    distinct
                    target_id
                    ,act
                    ,result
                from xlsd.review r 
                where act = 4
                    and result = 11
                    and target = 'product'
            )rv on pd.id = rv.target_id
            left join(
                select
                    id
                    ,name
                    ,commerce
                from xlsd.suppliers s
            ) sp on pd.supplier_id = sp.id
            left join(
                select
                    id
                    ,name
                from xlsd.users u
            ) us on sp.commerce = us.id
            where
                date(created_at) <= '2024-04-06' or (date(created_at) > '2024-04-06' and result is not null)
                and us.name not in ('张小卓', '方涌超', '张澜', '赵乙都', '雷江玲', '管理员')
            order by
                updated_at desc
        """
        return_product_df = pd.read_sql(sql, con=self.engine)
        return {
            'return_product_df': return_product_df
        }

    def process_data(self, data: Dict, **kwargs) -> Dict:
        process_data = data['return_product_df']
        process_data.updated_at = process_data.updated_at.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

        process_data = process_data.rename(columns={
            'pd_name': '商品名称',
            'number': '编号',
            'inner_number': '自编码',
            'sp_name': '供应商名称',
            'bd_name': '商务名称',
            'wh_count': '库存',
            'spec': '商品规格',
            'pd_status': '状态',
            'updated_at': '退回时间'
        })
        return {
            'process_data': process_data
        }

    def create_feishu_file(self, data_dic: Dict, **kwargs) -> Dict:
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        title = '品控成本退回数据_' + start_time.strftime('%Y%m%d%H%M%S')

        result = self.feishu_sheet.create_spreadsheet(
            title=title, folder_token='B0VAfm0XklFlyddHXG6crq8enDg'
        )
        spreadsheet_token = result['spreadsheet']['spreadsheet_token']
        url = result['spreadsheet']['url']
        cps_sheet_id = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='Result')
        cps_sheet_id = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        return {
            'spreadsheet_token': spreadsheet_token,
            'cps_sheet_id': cps_sheet_id,
            'url': url,
            'title': title
        }

    def render_feishu_format(
            self,
            processed_data: Dict,
            spreadsheet_token: str,
            sheet_id: str
    ):
        self.feishu_sheet.write_df_replace(processed_data['process_data'], spreadsheet_token, sheet_id)

        return processed_data

    def send_card(self, url: str, title: str, data_dic: Dict):
        data = {
            'title': '品控成本退回数据',
            'file_name': title,
            'url': url,
            'description': '数据请见下方链接附件'
        }

        robot = self.feishu_robot
        robot.send_msg_card(data=data, card_id=self.card_id, version_name='1.0.1')


return_product_dag = AbstractReturnProduct()
return_product_dag.create_dag()
