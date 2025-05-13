import logging
from typing import Dict

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.lei.abstract_method.abstract_base import AbstractDagTask

logger = logging.getLogger(__name__)


class AbstractSupplementFlow(AbstractDagTask):
    def __init__(self):
        super().__init__(
            dag_id='push_supplement_flow',
            schedule='0 21 * * *',
            default_args=
            {
                'owner': 'Lei Jiangling',
                'start_date': pendulum.datetime(2023, 1, 1),
                'provide_context': True
            },
            tags=['push', 'supplement_flow'],
            robot_url=Variable.get('CRYPTO'),
        )
        self.card_id: str = 'AAqRPrHrP2wKb'

    def fetch_data(self, **kwargs) -> Dict:
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        begin_time = start_time.subtract(days=92).strftime('%Y-%m-01')
        logger.info(f'获取数据, 数据开始日期:{begin_time}')

        sql = f"""
            select
                first_sell_date
                ,name.item_id
                ,item_title
                ,anchor_name
                ,origin_gmv
            from(
                select
                    item_id
                    ,GROUP_CONCAT(anchor_name ORDER BY anchor_name SEPARATOR ', ') anchor_name
                from(
                    select
                        distinct
                        item_id
                        ,anchor_name
                    from dws.dws_ks_big_tbl dbth
                    where order_date >= %(begin_time)s and order_date < current_date
                        and product_id is null
                        and account_id not in (
                            select
                                account_id
                            from dim.dim_ks_account_info dai
                            where group_leader = '刘海州'
                            )
                    )src
                group by
                    item_id
            ) name
            left join(
                select
                    item_id
                    ,sum(origin_gmv) origin_gmv
                from dws.dws_ks_big_tbl dbth
                where order_date >= %(begin_time)s and order_date < current_date
                    and product_id is null
                    and account_id not in (
                        select
                            account_id 
                        from dim.dim_ks_account_info dai
                        where group_leader = '刘海州'
                        )
                group by
                    item_id
            ) gmv on name.item_id = gmv.item_id
            left join(
                select
                    item_id
                    ,item_title
                from(
                    select
                        item_id
                        ,item_title
                        ,row_number() over (partition by item_id order by item_title desc) rn
                    from dws.dws_ks_big_tbl dbth ) info
                where rn = 1
            )iname on name.item_id = iname.item_id
            left join(
                select
                    order_date first_sell_date
                    ,item_id
                from(
                    select
                        order_date
                        ,item_id
                        ,row_number() over (partition by item_id order by order_date asc) rn
                    from dws.dws_ks_big_tbl dkeh 
                    where order_date >= '2024-10-01' and order_date < current_date) src
                where rn = 1
            )fsd on gmv.item_id = fsd.item_id
            order by
                origin_gmv desc
        """
        supplement_flow_data = pd.read_sql(sql, con=self.engine, params={'begin_time': begin_time})

        return {
            'supplement_flow_data': supplement_flow_data
        }

    def process_data(self, data: Dict, **kwargs) -> Dict:
        supplement_flow_df = data['supplement_flow_data']
        supplement_flow_df['first_sell_date'] = supplement_flow_df['first_sell_date'].astype(str)
        supplement_flow_df = supplement_flow_df.rename(
            columns={
                'first_sell_date': '初次售卖日期',
                'item_id': '商品ID',
                'item_title': '商品名称',
                'anchor_name': '售卖主播',
                'origin_gmv': '支付GMV'
            }
        )
        return {
            'process_supplement_flow_df': supplement_flow_df
        }

    def create_feishu_file(self, process_data_dict: Dict, **kwargs) -> Dict:
        logger.info(f'创建飞书文件')
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        title = ('待补流程链接_' + start_time.subtract(days=92).strftime('%Y%m01') + '_' +
                 start_time.subtract(days=1).strftime('%Y%m%d') + '_' + start_time.strftime('%Y%m%d%H%M'))

        result = self.feishu_sheet.create_spreadsheet(
            title=title, folder_token='AolgfOvb7lzF2KdWj6hclit2nvb'
        )
        spreadsheet_token = result['spreadsheet']['spreadsheet_token']
        url = result['spreadsheet']['url']
        cps_sheet_id = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='CPS')
        cps_sheet_id = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        return {
            'spreadsheet_token': spreadsheet_token,
            'cps_sheet_id': cps_sheet_id,
            'url': url,
            'title': title
        }

    def render_feishu_format(
            self,
            process_data_dict: Dict,
            file_info: Dict
    ) -> Dict:
        logger.info(f'渲染飞书格式')
        process_data = process_data_dict['process_supplement_flow_df']

        spreadsheet_token = file_info['spreadsheet_token']
        cps_sheet_id = file_info['cps_sheet_id']

        cps_style_dict = {
            'A1:' + self.col_convert(process_data.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                'backColor': '#FFFF00'
            },
            'A1:' + self.col_convert(process_data.shape[1]) + str(process_data.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER'
            }
        }

        self.feishu_sheet.write_df_replace(dat=process_data, spreadsheet_token=spreadsheet_token,
                                           sheet_id=cps_sheet_id)

        for key, value in cps_style_dict.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=cps_sheet_id, ranges=key, styles=value
            )
        return process_data_dict

    def send_card(self, file_info: Dict, process_data_dict):
        logger.info(f'发送卡片')
        title = file_info['title']
        url = file_info['url']

        data = {
            'title': '待补流程链接',
            'file_name': title,
            'url': url,
            'description': '数据请见下方链接附件'
        }

        robot = self.feishu_robot
        robot.send_msg_card(data=data, card_id=self.card_id, version_name='1.0.1')


supplement_flow_dag = AbstractSupplementFlow()
supplement_flow_dag.create_dag()
