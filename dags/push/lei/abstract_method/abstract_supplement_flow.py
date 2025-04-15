import logging
import string
from typing import Any, Dict

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.lei.abstract_method.abstract_base import AbstractDagTask

logger = logging.getLogger(__name__)


class AbstractSupplementFlow(AbstractDagTask):
    def __init__(self):
        super().__init__(
            dag_id='push_supplement_flow',
            schedule=None,
            default_args={'owner': 'Lei Jiangling'},
            tags=['push', 'supplement_flow'],
            robot_url=Variable.get('TEST'),
        )
        self.start_time: str = pendulum.now('Asia/Shanghai').subtract(days=92).strftime('%Y-%m-01')
        self.card_id: str = 'AAqRPrHrP2wKb'
        self.title: str = ('待补流程链接_' + pendulum.now('Asia/Shanghai').subtract(days=92).strftime('%Y%m01') + '_' +
                           pendulum.now('Asia/Shanghai').subtract(days=1).strftime('%Y%m%d') +
                           pendulum.now('Asia/Shanghai').strftime('%Y%m%d%H%M'))

    def fetch_data(self) -> pd.DataFrame:
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
                    from dws.dws_ks_ec_2hourly dbth
                    where order_date >= %(start_time)s and order_date < current_date
                        and product_id is null
                        and account_id not in (
                            select
                                account_id
                            from dim.dim_ks_anchor_info dai
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
                from dws.dws_ks_ec_2hourly dbth
                where order_date >= %(start_time)s and order_date < current_date
                    and product_id is null
                    and account_id not in (
                        select
                            account_id 
                        from dim.dim_ks_anchor_info dai
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
                    from dws.dws_ks_ec_2hourly dbth ) info
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
                    from dws.dws_ks_ec_2hourly dkeh 
                    where order_date >= '2024-10-01' and order_date < current_date) src
                where rn = 1
            )fsd on gmv.item_id = fsd.item_id
            order by
                origin_gmv desc
        """
        return pd.read_sql(sql, con=self.engine, params={'start_time': self.start_time})

    def process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        supplement_flow_df = data
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
        return supplement_flow_df

    def create_feishu_file(self) -> Dict:
        logger.info(f'创建飞书文件')

        result = self.feishu_sheet.create_spreadsheet(
            title=self.title, folder_token='GIygfK0b5lndXCdIZqUcBygan4c'
        )
        spreadsheet_token = result['spreadsheet']['spreadsheet_token']
        url = result['spreadsheet']['url']
        cps_sheet_id = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='CPS')
        cps_sheet_id = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        return {
            'spreadsheet_token': spreadsheet_token,
            'cps_sheet_id': cps_sheet_id,
            'url': url
        }

    def render_feishu_format(
            self,
            processed_data: pd.DataFrame,
            spreadsheet_token: str,
            cps_sheet_id: str
    ):
        def col_convert(col_num: int):
            alphabeta = string.ascii_uppercase[:26]
            alphabeta = [i for i in alphabeta] + [i + j for i in alphabeta for j in alphabeta]
            return alphabeta[col_num - 1]

        logger.info(f'渲染飞书格式')
        # 示例渲染
        logger.info(spreadsheet_token)
        logger.info(cps_sheet_id)

        cps_style_dict = {
            'A1:' + col_convert(processed_data.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                'backColor': '#FFFF00'
            },
            'A1:' + col_convert(processed_data.shape[1]) + str(processed_data.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER'
            }
        }

        self.feishu_sheet.write_df_replace(dat=processed_data, spreadsheet_token=spreadsheet_token,
                                           sheet_id=cps_sheet_id)

        for key, value in cps_style_dict.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=cps_sheet_id, ranges=key, styles=value
            )

    def send_card(self, url):
        logger.info(f'发送卡片')

        data = {
            'title': '待补流程链接',
            'file_name': self.title,
            'url': url,
            'description': '数据请见下方链接附件'
        }

        robot = self.feishu_robot
        robot.send_msg_card(data=data, card_id=self.card_id, version_name='1.0.1')


supplement_flow_dag = AbstractSupplementFlow()
supplement_flow_dag.create_dag()
