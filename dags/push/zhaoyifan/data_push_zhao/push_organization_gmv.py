from abc import ABC
from typing import Dict, Any
from dags.push.zhaoyifan.data_push_zhao.format_utils import *
import pendulum
import pandas as pd
from airflow.models import Variable
from dags.push.zhaoyifan.data_push_zhao.base_dag import BaseDag


class LeMoItem(BaseDag):
    def __init__(self):
        super().__init__(
            dag_id='push_organization_gmv',
            default_args={'owner': 'zhaoyifan'},
            tags=['push', 'item'],
            robot_url=Variable.get('SELFTEST'),
            schedule=None
        )
        self.card_id = 'AAq4uwQlcFsdQ'

    def fetch_data_logic(self, date_interval: Dict[str, Any]) -> Dict[str, Any]:
        gmv_sql = f'''
                    -- 公会GMV             
                    with tmp as (
                    select
                        anchor_name
                        ,origin_gmv+pk_final_gmv origin_gmv
                        ,final_gmv+pk_final_gmv final_gmv
                    from (
                        select
                            anchor_name
                            ,sum(origin_gmv) origin_gmv
                            ,sum(final_gmv) final_gmv
                            ,0 pk_final_gmv
                        from dws.dws_ks_ec_2hourly dkeh 
                        where order_date = '{date_interval['yes_ds']}'
                        group by
                            anchor_name
                        union all
                        select
                            anchor_name
                            ,0 origin_gmv
                            ,0 final_gmv
                            ,sum(final_gmv)pk_final_gmv
                        from dwd.dwd_order_pk_detail
                        where live_date = '{date_interval['yes_ds']}'
                        group by
                            anchor_name
                    )src
                    order by
                        origin_gmv desc
                    )
                    select
                        *
                    from (
                        select
                            *
                        from
                            tmp
                        union all	
                        select
                            '总计' anchor_name
                            ,sum(origin_gmv) origin_gmv
                            ,sum(final_gmv) final_gmv
                        from
                            tmp
                    )src
                    order by
                        case when anchor_name = '总计' then 1 else 0 end,origin_gmv desc
                '''
        gmv_df = pd.read_sql(gmv_sql, self.engine)

        return {
            'gmv_df': gmv_df,
            'date_interval': date_interval
        }

    def prepare_card_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        return {}

    def write_to_sheet_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        gmv_df = data_dict['gmv_df']
        style_dict = {
            'A1:' + col_convert(gmv_df.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                'backColor': '#FFFF00'
            },
            'A1:' + col_convert(gmv_df.shape[1]) + str(gmv_df.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER'
            }
        }

        gmv_df.rename(columns={
            'anchor_name': '主播名', 'origin_gmv': '支付GMV', 'final_gmv': '结算GMV'
        }, inplace=True)

        date_interval = data_dict['date_interval']
        sheet_title = f"公会运营GMV_{date_interval['yes_time']}_{date_interval['now_time']}"
        url, spreadsheet_token = self.feishu_sheet_supply.get_workbook_params(
            workbook_name=sheet_title, folder_token='EQnZf3u82ljlh2dXr3Ec3GbPnvd'
        )
        sheet_id = self.feishu_sheet_supply.get_sheet_params(spreadsheet_token, 'result')
        self.feishu_sheet.write_df_replace(gmv_df, spreadsheet_token, sheet_id, to_char=False)

        for key, value in style_dict.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=sheet_id, ranges=key, styles=value
            )

        return {
            'file_name': sheet_title,
            'url': url
        }

    def send_card_logic(self, card: Dict[str, Any], sheet):
        res = {
            'title': '乐总和墨晨夏售卖商品数据列表',
            **sheet,
            'description': '数据请见下方链接附件'
        }
        self.feishu_robot.send_msg_card(data=res, card_id=self.card_id, version_name='1.0.0')


dag = LeMoItem().create_dag()
