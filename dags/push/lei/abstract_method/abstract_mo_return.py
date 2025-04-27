import logging
from typing import Any, Dict

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.lei.abstract_method.abstract_base import AbstractDagTask

logger = logging.getLogger(__name__)


class AbstractMoReturn(AbstractDagTask):
    def __init__(self):
        super().__init__(
            dag_id='push_mo_return',
            schedule='0 9 * * *',
            default_args=
            {
                'owner': 'Lei Jiangling',
                'start_date': pendulum.datetime(2023, 1, 1)
            },
            tags=['push', 'mo_return'],
            robot_url=Variable.get('TEST')
        )
        self.card_id: str = 'AAqRPrHrP2wKb'

    def fetch_data(self, **kwargs) -> Dict:
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        begin_time = start_time.subtract(days=1).strftime('%Y-%m-%d')

        mo_detail_sql = """
            with tmp as (
                select
                     item_id
                    ,item_title
                    ,anchor_name
                    ,origin_gmv
                    ,final_gmv
                    ,1 - final_gmv/origin_gmv refund_rate
                from dws.dws_ks_big_tbl dkeh
                where
                    order_date = %(begin_time)s
                    and anchor_name = '墨晨夏'
                having origin_gmv > 500
                order by refund_rate desc)
            select * from tmp
            union all
            select
                "总计" item_id
                ,null item_title
                ,null anchor_name
                ,sum(origin_gmv) origin_gmv
                ,sum(final_gmv) final_gmv
                ,1 - sum(final_gmv)/sum(origin_gmv) refund_rate
            from tmp
        """
        mo_detail_df = pd.read_sql(mo_detail_sql, self.engine, params={'begin_time': begin_time})

        td_detail_sql = """
            with tmp as (
                select
                     item_id
                    ,item_title
                    ,anchor_name
                    ,origin_gmv
                    ,final_gmv
                    ,1 - final_gmv/origin_gmv refund_rate
                from dws.dws_ks_big_tbl dkeh
                where
                    order_date = %(begin_time)s
                    and
                    anchor_name in (
                        select
                            distinct
                            anchor_name
                        from dim.dim_ks_account_info dai
                        where other_commission_belong = '墨晨夏'
                    )
                    and anchor_name != '墨晨夏'
                having origin_gmv > 500
                order by refund_rate desc)
            select * from tmp
            union all
            select
                "合计" item_id
                ,null item_title
                ,null anchor_name
                ,sum(origin_gmv) origin_gmv
                ,sum(final_gmv) final_gmv
                ,1 - sum(final_gmv)/sum(origin_gmv) refund_rate
            from
                tmp
        """
        td_detail_df = pd.read_sql(td_detail_sql, self.engine, params={'begin_time': begin_time})

        group_total_sql = """
            with tmp as(
                select
                    anchor_name
                    ,sum(origin_gmv) origin_gmv
                    ,sum(final_gmv) final_gmv
                    ,1 - sum(final_gmv)/sum(origin_gmv) refund_rate
                from dws.dws_ks_big_tbl dkeh
                where
                    order_date = %(begin_time)s
                    and
                    anchor_name in (
                        select
                            distinct
                            anchor_name
                        from dim.dim_ks_account_info dkai
                        where other_commission_belong = '墨晨夏' or anchor_name = '墨晨夏'
                    )
                group by anchor_name
                having origin_gmv > 500
                order by refund_rate desc)
            select * from tmp
            union all
            select
                "合计" anchor_name
                ,sum(origin_gmv) origin_gmv
                ,sum(final_gmv) final_gmv
                ,1 - sum(final_gmv)/sum(origin_gmv) refund_rate
            from tmp
        """
        group_total_df = pd.read_sql(group_total_sql, self.engine, params={'begin_time': begin_time})

        return {
            'mo_detail_df': mo_detail_df,
            'td_detail_df': td_detail_df,
            'group_total_df': group_total_df
        }

    def process_data(self, data: Dict, **kwargs) -> Dict:
        mo_detail_data = data['mo_detail_df']
        td_detail_data = data['td_detail_df']
        group_total_data = data['group_total_df']

        mo_detail_data['refund_rate'] = mo_detail_data['refund_rate'].apply(self.percent_convert)
        td_detail_data['refund_rate'] = td_detail_data['refund_rate'].apply(self.percent_convert)
        group_total_data['refund_rate'] = group_total_data['refund_rate'].apply(self.percent_convert)

        mo_detail_data.rename(columns={
            'item_title': '商品名称',
            'item_id': '商品ID',
            'anchor_name': '主播',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'refund_rate': '退货率'
        })

        td_detail_data.rename(columns={
            'item_title': '商品名称',
            'item_id': '商品ID',
            'anchor_name': '主播',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'refund_rate': '退货率'
        })

        group_total_data.rename(columns={
            'item_title': '商品名称',
            'item_id': '商品ID',
            'anchor_name': '主播',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'refund_rate': '退货率'
        })

        return {
            'mo_detail_data': mo_detail_data,
            'td_detail_data': td_detail_data,
            'group_total_data': group_total_data
        }

    def create_feishu_file(self, data_dic: Dict, **kwargs) -> Dict:
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        title_date = start_time.subtract(days=1).strftime('%m月%d日')
        return {
            'spreadsheet_token': title_date,
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
        mo_detail_data = process_data_dict['mo_detail_data']
        td_detail_data = process_data_dict['td_detail_data']
        group_total_data = process_data_dict['group_total_data']

        title_date = spreadsheet_token

        # 墨晨夏商品明细
        style_dict = {
            'A1:' + self.col_convert(mo_detail_data.shape[1]) + str(
                mo_detail_data.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER'
            },
            'A3:A' + str(mo_detail_data.shape[0] + 1): {
                'backColor': '#DAEEF3'
            },
            'A' + str(mo_detail_data.shape[0] + 2) + ':C' + str(mo_detail_data.shape[0] + 2): {
                'backColor': '#FFFF00',
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER',
                'font': {
                    'bold': True
                }
            },
            'D' + str(mo_detail_data.shape[0] + 2) + ':F' + str(mo_detail_data.shape[0] + 2): {
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER'
            }
        }
        self.feishu_sheet.delete_row_col('FkUOsZFYzhxXMmtLbgucsZUcnTb', '147tf2', 'ROWS', 3, 200)
        self.feishu_sheet.add_row_col('FkUOsZFYzhxXMmtLbgucsZUcnTb', '147tf2', 'ROWS', 200)
        self.feishu_sheet.write_df_append(mo_detail_data, 'FkUOsZFYzhxXMmtLbgucsZUcnTb', '147tf2')
        self.feishu_sheet.write_cells('FkUOsZFYzhxXMmtLbgucsZUcnTb', '147tf2', 'A1:A1',
                                      [[f'{title_date}墨晨夏 挂车 卖货各品T+1退货率']])
        for key, value in style_dict.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token='FkUOsZFYzhxXMmtLbgucsZUcnTb',
                sheet_id='147tf2', ranges=key, styles=value
            )

        # 徒弟商品明细
        style_dict_td = {
            'A1:' + self.col_convert(td_detail_data.shape[1]) + str(
                td_detail_data.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER'
            },
            'A3:A' + str(td_detail_data.shape[0] + 1): {
                'backColor': '#DAEEF3'
            },
            'A' + str(td_detail_data.shape[0] + 2) + ':C' + str(td_detail_data.shape[0] + 2): {
                'backColor': '#FFFF00',
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER',
                'font': {
                    'bold': True
                }
            },
            'D' + str(td_detail_data.shape[0] + 2) + ':F' + str(td_detail_data.shape[0] + 2): {
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER'
            }
        }
        self.feishu_sheet.delete_row_col('FkUOsZFYzhxXMmtLbgucsZUcnTb', '17GwqQ', 'ROWS', 3, 200)
        self.feishu_sheet.add_row_col('FkUOsZFYzhxXMmtLbgucsZUcnTb', '17GwqQ', 'ROWS', 200)
        self.feishu_sheet.write_df_append(td_detail_data, 'FkUOsZFYzhxXMmtLbgucsZUcnTb', '17GwqQ')
        self.feishu_sheet.write_cells('FkUOsZFYzhxXMmtLbgucsZUcnTb', '17GwqQ', 'A1:A1',
                                      [[f'{title_date}墨晨夏团队 挂车 卖货各品T+1退货率']])
        for key, value in style_dict_td.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token='FkUOsZFYzhxXMmtLbgucsZUcnTb',
                sheet_id='17GwqQ', ranges=key, styles=value
            )

        # 团队汇总
        style_dict_total = {
            'A1:' + self.col_convert(group_total_data.shape[1]) + str(
                group_total_data.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER'
            },
            'A3:A' + str(group_total_data.shape[0] + 1): {
                'backColor': '#DAEEF3'
            },
            'A' + str(group_total_data.shape[0] + 2) + ':A' + str(group_total_data.shape[0] + 2): {
                'backColor': '#FFFF00',
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER',
                'font': {
                    'bold': True
                }
            },
            'B' + str(group_total_data.shape[0] + 2) + ':D' + str(group_total_data.shape[0] + 2): {
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER'
            }
        }
        self.feishu_sheet.delete_row_col('FkUOsZFYzhxXMmtLbgucsZUcnTb', '1b5wgU', 'ROWS', 3, 200)
        self.feishu_sheet.add_row_col('FkUOsZFYzhxXMmtLbgucsZUcnTb', '1b5wgU', 'ROWS', 200)
        self.feishu_sheet.write_df_append(group_total_data, 'FkUOsZFYzhxXMmtLbgucsZUcnTb', '1b5wgU')
        self.feishu_sheet.write_cells('FkUOsZFYzhxXMmtLbgucsZUcnTb', '1b5wgU', 'A1:A1',
                                      [[f'{title_date}墨晨夏 挂车 卖货T+1退货率']])
        for key, value in style_dict_total.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token='FkUOsZFYzhxXMmtLbgucsZUcnTb',
                sheet_id='1b5wgU', ranges=key, styles=value
            )

        return process_data_dict

    def send_card(self, url: str, title: str, data_dict: Dict):
        res = {
            'title': '退货率报表截图',
            'file_name': '退货率报表截图',
            'url': 'https://p0jq2j4e9xi.feishu.cn/sheets/FkUOsZFYzhxXMmtLbgucsZUcnTb?sheet=1dRnQ4',
            'description': '数据请见下方链接附件'
        }
        self.feishu_robot.send_msg_card(data=res, card_id=self.card_id, version_name='1.0.1')


mo_return_dag = AbstractMoReturn()
mo_return_dag.create_dag()
