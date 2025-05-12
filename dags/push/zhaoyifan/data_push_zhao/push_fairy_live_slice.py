import logging
from typing import Dict, Any

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.zhaoyifan.data_push_zhao.base_dag import BaseDag
from dags.push.zhaoyifan.data_push_zhao.format_utils import *

logger = logging.getLogger(__name__)


class FairyLiveSlice(BaseDag):
    def __init__(self):
        super().__init__(
            dag_id='push_fairy_live_slice',
            robot_url=Variable.get('TEST'),
            tags=['push', 'fairy_slice'],
            schedule='0 5 * * *'
        )
        self.card_id = 'AAq4rbCyGwNeJ'

    def fetch_data_logic(self, date_interval: Dict[str, Any]) -> Dict[str, Any]:
        all_sql = f'''
            select 
                order_date 
                ,'小仙女' slice_belong
                ,port
                ,account_id
                ,if(port = 'A', concat('剪手(',account_id,')'), concat('剪手(',port,'端)')) account_belong
                ,item_id 
                ,item_title 
                ,origin_gmv 
                ,origin_order_number 
                ,final_gmv 
                ,final_order_number 
                ,coalesce(estimated_income,0) + coalesce(estimated_service_income,0) tol_income 
                ,update_at
            from dws.dws_ks_slice_slicer dkss      
            where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                and account_id = '146458792'
            union all
            select 
                order_date 
                ,'小仙女' slice_belong
                ,port
                ,account_id
                ,if(port = 'A',concat('二创(',author_id ,')'), concat('二创(',port,'端)')) account_belong
                ,item_id 
                ,item_title 
                ,origin_gmv 
                ,origin_order_number 
                ,final_gmv 
                ,final_order_number 
                ,coalesce(estimated_income,0) + coalesce(estimated_service_income,0) tol_income 
                ,update_at
            from dws.dws_ks_slice_recreation dksr 
            where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                and account_id = '146458792'
        '''
        all_df = pd.read_sql(all_sql, self.engine)
        yes_tol_df = all_df[all_df.order_date.astype(str) == date_interval['yes_ds']]
        yes_tol_df = pd.DataFrame(
            yes_tol_df[['origin_gmv', 'final_gmv', 'origin_order_number', 'tol_income']].sum()).T
        tol_df = pd.DataFrame(all_df[['origin_gmv', 'final_gmv', 'origin_order_number', 'tol_income']].sum()).T

        merge_sql = f'''
            select 
                order_date 
                ,'小仙女' slice_belong
                ,port
                ,account_id
                ,if(port = 'A', concat('剪手(',account_id,')'), concat('剪手(',port,'端)')) account_belong
                ,item_id 
                ,item_title 
                ,origin_order_number 
                ,final_order_number 
                ,update_at
            from dws.dws_ks_slice_slicer dkss  
            where account_id = '146458792'
            union all
            select 
                order_date 
                ,'小仙女' slice_belong
                ,port
                ,account_id
                ,if(port = 'A',concat('二创(',author_id ,')'), concat('二创(',port,'端)')) account_belong
                ,item_id 
                ,item_title 
                ,origin_order_number 
                ,final_order_number 
                ,update_at
            from dws.dws_ks_slice_recreation dksr
            where account_id = '146458792'
        '''

        month_rank_items_sql = f'''
            select 
                row_number() over(order by current_origin_order_number desc,item_id) row_num
                ,item_id
                ,item_title
                ,current_origin_order_number
                ,current_return_rate
                ,tol_origin_order_number
                ,tol_return_rate
            from (
                select 
                      tmp.item_id
                      ,item.item_title
                      ,current_origin_order_number
                      ,1 - current_final_order_number / current_origin_order_number current_return_rate
                      ,tol_origin_order_number
                      ,1 - tol_final_order_number / tol_origin_order_number tol_return_rate 
                from (
                    select 
                        item_id 
                        ,sum(
                            case 
                              when order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'  then origin_order_number
                              else 0
                          end
                        ) current_origin_order_number
                        ,sum(
                            case 
                              when order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'  then final_order_number
                              else 0
                          end
                        ) current_final_order_number
                        ,sum(origin_order_number) tol_origin_order_number
                        ,sum(final_order_number) tol_final_order_number
                    from ({merge_sql}) slice
                    group by 
                        item_id
                ) tmp
                left join (
                    select 
                        item_id
                        ,item_title
                    from (
                        select 
                            item_id
                            ,item_title
                            ,row_number() over(partition by item_id order by update_at desc) rn
                        from dws.dws_ks_slice_daily dksd 
                    ) src 
                    where rn = 1 
                ) item on tmp.item_id = item.item_id
            ) tol
        '''
        month_rank_items_df = pd.read_sql(month_rank_items_sql, self.engine)

        yes_item_sql = f'''
            select
                row_number() over(order by origin_order_number desc) row_num
                ,item_id	
                ,item_title	
                ,origin_order_number	
                ,return_rate	
                ,account_id
            from (
                select 
                    item_id
                    ,item_title
                    ,sum(origin_order_number) origin_order_number
                    ,1 - sum(final_order_number) / sum(origin_order_number) return_rate
                    ,account_id
                from ({merge_sql}) slice
                where order_date = '{date_interval['yes_ds']}'
                group by 
                    item_id
                    ,item_title
                    ,account_id
            ) tmp
        '''
        yes_rank_items_df = pd.read_sql(yes_item_sql, self.engine)

        return {
            'all_df': all_df,
            'yes_tol_df': yes_tol_df,
            'tol_df': tol_df,
            'month_rank_items_df': month_rank_items_df,
            'yes_rank_items_df': yes_rank_items_df,
            'date_interval': date_interval
        }

    def prepare_card_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        yes_tol_df = data_dict['yes_tol_df']
        tol_df = data_dict['tol_df']

        # 日切片整体业绩
        slice_yes_origin_gmv = str(round(yes_tol_df.origin_gmv.iloc[0], 2))
        slice_yes_final_gmv = str(round(yes_tol_df.final_gmv.iloc[0], 2))
        slice_yes_origin_num = str(int(yes_tol_df.origin_order_number.iloc[0]))
        slice_yes_income = str(round(yes_tol_df.tol_income.iloc[0], 2))

        # 月切片整体业绩
        slice_tol_origin_gmv = str(round(tol_df.origin_gmv.iloc[0], 2))
        slice_tol_final_gmv = str(round(tol_df.final_gmv.iloc[0], 2))
        slice_tol_origin_num = str(int(tol_df.origin_order_number.iloc[0]))
        slice_tol_income = str(round(tol_df.tol_income.iloc[0], 2))

        return {
            'time_scope': data_dict['date_interval']['month_start_ds'] + ' to ' + data_dict['date_interval']['yes_ds'],
            'slice_yes_date': data_dict['date_interval']['yes_ds'],
            'slice_yes_origin_gmv': slice_yes_origin_gmv,
            'slice_yes_final_gmv': slice_yes_final_gmv,
            'slice_yes_origin_num': slice_yes_origin_num,
            'slice_yes_income': slice_yes_income,
            'slice_tol_origin_gmv': slice_tol_origin_gmv,
            'slice_tol_final_gmv': slice_tol_final_gmv,
            'slice_tol_origin_num': slice_tol_origin_num,
            'slice_tol_income': slice_tol_income,
        }

    def write_to_sheet_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        # 切片卖货数据
        all_df = data_dict['all_df']
        belong_sum_df = all_df.groupby(['slice_belong', 'port', 'account_belong'], as_index=False).sum(
            numeric_only=True)
        belong_sum_df = belong_sum_df.sort_values(['slice_belong', 'port'])
        belong_sum_df.rename(columns={
            "slice_belong": "切片归属",
            "account_belong": "账号归属",
            'port': '端口',
            "origin_gmv": "支付GMV",
            "origin_order_number": "支付订单数量",
            "final_gmv": "结算GMV",
            "final_order_number": "结算订单数量",
            "tol_income": "预估收入",
        }, inplace=True)

        month_rank_items_df = data_dict['month_rank_items_df']
        month_rank_items_df.rename(columns={
            'row_num': '排名',
            'item_title': '商品名称',
            'item_id': '商品ID',
            'current_origin_order_number': '当月订单总数',
            'current_return_rate': '当月退货率',
            'tol_origin_order_number': '累计订单总数',
            'tol_return_rate': '总退货率'
        }, inplace=True)

        yes_rank_items_df = data_dict['yes_rank_items_df']
        yes_rank_items_df.rename(columns={
            'row_num': '排名',
            'item_title': '商品名称',
            'item_id': '商品ID',
            'origin_order_number': '昨日订单数',
            'return_rate': '昨日退货率',
            'account_id': '账号'
        }, inplace=True)

        tol_df = data_dict['tol_df']
        tol_df.rename(columns={
            'slice_belong': '切片归属',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'origin_order_number': '支付订单数',
            'tol_income': '预估收入'
        }, inplace=True)

        sheet_title = f"小仙女切片卖货数据_{data_dict['date_interval']['month_start_time']}_{data_dict['date_interval']['yes_time']}_{data_dict['date_interval']['now_time']}"
        url, spreadsheet_token = self.feishu_sheet_supply.get_workbook_params(
            workbook_name=sheet_title, folder_token='HjCxf6ounlapzvdR1GqciIqTnMQ'
        )
        slice_sheet_id = self.feishu_sheet_supply.get_sheet_params(
            spreadsheet_token=spreadsheet_token, sheet_name='切片卖货数据'
        )
        month_item_sheet_id = self.feishu_sheet_supply.get_sheet_params(
            spreadsheet_token=spreadsheet_token, sheet_name='小仙女当月商品排名'
        )
        yes_item_sheet_id = self.feishu_sheet_supply.get_sheet_params(
            spreadsheet_token=spreadsheet_token, sheet_name='小仙女昨日商品排名'
        )
        total_sheet_id = self.feishu_sheet_supply.get_sheet_params(
            spreadsheet_token=spreadsheet_token, sheet_name='月汇总数据'
        )

        self.feishu_sheet.write_df_replace(
            dat=belong_sum_df, spreadsheet_token=spreadsheet_token, sheet_id=slice_sheet_id, to_char=False)
        self.feishu_sheet.write_df_replace(
            dat=month_rank_items_df, spreadsheet_token=spreadsheet_token, sheet_id=month_item_sheet_id, to_char=False)
        self.feishu_sheet.write_df_replace(
            dat=yes_rank_items_df, spreadsheet_token=spreadsheet_token, sheet_id=yes_item_sheet_id, to_char=False)
        self.feishu_sheet.write_df_replace(
            dat=tol_df, spreadsheet_token=spreadsheet_token, sheet_id=total_sheet_id, to_char=False)

        return {
            'sheet_params': {
                'sheet_title': sheet_title,
                'url': url
            },
            'date_interval': data_dict['date_interval']
        }

    def send_card_logic(self, card: Dict[str, Any], sheet):
        res = {
            **card,
            **sheet['sheet_params']
        }
        self.feishu_robot.send_msg_card(data=res, card_id=self.card_id, version_name='1.0.0')


dag = FairyLiveSlice().create_dag()
