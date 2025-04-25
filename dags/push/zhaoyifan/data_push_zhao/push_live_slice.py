import logging
from typing import Dict, Any

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.zhaoyifan.data_push_zhao.base_dag import BaseDag

logger = logging.getLogger(__name__)


class AbstractCrawlerLog(BaseDag):
    def __init__(self):
        super().__init__(
            dag_id='push_live_slice',
            default_args={'owner': 'zhaoyifan'},
            robot_url='SELFTEST',
            tags=['push', 'live_slice'],
            schedule=None
        )
        self. card_id = 'AAqRWKhJEyCYM',

    def fetch_data_logic(self, date_interval: Dict[str, Any]) -> Dict[str, Any]:
        all_sql = f"""
            select 
            order_date 
            ,case
                  when account_id in ('146458792') then '小仙女'
                  when port like '电商MCN%%' then '电商MCN(乐总)'
                  else '乐总'
                end slice_belong
            ,port 
            ,case 
                when port = 'A' and author_id = '-' then account_id
                when port = 'A' and author_id != '-' then author_id
                when port = '电商MCN_A' then account_id
                when port = 'B' and author_id = '-' then 'B端'
                when port = 'B' and author_id = '999999' then '二创(B端)'
                when port = '电商MCN_B' then '二创(B端)'
                when port = 'C' then 'C端'
              end account_id  
            ,author_id 
            ,item_id 
            ,item_title 
            ,origin_gmv 
            ,origin_order_number 
            ,final_gmv 
            ,final_order_number 
            ,coalesce(estimated_income,0) + coalesce(estimated_service_income,0) tol_income 
        from dws.dws_ks_slice_daily dksd 
        where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
        """
        month_item_ranks = f'''
            select 
                row_number() over(partition by slice_belong order by current_origin_order_number desc,item_id) row_num
                ,item_id
                ,item_title
                ,slice_belong
                ,current_origin_order_number
                ,current_return_rate
                ,tol_origin_order_number
                ,tol_return_rate
            from (
                select 
                      tmp.item_id
                      ,item.item_title
                      ,slice_belong
                      ,current_origin_order_number
                      ,1 - current_final_order_number / current_origin_order_number current_return_rate
                      ,tol_origin_order_number
                      ,1 - tol_final_order_number / tol_origin_order_number tol_return_rate 
                from (
                    select 
                        item_id 
                        ,case
                          when account_id in ('146458792') then '小仙女'
                          when port like '电商MCN%%' then '电商MCN(乐总)'
                          else '乐总'
                        end slice_belong
                        ,sum(
                            case 
                              when order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}' then origin_order_number
                              else 0
                          end
                        ) current_origin_order_number
                        ,sum(
                            case 
                              when order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}' then final_order_number
                              else 0
                          end
                        ) current_final_order_number
                        ,sum(origin_order_number) tol_origin_order_number
                        ,sum(final_order_number) tol_final_order_number
                    from dws.dws_ks_slice_daily dksd
                    group by 
                        item_id
                        ,slice_belong
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
        yes_item_ranks = f"""
            select
                row_number() over(partition by slice_belong order by origin_order_number desc) row_num
                ,item_id	
                ,item_title	
                ,origin_order_number	
                ,return_rate	
                ,account
                ,slice_belong
            from (
                select 
                    item_id
                    ,item_title
                    ,sum(origin_order_number) origin_order_number
                    ,1 - sum(final_order_number) / sum(origin_order_number) return_rate
                    ,case 
                        when account_id = '3054930335' then '切片01'
                        when account_id = '3892258892' then '切片02'
                        else account_id
                    end account
                    ,case
                      when account_id in ('146458792') then '小仙女'
                      when port like '电商MCN%%' then '电商MCN(乐总)'
                      else '乐总'
                    end slice_belong
                from dws.dws_ks_slice_daily dksd 
                where order_date = '{date_interval['yes_ds']}'
                group by 
                item_id
                ,item_title
                ,account
                ,slice_belong
            ) tmp
        """

        all_df = pd.read_sql(all_sql, self.engine)
        # 日切片整体业绩
        yes_tol_df = all_df[all_df.order_date.astype(str) >= date_interval['yes_ds']]
        yes_tol_df = pd.DataFrame(
            yes_tol_df[['origin_gmv', 'final_gmv', 'origin_order_number', 'tol_income']].sum()).T

        # 月切片整体业绩
        tol_df = pd.DataFrame(all_df[['origin_gmv', 'final_gmv', 'origin_order_number', 'tol_income']].sum()).T

        # 日各切片整体业绩
        yes_each_tol_df = all_df[all_df.order_date.astype(str) >= date_interval['yes_ds']]
        yes_each_tol_df = yes_each_tol_df[[
            'slice_belong', 'origin_gmv', 'final_gmv', 'origin_order_number', 'tol_income'
        ]].groupby('slice_belong', as_index=False).sum()

        # 月各切片整体业绩
        each_tol_df = all_df[[
            'slice_belong', 'origin_gmv', 'final_gmv', 'origin_order_number', 'tol_income'
        ]].groupby('slice_belong', as_index=False).sum()

        # 月汇总数据
        total_row = each_tol_df.sum(numeric_only=True)  # 仅对数值列求和
        total_row['slice_belong'] = '总计'
        df_total = pd.concat([each_tol_df, pd.DataFrame([total_row])], ignore_index=True)

        # 商品数据
        accum_rank_items = pd.read_sql(month_item_ranks, self.engine)
        accum_le_rank_items = accum_rank_items[accum_rank_items.slice_belong == '乐总']
        accum_le_rank_items = accum_le_rank_items[[
            'row_num', 'item_id', 'item_title', 'current_origin_order_number', 'current_return_rate',
            'tol_origin_order_number', 'tol_return_rate'
        ]]
        accum_other1_rank_items = accum_rank_items[accum_rank_items.slice_belong == '小仙女']
        accum_other1_rank_items = accum_other1_rank_items[[
            'row_num', 'item_id', 'item_title', 'current_origin_order_number', 'current_return_rate',
            'tol_origin_order_number', 'tol_return_rate'
        ]]
        accum_other2_rank_items = accum_rank_items[accum_rank_items.slice_belong == '电商MCN(乐总)']
        accum_other2_rank_items = accum_other2_rank_items[[
            'row_num', 'item_id', 'item_title', 'current_origin_order_number', 'current_return_rate',
            'tol_origin_order_number', 'tol_return_rate'
        ]]

        yes_rank_items = pd.read_sql(yes_item_ranks, self.engine)
        yes_le_rank_items = yes_rank_items[yes_rank_items.slice_belong == '乐总']
        yes_le_rank_items = yes_le_rank_items[[
            'row_num', 'item_id', 'item_title', 'origin_order_number', 'return_rate', 'account'
        ]]
        yes_other1_rank_items = yes_rank_items[yes_rank_items.slice_belong == '小仙女']
        yes_other1_rank_items = yes_other1_rank_items[[
            'row_num', 'item_id', 'item_title', 'origin_order_number', 'return_rate', 'account'
        ]]
        yes_other2_rank_items = yes_rank_items[yes_rank_items.slice_belong == '电商MCN(乐总)']
        yes_other2_rank_items = yes_other2_rank_items[[
            'row_num', 'item_id', 'item_title', 'origin_order_number', 'return_rate', 'account'
        ]]

        return {
            'df': {
                'all_df': all_df,
                'accum_rank_items': accum_rank_items,
                'yes_rank_items': yes_rank_items,
                'yes_tol_df': yes_tol_df,
                'tol_df': tol_df,
                'yes_each_tol_df': yes_each_tol_df,
                'each_tol_df': each_tol_df,
                'df_total': df_total,
                'accum_le_rank_items': accum_le_rank_items,
                'accum_other1_rank_items': accum_other1_rank_items,
                'accum_other2_rank_items': accum_other2_rank_items,
                'yes_le_rank_items': yes_le_rank_items,
                'yes_other1_rank_items': yes_other1_rank_items,
                'yes_other2_rank_items': yes_other2_rank_items
            },
            'date_interval': date_interval
        }

    def prepare_card_logic(self, data: Dict, **kwargs) -> Dict:
        yes_each_tol_df = data['df']['yes_each_tol_df']
        yes_group_slice_tol = []
        for i in range(yes_each_tol_df.shape[0]):
            yes_group_slice_tol.append({
                'belong': yes_each_tol_df.slice_belong.iloc[i],
                'origin_gmv': str(round(yes_each_tol_df.origin_gmv.iloc[i], 2)),
                'final_gmv': str(round(yes_each_tol_df.final_gmv.iloc[i], 2)),
                'origin_order_number': str(int(yes_each_tol_df.origin_order_number.iloc[i])),
                'tol_income': str(round(yes_each_tol_df.tol_income.iloc[i], 2))
            })
        each_tol_df = data['df']['each_tol_df']
        group_slice_tol = []
        for i in range(each_tol_df.shape[0]):
            group_slice_tol.append({
                'belong': each_tol_df.slice_belong.iloc[i],
                'origin_gmv': str(round(each_tol_df.origin_gmv.iloc[i], 2)),
                'final_gmv': str(round(each_tol_df.final_gmv.iloc[i], 2)),
                'origin_order_number': str(int(each_tol_df.origin_order_number.iloc[i])),
                'tol_income': str(round(each_tol_df.tol_income.iloc[i], 2))
            })
        return {
            'time_scope': data['date_interval']['month_start_ds'] + ' to ' + data['date_interval']['yes_ds'],
            'slice_yes_date': data['date_interval']['yes_ds'],
            'slice_yes_origin_gmv': str(round(data['df']['yes_tol_df'].origin_gmv.iloc[0], 2)),
            'slice_yes_final_gmv': str(round(data['df']['yes_tol_df'].final_gmv.iloc[0], 2)),
            'slice_yes_origin_num': str(round(data['df']['yes_tol_df'].origin_order_number.iloc[0], 2)),
            'slice_yes_income': str(round(data['df']['yes_tol_df'].tol_income.iloc[0], 2)),
            'slice_tol_origin_gmv': str(round(data['df']['tol_df'].origin_gmv.iloc[0], 2)),
            'slice_tol_final_gmv': str(round(data['df']['tol_df'].final_gmv.iloc[0], 2)),
            'slice_tol_origin_num': str(round(data['df']['tol_df'].origin_order_number.iloc[0], 2)),
            'slice_tol_income': str(round(data['df']['tol_df'].tol_income.iloc[0], 2)),
            'yes_group_slice_tol': yes_group_slice_tol,
            'group_slice_tol': group_slice_tol
        }

    def write_to_sheet_logic(self, data: Dict[str, Any]) -> Dict[str, Any]:
        all_df = data['df']['all_df']
        belong_sum_df = all_df.groupby(['slice_belong', 'port', 'account_id'], as_index=False).sum(
            numeric_only=True)
        belong_sum_df = belong_sum_df.sort_values(['slice_belong', 'port'])
        data['df']['df_total'].rename(columns={
            'slice_belong': '切片归属',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'origin_order_number': '支付订单数',
            'tol_income': '预估收入'
        }, inplace=True)
        data['df']['accum_le_rank_items'].rename(columns={
            'row_num': '排名',
            'item_title': '商品名称',
            'item_id': '商品ID',
            'current_origin_order_number': '当月订单总数',
            'current_return_rate': '当月退货率',
            'tol_origin_order_number': '累计订单总数',
            'tol_return_rate': '总退货率'
        }, inplace=True)
        data['df']['accum_other1_rank_items'].rename(columns={
            'row_num': '排名',
            'item_title': '商品名称',
            'item_id': '商品ID',
            'current_origin_order_number': '当月订单总数',
            'current_return_rate': '当月退货率',
            'tol_origin_order_number': '累计订单总数',
            'tol_return_rate': '总退货率'
        }, inplace=True)
        data['df']['accum_other2_rank_items'].rename(columns={
            'row_num': '排名',
            'item_title': '商品名称',
            'item_id': '商品ID',
            'current_origin_order_number': '当月订单总数',
            'current_return_rate': '当月退货率',
            'tol_origin_order_number': '累计订单总数',
            'tol_return_rate': '总退货率'
        }, inplace=True)
        data['df']['yes_le_rank_items'].rename(columns={
            'row_num': '排名',
            'item_title': '商品名称',
            'item_id': '商品ID',
            'origin_order_number': '昨日订单数',
            'return_rate': '昨日退货率',
            'account': '账号'
        }, inplace=True)
        data['df']['yes_other1_rank_items'].rename(columns={
            'row_num': '排名',
            'item_title': '商品名称',
            'item_id': '商品ID',
            'origin_order_number': '昨日订单数',
            'return_rate': '昨日退货率',
            'account': '账号'
        }, inplace=True)
        data['df']['yes_other2_rank_items'].rename(columns={
            'row_num': '排名',
            'item_title': '商品名称',
            'item_id': '商品ID',
            'origin_order_number': '昨日订单数',
            'return_rate': '昨日退货率',
            'account': '账号'
        }, inplace=True)
        belong_sum_df.rename(columns={
            "slice_belong": "切片归属",
            "account_id": "账号归属",
            'port': '端口',
            "origin_gmv": "支付GMV",
            "origin_order_number": "支付订单数量",
            "final_gmv": "结算GMV",
            "final_order_number": "结算订单数量",
            "tol_income": "预估收入",
        }, inplace=True)
        workbook_name = f"切片卖货数据_{data['date_interval']['month_start_time']}_{data['date_interval']['yes_time']}_{data['date_interval']['now_time']}"
        workbook_url, spreadsheet_token = self.feishu_sheet_supply.get_workbook_params(
            workbook_name=workbook_name, folder_token='Fn9ZfxSxylvMSsdwzGwcZPEGn9j')
        slice_sheet_token = self.feishu_sheet_supply.get_sheet_params(spreadsheet_token, '切片卖货数据')
        other2_rank_sheet_token = self.feishu_sheet_supply.get_sheet_params(spreadsheet_token,
                                                                            '电商MCN(乐总)当月商品排名')
        other1_rank_sheet_token = self.feishu_sheet_supply.get_sheet_params(spreadsheet_token, '小仙女当月商品排名')
        le_rank_sheet_token = self.feishu_sheet_supply.get_sheet_params(spreadsheet_token, '乐总当月商品排名')
        yes_other2_rank_sheet_token = self.feishu_sheet_supply.get_sheet_params(spreadsheet_token,
                                                                                '电商MCN(乐总)昨日商品排名')
        yes_other1_rank_sheet_token = self.feishu_sheet_supply.get_sheet_params(spreadsheet_token, '小仙女昨日商品排名')
        yes_le_rank_sheet_token = self.feishu_sheet_supply.get_sheet_params(spreadsheet_token, '乐总昨日商品排名')
        total_sheet_token = self.feishu_sheet_supply.get_sheet_params(spreadsheet_token, '月汇总数据')
        self.feishu_sheet.write_df_replace(belong_sum_df, spreadsheet_token, slice_sheet_token, to_char=False)
        self.feishu_sheet.write_df_replace(data['df']['accum_other2_rank_items'], spreadsheet_token,
                                           other2_rank_sheet_token, to_char=False)
        self.feishu_sheet.write_df_replace(data['df']['accum_other1_rank_items'], spreadsheet_token,
                                           other1_rank_sheet_token, to_char=False)
        self.feishu_sheet.write_df_replace(data['df']['accum_le_rank_items'], spreadsheet_token,
                                           le_rank_sheet_token, to_char=False)
        self.feishu_sheet.write_df_replace(data['df']['yes_other2_rank_items'], spreadsheet_token,
                                           yes_other2_rank_sheet_token, to_char=False)
        self.feishu_sheet.write_df_replace(data['df']['yes_other1_rank_items'], spreadsheet_token,
                                           yes_other1_rank_sheet_token, to_char=False)
        self.feishu_sheet.write_df_replace(data['df']['yes_le_rank_items'], spreadsheet_token,
                                           yes_le_rank_sheet_token, to_char=False)
        self.feishu_sheet.write_df_replace(data['df']['df_total'], spreadsheet_token, total_sheet_token, to_char=False)
        return {
            'sheet_params': {
                'sheet_title': workbook_name,
                'url': workbook_url
            },
            'date_interval': data['date_interval']
        }

    def send_card_logic(self, card: Dict[str, Any], sheet):
        res = {
            **card,
            **sheet['sheet_params']
        }
        self.feishu_robot.send_msg_card(data=res, card_id=self.card_id, version_name='1.0.3')


crawler_log_dag = AbstractCrawlerLog().create_dag()
