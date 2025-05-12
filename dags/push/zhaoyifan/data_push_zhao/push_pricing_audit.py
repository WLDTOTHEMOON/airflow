from abc import ABC
from typing import Dict, Any
from dags.push.zhaoyifan.data_push_zhao.format_utils import *
import pendulum
import pandas as pd
from airflow.models import Variable
from dags.push.zhaoyifan.data_push_zhao.base_dag import BaseDag


class PricingAudit(BaseDag):
    def __init__(self):
        super().__init__(
            dag_id='push_pricing_audit',
            tags=['push', 'pricing_audit'],
            robot_url=Variable.get('TEST'),
            schedule='0 21 * * 6'
        )
        self.card_id = 'AAq4mlhtYmLDs'

    def fetch_data_logic(self, date_interval: Dict[str, Any]) -> Dict[str, Any]:
        end_datetime = date_interval['end_datetime']
        week_start_date = end_datetime.start_of('week').subtract(days=1).format('YYYY-MM-DD')
        week_end_date = end_datetime.end_of('week').subtract(days=1).format('YYYY-MM-DD')
        week_start_time = end_datetime.start_of('week').subtract(days=1).format('YYYYMMDD')
        week_end_time = end_datetime.end_of('week').subtract(days=1).format('YYYYMMDD')
        week_start_format = end_datetime.start_of('week').subtract(days=1).format('MM月DD日')
        week_end_format = end_datetime.end_of('week').subtract(days=1).format('MM月DD日')
        now_datetime = end_datetime.format('YYYYMMDDHHmm')

        anchor_sql = f'''
            select 
                anchor_name  
                ,count(distinct target_id) submit_num 
                ,count(case when audit_num = 1 and `result` = '不通过' then 1 else null end) 1st_not_pass_num
                ,count(case when audit_num = 1 and `result` = '不通过' then 1 else null end) /  count(distinct target_id) abnormality_rate
                ,count(case when rn = 1 and `result` ='通过' then 1 else null end) final_pass_num
                ,count(case when rn = 1 and `result` ='通过' then 1 else null end) / count(distinct target_id) final_pass_rate
                ,audit_name audit_user_name
                ,'' remark
            from (
                select 
                    target_id 
                    ,pro.name product_name
                    ,`result`
                    ,row_number() over(partition by target_id order by review.created_at) audit_num
                    ,row_number() over(partition by target_id order by review.created_at desc) rn
                    ,class
                    ,anchor_user.name anchor_name
                    ,pro.created_at submit_time
                    ,review.created_at audit_time
                    ,audit_user.name audit_name
                from (
                    select 
                        target_id 
                        ,user_id 
                        ,case 
                            when `result`in (10,22) then '通过' 
                            when `result`in (9,20,25) then '不通过' 
                        end `result`
                        ,updated_at 
                        ,created_at 
                    from xlsd.review r 
                    where target = 'product'
                        and ((result in (9,10,22,25)) or (result = 20 and act != 3))
                        and user_id not in ('101279867','20b984bd-07ef-4b4b-95c8-404ac72a0081','25413777','25857336','5085936','21724093')
                        and date(created_at) between '{week_start_date}'  and '{week_end_date}'
                )review
                inner join (
                    select 
                        id 
                        ,user_id 
                        ,supplier_id 
                        ,name 
                        ,substring_index(class,',',1) class 
                        ,updated_at 
                        ,created_at 
                        ,case 
                            when anchor_live = '' then anchor
                            else anchor_live
                        end	anchor_live
                    from xlsd.products p 
                    where by_anchor = 1 
                )pro on review.target_id = pro.id
                left join (
                    select 
                        id,name
                    from xlsd.users u 
                )anchor_user on pro.anchor_live = anchor_user.id
                left join (
                select 
                  id,name 
                from xlsd.users u 
              )audit_user on review.user_id = audit_user.id
            )tol
            group by 
                anchor_name
                ,audit_name
        '''
        anchor_df = pd.read_sql(anchor_sql, self.engine)
        anchor_df.abnormality_rate = anchor_df.abnormality_rate.apply(percent_convert)
        anchor_df.final_pass_rate = anchor_df.final_pass_rate.apply(percent_convert)

        bd_sql = f'''
            select 
                group_name 
                ,bd_name  
                ,count(distinct target_id) submit_num 
                ,count(case when audit_num = 1 and `result` = '不通过' then 1 else null end) 1st_not_pass_num
                ,count(case when audit_num = 1 and `result` = '不通过' then 1 else null end) /  count(distinct target_id) abnormality_rate
                ,count(case when rn = 1 and `result` ='通过' then 1 else null end) final_pass_num
                ,count(case when rn = 1 and `result` ='通过' then 1 else null end) / count(distinct target_id) final_pass_rate
                ,audit_name audit_user_name
                ,'' remark
            from (
                select 
                    target_id 
                    ,pro.name product_name
                    ,`result`
                    ,row_number() over(partition by target_id order by review.created_at) audit_num
                    ,row_number() over(partition by target_id order by review.created_at desc) rn
                    ,class
                    ,group_name
                    ,bd_user.name bd_name
                    ,audit_user.name audit_name
                    ,pro.created_at submit_time
                    ,review.created_at audit_time
                from (
                    select 
                        target_id 
                        ,user_id 
                        ,case 
                            when `result`in (10,22) then '通过' 
                            when `result`in (9,20,25) then '不通过' 
                        end `result`
                        ,updated_at 
                        ,created_at 
                    from xlsd.review r 
                    where target = 'product'
                        and ((result in (9,10,22,25)) or (result = 20 and act != 3))
                        and user_id not in ('101279867','20b984bd-07ef-4b4b-95c8-404ac72a0081','25413777','25857336','5085936','21724093')
                        and date(created_at) between '{week_start_date}'  and '{week_end_date}'
                )review
                inner join (
                    select 
                        id 
                        ,user_id 
                        ,supplier_id
                        ,name 
                        ,substring_index(class,',',1) class 
                        ,updated_at 
                        ,created_at 
                        ,anchor_live 
                    from xlsd.products p 
                    where by_anchor = 0 
                )pro on review.target_id = pro.id
                left join (
                    select 
                        id,commerce
                    from xlsd.suppliers s 
                )sup on pro.supplier_id = sup.id
                left join (
                    select 
                        id,name
                    from xlsd.users u 
                )bd_user on sup.commerce = bd_user.id
                left join (
                    select 
                        group_name,bd_name
                    from tmp.tmp_bd_name tbn 
                )divide on bd_user.name = divide.bd_name
                left join (
                select 
                  id,name 
                from xlsd.users u 
              )audit_user on review.user_id = audit_user.id
            )tol
            group by 
                group_name
                ,bd_name
                ,audit_name
        '''
        bd_df = pd.read_sql(bd_sql, self.engine)
        bd_df.abnormality_rate = bd_df.abnormality_rate.apply(percent_convert)
        bd_df.final_pass_rate = bd_df.final_pass_rate.apply(percent_convert)

        return {
            'anchor_df': anchor_df,
            'bd_df': bd_df,
            'week_start_date': week_start_date,
            'week_end_date': week_end_date,
            'week_start_time': week_start_time,
            'week_end_time': week_end_time,
            'week_start_format': week_start_format,
            'week_end_format': week_end_format,
            'now_datetime': now_datetime
        }

    def prepare_card_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        return {}

    def write_to_sheet_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        anchor_df = data_dict['anchor_df']
        bd_df = data_dict['bd_df']
        sheet_title = f"成本审核数据_{data_dict['week_start_time']}_{data_dict['week_end_time']}_{data_dict['now_datetime']}"
        url, spreadsheet_token = self.feishu_sheet_supply.get_workbook_params(
            workbook_name=sheet_title, folder_token='T8Erfo3HnlBnFsd3EgscvfKonEg'
        )

        sql_list = [bd_df, anchor_df]
        sheet_list = ['招商', '自采']
        for i, df in enumerate(sql_list):
            style_dict = {
                'A1:' + col_convert(df.shape[1]) + '2': {
                    'font': {
                        'bold': True
                    },
                    'backColor': '#FFFF00'
                },
                'A1:' + col_convert(df.shape[1]) + str(df.shape[0] + 2): {
                    'hAlign': 1,
                    'vAlign': 1,
                    # 'borderType': 'FULL_BORDER'
                },
                'A1:A1': {
                    'font': {
                        'fontSize': '14pt/1.5'
                    }
                }
            }
            merge_cell_dict = {
                'A1:' + str(col_convert(df.shape[1])) + '1': {
                    'range': 'A1:' + str(col_convert(df.shape[1])) + '1',
                    'mergeType': "MERGE_ALL"
                }
            }
            insert_row = {
                'row': {
                    "startIndex": 0,
                    "endIndex": 1
                }
            }
            insert_data = {
                'row': {
                    "range": 'A1:A1',
                    "value":
                        f"成本审核{data_dict['week_start_format']}至{data_dict['week_end_format']}数据"
                }
            }

            if df is anchor_df:
                df = anchor_df.rename(columns={
                    'anchor_name': '主播', 'submit_num': '提报数量', '1st_not_pass_num': '首次未通过数量',
                    'abnormality_rate': '异常率', 'final_pass_num': '最终通过数量', 'final_pass_rate': '最终通过率',
                    'audit_user_name': '审核人', 'remark': '备注'
                })
            else:
                df = bd_df.rename(columns={
                    'group_name': '分组', 'bd_name': '商务', 'submit_num': '提报数量',
                    '1st_not_pass_num': '首次未通过数量',
                    'abnormality_rate': '异常率', 'final_pass_num': '最终通过数量', 'final_pass_rate': '最终通过率',
                    'audit_user_name': '审核人', 'remark': '备注'
                })

            sheet_id = self.feishu_sheet_supply.get_sheet_params(spreadsheet_token, sheet_list[i])
            self.feishu_sheet.write_df_replace(df, spreadsheet_token, sheet_id, to_char=False)

            for key, value in insert_row.items():
                self.feishu_sheet.insert_row(
                    spreadsheet_token=spreadsheet_token, sheet_id=sheet_id, styles=value
                )
            for key, value in insert_data.items():
                self.feishu_sheet.insert_data_fs(
                    spreadsheet_token=spreadsheet_token, sheet_id=sheet_id, styles=value
                )
            for key, value in merge_cell_dict.items():
                self.feishu_sheet.merge_cell(
                    spreadsheet_token=spreadsheet_token, sheet_id=sheet_id, styles=value
                )
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
            'title': '成本审核数据',
            **card,
            **sheet,
            'description': '数据请见下方链接附件'
        }
        self.feishu_robot.send_msg_card(data=res, card_id=self.card_id, version_name='1.0.0')


dag = PricingAudit().create_dag()
