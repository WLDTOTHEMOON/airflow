from abc import ABC
from typing import Dict, Any
from dags.push.zhaoyifan.data_push_zhao.format_utils import *
import pendulum
import pandas as pd
from airflow.models import Variable
from dags.push.zhaoyifan.data_push_zhao.base_dag import BaseDag


class SupplierSend(BaseDag):
    def __init__(self):
        super().__init__(
            dag_id='push_supplier_send',
            default_args={'owner': 'zhaoyifan'},
            tags=['push', 'supplier_send'],
            robot_url=Variable.get('TEST'),
            schedule='0 11 * * 1'
        )
        self.card_id = 'AAq4HQvobYgca'

    def fetch_data_logic(self, date_interval: Dict[str, Any]) -> Dict[str, Any]:
        end_datetime = date_interval['end_datetime']
        two_weeks_ago_start_time = end_datetime.start_of('week').subtract(weeks=2).format(
            'YYYY-MM-DD 04:00:00')
        two_weeks_ago_end_time = end_datetime.end_of('week').subtract(weeks=2).format(
            'YYYY-MM-DD 04:00:00')
        two_weeks_ago_start_date = end_datetime.start_of('week').subtract(weeks=2).format(
            'YYYYMMDD')
        two_weeks_ago_end_date = end_datetime.end_of('week').subtract(weeks=2).format('YYYYMMDD')
        now_datetime = end_datetime.format('YYYYMMDDHHmm')

        sql = f'''
            select 
                supplier_name
                ,mfr_name
                ,sum(timestampdiff(minute,order_create_time,if(cps_order_status = '已失效',order_create_time,if(send_time is null,current_timestamp,send_time))) 
                ) / 60 / sum(if(cps_order_status = '已失效',0,1)) avg_send_diff_hour
                ,count(case 
                    when 
                        timestampdiff(
                            minute,order_create_time,if(cps_order_status = '已失效',order_create_time,if(send_time is null,current_timestamp,send_time))
                        ) / 60 > 72 then o_id
                    else null
                end
                ) overtime_send_number
                ,count(case 
                    when 
                        timestampdiff(
                            minute,order_create_time,if(cps_order_status = '已失效',order_create_time,if(send_time is null,current_timestamp,send_time))
                        ) / 60 > 72 then o_id
                    else null
                end
                ) / count(if(cps_order_status = '已失效',null,o_id)) overtime_rate
                ,count(case 
                    when send_status = '已发货' and cps_order_status = '已失效' then o_id
                    else null
                end) / count(case when send_status = '已发货' then o_id else null end) return_rate 
            from (
                select
                    o_id
                    ,cps.item_id
                    ,item_title
                    ,product_id
                    ,pro.name product_name
                    ,sup.name supplier_name
                    ,mfr_name
                    ,order_create_time
                    ,cps_order_status
                    ,send_status
                    ,send_time
                from (
                    select 
                        item_id 
                        ,item_title
                        ,o_id
                        ,order_create_time 
                        ,cps_order_status 
                        ,send_time 
                        ,send_status
                    from dwd.dwd_ks_cps_order dkco 
                    where order_create_time between '{two_weeks_ago_start_time}' and '{two_weeks_ago_end_time}'
                        and account_id != '146458792'
                )cps 
                inner join (
                    select 
                        item_id
                        ,product_id
                    from (
                        select
                            item_id
                            ,product_id
                            ,row_number() over(partition by item_id order by updated_at desc) rn
                        from dwd.dwd_pf_links dpl
                        where status = '通过' and by_anchor = 0
                    ) tmp
                    where rn = 1
                )links on cps.item_id = links.item_id
                left join (
                    select 
                        id
                        ,supplier_id 
                        ,name
                        ,mfr_name 
                    from dwd.dwd_pf_products_bd dppb
                )pro on links.product_id = pro.id
                left join (
                    select
                        id 
                        ,name
                    from dwd.dwd_pf_suppliers dps
                )sup on pro.supplier_id = sup.id
            ) tol
            group by 
                supplier_name
                ,mfr_name
            having avg_send_diff_hour is not null or return_rate is not null
            order by 3 desc
        '''
        df = pd.read_sql(sql, self.engine)

        return {
            'df': df,
            'two_weeks_ago_start_time': two_weeks_ago_start_time,
            'two_weeks_ago_end_time': two_weeks_ago_end_time,
            'two_weeks_ago_start_date': two_weeks_ago_start_date,
            'two_weeks_ago_end_date': two_weeks_ago_end_date,
            'now_datetime': now_datetime
        }

    def prepare_card_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        return {}

    def write_to_sheet_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        df = data_dict['df']

        style_dict = {
            'A1:' + col_convert(df.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                # 'backColor': '#FFFF00'
            },
            'A1:' + col_convert(df.shape[1]) + '1': {
                'hAlign': 1,
                'vAlign': 1,
                # 'borderType': 'FULL_BORDER'
            },
            'C2:C' + str(df.shape[0] + 1): {
                'formatter': "#,##0.00"
            },
            'E2:F' + str(df.shape[0] + 1): {
                'formatter': "0.00%"
            }
        }

        df.rename(columns={
            'supplier_name': '供应商',
            'mfr_name': '生产商',
            'avg_send_diff_hour': '平均发货时长（小时）',
            'overtime_send_number': '超72小时发货订单数',
            'overtime_rate': '超时率',
            'return_rate': '退货率（已发货）',
        }, inplace=True)

        sheet_title = (f"供应商发货数据_{data_dict['two_weeks_ago_start_date']}"
                       f"_{data_dict['two_weeks_ago_end_date']}_{data_dict['now_datetime']}")
        url, spreadsheet_token = self.feishu_sheet_supply.get_workbook_params(
            workbook_name=sheet_title, folder_token='NsO8fBPXJlYnVrdKQ0ac0BWJnMh'
        )

        sheet_id = self.feishu_sheet_supply.get_sheet_params(spreadsheet_token, 'Result')
        self.feishu_sheet.write_df_replace(df, spreadsheet_token, sheet_id, to_char=False)

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
            'title': '供应商发货数据',
            **sheet,
            'description': '数据请见下方链接附件'
        }
        self.feishu_robot.send_msg_card(data=res, card_id=self.card_id, version_name='1.0.0')


dag = SupplierSend().create_dag()
