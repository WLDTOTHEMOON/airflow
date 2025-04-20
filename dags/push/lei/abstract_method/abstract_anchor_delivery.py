import logging
from typing import Dict

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.lei.abstract_method.abstract_base import AbstractDagTask

logger = logging.getLogger(__name__)


class AbstractAnchorDelivery(AbstractDagTask):
    def __init__(self):
        super().__init__(
            dag_id='push_anchor_delivery',
            schedule='0 3,9 * * *',
            default_args=
            {
                'owner': 'Lei Jiangling',
                'start_date': pendulum.datetime(2023, 1, 1)
            },
            tags=['push', 'anchor_delivery'],
            robot_url=Variable.get('TEST'),
        )
        self.card_id: str = 'AAqRbcLWUH0l4'

    def fetch_data(self, **kwargs) -> Dict:
        anchor_delivery_sql = f"""
            select 
                order_date
                ,account_id 
                ,anchor_name
                ,item_id 
                ,item_title 
                ,bd_name 
                ,origin_order_number 
                ,final_order_number 
                ,send_order_number 
                ,final_order_number - send_order_number un_delivery_residue_order
                ,round(case when final_order_number = 0 then 0 else send_order_number/final_order_number end , 4) deliver_rate
                ,send_order_number_yesterday 
                ,origin_order_number - final_order_number lose_order
                ,round(final_order_number/origin_order_number,4) refund_rate
            from dws.dws_ks_big_tbl dkeh 
            where order_date > date(date_sub(now(), interval 21 day)) and anchor_name != '乐总'
                and origin_order_number > 50 
                and round(case when final_order_number = 0 then 0 else send_order_number/final_order_number end , 4) < 0.95 
                and (final_order_number - send_order_number) >0
            order by 
                anchor_name desc , 
                order_date desc
        """
        anchor_delivery_df = pd.read_sql(anchor_delivery_sql, self.engine)

        group_data_sql = """
            select
                anchor_name
                ,count(case when (send_rate >=0 AND send_rate <=0.3) then 1 else null END) rate_range1
                ,count(case when (send_rate >0.3 AND send_rate <=0.6) then 1 else null END) rate_range2
                ,count(case when (send_rate >0.6 AND send_rate <0.95) then 1 else null END) rate_range3
            from
                (select 
                        order_date
                        ,account_id 
                        ,anchor_name
                        ,item_id 
                        ,item_title 
                        ,origin_order_number 
                        ,final_order_number 
                        ,send_order_number 
                        ,final_order_number - send_order_number un_delivery_residue_order
                        ,round(case when final_order_number = 0 then 0 else send_order_number/final_order_number end , 4) send_rate
                        ,send_order_number_yesterday 
                        ,origin_order_number - final_order_number lose_order
                        ,round(final_order_number/origin_order_number,4) refund_rate
                from dws.dws_ks_big_tbl dkeh
                where order_date > date(date_sub(now(), interval 21 day)) and anchor_name != '乐总'
                )src
            where src.origin_order_number > 50 and src.send_rate < 0.95 and (src.final_order_number - send_order_number) > 0
            group by 
                anchor_name
            order by 
                rate_range1 desc
        """
        group_df = pd.read_sql(group_data_sql, self.engine)

        return {
            'anchor_delivery_df': anchor_delivery_df,
            'group_df': group_df
        }

    def process_data(self, data: Dict, **kwargs) -> Dict:
        processed_data = data['anchor_delivery_df']
        processed_data = processed_data.astype(
            {
             'order_date': str
             }
        )

        group_table_data = data['group_df']
        res = []
        for i in range(group_table_data.shape[0]):
            res.append({
                'anchor_name': group_table_data.anchor_name.iloc[i],
                'rate_range1': str(group_table_data.rate_range1.iloc[i]),
                'rate_range2': str(group_table_data.rate_range2.iloc[i]),
                'rate_range3': str(group_table_data.rate_range3.iloc[i])
            })

        processed_data.deliver_rate = processed_data.deliver_rate.apply(self.percent_convert)
        processed_data.refund_rate = processed_data.refund_rate.apply(self.percent_convert)
        processed_data.rename(columns={
            'order_date': '卖货时间',
            'account_id': '主播ID',
            'anchor_name': '主播名称',
            'item_id': '商品ID',
            'item_title': '商品名称',
            'bd_name': '对应商务',
            'origin_order_number': '总下单量',
            'final_order_number': '剩余单量',
            'send_order_number': '剩余已发货单量',
            'un_delivery_residue_order': '剩余未发货单量',
            'deliver_rate': '订单发货率',
            'send_order_number_yesterday': '昨日发货数量(昨日10点-今日10点)',
            'lose_order': '失效单量',
            'refund_rate': '订单退货率(订单数量)'
        }, inplace=True)
        return {
            'file_data': processed_data,
            'unprocess_data': data['anchor_delivery_df'],
            'group_data': res
        }

    def create_feishu_file(self, process_data_dict: Dict, **kwargs) -> Dict:
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        title = '樊星主播发货进度监控_' + start_time.strftime('%Y%m%d') + '_' + start_time.strftime('%Y%m%d%H%M%S')

        result = self.feishu_sheet.create_spreadsheet(
            title=title, folder_token='KwysfLq9HlKfpCdcbiIcrdNMnDd'
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
            process_data_dict: Dict,
            spreadsheet_token: str,
            cps_sheet_id: str
    ) -> Dict:
        logger.info(f'渲染飞书格式')
        processed_data = process_data_dict['file_data']
        un_process_data = process_data_dict['unprocess_data']

        style_dict = {
            'A1:' + self.col_convert(processed_data.shape[1]) + '1': {
                'font': {
                    'bold': True
                }
            }
        }

        col_num = self.col_convert(un_process_data.columns.get_loc('deliver_rate') + 1)
        for i in range(un_process_data.shape[0]):
            ranges = col_num + str(i + 2) + ':' + col_num + str(i + 2)
            if un_process_data.deliver_rate.iloc[i] >= 0.9:
                color = '#B3D600'
            elif 0.9 > un_process_data.deliver_rate.iloc[i] >= 0.6:
                color = '#FFF258'
            elif un_process_data.deliver_rate.iloc[i] >= 0.3:
                color = '#FF8800'
            else:
                color = '#F64A46'
            style_dict[ranges] = {'backColor': color}

        self.feishu_sheet.write_df_replace(dat=processed_data, spreadsheet_token=spreadsheet_token,
                                           sheet_id=cps_sheet_id)

        for key, value in style_dict.items():
            self.feishu_sheet.style_cells(spreadsheet_token=spreadsheet_token, sheet_id=cps_sheet_id, ranges=key,
                                          styles=value)
        return process_data_dict

    def send_card(self, url: str, title: str, process_data_dict: Dict):
        logger.info(f'发送卡片')

        group_data = process_data_dict['group_data']
        res = {
            'title': title,
            'url': url,
            'group_table': group_data
        }

        robot = self.feishu_robot
        robot.send_msg_card(data=res, card_id=self.card_id, version_name='1.0.0')


anchor_delivery = AbstractAnchorDelivery()
anchor_delivery.create_dag()
