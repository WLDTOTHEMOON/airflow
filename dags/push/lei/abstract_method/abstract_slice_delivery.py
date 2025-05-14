import logging
from typing import Any, Dict

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.lei.abstract_method.abstract_base import AbstractDagTask

logger = logging.getLogger(__name__)


class AbstractSliceDelivery(AbstractDagTask):
    def __init__(self):
        super().__init__(
            dag_id='push_slice_delivery',
            schedule='0 3 * * *',
            default_args=
            {
                'owner': 'Lei Jiangling',
                'start_date': pendulum.datetime(2023, 1, 1),
                'provide_context': True
            },
            tags=['push', 'slice_delivery'],
            robot_url=Variable.get('MiyamuraIzumi')
        )
        self.card_id: str = 'AAqRLrhJaJ8IV'

    def fetch_data(self, **kwargs) -> Dict:
        logger.info(f'获取数据')
        slice_delivery_sql = """
            select
                order_date
                ,item_id
                ,item_title
                ,sum(origin_gmv) origin_gmv
                ,sum(final_gmv) final_gmv
                ,sum(origin_order_number) origin_order_number
                ,sum(final_order_number) final_order_number
                ,sum(send_order_number) send_order_number
                ,sum(wait_send_order_number) wait_send_order_number
                ,sum(send_order_number_yesterday) send_order_number_yesterday
                ,COALESCE(round(sum(send_order_number)/sum(final_order_number),4),0) send_rate
                ,sum(origin_order_number - final_order_number) invalid_order_num
                ,round(1 - sum(final_order_number)/sum(origin_order_number),4) invalid_rate
            from(
                (select
                    order_date
                    ,item_id
                    ,item_title
                    ,origin_gmv
                    ,final_gmv
                    ,origin_order_number
                    ,final_order_number
                    ,send_order_number
                    ,final_order_number-send_order_number wait_send_order_number
                    ,send_order_number_yesterday
                from
                    dws.dws_ks_slice_mcn dksm
                where
                    coalesce(round(send_order_number/final_order_number,4),0)!=1
                    and origin_order_number > 10
                    and final_order_number > 0
                    and order_date < date_sub(current_date, interval 1 day)
                order by
                    wait_send_order_number DESC)
                union all
                (select
                    order_date
                    ,item_id
                    ,item_title
                    ,origin_gmv
                    ,final_gmv
                    ,origin_order_number
                    ,final_order_number
                    ,send_order_number
                    ,final_order_number-send_order_number wait_send_order_number
                    ,send_order_number_yesterday
                from
                    dws.dws_ks_slice_recreation
                where
                    coalesce(round(send_order_number/final_order_number,4),0)!=1
                    and origin_order_number > 10
                    and final_order_number > 0
                    and order_date < date_sub(current_date, interval 1 day)
                order by
                    wait_send_order_number desc)
                union all
                (select
                    order_date
                    ,item_id
                    ,item_title
                    ,origin_gmv
                    ,final_gmv
                    ,origin_order_number
                    ,final_order_number
                    ,send_order_number
                    ,final_order_number-send_order_number wait_send_order_number
                    ,send_order_number_yesterday
                from
                    dws.dws_ks_slice_slicer dkss
                where
                    coalesce(round(send_order_number/final_order_number,4),0)!=1
                    and origin_order_number > 10
                    and final_order_number > 0
                    and order_date < date_sub(current_date, interval 1 day)
                order by
                    wait_send_order_number desc)) src
            group by
                1,2,3
        """
        slice_delivery_df = pd.read_sql(slice_delivery_sql, self.engine)
        return {
            'slice_delivery_data': slice_delivery_df
        }

    def process_data(self, data: Dict, **kwargs) -> Dict:
        logger.info(f'处理数据')
        process_data = data['slice_delivery_data']

        # 卡片数据处理
        group_data = process_data[process_data.send_rate < 0.6]
        group_data = group_data.groupby('item_title')[['final_order_number', 'wait_send_order_number']].sum().reset_index()
        group_data['send_rate'] = 1 - group_data['wait_send_order_number'] / group_data['final_order_number']
        group_data = group_data.sort_values('wait_send_order_number', ascending=False)
        res = []
        for i in range(group_data.shape[0]):
            res.append({
                'item_title': group_data.item_title.iloc[i],
                'valid_order_number': str(int(group_data.final_order_number.iloc[i])),
                'undelivered_number': str(int(group_data.wait_send_order_number.iloc[i])),
                'delivery_rate': str(self.percent_convert(group_data.send_rate.iloc[i]))
            })

        # 文件数据处理
        process_data.order_date = process_data.order_date.astype(str)
        process_data.send_rate = process_data.send_rate.apply(self.percent_convert)
        process_data.invalid_rate = process_data.invalid_rate.apply(self.percent_convert)
        process_data = process_data.astype({
            'send_rate': str,
            'invalid_rate': str,
        })

        process_data.rename(columns={
            'order_date': '卖货时间',
            'item_id': '商品ID',
            'item_title': '商品名称',
            'origin_gmv': '总销售额',
            'final_gmv': '剩余销售额',
            'origin_order_number': '总下单量',
            'final_order_number': '剩余单量',
            'send_order_number': '剩余已发货单量',
            'wait_send_order_number': '剩余未发货单量(降序)',
            'send_order_number_yesterday': '昨日发货量(昨日10点~今日10点)',
            'send_rate': '订单发货率(订单量)',
            'invalid_order_num': '失效单量(订单量)',
            'invalid_rate': '订单退货率(订单量)',
            # 'update_at': '数据更新时间'
        }, inplace=True)

        return {
            'process_data': process_data,
            'group_data': res
        }

    def create_feishu_file(self, data_dic: Dict, **kwargs) -> Dict:
        logger.info('创建飞书文件')
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        title = '切片发货进度监控_' + start_time.strftime('%Y%m%d%H%M%S')

        result = self.feishu_sheet.create_spreadsheet(
            title=title, folder_token='Q3fHfTZuRlRBsFdv9xqcT1panUm'
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
            file_info: Dict
    ):
        logger.info('渲染飞书格式')
        process_data = processed_data['process_data']

        spreadsheet_token = file_info['spreadsheet_token']
        sheet_id = file_info['cps_sheet_id']

        style_dict = {
            'A1:' + self.col_convert(process_data.shape[1]) + '1': {
                'font': {
                    'bold': True
                }
            }
        }

        self.feishu_sheet.write_df_replace(process_data, spreadsheet_token, sheet_id)

        for key, value in style_dict.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=sheet_id, ranges=key, styles=value
            )
        return processed_data

    def send_card(self, file_info: Dict, data_dic: Dict):
        logger.info('发送卡片')
        group_data = data_dic['group_data']

        title = file_info['title']
        url = file_info['url']

        data = {
            'title': '发货进度监控（切片）',
            'file_name': title,
            'url': url,
            'group_table': group_data
        }

        robot = self.feishu_robot
        robot.send_msg_card(data=data, card_id=self.card_id, version_name='1.0.0')


slice_delivery_dag = AbstractSliceDelivery()
slice_delivery_dag.create_dag()

