import logging
from typing import Any, Dict

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.lei.abstract_method.abstract_base import AbstractDagTask

logger = logging.getLogger(__name__)


class AbstractFinalData(AbstractDagTask):
    def __init__(self):
        super().__init__(
            dag_id='push_final_data',
            schedule='0 7 * * *',
            default_args=
            {
                'owner': 'Lei Jiangling',
                'start_date': pendulum.datetime(2023, 1, 1),
                'provide_context': True
            },
            tags=['push', 'final_data'],
            robot_url=Variable.get('HINATA'),
        )
        self.card_id: str = 'AAqRPrHrP2wKb'

    def fetch_data(self, **kwargs) -> Dict:
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        end_time = start_time.subtract(days=1).strftime('%Y-%m-%d')

        anchor_info = {
            '钱亮': ('1291135119', '2025-01-08', end_time),
            '墨晨夏': ('2884165591', '2024-01-01', end_time),
            '乐总': ('18541124', '2024-01-01', end_time)
        }

        df_dict = {}
        for anchor_name, (account_id, start_time, end_time) in anchor_info.items():
            final_sql = f'''
                        select
                            order_date
                            ,item_id
                            ,item_title
                            ,origin_gmv
                            ,final_gmv
                            ,final_gmv / origin_gmv final_rate
                            ,estimated_income + coalesce(estimated_service_income,0) estimated_income
                            ,(estimated_income + coalesce(estimated_service_income,0)) / final_gmv estimated_income_rate
                        from dws.dws_ks_big_tbl dkeh 
                        where order_date between %(begin_time)s and %(end_time)s
                            and account_id in (%(account_id)s)
                        having 
                            final_gmv > 10000
                        order by
                            order_date asc
                            ,origin_gmv desc
                    '''
            final_df = pd.read_sql(final_sql, self.engine,
                                   params={'begin_time': start_time, 'end_time': end_time, 'account_id': account_id})
            df_dict[anchor_name] = final_df

        item_category_sql = '''
                            select
                                substring_index(substring_index(item_category,'>',1),'>',-1) category_one
                                ,substring_index(substring_index(item_category,'>',2),'>',-1) category_two
                                ,substring_index(substring_index(item_category,'>',3),'>',-1) category_three
                                ,anchor_name
                                ,order_date
                                ,item_id
                                ,item_title
                                ,sum(origin_gmv) origin_gmv
                                ,sum(final_gmv) final_gmv
                                ,1-sum(final_gmv)/sum(origin_gmv) return_rate
                                ,sum(estimated_income + coalesce(estimated_service_income,0)) estimated_income
                                ,sum(estimated_income + coalesce(estimated_service_income,0))/sum(final_gmv) estimated_income_rate
                            from dws.dws_ks_big_tbl dkeh 
                            where 
                                (((account_id = '1291135119') and (order_date between '2025-01-08' and %(end_time)s))
                                or ((account_id = '18541124') and (order_date between '2024-01-01' and %(end_time)s))
                                or ((account_id = '2884165591') and (order_date between '2024-01-01' and %(end_time)s)))
                                and final_gmv > 10000
                            group by
                                1,2,3,4,5,6,7
                            order by
                                anchor_name asc
                                ,order_date desc
                                ,origin_gmv desc
                            '''
        item_category_df = pd.read_sql(item_category_sql, self.engine, params={'end_time': end_time})
        df_dict['item_category'] = item_category_df
        return df_dict

    def process_data(self, data: Dict, **kwargs) -> Dict:
        item_category_df = data['item_category']
        item_category_df.order_date = item_category_df.order_date.astype(str)
        item_category_df.return_rate = item_category_df.return_rate.apply(self.percent_convert)
        item_category_df.estimated_income_rate = item_category_df.estimated_income_rate.apply(self.percent_convert)
        item_category_df = item_category_df.rename(columns={
            'category_one': '一级类目', 'category_two': '二级类目', 'category_three': '三级类目',
            'anchor_name': '主播名称', 'order_date': '售卖日期', 'item_id': '商品ID', 'item_title': '商品名称',
            'origin_gmv': '支付GMV', 'final_gmv': '结算GMV', 'return_rate': '退货率', 'estimated_income': '佣金',
            'estimated_income_rate': '佣金率'
        })

        df_dict = {}
        for anchor_name, df in data.items():
            if anchor_name == 'item_category':
                continue
            df.final_rate = df.final_rate.apply(self.percent_convert)
            df.estimated_income_rate = df.estimated_income_rate.apply(self.percent_convert)
            df.order_date = df.order_date.astype(str)
            df = df.rename(columns={
                'order_date': '售卖日期',
                'item_id': '商品ID',
                'item_title': '商品名称',
                'origin_gmv': '支付GMV',
                'final_gmv': '结算GMV',
                'final_rate': '结算率',
                'estimated_income': '佣金',
                'estimated_income_rate': '佣金率'
            })
            df_dict[anchor_name] = df
        df_dict['item_category'] = item_category_df
        return df_dict

    def create_feishu_file(self, processed_data: Dict, **kwargs) -> Dict:
        logger.info(f'创建飞书文件')
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        title = ('主播结算数据_20240101_' + start_time.subtract(days=1).strftime('%Y%m%d') + '_' + start_time.strftime(
                 '%Y%m%d%H%M'))

        result = self.feishu_sheet.create_spreadsheet(
            title=title, folder_token='Mu4Mfboavll0HwdRUdpcGCVBnqh'
        )
        spreadsheet_token = result['spreadsheet']['spreadsheet_token']
        url = result['spreadsheet']['url']
        cps_sheet_id = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='TOTAL')
        cps_sheet_id_1 = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        cps_sheet_id = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='钱亮结算大于一万品')
        cps_sheet_id_2 = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        cps_sheet_id = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='墨晨夏结算大于一万品')
        cps_sheet_id_3 = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        cps_sheet_id = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='乐总结算大于一万品')
        cps_sheet_id_4 = cps_sheet_id['replies'][0]['addSheet']['properties']['sheetId']

        return {
            'spreadsheet_token': spreadsheet_token,
            'cps_sheet_id': [cps_sheet_id_1, cps_sheet_id_2, cps_sheet_id_3, cps_sheet_id_4],
            'url': url,
            'title': title
        }

    def render_feishu_format(
            self,
            processed_data: Dict,
            file_info: Dict
    ) -> Dict:
        logger.info(f'渲染飞书格式')
        spreadsheet_token = file_info['spreadsheet_token']
        cps_sheet_id = file_info['cps_sheet_id']

        cps_style_dict = {
            'A1:' + self.col_convert(processed_data['item_category'].shape[1]) + '1': {
                'font': {
                    'bold': True
                }
            }
        }

        self.feishu_sheet.write_df_replace(processed_data['item_category'], spreadsheet_token, cps_sheet_id[0])
        for key, value in cps_style_dict.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=cps_sheet_id[0], ranges=key, styles=value
            )

        sheet_id_num = 1
        for anchor_name, df in processed_data.items():
            if anchor_name == 'item_category':
                continue
            cps_style_dict = {
                'A1:' + self.col_convert(df.shape[1]) + '1': {
                    'font': {
                        'bold': True
                    }
                }
            }
            self.feishu_sheet.write_df_replace(dat=df, spreadsheet_token=spreadsheet_token,
                                               sheet_id=cps_sheet_id[sheet_id_num])
            for key, value in cps_style_dict.items():
                self.feishu_sheet.style_cells(
                    spreadsheet_token=spreadsheet_token, sheet_id=cps_sheet_id[sheet_id_num], ranges=key, styles=value
                )
            sheet_id_num += 1

        return processed_data

    def send_card(self, file_info: Dict, data_dic: Dict):
        logger.info(f'发送卡片')
        title = file_info['title']
        url = file_info['url']

        data = {
            'title': '主播结算数据',
            'file_name': title,
            'url': url,
            'description': '数据请见下方链接附件'
        }

        robot = self.feishu_robot
        robot.send_msg_card(data=data, card_id=self.card_id, version_name='1.0.1')


final_data_dag = AbstractFinalData()
final_data_dag.create_dag()
