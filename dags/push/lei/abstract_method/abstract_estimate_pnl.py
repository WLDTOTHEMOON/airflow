import logging
from typing import Any, Dict

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.lei.abstract_method.abstract_base import AbstractDagTask

logger = logging.getLogger(__name__)


class AbstractEstimatePnl(AbstractDagTask):
    def __init__(self):
        super().__init__(
            dag_id='push_estimate_pnl',
            schedule='0 5 * * *',
            default_args=
            {
                'owner': 'Lei Jiangling',
                'start_date': pendulum.datetime(2023, 1, 1),
                'provide_context': True
            },
            tags=['push', 'estimate_pnl'],
            robot_url=Variable.get('MATSUMOTO'),
        )
        self.card_id: str = 'AAqRPrHrP2wKb'

    def fetch_data(self, **kwargs) -> Dict:
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        begin_time = start_time.subtract(days=1).strftime('%Y-%m-01')
        end_time = start_time.subtract(days=1).strftime('%Y-%m-%d')
        detail_sql = """
            select
                order_date
                ,src.anchor_name
                ,valid_estimated_income
                ,profit_rate
                ,estimated_income cps_predict_income
                ,final_commission pk_income
                ,estimated_income + final_commission all_income
                ,organization_commission_rate organization_commission_rate
                ,predict_income_belong_org cps_organization_predict_income
                ,final_commission*coalesce(commission_belong_organization_rate,0) pk_organization_income
                ,predict_income_belong_org + (final_commission*coalesce(commission_belong_organization_rate,0)) predict_income_belong_org
            from
            (
                select 
                    order_date
                    ,anchor_name
                    ,sum(valid_estimated_income) valid_estimated_income
                    ,sum(profit_rate) profit_rate
                    ,sum(predict_income) estimated_income 
                    ,sum(final_commission) final_commission
                    ,sum(predict_income_belong_org) predict_income_belong_org
                    ,commission_belong_organization_rate
                from
                    (
                    select 
                        order_date
                        ,anchor_name
                        ,valid_estimated_income
                        ,profit_rate
                        ,predict_income
                        ,predict_income_belong_org
                        ,0 final_commission
                        ,null commission_belong_organization_rate
                    from
                        ads.ads_ks_estimated_pnl aep
                    )aa
                group by
                    order_date
                    ,anchor_name
                    ,commission_belong_organization_rate
                )src
                left join
                    (
                    select 
                        anchor_name
                        ,coalesce(organization_commission_rate,1) organization_commission_rate
                    from
                        dim.dim_ks_account_info
                    where anchor_name is not null
                    group by 
                        anchor_name
                        ,coalesce(organization_commission_rate,1)
                    )info
                on src.anchor_name = info.anchor_name
                order by
                    order_date asc
                    ,anchor_name asc
        """
        detail_df = pd.read_sql(detail_sql, self.engine, params={'begin_time': begin_time, 'end_time': end_time})

        summary_sql = """
            select
                *
                ,predict_income_belong_org/sum(predict_income_belong_org) over() all_organization_income_rate
            from
            (
                select 
                    src.anchor_name
                    ,sum(estimated_income) cps_predict_income
                    ,sum(final_commission) pk_income
                    ,sum(estimated_income + final_commission) all_income
                    ,organization_commission_rate organization_commission_rate
                    ,sum(predict_income_belong_org) cps_organization_predict_income
                    ,sum(final_commission*coalesce(commission_belong_organization_rate,0)) pk_organization_income
    --                 ,sum(estimated_income*COALESCE(organization_commission_rate,0))+sum(final_commission*COALESCE(commission_belong_organization_rate,0)) all_organization_income
                    ,sum(predict_income_belong_org + (final_commission*coalesce(commission_belong_organization_rate,0))) predict_income_belong_org
                from
                (
                    select 
                        order_date
                        ,anchor_name
                        ,sum(predict_income) estimated_income
                        ,sum(final_commission) final_commission
                        ,sum(predict_income_belong_org) predict_income_belong_org
                        ,commission_belong_organization_rate
                    from 
                        (	
                        select
                            order_date
                            ,anchor_name
                            ,predict_income
                            ,predict_income_belong_org
                            ,0 final_commission
                            ,null commission_belong_organization_rate
                        from
                            ads.ads_ks_estimated_pnl aep 
                        )aa
                    group by
                        order_date
                        ,anchor_name
                        ,commission_belong_organization_rate
                    )src
                    left join
                        (
                        select 
                            anchor_name
                            ,coalesce(organization_commission_rate,1) organization_commission_rate
                        from
                            dim.dim_ks_account_info dkai
                        where anchor_name is not null
                        group by 
                            anchor_name
                            ,coalesce(organization_commission_rate,1)
                        )info
                    on src.anchor_name = info.anchor_name
                group by
                    src.anchor_name
                    ,organization_commission_rate
            )aa
        """
        summary_df = pd.read_sql(summary_sql, self.engine, params={'begin_time': begin_time,  'end_time': end_time})

        slice_sql = """
            select
                order_date
                ,sum(origin_gmv) origin_gmv
                ,sum(final_gmv) final_gmv
                ,sum(estimated_income) estimated_income 
            from(
                select
                    order_date
                    ,sum(origin_gmv) origin_gmv
                    ,sum(final_gmv) final_gmv
                    ,sum(estimated_income + coalesce(estimated_service_income,0)) estimated_income 
                from dws.dws_ks_slice_mcn dksm
                where order_date between %(begin_time)s and %(end_time)s
                group by
                    order_date
                union all
                select
                    order_date
                    ,sum(origin_gmv) origin_gmv
                    ,sum(final_gmv) final_gmv
                    ,sum(estimated_income + coalesce(estimated_service_income,0)) estimated_income 
                from dws.dws_ks_slice_recreation dksr
                where order_date between %(begin_time)s and %(end_time)s
                group by
                    order_date
                union all
                select
                    order_date
                    ,sum(origin_gmv) origin_gmv
                    ,sum(final_gmv) final_gmv
                    ,sum(estimated_income + coalesce(estimated_service_income,0)) estimated_income 
                from dws.dws_ks_slice_slicer dkss
                where order_date between %(begin_time)s and %(end_time)s
                group by
                    order_date) src
            group by
                order_date
        """
        slice_df = pd.read_sql(slice_sql, self.engine, params={'begin_time': begin_time, 'end_time': end_time})

        return {
            'detail_df': detail_df,
            'summary_df': summary_df,
            'slice_df': slice_df
        }

    def process_data(self, data: Dict, **kwargs) -> Dict:
        detail_data = data['detail_df']
        summary_data = data['summary_df']
        slice_data = data['slice_df']

        detail_data.loc[:, ["order_date"]] = detail_data.loc[:, ["order_date"]].astype(str)
        slice_data.loc[:, ["order_date"]] = slice_data.loc[:, ["order_date"]].astype(str)
        columns = {
            'order_date': '卖货日期', 'anchor_name': '主播名', 'valid_estimated_income': 'CPS未失效收入',
            'profit_rate': '折算比例', 'cps_predict_income': 'CPS最终预估收入',
            'pk_income': 'PK/现结收入', 'all_income': '总收入', 'organization_commission_rate': '归属公司比例',
            'cps_organization_predict_income': 'CPS实际归属公司收入', 'pk_organization_income': 'PK/现结归属公司收入',
            'all_organization_income': '归属公司总收入', 'all_organization_income_rate': '归属公司总收入占比',
            'predict_income_belong_org': '实际归属公司总收入'
        }
        slice_columns = {
            'order_date': '卖货日期', 'origin_gmv': '支付GMV', 'final_gmv': '结算GMV', 'estimated_income': '预估收入'
        }

        detail = detail_data.rename(columns=columns)
        summary = summary_data.rename(columns=columns)
        slice_income = slice_data.rename(columns=slice_columns)

        return {
            'detail': detail,
            'summary': summary,
            'slice_income': slice_income
        }

    def create_feishu_file(self, data_dic: Dict, **kwargs) -> Dict:
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        title = ('公司收入预测_' + start_time.subtract(days=1).strftime('%Y%m01') + '_' +
                 start_time.subtract(days=1).strftime('%Y%m%d') + '_' + start_time.strftime('%Y%m%d%H%M'))

        result = self.feishu_sheet.create_spreadsheet(
            title=title, folder_token='JbpAfKbL8lZn7cd0Dyvcey0injd'
        )
        spreadsheet_token = result['spreadsheet']['spreadsheet_token']
        url = result['spreadsheet']['url']
        cps_sheet_id_slice = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='切片收入')
        cps_sheet_id_slice = cps_sheet_id_slice['replies'][0]['addSheet']['properties']['sheetId']

        cps_sheet_id_detail = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='Detail')
        cps_sheet_id_detail = cps_sheet_id_detail['replies'][0]['addSheet']['properties']['sheetId']

        cps_sheet_id_summary = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='Summary')
        cps_sheet_id_summary = cps_sheet_id_summary['replies'][0]['addSheet']['properties']['sheetId']

        return {
            'spreadsheet_token': spreadsheet_token,
            'cps_sheet_id': [cps_sheet_id_slice, cps_sheet_id_detail, cps_sheet_id_summary],
            'url': url,
            'title': title
        }

    def render_feishu_format(
            self,
            processed_data: Dict,
            file_info: Dict
    ):
        detail = processed_data['detail']
        summary = processed_data['summary']
        slice_income = processed_data['slice_income']

        spreadsheet_token = file_info['spreadsheet_token']
        sheet_id = file_info['cps_sheet_id']

        style_dict_detail = {
            'A1:' + self.col_convert(detail.shape[1]) + '1': {
                'font': {
                    'bold': True
                }
            },
            'C2:C' + str(detail.shape[0] + 1): {
                'formatter': '#,##0.00'
            },
            'F2:I' + str(detail.shape[0] + 1): {
                'formatter': '#,##0.00'
            },
            'L2:L' + str(detail.shape[0] + 1): {
                'formatter': '#,##0.00'
            },
            'H2:H' + str(detail.shape[0] + 1): {
                'formatter': '0.00%'
            },
            'K2:K' + str(detail.shape[0] + 1): {
                'formatter': '#,##0.00'
            }
        }

        style_dict_summary = {
            'A1:' + self.col_convert(summary.shape[1]) + '1': {
                'font': {
                    'bold': True
                }
            },
            'B2:H' + str(summary.shape[0] + 1): {
                'formatter': '#,##0.00'
            },
            'I2:I' + str(summary.shape[0] + 1): {
                'formatter': '0.00%'
            },
            'E2:E' + str(summary.shape[0] + 1): {
                'formatter': '0.00%'
            }
        }

        style_dict_slice = {
            'A1:' + self.col_convert(slice_income.shape[1]) + '1': {
                'font': {
                    'bold': True
                }
            },
            'B2:D' + str(slice_income.shape[0] + 1): {
                'formatter': '#,##0.00'
            }
        }

        self.feishu_sheet.write_df_replace(dat=slice_income, spreadsheet_token=spreadsheet_token,
                                           sheet_id=sheet_id[0])

        for key, value in style_dict_slice.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=sheet_id[0], ranges=key, styles=value
            )

        self.feishu_sheet.write_df_replace(dat=detail, spreadsheet_token=spreadsheet_token,
                                           sheet_id=sheet_id[1])

        for key, value in style_dict_detail.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=sheet_id[1], ranges=key, styles=value
            )

        self.feishu_sheet.write_df_replace(dat=summary, spreadsheet_token=spreadsheet_token,
                                           sheet_id=sheet_id[2])

        for key, value in style_dict_summary.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=sheet_id[2], ranges=key, styles=value
            )
        return processed_data

    def send_card(self, file_info: Dict, data_dict: Dict):
        logger.info(f'发送卡片')
        title = file_info['title']
        url = file_info['url']

        data = {
            'title': '公司收入预测',
            'file_name': title,
            'url': url,
            'description': '数据请见下方链接附件'
        }

        robot = self.feishu_robot
        robot.send_msg_card(data=data, card_id=self.card_id, version_name='1.0.1')


estimate_pnl_dag = AbstractEstimatePnl()
estimate_pnl_dag.create_dag()

