import logging
from typing import Dict, Any

import pandas as pd
import pendulum
from airflow.models import Variable
from dags.push.lei.abstract_method.abstract_base import AbstractDagTask

logger = logging.getLogger(__name__)


class AbstractBdWeek(AbstractDagTask):
    def __init__(self):
        super().__init__(
            dag_id='push_bd_week',
            schedule='0 21 * * *',
            default_args=
            {
                'owner': 'Lei Jiangling',
                'start_date': pendulum.datetime(2023, 1, 1),
                'provide_context': True
            },
            tags=['push', 'bd_week'],
            robot_url=Variable.get('TEST'),
        )
        self.card_id: str = 'AAqRPrHrP2wKb'

    def fetch_data(self, **kwargs) -> Dict:
        start_time = kwargs['data_interval_start'].in_tz('Asia/Shanghai')
        begin_time = start_time.subtract(days=1).start_of('week').strftime('%Y-%m-%d')
        end_time = start_time.subtract(days=1).end_of('week').strftime('%Y-%m-%d')
        logger.info(begin_time + '-' + end_time)

        # 商务业绩
        bd_gmv_sql = """
            with tmp as(
                (select
                    bd_name
                    ,sum(origin_gmv) origin_gmv
                    ,sum(final_gmv) final_gmv 
                    ,sum(estimated_income + coalesce(estimated_service_income,0)) estimated_income 
                    ,sum(estimated_income + coalesce(estimated_service_income,0))/sum(final_gmv) rate
                from dws.dws_ks_ec_2hourly dkeh
                where order_date between %(begin_time)s and %(end_time)s
                    and bd_name is not null
                group by
                    bd_name
                order by origin_gmv desc)
                union all
                select
                    '主播自采' bd_name
                    ,sum(origin_gmv) origin_gmv
                    ,sum(final_gmv) final_gmv 
                    ,sum(estimated_income + coalesce(estimated_service_income,0)) estimated_income 
                    ,sum(estimated_income + coalesce(estimated_service_income,0))/sum(final_gmv) rate
                from dws.dws_ks_ec_2hourly dkeh
                where product_id is not null 
                    and bd_name is null
                    and order_date between %(begin_time)s and %(end_time)s
                union all
                select
                    '未走流程' bd_name
                    ,sum(origin_gmv) origin_gmv
                    ,sum(final_gmv) final_gmv 
                    ,sum(estimated_income + coalesce(estimated_service_income,0)) estimated_income 
                    ,sum(estimated_income + coalesce(estimated_service_income,0))/sum(final_gmv) rate
                from dws.dws_ks_ec_2hourly dkeh
                where product_id is null
                    and order_date between %(begin_time)s and %(end_time)s)
            select
                *
            from tmp
            union all
            select
                '合计' bd_name
                ,sum(origin_gmv) origin_gmv
                ,sum(final_gmv) final_gmv 
                ,sum(estimated_income) estimated_income 
                ,sum(estimated_income)/sum(final_gmv) rate
            from tmp
        """
        bd_gmv_df = pd.read_sql(bd_gmv_sql, self.engine, params={'begin_time': begin_time, 'end_time': end_time})

        # 商务主播上品数量
        bd_product_sql = """
            select
                bd_name
                ,anchor_name
                ,count(distinct product_id) num
            from(
                select
                        DISTINCT
                        bd_name
                        ,account_id
                        ,anchor_name
                        ,product_id
                from dws.dws_ks_big_tbl dkeh
                where order_date between %(begin_time)s and %(end_time)s
                        and bd_name is not null
                        and origin_gmv >= 200
                        ) src
            group by
                bd_name
                ,anchor_name
            order by
                bd_name asc
                ,num desc
        """
        bd_product_df = pd.read_sql(bd_product_sql, self.engine,
                                    params={'begin_time': begin_time, 'end_time': end_time})

        # 商品待入库和入库数量
        bd_put_sql = """
            select
                bd_name
                ,count(case when src.status = '成本-通过 | 待入库' then 1 else null end) wait_put
                ,count(case when src.status = '入库' then 1 else null end) put
            from(
            select
                    id
                    ,user_id
                    ,supplier_id
                    ,name
                    ,case status
                        when 0 then '待审核 | 初审中 | 特批-初审中'
                        when 1 then '初审不通过'
                        when 2 then '初审通过 | 复审中 | 特批-复审中'
                        when 3 then '复审不通过'
                        when 4 then '复审不通过（不可再次提交）'
                        when 5 then '复审通过 | 成本审核中'
                        when 6 then '待成本审核 - 在成本审核不通过后，供应商可以选择只修改机制，从而无需再次【初审】和【复审】，直接到达【成本终审】（6是个意外）'
                        when 7 then '特批-复审不通过'
                        when 8 then '特批-审核通过 | 成本审核中'
                        when 9 then '成本-不通过'
                        when 10 then '成本-通过 | 待入库'
                        when 11 then '入库'
                        when 12 then '不允许入库'
                        when 13 then '复审不通过 - 退回初审，包括招商商品和主播自采'
                        when 14 then '退回 - 把已通过的退回，编辑后重新审核'
                        when -1 then '编辑中，这是老平台的数据'
                        else '未知状态'
                    end status
                    ,created_at
                    ,updated_at
            from xlsd.products p 
            where by_anchor = 0
                    and date(reviewed_at) between %(begin_time)s and %(end_time)s)src
            left join(
                    select
                            id
                            ,name
                            ,commerce
                    from xlsd.suppliers s  
            )info on src.supplier_id = info.id
            left join(
                    select
                            id
                            ,name bd_name
                    from xlsd.users u 
            ) us on info.commerce = us.id
            where status in ('入库', '成本-通过 | 待入库')
                and bd_name not in ('张小卓','张澜','方涌超','赵乙都')
            group by bd_name
        """
        bd_put_df = pd.read_sql(bd_put_sql, self.engine, params={'begin_time': begin_time, 'end_time': end_time})

        # 签合同数及入驻数量
        bd_contract_sql = """
            select
                    bd_name
                    ,count(case when status = '审核通过' then 1 else null end) enter_num
                    ,count(case when contract = '[]' or contract is null or status != '审核通过' then null else 1 end) contract_num
            from(
                    select
                            src.name
                            ,info.name bd_name
                            ,case src.status 
                            when 0 then '待审核'
                            when 1 then '初审不通过'
                            when 2 then '初审通过'
                            when 3 then '复审不通过'
                            when 4 then '复审不通过_不可再次提交'
                            when 5 then '审核通过'
                            when 13 then '复审不通过_退回初审'
                            when 14 then '退回'
                            when -1 then '主动撤回编辑中'
                            else '未知状态'
                        end status
                        ,contract
                    from(
                    select
                            *
                    from xlsd.suppliers s 
                    where date(created_at) between %(begin_time)s and %(end_time)s) src
                    left join(
                            select
                                    id
                                    ,name
                            from xlsd.users u 
                    ) info on src.commerce = info.id) lst
            where bd_name not in ('张小卓','张澜','方涌超','赵乙都')
            group by bd_name
            order by count(case when status = '审核通过' then 1 else null end) desc
        """
        bd_contract_df = pd.read_sql(bd_contract_sql, self.engine,
                                     params={'begin_time': begin_time, 'end_time': end_time})

        # 品类入库数量
        category_num_sql = '''
                    select
                        bd_name
                        ,category_name
                        ,count(case when src.status = '入库' then 1 else null end) put
                    from(
                    select
                            id
                            ,user_id
                            ,supplier_id
                            ,name
                            ,case status
                                when 0 then '待审核 | 初审中 | 特批-初审中'
                                when 1 then '初审不通过'
                                when 2 then '初审通过 | 复审中 | 特批-复审中'
                                when 3 then '复审不通过'
                                when 4 then '复审不通过（不可再次提交）'
                                when 5 then '复审通过 | 成本审核中'
                                when 6 then '待成本审核 - 在成本审核不通过后，供应商可以选择只修改机制，从而无需再次【初审】和【复审】，直接到达【成本终审】（6是个意外）'
                                when 7 then '特批-复审不通过'
                                when 8 then '特批-审核通过 | 成本审核中'
                                when 9 then '成本-不通过'
                                when 10 then '成本-通过 | 待入库'
                                when 11 then '入库'
                                when 12 then '不允许入库'
                                when 13 then '复审不通过 - 退回初审，包括招商商品和主播自采'
                                when 14 then '退回 - 把已通过的退回，编辑后重新审核'
                                when -1 then '编辑中，这是老平台的数据'
                                else '未知状态'
                            end status
                            ,substring_index(class,',',1) item_category
                            ,created_at
                            ,updated_at
                    from xlsd.products p 
                    where by_anchor = 0
                            and date(reviewed_at) between %(begin_time)s and %(end_time)s)src
                    left join(
                            select
                                    id
                                    ,name
                                    ,commerce
                            from xlsd.suppliers s  
                    )info on src.supplier_id = info.id
                    left join(
                            select
                                    id
                                    ,name bd_name
                            from xlsd.users u 
                    ) us on info.commerce = us.id
                    left join(
                        select 
                            *
                        from ods.ods_item_category_path
                    ) it ON src.item_category COLLATE utf8mb4_general_ci = it.category_id COLLATE utf8mb4_general_ci
                    where status in ('入库')
                        and bd_name not in ('张小卓','张澜','方涌超','赵乙都')
                    group by bd_name,category_name
                    order by
                        bd_name asc
                        ,put desc
                '''
        category_num_df = pd.read_sql(category_num_sql, self.engine,
                                      params={'begin_time': begin_time, 'end_time': end_time})

        # 徒弟主播覆盖率
        td_cover_sql = """
            select
                bd_name
                ,13 all_num
                ,count(distinct anchor_name) cover_num
                ,case when count(distinct anchor_name) = 0 then 0 else count(distinct anchor_name) / 13 end cover_rate
            from(
                select
                        DISTINCT
                        bd_name
                        ,order_date
                        ,account_id
                        ,anchor_name
                        ,product_id
                from dws.dws_ks_big_tbl dkeh
                where order_date between %(begin_time)s and %(end_time)s
                        and bd_name is not null
                        and origin_gmv > 200
                        and anchor_name in ('墨晨夏','钱亮','古丽','周兰','羽儿','杜彪','小米粒','林一泽','赵龙','阿凯','毛柯泽','兰姐','宋铭恩')
                        ) src
            group by
                bd_name
        """
        td_cover_df = pd.read_sql(td_cover_sql, self.engine, params={'begin_time': begin_time, 'end_time': end_time})

        # 徒弟主播上品数据
        td_product_sql = f"""
                    select
                        bd_name
                        ,anchor_name
                        ,count(*) pd_num
                    from(
                        select
                                DISTINCT
                                bd_name
                                ,anchor_name
                                ,product_id
                        from dws.dws_ks_ec_2hourly dkeh
                        where order_date between %(begin_time)s and %(end_time)s
                                and bd_name is not null
                                and origin_gmv > 200
                                and anchor_name in ('墨晨夏','钱亮','古丽','周兰','羽儿','杜彪','小米粒','林一泽','赵龙','阿凯','毛柯泽','兰姐','宋铭恩')
                                ) src
                    group by
                        bd_name
                        ,anchor_name
                    order by
                        bd_name asc
                        ,pd_num desc
                """
        td_product_df = pd.read_sql(td_product_sql, self.engine,
                                    params={'begin_time': begin_time, 'end_time': end_time})

        # 爆品数据
        explosive_product_sql = f'''
                    select
                        item_id
                        ,item_title
                        ,anchor_name
                        ,bd_name
                        ,sum(origin_gmv) origin_gmv
                        ,sum(final_gmv) final_gmv
                        ,sum(final_gmv)/sum(origin_gmv) final_rate
                        ,sum(estimated_income + coalesce(estimated_service_income,0)) estimated_income 
                        ,case when sum(final_gmv) = 0 then 0 else sum(estimated_income + coalesce(estimated_service_income,0))/sum(final_gmv) end estimated_rate
                    from dws.dws_ks_ec_2hourly dkeh 
                    where order_date between %(begin_time)s and %(end_time)s
                    group by
                        item_id
                        ,item_title
                        ,anchor_name
                        ,bd_name
                    order by
                        origin_gmv desc
                '''
        explosive_product_df = pd.read_sql(explosive_product_sql, self.engine,
                                           params={'begin_time': begin_time, 'end_time': end_time})

        # 售卖商品退货情况
        return_rate_sql = """
             select
                order_date
                ,item_id
                ,item_title
                ,origin_gmv
                ,final_gmv
                ,1-final_gmv/origin_gmv return_rate
            from dws.dws_ks_big_tbl dkeh
            where account_id = '18541124'
                and order_date between %(begin_time)s and %(end_time)s
            order by
                origin_gmv desc
        """
        return_rate_df = pd.read_sql(return_rate_sql, self.engine,
                                     params={'begin_time': begin_time, 'end_time': end_time})

        return {
            'bd_gmv_df': bd_gmv_df,
            'bd_product_df': bd_product_df,
            'bd_put_df': bd_put_df,
            'bd_contract_df': bd_contract_df,
            'category_num_df': category_num_df,
            'td_cover_df': td_cover_df,
            'td_product': td_product_df,
            'explosive_product_df': explosive_product_df,
            'return_rate_df': return_rate_df
        }

    def process_data(self, data: Dict, **kwargs) -> Dict:
        bd_gmv_df = data['bd_gmv_df']
        bd_product_df = data['bd_product_df']
        bd_put_df = data['bd_put_df']
        bd_contract_df = data['bd_contract_df']
        category_num_df = data['category_num_df']
        td_cover_df = data['td_cover_df']
        td_product_df = data['td_product']
        explosive_product_df = data['explosive_product_df']
        return_rate_df = data['return_rate_df']

        bd_gmv_df.rate = bd_gmv_df.rate.apply(self.percent_convert)
        bd_gmv_df = bd_gmv_df.rename(columns={
            'bd_name': '商务名',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'estimated_income': '佣金',
            'rate': '佣金率'
        })

        bd_product_df = bd_product_df.rename(columns={
            'bd_name': '商务名',
            'anchor_name': '主播名',
            'num': '上品数量'
        })

        bd_put_df = bd_put_df.rename(columns={
            'bd_name': '商务名',
            'wait_put': '待入库数量',
            'put': '入库数量'
        })

        bd_contract_df = bd_contract_df.rename(columns={
            'bd_name': '商务名',
            'enter_num': '入驻数量',
            'contract_num': '合同数量'
        })

        category_num_df = category_num_df.rename(columns={
            'bd_name': '商务名',
            'category_name': '类目',
            'put': '入库数量'
        })

        td_cover_df.cover_rate = td_cover_df.cover_rate.apply(self.percent_convert)
        td_cover_df = td_cover_df.rename(columns={
            'bd_name': '商务',
            'all_num': '有目标徒弟主播数',
            'cover_num': '覆盖主播数',
            'cover_rate': '覆盖率'
        })

        td_product_df = td_product_df.rename(columns={
            'bd_name': '商务名',
            'anchor_name': '主播名',
            'pd_num': '上品数'
        })

        explosive_product_df.final_rate = explosive_product_df.final_rate.apply(self.percent_convert)
        explosive_product_df.estimated_rate = explosive_product_df.estimated_rate.apply(self.percent_convert)
        explosive_product_df = explosive_product_df.rename(columns={
            'item_id': '商品ID',
            'item_title': '商品名称',
            'anchor_name': '主播名称',
            'bd_name': '商务',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'final_rate': '结算率',
            'estimated_income': '佣金',
            'estimated_rate': '综合佣金率'
        })

        return_rate_df.return_rate = return_rate_df.return_rate.apply(self.percent_convert)
        return_rate_df.order_date = return_rate_df.order_date.apply(lambda x: x.strftime('%Y-%m-%d'))
        return_rate_df = return_rate_df.rename(columns={
            'order_date': '售卖日期',
            'item_id': '商品ID',
            'item_title': '商品名称',
            'origin_gmv': '支付GMV',
            'final_gmv': '结算GMV',
            'return_rate': '退货率'
        })

        return {
            'bd_gmv_df': bd_gmv_df,
            'bd_product_df': bd_product_df,
            'bd_put_df': bd_put_df,
            'bd_contract_df': bd_contract_df,
            'category_num_df': category_num_df,
            'td_cover_df': td_cover_df,
            'td_product_df': td_product_df,
            'explosive_product_df': explosive_product_df,
            'return_rate_df': return_rate_df
        }

    def create_feishu_file(self, data_dic: Dict, **kwargs) -> Dict:
        logger.info(f'创建飞书文件')
        start_time = kwargs['data_interval_start'].in_tz('Asia/Shanghai')
        begin_time = start_time.subtract(days=1).start_of('week').strftime('%Y%m%d')
        end_time = start_time.subtract(days=1).end_of('week').strftime('%Y%m%d')
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')
        title = ('商务周报数据_' + begin_time + '_' + end_time + '_' + start_time.strftime('%Y%m%d%H%M'))

        result = self.feishu_sheet.create_spreadsheet(
            title=title, folder_token='LD2YfIEuKlHwg6dUS3OcGQNMnhd'
        )
        spreadsheet_token = result['spreadsheet']['spreadsheet_token']
        url = result['spreadsheet']['url']
        cps_sheet_gmv = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='商务GMV')
        cps_sheet_gmv = cps_sheet_gmv['replies'][0]['addSheet']['properties']['sheetId']

        cps_sheet_pd = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='商务主播上品数量')
        cps_sheet_pd = cps_sheet_pd['replies'][0]['addSheet']['properties']['sheetId']

        cps_sheet_put = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='商务入库数量')
        cps_sheet_put = cps_sheet_put['replies'][0]['addSheet']['properties']['sheetId']

        cps_sheet_contract = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='商务合同数量')
        cps_sheet_contract = cps_sheet_contract['replies'][0]['addSheet']['properties']['sheetId']

        cps_sheet_category = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='类目入库数量')
        cps_sheet_category = cps_sheet_category['replies'][0]['addSheet']['properties']['sheetId']

        cps_sheet_td_cover = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='徒弟主播覆盖率')
        cps_sheet_td_cover = cps_sheet_td_cover['replies'][0]['addSheet']['properties']['sheetId']

        cps_sheet_td_product = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token,
                                                              title='徒弟主播上品数量')
        cps_sheet_td_product = cps_sheet_td_product['replies'][0]['addSheet']['properties']['sheetId']

        cps_sheet_explosive = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='爆品数据')
        cps_sheet_explosive = cps_sheet_explosive['replies'][0]['addSheet']['properties']['sheetId']

        cps_sheet_return = self.feishu_sheet.create_sheet(spreadsheet_token=spreadsheet_token, title='售卖商品退货情况')
        cps_sheet_return = cps_sheet_return['replies'][0]['addSheet']['properties']['sheetId']

        return {
            'spreadsheet_token': spreadsheet_token,
            'cps_sheet_id': [cps_sheet_gmv, cps_sheet_pd, cps_sheet_put, cps_sheet_contract, cps_sheet_category,
                             cps_sheet_td_cover, cps_sheet_td_product, cps_sheet_explosive, cps_sheet_return],
            'url': url,
            'title': title
        }

    def render_feishu_format(self, processed_data: Dict, spreadsheet_token: str, sheet_id: str):
        bd_gmv = processed_data['bd_gmv_df']
        bd_product_num = processed_data['bd_product_df']
        bd_put_num = processed_data['bd_put_df']
        bd_contract_num = processed_data['bd_contract_df']
        category_num = processed_data['category_num_df']
        td_cover = processed_data['td_cover_df']
        td_product = processed_data['td_product_df']
        explosive_product = processed_data['explosive_product_df']
        return_rate = processed_data['return_rate_df']

        style_dict = {
            'A1:' + self.col_convert(bd_gmv.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                'backColor': '#FFFF00'
            },
            'A1:' + self.col_convert(bd_gmv.shape[1]) + str(bd_gmv.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER'
            }
        }
        style_dict_1 = {
            'A1:' + self.col_convert(bd_product_num.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                'backColor': '#FFFF00'
            },
            'A1:' + self.col_convert(bd_product_num.shape[1]) + str(bd_product_num.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER'
            }
        }
        style_dict_2 = {
            'A1:' + self.col_convert(bd_put_num.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                'backColor': '#FFFF00'
            },
            'A1:' + self.col_convert(bd_put_num.shape[1]) + str(bd_put_num.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER'
            }
        }
        style_dict_3 = {
            'A1:' + self.col_convert(bd_contract_num.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                'backColor': '#FFFF00'
            },
            'A1:' + self.col_convert(bd_contract_num.shape[1]) + str(bd_contract_num.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER'
            }
        }
        style_dict_4 = {
            'A1:' + self.col_convert(category_num.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                'backColor': '#FFFF00'
            },
            'A1:' + self.col_convert(category_num.shape[1]) + str(category_num.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER'
            }
        }
        style_dict_5 = {
            'A1:' + self.col_convert(td_cover.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                'backColor': '#FFFF00'
            },
            'A1:' + self.col_convert(td_cover.shape[1]) + str(td_cover.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER'
            }
        }
        style_dict_6 = {
            'A1:' + self.col_convert(explosive_product.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                'backColor': '#FFFF00'
            },
            'A1:' + self.col_convert(explosive_product.shape[1]) + str(explosive_product.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER'
            }
        }
        style_dict_7 = {
            'A1:' + self.col_convert(return_rate.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                'backColor': '#FFFF00'
            },
            'A1:' + self.col_convert(return_rate.shape[1]) + str(return_rate.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER'
            }
        }
        style_dict_8 = {
            'A1:' + self.col_convert(td_product.shape[1]) + '1': {
                'font': {
                    'bold': True
                },
                'backColor': '#FFFF00'
            },
            'A1:' + self.col_convert(td_product.shape[1]) + str(td_product.shape[0] + 1): {
                'hAlign': 1,
                'vAlign': 1,
                'borderType': 'FULL_BORDER'
            }
        }

        self.feishu_sheet.write_df_replace(dat=bd_gmv, spreadsheet_token=spreadsheet_token, sheet_id=sheet_id[0],
                                           to_char=False)
        self.feishu_sheet.write_df_replace(dat=bd_product_num, spreadsheet_token=spreadsheet_token,
                                           sheet_id=sheet_id[1], to_char=False)
        self.feishu_sheet.write_df_replace(dat=bd_put_num, spreadsheet_token=spreadsheet_token, sheet_id=sheet_id[2],
                                           to_char=False)
        self.feishu_sheet.write_df_replace(dat=bd_contract_num, spreadsheet_token=spreadsheet_token,
                                           sheet_id=sheet_id[3], to_char=False)
        self.feishu_sheet.write_df_replace(dat=category_num, spreadsheet_token=spreadsheet_token,
                                           sheet_id=sheet_id[4], to_char=False)
        self.feishu_sheet.write_df_replace(dat=td_cover, spreadsheet_token=spreadsheet_token,
                                           sheet_id=sheet_id[5], to_char=False)
        self.feishu_sheet.write_df_replace(dat=td_product, spreadsheet_token=spreadsheet_token,
                                           sheet_id=sheet_id[6], to_char=False)
        self.feishu_sheet.write_df_replace(dat=explosive_product, spreadsheet_token=spreadsheet_token,
                                           sheet_id=sheet_id[7], to_char=False)
        self.feishu_sheet.write_df_replace(dat=return_rate, spreadsheet_token=spreadsheet_token,
                                           sheet_id=sheet_id[8], to_char=False)
        for key, value in style_dict.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=sheet_id[0], ranges=key, styles=value
            )
        for key, value in style_dict_1.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=sheet_id[1], ranges=key, styles=value
            )
        for key, value in style_dict_2.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=sheet_id[2], ranges=key, styles=value
            )
        for key, value in style_dict_3.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=sheet_id[3], ranges=key, styles=value
            )
        for key, value in style_dict_4.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=sheet_id[4], ranges=key, styles=value
            )
        for key, value in style_dict_5.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=sheet_id[5], ranges=key, styles=value
            )
        for key, value in style_dict_8.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=sheet_id[6], ranges=key, styles=value
            )
        for key, value in style_dict_6.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=sheet_id[7], ranges=key, styles=value
            )
        for key, value in style_dict_7.items():
            self.feishu_sheet.style_cells(
                spreadsheet_token=spreadsheet_token, sheet_id=sheet_id[8], ranges=key, styles=value
            )

        return processed_data

    def send_card(self, url: str, title: str, data_dict: Dict):
        logger.info(f'发送卡片')

        data = {
            'title': '商务周报数据',
            'file_name': title,
            'url': url,
            'description': '数据请见下方链接附件'
        }

        robot = self.feishu_robot
        robot.send_msg_card(data=data, card_id=self.card_id, version_name='1.0.1')


bd_week_dag = AbstractBdWeek()
bd_week_dag.create_dag()
