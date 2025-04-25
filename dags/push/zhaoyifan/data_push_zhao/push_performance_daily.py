from abc import ABC
from typing import Dict, Any
import pendulum

import pandas as pd
from airflow.models import Variable
from dags.push.zhaoyifan.data_push_zhao.base_dag import BaseDag


class BasePerformanceDaily(BaseDag):
    def __init__(self):
        super().__init__(
            dag_id='push_performance_daily',
            default_args={'owner': 'zhaoyifan',},
            tags=['push', 'performance_daily'],
            robot_url=Variable.get('SELFTEST'),
            schedule=None
        )
        self.card_id = ['AAq4Y3orMfVcg', 'AAq4Y35W9y9hv']

    def fetch_data_logic(self, date_interval: Dict[str, Any]) -> Dict[str, Any]:
        ec_month_detail_sql = f'''
            select
                dws.order_date
                ,dws.anchor_name
                ,origin_gmv
                ,final_gmv
                ,predict_income_belong_org tol_income
            from (
            select
                order_date
                ,anchor_name
                ,sum(origin_gmv) origin_gmv
                ,sum(final_gmv) final_gmv
            from dws.dws_ks_ec_2hourly dkeh
            where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
            group by 1,2
            ) dws
            left join (
                select
                    order_date
                    ,anchor_name
                    ,sum(coalesce(predict_income_belong_org,0)) predict_income_belong_org
                from ads.ads_estimated_pnl aep
                where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                group by 1,2
            )ads on dws.anchor_name = ads.anchor_name and dws.order_date = ads.order_date
            union all
            select
                    order_date
                    ,'切片' anchor_name
                ,sum(origin_gmv) origin_gmv
                ,sum(final_gmv) final_gmv
                ,sum(coalesce(estimated_income,0) + coalesce(estimated_service_income,0)) tol_income
            from dws.dws_ks_slice_daily dksd
            where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
            group by 1,2
        '''
        target_sql = f'''
            select
                sum(target_final * 10000) final_target
            from ods.ods_gmv_target ogt
            where month = '{date_interval['month']}'
        '''

        ec_month_detail_df = pd.read_sql(ec_month_detail_sql, self.engine)
        ec_month_df = pd.DataFrame(ec_month_detail_df.sum(numeric_only=True)).T
        ec_month_df['final_rate'] = ec_month_df['final_gmv'] / ec_month_df['origin_gmv']
        target_df = pd.read_sql(target_sql, self.engine)
        ec_month_df['target_success_rate'] = ec_month_df['final_rate'] / target_df['final_target']
        ec_yes_df = ec_month_detail_df[ec_month_detail_df.order_date.astype(str) >= date_interval['yes_ds']]
        ec_yes_df = pd.DataFrame(ec_yes_df.sum(numeric_only=True)).T

        leader_day_pf_sql = f'''
               select
                   case
                       when gmv.anchor_name = '乐总' then '乐总'
                       when gmv.anchor_name = '墨晨夏' then '墨晨夏'
                       when other_commission_belong = '墨晨夏' then '墨晨夏团队'
                       else '其他主播'
                   end project
                   ,sum(origin_gmv) origin_gmv
                   ,sum(final_gmv) final_gmv
                   ,sum(final_gmv) / sum(origin_gmv) final_rate
                   ,sum(coalesce(predict_income_belong_org,0)) commission_income
               from (
                   select
                       anchor_name
                       ,sum(origin_gmv) origin_gmv
                       ,sum(final_gmv) final_gmv
                   from dws.dws_ks_ec_2hourly dkeh
                   where order_date  = '{date_interval['yes_ds']}'
                   group by
                       anchor_name
               )gmv
               left join (
                   select
                       distinct anchor_name
                       ,other_commission_belong
                       ,line
                   from dim.dim_ks_anchor_info dkai
               )mo on gmv.anchor_name = mo.anchor_name
               left join (
                   select
                       anchor_name
                       ,predict_income_belong_org
                   from ads.ads_estimated_pnl aep
                   where order_date = '{date_interval['yes_ds']}'
               )ads on gmv.anchor_name = ads.anchor_name
               group by
                   case
                       when gmv.anchor_name = '乐总' then '乐总'
                       when gmv.anchor_name = '墨晨夏' then '墨晨夏'
                       when other_commission_belong = '墨晨夏' then '墨晨夏团队'
                       else '其他主播'
                   end
               union all
               select
                   '切片' project
                   ,sum(origin_gmv) origin_gmv
                   ,sum(final_gmv) final_gmv
                   ,sum(final_gmv) / sum(origin_gmv) final_rate
                   ,sum(coalesce(estimated_income,0)) + sum(coalesce(estimated_service_income,0)) commission_income
               from dws.dws_ks_slice_daily dksd
               where order_date  = '{date_interval['yes_ds']}'
               order by
                   case project
                       when '乐总' then 1
                       when '墨晨夏' then 2
                       when '墨晨夏团队' then 3
                       when '其他主播' then 4
                       when '切片' then 5
                       else 6
                 end
          '''

        yes_tol_df = pd.read_sql(leader_day_pf_sql, self.engine)
        yes_tol_df.origin_gmv = yes_tol_df.origin_gmv.apply(self.amount_convert)
        yes_tol_df.final_gmv = yes_tol_df.final_gmv.apply(self.amount_convert)
        yes_tol_df.commission_income = yes_tol_df.commission_income.apply(self.amount_convert)

        leader_month_target_sql = f'''
            select
                 case
                  when mo.anchor_name = '乐总' then '乐总'
                  when mo.anchor_name = '墨晨夏' then '墨晨夏'
                  when other_commission_belong = '墨晨夏' then '墨晨夏团队'
                  else '其他主播'
                end project
                ,sum(origin_gmv) origin_gmv
                ,sum(final_gmv) final_gmv
                ,sum(final_gmv) / sum(origin_gmv) final_rate
                ,sum(coalesce(target_final,0)) target_final
                ,sum(final_gmv) / sum(coalesce(target_final,0)) target_success_rate
                ,sum(coalesce(predict_income_belong_org,0)) income
            from  (
                select
                    distinct anchor_name
                    ,other_commission_belong
                    ,line
                from dim.dim_ks_anchor_info dkai
                where line != '切片'
            ) mo
            left join (
                select
                  anchor
                  ,target_final * 10000 target_final
                from ods.ods_gmv_target ogt
                where month = '{date_interval['month']}'
            ) tar on mo.anchor_name	= tar.anchor
            left join (
                select
                    anchor_name
                    ,sum(origin_gmv) origin_gmv
                    ,sum(final_gmv) final_gmv
                from dws.dws_ks_ec_2hourly dkeh
                where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                group by
                    anchor_name
            ) gmv on gmv.anchor_name = mo.anchor_name
            left join (
                select
                    anchor_name
                    ,sum(coalesce(predict_income_belong_org,0)) predict_income_belong_org
                from ads.ads_estimated_pnl aep
                where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                group by
                    anchor_name
            )ads on gmv.anchor_name = ads.anchor_name
            group by
                 case
                  when mo.anchor_name = '乐总' then '乐总'
                  when mo.anchor_name = '墨晨夏' then '墨晨夏'
                  when other_commission_belong = '墨晨夏' then '墨晨夏团队'
                  else '其他主播'
                end
            union all
            select
                project
                ,origin_gmv
                ,final_gmv
                ,final_gmv / origin_gmv final_rate
                ,target_final
                ,final_gmv / target_final target_success_rate
                ,income
            from (
                select
                  '切片' project
                  ,sum(origin_gmv) origin_gmv
                  ,sum(final_gmv) final_gmv
                  ,sum(coalesce(estimated_income,0) + coalesce(estimated_service_income,0)) income
              from dws.dws_ks_slice_daily dksd
              where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
            )gmv,
            (
                select
                    sum(coalesce(target_final,0) * 10000) target_final
                from ods.ods_gmv_target ogt
                where month = '{date_interval['month']}' and anchor = '刘海州'
            )tar
            order by
                case project
                  when '乐总' then 1
                  when '墨晨夏' then 2
                  when '墨晨夏团队' then 3
                  when '其他主播' then 4
                  when '切片' then 5
                  else 6
            end
        '''
        tol_df = pd.read_sql(leader_month_target_sql, self.engine)
        month_tol_df = tol_df[['project', 'origin_gmv', 'final_gmv', 'final_rate', 'income']]
        month_tol_df.origin_gmv = month_tol_df.origin_gmv.apply(self.amount_convert)
        month_tol_df.final_gmv = month_tol_df.final_gmv.apply(self.amount_convert)
        month_tol_df.income = month_tol_df.income.apply(self.amount_convert)

        month_target_tol_df = tol_df[['project', 'final_gmv', 'target_final', 'target_success_rate', 'income']]

        sql = f'''
           select
               ROW_NUMBER() over( order by
               case
                   when project = '乐总' then 1
                   when project = '墨晨夏' then 2
                   when project = '切片' then 3
                   when project = '仙乐甄选' then 5
                   else 4
               end asc
               ,final_gmv desc
               ,target_final desc) number
               ,project
               ,final_gmv
               ,coalesce(target_final,0) target_final
               ,target_success_rate
               ,remark
           from(
               (select
                   project
                   ,final_gmv
                   ,target_final
                   ,target_success_rate
                   ,remark
               from(
                   select
                       account_name project
                       ,coalesce(sum(final_gmv),0) final_gmv
                       ,sum(target_final) target_final
                       ,coalesce(case when sum(target_final) = 0 then 0 else sum(final_gmv) / sum(target_final) end,0) target_success_rate
                       ,case when other_commission_belong = '墨晨夏' then '墨晨夏徒弟' else ' ' end remark
                   from (
                       select
                           distinct anchor_name account_name
                           ,other_commission_belong
                       from dim.dim_ks_anchor_info dkai
                       where line = '直播电商' or line = '其它'
                   ) tar
                   left join(
                           select
                         anchor
                         ,target_final * 10000 target_final
                       from ods.ods_gmv_target ogt
                       where month = '{date_interval['month']}' and anchor != '刘海州'
                   ) tg on tar.account_name = tg.anchor
                   left join (
                       select
                           anchor_name
                           ,sum(final_gmv) final_gmv
                       from dws.dws_ks_ec_2hourly dkeh
                       where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                       group by
                           anchor_name
                   )gmv on gmv.anchor_name = tar.account_name
                   group by
                       account_name
                       ,case when other_commission_belong = '墨晨夏' then '墨晨夏徒弟' else ' ' end) lg
                   where final_gmv != 0 or target_final != 0)
                   union all(
                       select
                           project
                           ,final_gmv
                           ,target_final
                           ,final_gmv / target_final target_success_rate
                           ,' ' remark
                       from (
                           select
                               '切片' project
                               ,sum(final_gmv) final_gmv
                           from dws.dws_ks_slice_daily dksd
                           where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                       )gmv,
                       (
                           select
                               sum(target_final * 10000) target_final
                           from ods.ods_gmv_target ogt
                           where month = '{date_interval['month']}' and anchor = '刘海州'
                       )tar)
                   ) lst
           '''
        monitor_df = pd.read_sql(sql, self.engine)

        return {
            'df': {
                'ec_month_df': ec_month_df,
                'target_df': target_df,
                'ec_yes_df': ec_yes_df,
                'yes_tol_df': yes_tol_df,
                'month_tol_df': month_tol_df,
                'month_target_tol_df': month_target_tol_df,
                'monitor_df': monitor_df
            },
            'date_interval': date_interval
        }

    def prepare_card_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        ec_month_df = data_dict['df']['ec_month_df']
        ec_month_df['final_rate'] = ec_month_df['final_gmv'] / ec_month_df['origin_gmv']
        ec_origin_gmv = ec_month_df.origin_gmv.iloc[0]
        if ec_origin_gmv == 0:
            company_origin_gmv = '\\-'
            company_final_rate = '\\-'
        else:
            company_origin_gmv = str(round(ec_month_df.origin_gmv.iloc[0] / 10000, 1))
            company_final_rate = str(
                '{:.2f}%'.format(ec_month_df.final_rate.iloc[0] * 100))

        target_df = data_dict['df']['target_df']
        ec_month_df['target_success_rate'] = ec_month_df['final_gmv'] / target_df['final_target']
        company_target = target_df.final_target.iloc[0]
        if company_target == 0 or pd.isnull(company_target):
            company_target = '\\-'
            company_target_success_rate = '\\-'
        else:
            company_target = str(round(target_df.final_target.iloc[0] / 10000, 1))
            company_target_success_rate = str(
                '{:.2f}%'.format(ec_month_df.target_success_rate.iloc[0] * 100))

        ec_yes_df = data_dict['df']['ec_yes_df']
        ec_yes_df['final_rate'] = ec_yes_df['final_gmv'] / ec_yes_df['origin_gmv']
        company_yes_origin_gmv = ec_yes_df.origin_gmv.iloc[0]
        if company_yes_origin_gmv == 0:
            company_yes_origin_gmv = '\\-'
            company_yes_final_rate = '\\-'
        else:
            company_yes_origin_gmv = str(round(ec_yes_df.origin_gmv.iloc[0] / 10000, 1))
            company_yes_final_rate = str(
                '{:.2f}%'.format(ec_yes_df.final_rate.iloc[0] * 100))

        yes_tol_df = data_dict['df']['yes_tol_df']
        print(yes_tol_df.to_dict)
        leader_yes_data = []
        for i in range(yes_tol_df.shape[0]):
            yes_origin_gmv = yes_tol_df.origin_gmv.iloc[i]
            if yes_origin_gmv == '0.0元':
                yes_origin_gmv = '\\-'
                yes_final_rate = '\\-'
                yes_final_gmv = '\\-'
            else:
                yes_origin_gmv = str(yes_tol_df.origin_gmv.iloc[i])
                yes_final_rate = str(
                    '{:.2f}%'.format(yes_tol_df.final_rate.iloc[i] * 100))
                yes_final_gmv = str(yes_tol_df.final_gmv.iloc[i])
            leader_yes_data.append({
                'anchor_leader': str(yes_tol_df.project.iloc[i]),
                'leader_yes_origin_gmv': str(yes_origin_gmv),
                'leader_yes_residue_gmv': str(yes_final_gmv),
                'leader_yes_commission_income': str(yes_tol_df.commission_income.iloc[i]),
                'leader_yes_retrun_rate': str(yes_final_rate)
            })
        print(leader_yes_data)

        month_tol_df = data_dict['df']['month_tol_df']
        leader_month_data = []
        for i in range(month_tol_df.shape[0]):
            month_origin_gmv = month_tol_df.origin_gmv.iloc[i]
            if month_origin_gmv == '0.0元':
                month_origin_gmv = '\\-'
                month_final_rate = '\\-'
                month_final_gmv = '\\-'
            else:
                month_origin_gmv = str(month_tol_df.origin_gmv.iloc[i])
                month_final_rate = str(
                    '{:.2f}%'.format(month_tol_df.final_rate.iloc[i] * 100))
                month_final_gmv = str(month_tol_df.final_gmv.iloc[i])
            leader_month_data.append({
                'anchor_leader': str(month_tol_df.project.iloc[i]),
                'leader_month_origin_gmv': str(month_origin_gmv),
                'leader_month_residue_gmv': str(month_final_gmv),
                'leader_month_commission_income': str(month_tol_df.income.iloc[i]),
                'leader_success_rate': str(month_final_rate)
            })

        month_target_tol_df = data_dict['df']['month_target_tol_df']
        month_target_tol_df.final_gmv = month_target_tol_df.final_gmv.apply(self.amount_convert)
        month_target_tol_df.target_final = month_target_tol_df.target_final.apply(self.amount_convert)
        anchor_data = []
        for i in range(month_target_tol_df.shape[0]):
            anchor_target_formatted = month_target_tol_df.target_final.iloc[i]
            if anchor_target_formatted == '0.0元' or pd.isnull(anchor_target_formatted):
                anchor_target_formatted = '\\-'
                target_success_formatted = '\\-'
            else:
                anchor_target_formatted = str(month_target_tol_df.target_final.iloc[i])
                target_success_formatted = str(
                    '{:.2f}%'.format(month_target_tol_df.target_success_rate.iloc[i] * 100))
            anchor_data.append({
                'anchor_leader': str(month_target_tol_df.project.iloc[i]),
                'leader_month_origin_gmv': str(month_target_tol_df.final_gmv.iloc[i]),
                'leader_month_target': str(anchor_target_formatted),
                'leader_month_target_success_rate': str(target_success_formatted)
            })

        return {
            'card_1': {
                'time_score': data_dict['date_interval']['month_start_ds'] + ' to ' + data_dict['date_interval'][
                    'yes_ds'],
                'yes_day': data_dict['date_interval']['yes_ds'],
                'leader_yes_origin_gmv': str(company_yes_origin_gmv),
                'leader_yes_residue_gmv': str(round(ec_yes_df['final_gmv'].iloc[0] / 10000, 1)),
                'leader_yes_commission_income': str(round(ec_yes_df['tol_income'].iloc[0] / 10000, 1)),
                'leader_yes_retrun_rate': str(company_yes_final_rate),
                'gmv_sum': str(company_origin_gmv),
                'residue_gmv_sum': str(round(ec_month_df['final_gmv'].iloc[0] / 10000, 1)),
                'commission_sum': str(round(ec_month_df['tol_income'].iloc[0] / 10000, 1)),
                'anchor_return_date': str(company_final_rate),
                'anchor_target': str(company_target),
                'anchor_target_success_rate': str(company_target_success_rate),
            },
            'card_2': {
                'time_score': data_dict['date_interval']['month_start_ds'] + ' to ' + data_dict['date_interval'][
                    'yes_ds'],
                'yes_day': data_dict['date_interval']['yes_ds'],
                'leader_yes_data': leader_yes_data,
                'anchor_data': anchor_data,
                'leader_month_data': leader_month_data
            }
        }

    def write_to_sheet_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        month_target_tol_df = data_dict['df']['month_target_tol_df']
        monitor_df = data_dict['df']['monitor_df']
        self.feishu_sheet.write_df_to_cell('L33FsgXl6h3aZ0tVCzEcsf0qnGe', 'UGZTH6', month_target_tol_df, 1, 1,
                                           True, False)
        monitor_df['number'] = range(1, len(monitor_df) + 1)
        self.feishu_sheet.write_df_to_cell('L33FsgXl6h3aZ0tVCzEcsf0qnGe', 'Sfy8px', monitor_df, 1, 1, True, False)

        return {}

    def send_card_logic(self, card: Dict[str, Any], sheet):
        res_1 = {**card['card_1']}
        res_2 = {**card['card_2']}
        self.feishu_robot.send_msg_card(data=res_1, card_id=self.card_id[0], version_name='1.0.0')
        self.feishu_robot.send_msg_card(data=res_2, card_id=self.card_id[1], version_name='1.0.2')


dag = BasePerformanceDaily().create_dag()
