from abc import ABC
from typing import Dict, Any
import pendulum

import pandas as pd
from airflow.models import Variable
from dags.push.zhaoyifan.data_push_zhao.base_dag import BaseDag
from dags.push.zhaoyifan.data_push_zhao.format_utils import *


class PerformanceDaily(BaseDag):
    def __init__(self):
        super().__init__(
            dag_id='push_performance_daily_v2',
            tags=['push', 'performance_daily'],
            robot_url=Variable.get('ROBA'),
            schedule='0 5 * * *'
        )
        self.card_id = ['AAq4Y3orMfVcg', 'AAq4WshUF943P', 'AAq4WshUF943P', 'AAq4WslajpK40']

    def fetch_data_logic(self, date_interval: Dict[str, Any]) -> Dict[str, Any]:
        # date_interval = {
        #     "yes_ds": date_interval['yes_ds'],
        #     "month_start_ds": date_interval['month_start_ds'],
        #     "month": '2025-06'
        # }

        ec_month_detail_sql = f'''
            select 
                bt.order_date
                ,bt.anchor_name
                ,origin_gmv
                ,final_gmv
                ,coalesce(predict_income_belong_org,0) tol_income
            from (
                select 
                    order_date 
                    ,anchor_name 
                    ,sum(origin_gmv) origin_gmv 
                    ,sum(final_gmv) final_gmv 
                from dws.dws_ks_big_tbl dkbt 
                where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                group by 1,2
            ) bt 
            left join (
                select 
                    order_date 
                    ,anchor_name 
                    ,sum(coalesce(predict_income_belong_org,0)) predict_income_belong_org
                from ads.ads_ks_estimated_pnl
                where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                group by 1,2
            ) pd on bt.anchor_name = pd.anchor_name and bt.order_date =  pd.order_date
            union all 
            select 
                order_date
                ,'切片' anchor_name
                ,sum(origin_gmv) origin_gmv
                ,sum(final_gmv) final_gmv
                ,sum(tol_income) tol_income
            from (
                select 
                    order_date 
                    ,origin_gmv
                    ,final_gmv
                    ,coalesce(estimated_income,0) + coalesce(estimated_service_income,0) tol_income
                from dws.dws_ks_slice_slicer dkss 
                where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                union all
                select 
                    order_date 
                    ,origin_gmv
                    ,final_gmv
                    ,coalesce(estimated_income,0) + coalesce(estimated_service_income,0) tol_income
                from dws.dws_ks_slice_recreation dksr 
                where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                union all
                select 
                    order_date 
                    ,origin_gmv
                    ,final_gmv
                    ,coalesce(estimated_income,0) + coalesce(estimated_service_income,0) tol_income
                from dws.dws_ks_slice_mcn dksm
                where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
            ) slice
            group by 1,2
        '''

        target_sql = f'''
            select
                sum(target_final * 10000) final_target
            from ods.ods_fs_gmv_target ofgt
            where month = '{date_interval['month']}'
        '''

        ec_month_detail_df = pd.read_sql(ec_month_detail_sql, self.engine)
        ec_month_df = pd.DataFrame(ec_month_detail_df.sum(numeric_only=True)).T
        ec_month_df['final_rate'] = ec_month_df['final_gmv'] / ec_month_df['origin_gmv']
        target_df = pd.read_sql(target_sql, self.engine)
        ec_month_df['target_success_rate'] = ec_month_df['final_gmv'] / target_df['final_target']
        ec_yes_df = ec_month_detail_df[ec_month_detail_df.order_date.astype(str) == date_interval['yes_ds']]
        ec_yes_df = pd.DataFrame(ec_yes_df.sum(numeric_only=True)).T
        ec_yes_df['final_rate'] = ec_yes_df['final_gmv'] / ec_yes_df['origin_gmv']

        # leader_day_pf_sql = f'''
        #     select
        #        gmv.anchor_name project
        #        ,sum(origin_gmv) origin_gmv
        #        ,sum(final_gmv) final_gmv
        #        ,sum(final_gmv) / sum(origin_gmv) final_rate
        #        ,sum(coalesce(predict_income_belong_org,0)) commission_income
        #    from (
        #        select
        #            anchor_name
        #            ,sum(origin_gmv) origin_gmv
        #            ,sum(final_gmv) final_gmv
        #        from dws.dws_ks_big_tbl dkbt
        #        where order_date  = '{date_interval['yes_ds']}'
        #        group by
        #            anchor_name
        #    )gmv
        #    left join (
        #        select
        #            anchor_name
        #            ,predict_income_belong_org
        #        from ads.ads_ks_estimated_pnl akep
        #        where order_date = '{date_interval['yes_ds']}'
        #    )ads on gmv.anchor_name = ads.anchor_name
        #    group by 1
        #    union all
        #    select
        #       '切片' project
        #       ,sum(origin_gmv) origin_gmv
        #       ,sum(final_gmv) final_gmv
        #       ,sum(final_gmv) / sum(origin_gmv) final_rate
        #       ,sum(tol_income) tol_income
        #    from (
        #         select
        #             order_date
        #             ,origin_gmv
        #             ,final_gmv
        #             ,coalesce(estimated_income,0) + coalesce(estimated_service_income,0) tol_income
        #         from dws.dws_ks_slice_slicer dkss
        #         where order_date  = '{date_interval['yes_ds']}'
        #         union all
        #         select
        #             order_date
        #             ,origin_gmv
        #             ,final_gmv
        #             ,coalesce(estimated_income,0) + coalesce(estimated_service_income,0) tol_income
        #         from dws.dws_ks_slice_recreation dksr
        #         where order_date  = '{date_interval['yes_ds']}'
        #         union all
        #         select
        #             order_date
        #             ,origin_gmv
        #             ,final_gmv
        #             ,coalesce(estimated_income,0) + coalesce(estimated_service_income,0) tol_income
        #         from dws.dws_ks_slice_mcn dksm
        #         where order_date  = '{date_interval['yes_ds']}'
        #    ) slice
        #    group by 1
        #    order by
        #      case project
        #          when '乐总' then 1
        #          when '墨晨夏' then 2
        #          when '切片' then 3
        #          else 4
        #      end
        #      ,origin_gmv desc
        #   '''
        leader_day_pf_sql = f'''
            select
				gmv.order_date
               ,gmv.anchor_name project
               ,sum(origin_gmv) origin_gmv
               ,sum(final_gmv) final_gmv
               ,sum(final_gmv) / sum(origin_gmv) final_rate
               ,sum(coalesce(predict_income_belong_org,0)) commission_income
           from (
               select
               		order_date
                   ,anchor_name
                   ,sum(origin_gmv) origin_gmv
                   ,sum(final_gmv) final_gmv
               from dws.dws_ks_big_tbl dkbt 
               where order_date  between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
               group by
                   1,2
           )gmv
           left join (
               select
               		order_date
                   ,anchor_name
                   ,sum(predict_income_belong_org) predict_income_belong_org
               from ads.ads_ks_estimated_pnl akep 
               where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
               group by 1,2
           )ads on gmv.anchor_name = ads.anchor_name and gmv.order_date = ads.order_date
           group by 1,2
           union all
           select 
           	   order_date
              ,'切片' project
              ,sum(origin_gmv) origin_gmv
              ,sum(final_gmv) final_gmv
              ,sum(final_gmv) / sum(origin_gmv) final_rate
              ,sum(tol_income) tol_income
           from (
                select 
                    order_date 
                    ,origin_gmv
                    ,final_gmv
                    ,coalesce(estimated_income,0) + coalesce(estimated_service_income,0) tol_income
                from dws.dws_ks_slice_slicer dkss 
                where order_date  between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                union all
                select 
                    order_date 
                    ,origin_gmv
                    ,final_gmv
                    ,coalesce(estimated_income,0) + coalesce(estimated_service_income,0) tol_income
                from dws.dws_ks_slice_recreation dksr 
                where order_date  between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                union all
                select 
                    order_date 
                    ,origin_gmv
                    ,final_gmv
                    ,coalesce(estimated_income,0) + coalesce(estimated_service_income,0) tol_income
                from dws.dws_ks_slice_mcn dksm
                where order_date  between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
           ) slice
           group by 1,2
           order by
           	  order_date
             ,case project
                 when '乐总' then 1
                 when '墨晨夏' then 2
                 when '切片' then 3
                 else 4
             end
             ,origin_gmv desc
        '''

        tol_df = pd.read_sql(leader_day_pf_sql, self.engine)
        yes_tol_df = tol_df[tol_df.order_date.astype(str) == date_interval['yes_ds']]
        month_tol_df = tol_df[['project', 'origin_gmv', 'final_gmv', 'commission_income']]
        month_tol_df = month_tol_df.groupby('project').sum().reset_index()
        custom_order = {'乐总': 0, '墨晨夏': 1, '切片': 2}
        month_tol_df['sort_key'] = month_tol_df['project'].map(custom_order)
        month_tol_df = month_tol_df.sort_values(
            ['sort_key', 'origin_gmv'], ascending=[True, False], na_position='last'
        ).drop('sort_key', axis=1)
        month_tol_df['final_rate'] = month_tol_df.final_gmv / month_tol_df.origin_gmv

        # leader_month_target_sql = f'''
        #     select
        #      	  project
        #      	  ,sum(origin_gmv) origin_gmv
	    #           ,sum(final_gmv) final_gmv
	    #           ,sum(final_gmv) / sum(origin_gmv) final_rate
	    #           ,sum(target_final) target_final
	    #           ,sum(final_gmv) / sum(target_final) target_success_rate
	    #           ,sum(income) income
	    #     from (
	    #          select
	    #              gmv.anchor_name project
	    #             ,sum(origin_gmv) origin_gmv
	    #             ,sum(final_gmv) final_gmv
	    #             ,0 target_final
	    #             ,sum(coalesce(predict_income_belong_org,0)) income
        #         from  (
        #             select
        #                 anchor_name
        #                 ,sum(origin_gmv) origin_gmv
        #                 ,sum(final_gmv) final_gmv
        #             from dws.dws_ks_big_tbl dkbt
        #             where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
        #             group by
        #                 anchor_name
        #         ) gmv
        #         left join (
        #             select
        #                 anchor_name
        #                 ,sum(coalesce(predict_income_belong_org,0)) predict_income_belong_org
        #             from ads.ads_ks_estimated_pnl
        #             where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
        #             group by
        #                 anchor_name
        #         )ads on gmv.anchor_name = ads.anchor_name
        #         group by 1,4
        #         union all
	    #         select
	    #            anchor
        #           ,0 origin_gmv
        #           ,0 final_gmv
        #           ,sum(target_final * 10000) target_final
        #           ,0 income
	    #         from ods.ods_fs_gmv_target ofgt
	    #         where month = '{date_interval['month']}'
	    #         	and anchor != '刘海州'
	    #         group by 1,2,3,5
        #     )src
        #     group by 1
        #     union all
        #     select
        #         '切片' project
        #         ,origin_gmv
        #         ,final_gmv
        #         ,final_gmv / origin_gmv final_rate
        #         ,coalesce(target_final,0) target_final
        #         ,final_gmv  / coalesce(target_final,0) target_success_rate
        #         ,tol_income income
        #     from (
        #         select
        #             sum(origin_gmv) origin_gmv
        #             ,sum(final_gmv) final_gmv
        #             ,sum(tol_income) tol_income
        #         from (
        #             select
        #                 origin_gmv
        #                 ,final_gmv
        #                 ,coalesce(estimated_income,0) + coalesce(estimated_service_income,0) tol_income
        #             from dws.dws_ks_slice_slicer dkss
        #             where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
        #             union all
        #             select
        #                 origin_gmv
        #                 ,final_gmv
        #                 ,coalesce(estimated_income,0) + coalesce(estimated_service_income,0) tol_income
        #             from dws.dws_ks_slice_recreation dksr
        #             where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
        #             union all
        #             select
        #                 origin_gmv
        #                 ,final_gmv
        #                 ,coalesce(estimated_income,0) + coalesce(estimated_service_income,0) tol_income
        #             from dws.dws_ks_slice_mcn dksm
        #             where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
        #         ) tol
        #     ) slice,
        #     (
        #         select
        #             sum(coalesce(target_final,0) * 10000) target_final
        #         from ods.ods_fs_gmv_target ofgt
        #         where month = '{date_interval['month']}' and anchor = '刘海州'
        #     )tar
        #     order by
        #         case project
        #           when '乐总' then 1
        #           when '墨晨夏' then 2
        #           when '切片' then 3
        #           else 4
        #         end
        #         ,origin_gmv desc
        # '''
        leader_month_target_sql = f'''
            select 
                   project
                  ,sum(final_gmv) final_gmv
                  ,sum(target_final) target_final
                  ,sum(final_gmv) / sum(target_final) target_success_rate
            from (
                  select
                     anchor_name project
                    ,sum(final_gmv) final_gmv
                    ,0 target_final
                  from dws.dws_ks_big_tbl dkbt
                  where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                  group by
                      1,3
                  union all
                  select 
                     anchor project
                    ,0 final_gmv
                    ,sum(target_final * 10000) target_final
                  from ods.ods_fs_gmv_target ofgt 
                  where month = '{date_interval['month']}'
                    and anchor != '刘海州'
                  group by 1,2
            ) src
            group by 1
            union all
            select
                '切片' project
                ,final_gmv
                ,target_final
                ,final_gmv  / target_final target_success_rate
            from (
                select 
                    sum(final_gmv) final_gmv
                from (
                    select 
                        final_gmv
                        ,coalesce(estimated_income,0) + coalesce(estimated_service_income,0) tol_income
                    from dws.dws_ks_slice_slicer dkss 
                    where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                    union all
                    select 
                        final_gmv
                        ,coalesce(estimated_income,0) + coalesce(estimated_service_income,0) tol_income
                    from dws.dws_ks_slice_recreation dksr 
                    where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                    union all
                    select 
                        final_gmv
                        ,coalesce(estimated_income,0) + coalesce(estimated_service_income,0) tol_income
                    from dws.dws_ks_slice_mcn dksm
                    where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                ) tol
            ) slice,
            (
                select
                    sum(coalesce(target_final,0) * 10000) target_final
                from ods.ods_fs_gmv_target ofgt 
                where month = '{date_interval['month']}' and anchor = '刘海州'
            )tar
            order by
                case project
                  when '乐总' then 1
                  when '墨晨夏' then 2
                  when '切片' then 3
                  else 4
                end
                ,final_gmv desc
        '''
        month_target_tol_df = pd.read_sql(leader_month_target_sql, self.engine)

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
                       from dim.dim_ks_account_info dkai
                       where line = '直播电商' or line = '其它'
                   ) tar
                   left join(
                           select
                         anchor
                         ,target_final * 10000 target_final
                       from ods.ods_fs_gmv_target ofgt
                       where month = '{date_interval['month']}' and anchor != '刘海州'
                   ) tg on tar.account_name = tg.anchor
                   left join (
                       select
                           anchor_name
                           ,sum(final_gmv) final_gmv
                       from dws.dws_ks_big_tbl dkbt 
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
                           from (
                                select 
                                    final_gmv
                                from dws.dws_ks_slice_slicer dkss 
                                where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                                union all
                                select
                                    final_gmv
                                from dws.dws_ks_slice_recreation dksr 
                                where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                                union all
                                select 
                                    final_gmv
                                from dws.dws_ks_slice_mcn dksm
                                where order_date between '{date_interval['month_start_ds']}' and '{date_interval['yes_ds']}'
                           ) slice
                       )gmv,
                       (
                           select
                               sum(target_final * 10000) target_final
                           from ods.ods_fs_gmv_target ofgt 
                           where month = '{date_interval['month']}' and anchor = '刘海州'
                       )tar)
                   ) lst
           '''
        monitor_df = pd.read_sql(sql, self.engine)

        return {
            'ec_month_df': ec_month_df,
            'target_df': target_df,
            'ec_yes_df': ec_yes_df,
            'yes_tol_df': yes_tol_df,
            'month_tol_df': month_tol_df,
            'month_target_tol_df': month_target_tol_df,
            'monitor_df': monitor_df,
            'date_interval': date_interval
        }

    def prepare_card_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        ec_month_df = data_dict['ec_month_df']
        ec_origin_gmv = ec_month_df.origin_gmv.iloc[0]
        if ec_origin_gmv == 0:
            company_origin_gmv = '\\-'
            company_final_rate = '\\-'
        else:
            company_origin_gmv = str(round(ec_month_df.origin_gmv.iloc[0] / 10000, 1))
            company_final_rate = str(
                '{:.2f}%'.format(ec_month_df.final_rate.iloc[0] * 100))

        target_df = data_dict['target_df']
        company_target = target_df.final_target.iloc[0]
        if company_target == 0 or pd.isnull(company_target):
            company_target = '\\-'
            company_target_success_rate = '\\-'
        else:
            company_target = str(round(target_df.final_target.iloc[0] / 10000, 1))
            company_target_success_rate = str(
                '{:.2f}%'.format(ec_month_df.target_success_rate.iloc[0] * 100))

        ec_yes_df = data_dict['ec_yes_df']
        company_yes_origin_gmv = ec_yes_df.origin_gmv.iloc[0]
        if company_yes_origin_gmv == 0:
            company_yes_origin_gmv = '\\-'
            company_yes_final_rate = '\\-'
        else:
            company_yes_origin_gmv = str(round(ec_yes_df.origin_gmv.iloc[0] / 10000, 1))
            company_yes_final_rate = str(
                '{:.2f}%'.format(ec_yes_df.final_rate.iloc[0] * 100))

        yes_tol_df = data_dict['yes_tol_df']
        print(yes_tol_df.to_dict)
        leader_yes_data = []
        for i in range(yes_tol_df.shape[0]):
            yes_origin_gmv = yes_tol_df.origin_gmv.iloc[i]
            if yes_origin_gmv == 0 or pd.isnull(yes_origin_gmv):
                yes_origin_gmv = '\\-'
                yes_final_rate = '\\-'
                yes_final_gmv = '\\-'
            else:
                yes_origin_gmv = str(yes_tol_df.origin_gmv.apply(amount_convert).iloc[i])
                yes_final_rate = str(
                    '{:.2f}%'.format(yes_tol_df.final_rate.iloc[i] * 100))
                yes_final_gmv = str(yes_tol_df.final_gmv.apply(amount_convert).iloc[i])
            leader_yes_data.append({
                'anchor_leader': str(yes_tol_df.project.iloc[i]),
                'leader_yes_origin_gmv': str(yes_origin_gmv),
                'leader_yes_residue_gmv': str(yes_final_gmv),
                'leader_yes_commission_income': str(yes_tol_df.commission_income.apply(amount_convert).iloc[i]),
                'leader_yes_retrun_rate': str(yes_final_rate)
            })
        print(leader_yes_data)

        month_tol_df = data_dict['month_tol_df']
        leader_month_data = []
        for i in range(month_tol_df.shape[0]):
            month_origin_gmv = month_tol_df.origin_gmv.iloc[i]
            if month_origin_gmv == 0 or pd.isnull(month_origin_gmv):
                month_origin_gmv = '\\-'
                month_final_rate = '\\-'
                month_final_gmv = '\\-'
            else:
                month_origin_gmv = str(month_tol_df.origin_gmv.apply(amount_convert).iloc[i])
                month_final_rate = str(
                    '{:.2f}%'.format(month_tol_df.final_rate.iloc[i] * 100))
                month_final_gmv = str(month_tol_df.final_gmv.apply(amount_convert).iloc[i])
            leader_month_data.append({
                'anchor_leader': str(month_tol_df.project.iloc[i]),
                'leader_yes_origin_gmv': str(month_origin_gmv),
                'leader_yes_residue_gmv': str(month_final_gmv),
                'leader_yes_commission_income': str(month_tol_df.commission_income.apply(amount_convert).iloc[i]),
                'leader_yes_retrun_rate': str(month_final_rate)
            })

        month_target_tol_df = data_dict['month_target_tol_df']
        print(month_target_tol_df)
        print(month_target_tol_df.target_final)
        anchor_data = []
        for i in range(month_target_tol_df.shape[0]):
            anchor_target_formatted = month_target_tol_df.target_final.iloc[i]
            if anchor_target_formatted == 0 or pd.isnull(anchor_target_formatted):
                anchor_target_formatted = '\\-'
                target_success_formatted = '\\-'
            else:
                anchor_target_formatted = str(month_target_tol_df.target_final.apply(amount_convert).iloc[i])
                target_success_formatted = str(
                    '{:.2f}%'.format(month_target_tol_df.target_success_rate.iloc[i] * 100))
            anchor_data.append({
                'anchor_leader': str(month_target_tol_df.project.iloc[i]),
                'leader_month_origin_gmv': str(month_target_tol_df.final_gmv.apply(amount_convert).iloc[i]),
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
                'yes_day': data_dict['date_interval']['yes_ds'],
                'leader_yes_data': leader_yes_data
            },
            'card_3': {
                'yes_day': data_dict['date_interval']['month_start_ds'] + ' to ' + data_dict['date_interval'][
                    'yes_ds'],
                'leader_yes_data': leader_month_data

            },
            'card_4': {
                'time_score': data_dict['date_interval']['month_start_ds'] + ' to ' + data_dict['date_interval'][
                    'yes_ds'],
                'anchor_data': anchor_data
            }
        }

    def write_to_sheet_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        month_target_tol_df = data_dict['month_target_tol_df']
        monitor_df = data_dict['monitor_df']
        self.feishu_sheet.write_df_to_cell('L33FsgXl6h3aZ0tVCzEcsf0qnGe', 'UGZTH6', month_target_tol_df, 1, 1,
                                           True, False)
        monitor_df['number'] = range(1, len(monitor_df) + 1)
        self.feishu_sheet.write_df_to_cell('L33FsgXl6h3aZ0tVCzEcsf0qnGe', 'Sfy8px', monitor_df, 1, 1, True, False)

        return {}

    def send_card_logic(self, card: Dict[str, Any], sheet):
        res_1 = {**card['card_1']}
        res_2 = {
            **card['card_2'],
            'title': '每日各主播业绩'
        }
        res_3 = {
            **card['card_3'],
            'title': '月度各主播业绩'
        }
        res_4 = {
            **card['card_4']
        }
        self.feishu_robot.send_msg_card(data=res_1, card_id=self.card_id[0], version_name='1.0.2')
        self.feishu_robot.send_msg_card(data=res_2, card_id=self.card_id[1], version_name='1.0.1')
        self.feishu_robot.send_msg_card(data=res_3, card_id=self.card_id[2], version_name='1.0.1')
        self.feishu_robot.send_msg_card(data=res_4, card_id=self.card_id[3], version_name='1.0.1')


dag = PerformanceDaily().create_dag()
