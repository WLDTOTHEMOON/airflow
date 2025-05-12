from abc import ABC
from typing import Dict, Any
from dags.push.zhaoyifan.data_push_zhao.format_utils import *
import pendulum
import pandas as pd
from airflow.models import Variable
from dags.push.zhaoyifan.data_push_zhao.base_dag import BaseDag


class RiskWeekly(BaseDag):
    def __init__(self):
        super().__init__(
            dag_id='push_risk_weekly',
            tags=['push', 'risk_weekly'],
            robot_url=Variable.get('TEST'),
            schedule='0 21 * * 7'
        )
        self.card_id = 'AAq4w6tLjK0Iy'

    def fetch_data_logic(self, date_interval: Dict[str, Any]) -> Dict[str, Any]:
        end_datetime = date_interval['end_datetime']
        week_start_date = end_datetime.start_of('week').subtract(weeks=1).format('YYYY-MM-DD')
        week_end_date = end_datetime.end_of('week').subtract(weeks=1).format('YYYY-MM-DD')
        week_start_time = end_datetime.start_of('week').subtract(weeks=1).format('YYYYMMDD')
        week_end_time = end_datetime.end_of('week').subtract(weeks=1).format('YYYYMMDD')
        now_datetime = end_datetime.format('YYYYMMDDHHmm')

        # date_interval = {
        #     'week_start_date': '2025-04-14',
        #     'week_end_date': '2025-04-20',
        #     'week_start_time': '20250414',
        #     'week_end_time': '20250414',
        #     "now_time": date_interval['now_time']
        # }

        print('读取数据，请稍后')
        audit_count_sql = f'''
            with tol as(
                select 
                    pro.id product_id
                    ,pro.name product_name
                    ,brand
                    ,cat.category_name
                    ,sup.name supplier_name
                    ,pro.mfrName mfrname
                    ,bd.name bd_name
                    ,is_white
                    ,roles
                    ,act
                    ,roles_count
                    ,case 
                        when roles is null and status != 2 then null
                        when roles is null and status = 2 then '质控审核中'
                        when act != '退回' and result = '不通过'
                            or act = '退回' and roles_count = 1 and result = '不通过'
                            or act = '退回' and roles_count > 1 and roles = '成本'  and result = '不通过'
                            or act = '退回' and roles_count > 1 and roles = '质控' and result = '不通过' and deny_type != ''
                            then '不通过'
                        when act != '退回' and result = '不通过不可再提交'
                            or act = '退回' and roles_count = 1 and result = '不通过不可再提交'
                            or act = '退回' and roles_count > 1 and roles = '成本' and result = '不通过不可再提交'
                            or act = '退回' and roles_count > 1 and roles = '质控' and result = '不通过不可再提交' and deny_type != ''
                            then '不通过不可再提交'
                        when act != '退回' and result = '通过'
                            or act = '退回' and roles_count > 1 and roles = '质控' and deny_type = '' 
                            then '通过'
                        else '未知结果'
                    end result 
                    ,deny_type
                    ,last_roles
                    ,last_result
                    ,roles_audit_num
                    ,roles_desc_audit_num
                    ,audit_num
                    ,desc_audit_num
                    ,advice
                    ,audit.name audit_users_name
                    ,submit_time
                    ,audit_time
                from (
                    select 
                        id 
                        ,name 
                        ,brand
                        ,mfrName
                        ,status
                        ,supplier_id 
                        ,SUBSTRING_INDEX(class,',',1) class 
                        ,created_at submit_time
                        ,if(is_white = 0,'非白名单','白名单') is_white
                    from xlsd.products p 
                    where by_anchor = 0
                        and date(created_at) between '{week_start_date}' and '{week_end_date}'
                )pro 
                left join (
                    select
                        *
                        ,count(if(roles_audit_num = 1,roles,null)) over(partition by target_id) roles_count
                        ,lag(roles) over(partition by target_id order by audit_num) last_roles
                        ,lag(result) over(partition by target_id order by audit_num) last_result
                    from(
                        select 
                            rev.*
                            ,row_number() over(partition by rev.target_id,roles order by audit_time) roles_audit_num
                            ,row_number() over(partition by rev.target_id,roles order by audit_time desc) roles_desc_audit_num
                        from (
                            select 
                                target_id 
                                ,user_id	
                                ,case 
                                    when act in (0,1) then '质控'
                                    when act in (3,15,12) and result != 23 then '成本'
                                    when act = 12 and result = 23 then '管理员'
                                    else '未知'
                                end roles
                                ,case 
                                    when act = 1 and result in (3,4,13,14,5,18) and user_id not in ('admin') then '复审'
                                    when (act = 0 or user_id ='admin') and result in (4,14) or act = 12 and result = 20 then '退回'
                                    when act in (3,15,12) and result != 23 then '终审'
                                    when act = 12 and result = 23 then '走白名单跳过品控'
                                    else '未知'
                                end act
                                ,case 
                                    when result in (3,13,14,9,20,25) then '不通过'
                                    when result = 4 then '不通过不可再提交'
                                    when result in (5,18,10,22,23) then '通过'
                                    else '未知'
                                end result
                                ,case 
                                        when deny_type_1 = '' then ''
                                        when deny_type_1 != '' and deny_type_2 = '' then deny_type_1 
                                        else concat(deny_type_1,',',deny_type_2)
                                end deny_type
                                ,row_number() over(partition by target_id order by created_at) audit_num
                                ,row_number() over(partition by target_id order by created_at desc) desc_audit_num
                                ,replace(advice,'\n',';')  advice 
                                ,created_at audit_time
                            from xlsd.review r 
                            where target = 'product'
                                and status = 1
                                and result in (3,4,13,14,5,18,9,10,20,22,23,25)
                                and act not in (4,8)
                        ) rev
                    )rev
                )rev on pro.id = rev.target_id
                left join (
                    select 
                        id,commerce,name 
                    from xlsd.suppliers s 
                )sup on pro.supplier_id = sup.id
                left join (
                    select 
                        id,name
                    from xlsd.users u 
                )bd on sup.commerce = bd.id
                left join (	
                    select 
                        id,name
                    from xlsd.users u 
                )audit on rev.user_id = audit.id
                left join (
                  select category_id, category_name
                  from ods.ods_item_category oic 
                ) cat on pro.class collate utf8mb4_0900_ai_ci = cat.category_id
            )
            select 
                bd_name '商务'
                ,count(distinct product_id) '提报数量'
                ,count(distinct if(result is null,product_id,null)) '未到质控'
                ,count(distinct if(result = '质控审核中',product_id,null)) '质控审核中'
                ,count(distinct if(roles_desc_audit_num = 1 and roles = '质控' and result in ('不通过','不通过不可再提交'),product_id,null)) '质控驳回'
                ,count(distinct if(roles_desc_audit_num = 1 and roles = '质控' and result = '通过',product_id,null)) '质控通过'
                ,count(distinct if(roles = '管理员',product_id,null)) '走白名单跳过品控'
                ,count(distinct if(desc_audit_num = 1 and roles_count = 1 and result = '通过',product_id,null)) '成本审核中'
                ,count(distinct if(
                    desc_audit_num = 1 and (roles = '成本' and result = '不通过' or roles in ('质控','管理员') and act = '退回' and result = '通过' and last_roles = '成本' and last_result = '不通过')
                    ,product_id,null)
                ) '成本驳回'
                ,count(distinct if(
                    desc_audit_num = 1 and (roles = '成本' and result = '通过' or roles = '质控' and act = '退回' and result = '通过' and last_roles = '成本' and last_result = '通过')
                    ,product_id,null)
                ) '成本通过'
            from tol 
            where bd_name not in ('方涌超', '张澜', '管理员', '雷江玲', '赵乙都', '张小卓')
            group by 1
            order by `提报数量` desc
        '''
        audit_count_df = pd.read_sql(audit_count_sql, self.engine)

        qc_audit_sql = f'''
            with tol as(
                select 
                    pro.id product_id
                    ,pro.name product_name
                    ,brand
                    ,cat.category_name
                    ,sup.name supplier_name
                    ,pro.mfrName mfrname
                    ,bd.name bd_name
                    ,is_white
                    ,roles
                    ,act
                    ,roles_count
                    ,case 
                        when roles is null and status != 2 then null
                        when roles is null and status = 2 then '质控审核中'
                        when act != '退回' and result = '不通过'
                            or act = '退回' and roles_count = 1 and result = '不通过'
                            or act = '退回' and roles_count > 1 and roles = '成本'  and result = '不通过'
                            or act = '退回' and roles_count > 1 and roles = '质控' and result = '不通过' and deny_type != ''
                            then '不通过'
                        when act != '退回' and result = '不通过不可再提交'
                            or act = '退回' and roles_count = 1 and result = '不通过不可再提交'
                            or act = '退回' and roles_count > 1 and roles = '成本' and result = '不通过不可再提交'
                            or act = '退回' and roles_count > 1 and roles = '质控' and result = '不通过不可再提交' and deny_type != ''
                            then '不通过不可再提交'
                        when act != '退回' and result = '通过'
                            or act = '退回' and roles_count > 1 and roles = '质控' and deny_type = '' 
                            then '通过'
                        else '未知结果'
                    end result 
                    ,deny_type
                    ,last_roles
                    ,last_result
                    ,roles_audit_num
                    ,roles_desc_audit_num
                    ,audit_num
                    ,desc_audit_num
                    ,advice
                    ,audit.name audit_users_name
                    ,submit_time
                    ,audit_time
                from (
                    select 
                        id 
                        ,name 
                        ,brand
                        ,mfrName
                        ,status
                        ,supplier_id 
                        ,SUBSTRING_INDEX(class,',',1) class 
                        ,created_at submit_time
                        ,if(is_white = 0,'非白名单','白名单') is_white
                    from xlsd.products p 
                    where by_anchor = 0
                        and date(created_at) between '{week_start_date}' and '{week_end_date}'
                )pro 
                left join (
                    select
                        *
                        ,count(if(roles_audit_num = 1,roles,null)) over(partition by target_id) roles_count
                        ,lag(roles) over(partition by target_id order by audit_num) last_roles
                        ,lag(result) over(partition by target_id order by audit_num) last_result
                    from(
                        select 
                            rev.*
                            ,row_number() over(partition by rev.target_id,roles order by audit_time) roles_audit_num
                            ,row_number() over(partition by rev.target_id,roles order by audit_time desc) roles_desc_audit_num
                        from (
                            select 
                                target_id 
                                ,user_id	
                                ,case 
                                    when act in (0,1) then '质控'
                                    when act in (3,15,12) and result != 23 then '成本'
                                    when act = 12 and result = 23 then '管理员'
                                    else '未知'
                                end roles
                                ,case 
                                    when act = 1 and result in (3,4,13,14,5,18) and user_id not in ('admin') then '复审'
                                    when (act = 0 or user_id ='admin') and result in (4,14) or act = 12 and result = 20 then '退回'
                                    when act in (3,15,12) and result != 23 then '终审'
                                    when act = 12 and result = 23 then '走白名单跳过品控'
                                    else '未知'
                                end act
                                ,case 
                                    when result in (3,13,14,9,20,25) then '不通过'
                                    when result = 4 then '不通过不可再提交'
                                    when result in (5,18,10,22,23) then '通过'
                                    else '未知'
                                end result
                                ,case 
                                        when deny_type_1 = '' then ''
                                        when deny_type_1 != '' and deny_type_2 = '' then deny_type_1 
                                        else concat(deny_type_1,',',deny_type_2)
                                end deny_type
                                ,row_number() over(partition by target_id order by created_at) audit_num
                                ,row_number() over(partition by target_id order by created_at desc) desc_audit_num
                                ,replace(advice,'\n',';')  advice 
                                ,created_at audit_time
                            from xlsd.review r 
                            where target = 'product'
                                and status = 1
                                and result in (3,4,13,14,5,18,9,10,20,22,23,25)
                                and act not in (4,8)
                        ) rev
                    )rev
                )rev on pro.id = rev.target_id
                left join (
                    select 
                        id,commerce,name 
                    from xlsd.suppliers s 
                )sup on pro.supplier_id = sup.id
                left join (
                    select 
                        id,name
                    from xlsd.users u 
                )bd on sup.commerce = bd.id
                left join (	
                    select 
                        id,name
                    from xlsd.users u 
                )audit on rev.user_id = audit.id
                left join (
                  select category_id, category_name
                  from ods.ods_item_category oic 
                ) cat on pro.class collate utf8mb4_0900_ai_ci = cat.category_id
            )
            select 
                bd_name '商务'
                ,count(distinct if(roles = '质控',product_id,null)) '到质控的提报数量'
                ,count(distinct if(roles_audit_num = 1 and roles = '质控' and result in ('不通过','不通过不可再提交'),product_id,null)) '质控一审驳回'
                ,count(distinct if(roles_audit_num = 1 and roles = '质控' and result = '通过',product_id,null)) '质控一审通过'
                ,count(distinct if(roles_desc_audit_num = 1 and roles = '质控' and result in ('不通过','不通过不可再提交'),product_id,null)) '质控终审驳回'
                ,count(distinct if(roles_desc_audit_num = 1 and roles = '质控' and result = '通过',product_id,null)) '质控终审通过'
            from tol 
            group by 1
            having `商务` not in ('方涌超', '张澜', '管理员', '雷江玲', '赵乙都', '张小卓') 
                and `到质控的提报数量` != 0
            order by `到质控的提报数量` desc    
        '''
        qc_audit_df = pd.read_sql(qc_audit_sql, self.engine)

        bd_advice_sql = f'''
            with tol as(
                select 
                    pro.id product_id
                    ,pro.name product_name
                    ,brand
                    ,cat.category_name
                    ,sup.name supplier_name
                    ,pro.mfrName mfrname
                    ,bd.name bd_name
                    ,is_white
                    ,roles
                    ,act
                    ,roles_count
                    ,case 
                        when roles is null and status != 2 then null
                        when roles is null and status = 2 then '质控审核中'
                        when act != '退回' and result = '不通过'
                            or act = '退回' and roles_count = 1 and result = '不通过'
                            or act = '退回' and roles_count > 1 and roles = '成本'  and result = '不通过'
                            or act = '退回' and roles_count > 1 and roles = '质控' and result = '不通过' and deny_type != ''
                            then '不通过'
                        when act != '退回' and result = '不通过不可再提交'
                            or act = '退回' and roles_count = 1 and result = '不通过不可再提交'
                            or act = '退回' and roles_count > 1 and roles = '成本' and result = '不通过不可再提交'
                            or act = '退回' and roles_count > 1 and roles = '质控' and result = '不通过不可再提交' and deny_type != ''
                            then '不通过不可再提交'
                        when act != '退回' and result = '通过'
                            or act = '退回' and roles_count > 1 and roles = '质控' and deny_type = '' 
                            then '通过'
                        else '未知结果'
                    end result 
                    ,deny_type
                    ,last_roles
                    ,last_result
                    ,roles_audit_num
                    ,roles_desc_audit_num
                    ,audit_num
                    ,desc_audit_num
                    ,advice
                    ,audit.name audit_users_name
                    ,submit_time
                    ,audit_time
                from (
                    select 
                        id 
                        ,name 
                        ,brand
                        ,mfrName
                        ,status
                        ,supplier_id 
                        ,SUBSTRING_INDEX(class,',',1) class 
                        ,created_at submit_time
                        ,if(is_white = 0,'非白名单','白名单') is_white
                    from xlsd.products p 
                    where by_anchor = 0
                        and date(created_at) between '{week_start_date}' and '{week_end_date}'
                )pro 
                left join (
                    select
                        *
                        ,count(if(roles_audit_num = 1,roles,null)) over(partition by target_id) roles_count
                        ,lag(roles) over(partition by target_id order by audit_num) last_roles
                        ,lag(result) over(partition by target_id order by audit_num) last_result
                    from(
                        select 
                            rev.*
                            ,row_number() over(partition by rev.target_id,roles order by audit_time) roles_audit_num
                            ,row_number() over(partition by rev.target_id,roles order by audit_time desc) roles_desc_audit_num
                        from (
                            select 
                                target_id 
                                ,user_id	
                                ,case 
                                    when act in (0,1) then '质控'
                                    when act in (3,15,12) and result != 23 then '成本'
                                    when act = 12 and result = 23 then '管理员'
                                    else '未知'
                                end roles
                                ,case 
                                    when act = 1 and result in (3,4,13,14,5,18) and user_id not in ('admin') then '复审'
                                    when (act = 0 or user_id ='admin') and result in (4,14) or act = 12 and result = 20 then '退回'
                                    when act in (3,15,12) and result != 23 then '终审'
                                    when act = 12 and result = 23 then '走白名单跳过品控'
                                    else '未知'
                                end act
                                ,case 
                                    when result in (3,13,14,9,20,25) then '不通过'
                                    when result = 4 then '不通过不可再提交'
                                    when result in (5,18,10,22,23) then '通过'
                                    else '未知'
                                end result
                                ,case 
                                        when deny_type_1 = '' then ''
                                        when deny_type_1 != '' and deny_type_2 = '' then deny_type_1 
                                        else concat(deny_type_1,',',deny_type_2)
                                end deny_type
                                ,row_number() over(partition by target_id order by created_at) audit_num
                                ,row_number() over(partition by target_id order by created_at desc) desc_audit_num
                                ,replace(advice,'\n',';')  advice 
                                ,created_at audit_time
                            from xlsd.review r 
                            where target = 'product'
                                and status = 1
                                and result in (3,4,13,14,5,18,9,10,20,22,23,25)
                                and act not in (4,8)
                        ) rev
                    )rev
                )rev on pro.id = rev.target_id
                left join (
                    select 
                        id,commerce,name 
                    from xlsd.suppliers s 
                )sup on pro.supplier_id = sup.id
                left join (
                    select 
                        id,name
                    from xlsd.users u 
                )bd on sup.commerce = bd.id
                left join (	
                    select 
                        id,name
                    from xlsd.users u 
                )audit on rev.user_id = audit.id
                left join (
                  select category_id, category_name
                  from ods.ods_item_category oic 
                ) cat on pro.class collate utf8mb4_0900_ai_ci = cat.category_id
            )
            select  
                bd_name '商务'
                ,count(distinct product_id) '到质控的提报数量'
                ,count(distinct if(roles_audit_num = 1 and level = 1,product_id,null)) '一审S'
                ,count(distinct if(roles_audit_num = 1 and level = 2,product_id,null)) '一审A'
                ,count(distinct if(roles_audit_num = 1 and level = 3,product_id,null)) '一审B'
                ,count(distinct if(roles_audit_num = 1 and level = 4,product_id,null)) '一审C'
                ,count(distinct if(roles_desc_audit_num = 1 and level = 1,product_id,null)) '终审S'
                ,count(distinct if(roles_desc_audit_num = 1 and level = 2,product_id,null)) '终审A'
                ,count(distinct if(roles_desc_audit_num = 1 and level = 3,product_id,null)) '终审B'
                ,count(distinct if(roles_desc_audit_num = 1 and level = 4,product_id,null)) '终审C'
            from (
                select 
                    *
                from (
                    select 
                        rev.*
                        ,code
                        ,name
                        ,level
                        ,row_number() over(partition by product_id,audit_num order by coalesce(level,5)) level_rn 
                    from (
                        select
                            *
                            ,substring_index(substring_index(deny_type, ',', n), ',', -1) as split_deny_type
                        from (
                            select 
                                *
                            from tol 
                            where roles = '质控' 
                        ) src
                        join(
                            select 1 as n union all select 2 union all select 3 union all
                          select 4 union all select 5 union all select 6 union all
                            select 7 union all select 8 union all select 9 union all
                            select 10 union all select 11 union all select 12
                        ) numbers
                        on char_length(deny_type) - char_length(replace(deny_type, ',', '')) >= n - 1 
                    ) rev 
                    left join (
                        select 
                            id,code,name,level
                        from xlsd.tree 
                        where type = 'review-qc'
                    ) tree on rev.split_deny_type = tree.id
                ) tmp
                where level_rn = 1
            ) tol
            group by 1
            having `商务` not in ('方涌超', '张澜', '管理员', '雷江玲', '赵乙都', '张小卓')
                and `到质控的提报数量` != 0
            order by `到质控的提报数量` desc
        '''
        bd_advice_df = pd.read_sql(bd_advice_sql, self.engine)

        adjust_bd_sql = f'''
            with tmp as(
              select
                 pro1.id pro_id
                ,pro1.name pro_name
                ,result
                ,status
                ,audit_user.name audit_user_name
                ,created_at
                ,bd.name bd_name
                ,pro1.priceSale priceSale
                ,pro1.comRatio comRatio
                ,pro2.priceSale pre_priceSale
                ,pro2.comRatio pre_comRatio 
                ,deny_type_1
                ,case 
                        when `level` = 1 then 'S'
                        when `level` = 2 then 'A'
                        when `level` = 3 then 'B'
                        when `level` = 4 then 'C'
                        else null
                    end level
                ,row_number() over(partition by pro1.id order by re.created_at) rn
                ,row_number() over(partition by pro1.id order by re.created_at desc) desc_rn
              from (
                select 
                  target_id,created_at,user_id,deny_type_1,result
                from xlsd.review r 
                where target = 'product'
                    and `result` in (9,20,25) 
                    and user_id not in ('101279867','20b984bd-07ef-4b4b-95c8-404ac72a0081','25413777','25857336','5085936','21724093')
              ) re
              inner join (
                select 
                  id
                  ,name 
                  ,priceSale 
                  ,comRatio
                  ,status
                  ,supplier_id
                from xlsd.products p  
                where by_anchor = 0
                    and date(created_at) between '{week_start_date}' and '{week_end_date}'  	
              )pro1 on re.target_id = pro1.id
              left join (
                select 
                  product_id
                  ,priceSale 
                  ,comRatio
                from (
                  select 
                    product_id
                    ,priceSale 
                    ,comRatio
                    ,row_number() over(partition by product_id order by created_at) rn_asc
                  from xlsd.products_log pl
                  where status in (9,20,25) 
                )tmp 
                where rn_asc= 1               
              )pro2 on re.target_id = pro2.product_id
              left join (
                    select 
                        id,name
                    from xlsd.users u 
                )audit_user on re.user_id = audit_user.id
                left join (
                    select 
                        id,commerce
                    from xlsd.suppliers s 
                )sup on pro1.supplier_id = sup.id
                left join (
                    select 
                        id,name 
                    from xlsd.users u 
                )bd on sup.commerce = bd.id
              left join (
                select 
                    id,level
                from xlsd.tree
                where type = 'review-cost'
              )tree on re.deny_type_1 = tree.id
            ),
            mecha as (
                select
                    pro_id
                    ,pro_name
                    ,group_concat(audit_user_name order by src.created_at) audit_user_name
                    ,group_concat(src.created_at order by src.created_at) created_at
                    ,bd_name
                    ,round(pre_priceSale,2) pre_priceSale
                    ,round(pre_comRatio,2) pre_comRatio
                    ,round(now_priceSale,2) now_priceSale
                    ,now_comRatio
                from (
                    select 
                    pro_id
                    ,pro_name
                    ,audit_user_name
                    ,created_at
                        ,bd_name
                    ,case 
                          when status not in (10,11) and pre_priceSale is null then priceSale
                          else pre_priceSale
                    end pre_priceSale
                    ,case
                        when status not in (10,11) and pre_comRatio is null then comRatio
                        else pre_comRatio
                    end pre_comRatio        
                    ,case 
                        when status in (1,8,9,20,25) then null
                        else priceSale
                    end now_priceSale
                    ,case 
                        when status in (1,8,9,20,25) then null
                        else comRatio
                    end now_comRatio 
                    from tmp
                )src
                group by 
                    pro_id
                    ,pro_name
                    ,bd_name 
                    ,src.pre_priceSale
                    ,src.pre_comRatio
                    ,src.now_priceSale
                    ,src.now_comRatio
            )
            select 
                mecha.pro_id '产品ID'
                ,pro_name '产品名称'
                ,audit_user_name '审核人'
                ,created_at '审核时间'
                ,bd_name '商务'
                ,pre_priceSale '成本审核前直播价'
                ,pre_comRatio '成本审核前佣金比例(%%)'
                ,now_priceSale '成本审核后直播价'
                ,now_comRatio '成本审核后佣金比例(%%)'
                ,1st_lev '一审驳回意见类型'
                ,case 
                    when now_priceSale is not null then null
                    else last_lev
                end '最新驳回意见类型'
            from (
                select 
                    *
                from mecha
            )mecha
            left join (
                select 
                    pro_id
                    ,level 1st_lev
                from tmp
                where rn = 1
            )lev_1 on mecha.pro_id = lev_1.pro_id
            left join (
                select 
                    pro_id
                    ,level last_lev
                from tmp
                where desc_rn = 1
            )lev_2 on mecha.pro_id = lev_2.pro_id
        '''
        adjust_bd_df = pd.read_sql(adjust_bd_sql, self.engine)

        adjust_anchor_sql = f'''
            with tmp as(
                select
                 pro1.id pro_id
                ,pro1.name pro_name
                ,result
                ,status
                ,audit_user.name audit_user_name
                ,submit_user.name submit_user_name
                ,created_at
                ,pro1.priceSale priceSale
                ,pro1.comRatio comRatio
                ,pro2.priceSale pre_priceSale
                ,pro2.comRatio pre_comRatio 
                ,deny_type_1
                ,case 
                        when `level` = 1 then 'S'
                        when `level` = 2 then 'A'
                        when `level` = 3 then 'B'
                        when `level` = 4 then 'C'
                        else null
                    end level
                ,row_number() over(partition by pro1.id order by re.created_at) rn
                ,row_number() over(partition by pro1.id order by re.created_at desc) desc_rn
              from (
                select 
                  target_id,created_at,user_id,deny_type_1,result
                from xlsd.review r 
                where target = 'product'
                    and `result` in (9,20,25) 
                    and user_id not in ('101279867','20b984bd-07ef-4b4b-95c8-404ac72a0081','25413777','25857336','5085936','21724093')
              ) re
              inner join (
                select 
                  id
                  ,name 
                  ,priceSale 
                  ,comRatio
                  ,status 
                  ,user_id
                from xlsd.products p  
                where date(created_at) between '{week_start_date}' and '{week_end_date}' 
                    and by_anchor = 1
              )pro1 on re.target_id = pro1.id
              left join (
                select 
                  product_id
                  ,priceSale 
                  ,comRatio
                from (
                  select 
                    product_id
                    ,priceSale 
                    ,comRatio
                    ,row_number() over(partition by product_id order by created_at) rn_asc
                  from xlsd.products_log pl
                  where status in (9,20,25) 
                )tmp 
                where rn_asc= 1               
              )pro2 on re.target_id = pro2.product_id
               left join (
                    select 
                        id,name
                    from xlsd.users u 
                )audit_user on re.user_id = audit_user.id
              left join (
                    select 
                        id,name
                    from xlsd.users u 
                )submit_user on pro1.user_id = submit_user.id
              left join (
                select 
                    id,level
                from xlsd.tree
                where type = 'review-cost'
              )tree on re.deny_type_1 = tree.id
            ),
            mecha as (
                select
                    pro_id
                    ,pro_name
                    ,group_concat(audit_user_name order by src.created_at) audit_user_name
                    ,group_concat(src.created_at order by src.created_at) created_at
                    ,case 
                        when submit_user_name like '%%仙乐时代供应商%%' then '婷美惠'
                        else submit_user_name
                    end submit_user_name
                    ,round(pre_priceSale,2) pre_priceSale
                    ,round(pre_comRatio,2) pre_comRatio
                    ,round(now_priceSale,2) now_priceSale
                    ,now_comRatio
                from (
                    select 
                    pro_id
                    ,pro_name
                    ,audit_user_name
                    ,created_at
                        ,submit_user_name
                    ,case 
                          when status not in (10,11) and pre_priceSale is null then priceSale
                          else pre_priceSale
                    end pre_priceSale
                    ,case
                        when status not in (10,11) and pre_comRatio is null then comRatio
                        else pre_comRatio
                    end pre_comRatio        
                    ,case 
                        when status in (1,8,9,20,25) then null
                        else priceSale
                    end now_priceSale
                    ,case 
                        when status in (1,8,9,20,25) then null
                        else comRatio
                    end now_comRatio 
                    from tmp
                )src
                group by 
                    pro_id
                    ,pro_name
                    ,src.submit_user_name 
                    ,src.pre_priceSale
                    ,src.pre_comRatio
                    ,src.now_priceSale
                    ,src.now_comRatio
            )
            select 
                mecha.pro_id '产品ID'
                ,pro_name '产品名称'
                ,audit_user_name '审核人'
                ,created_at '审核时间'
                ,submit_user_name '提报人'
                ,pre_priceSale '成本审核前直播价'
                ,pre_comRatio '成本审核前佣金比例(%%)'
                ,now_priceSale '成本审核后直播价'
                ,now_comRatio '成本审核后佣金比例(%%)'
                ,1st_lev '一审驳回意见类型'
                ,case 
                    when now_priceSale is not null then null
                    else last_lev
                end '最新驳回意见类型'
            from (
                select 
                    *
                from mecha
            )mecha
            left join (
                select 
                    pro_id
                    ,level 1st_lev
                from tmp
                where rn = 1
            )lev_1 on mecha.pro_id = lev_1.pro_id
            left join (
                select 
                    pro_id
                    ,level last_lev
                from tmp
                where desc_rn = 1
            )lev_2 on mecha.pro_id = lev_2.pro_id
        '''
        adjust_anchor_df = pd.read_sql(adjust_anchor_sql, self.engine)

        return {
            'audit_count_df': audit_count_df,
            'qc_audit_df': qc_audit_df,
            'bd_advice_df': bd_advice_df,
            'adjust_bd_df': adjust_bd_df,
            'adjust_anchor_df': adjust_anchor_df,
            'week_start_date': week_start_date,
            'week_end_date': week_end_date,
            'week_start_time': week_start_time,
            'week_end_time': week_end_time,
            'now_datetime': now_datetime
        }

    def prepare_card_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        return {}

    def write_to_sheet_logic(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        audit_count_df = data_dict['audit_count_df']
        qc_audit_df = data_dict['qc_audit_df']
        bd_advice_df = data_dict['bd_advice_df']
        adjust_anchor_df = data_dict['adjust_anchor_df']
        adjust_bd_df = data_dict['adjust_bd_df']

        try:
            c1 = audit_count_df['提报数量'] == (
                    audit_count_df['未到质控'] + audit_count_df['质控审核中'] + audit_count_df['质控驳回'] +
                    audit_count_df['质控通过'] + audit_count_df['走白名单跳过品控']
            )
            c2 = (audit_count_df['质控通过'] + audit_count_df['走白名单跳过品控']) == (
                    audit_count_df['成本审核中'] + audit_count_df['成本驳回'] + audit_count_df['成本通过']
            )
            c3 = qc_audit_df["到质控的提报数量"] = qc_audit_df["质控一审驳回"] + qc_audit_df["质控一审通过"]
            c4 = qc_audit_df["到质控的提报数量"] = qc_audit_df["质控终审驳回"] + qc_audit_df["质控终审通过"]
            merged_df1 = pd.merge(audit_count_df, qc_audit_df, on='商务', how='inner',
                                  suffixes=('_audit_count_df', '_qc_audit_df'))
            c5 = merged_df1["质控驳回"] == merged_df1["质控终审驳回"]
            c6 = merged_df1["质控通过"] == merged_df1["质控终审通过"]
            merged_df2 = pd.merge(qc_audit_df, bd_advice_df, on='商务', how='inner',
                                  suffixes=('_qc_audit_df', '_bd_advice_df'))
            c7 = merged_df2["质控一审驳回"] == merged_df2["一审S"] + merged_df2["一审A"] + merged_df2["一审B"] + \
                 merged_df2["一审C"]
            c8 = merged_df2["质控终审驳回"] == merged_df2["终审S"] + merged_df2["终审A"] + merged_df2["终审B"] + \
                 merged_df2["终审C"]

            if not (
                    c1.all() and c2.all() and c3.all() and c4.all() and c5.all() and c6.all() and c7.all() and c8.all()):
                # 如果验证未通过，抛出异常
                raise ValueError("数据验证未通过！")
                # print(f"数据验证失败")

            print("数据验证通过，进程继续。")

            sheet_title = f"风控部周报数据_{data_dict['week_start_time']}_{data_dict['week_end_time']}_{data_dict['now_datetime']}"
            url, spreadsheet_token = self.feishu_sheet_supply.get_workbook_params(
                workbook_name=sheet_title, folder_token='QidBfm7zOlDGOsdGqnycUbe0nKd'
            )

            bd_adv_sheet_id = self.feishu_sheet_supply.get_sheet_params(spreadsheet_token, '入库商品SABC汇总')
            qc_sheet_id = self.feishu_sheet_supply.get_sheet_params(spreadsheet_token, '商务提报&审核数据（质控）')
            anchor_sheet_id = self.feishu_sheet_supply.get_sheet_params(spreadsheet_token,
                                                                        '驳回品机制调整和意见类型数据（自采）')
            bd_sheet_id = self.feishu_sheet_supply.get_sheet_params(spreadsheet_token,
                                                                    '驳回品机制调整和意见类型数据（招商）')
            sheet_id = self.feishu_sheet_supply.get_sheet_params(spreadsheet_token, '商务提报&审核数据（风控部）')
            self.feishu_sheet.write_df_replace(bd_advice_df, spreadsheet_token, bd_adv_sheet_id, to_char=False)
            self.feishu_sheet.write_df_replace(qc_audit_df, spreadsheet_token, qc_sheet_id, to_char=False)
            self.feishu_sheet.write_df_replace(adjust_anchor_df, spreadsheet_token, anchor_sheet_id, to_char=False)
            self.feishu_sheet.write_df_replace(adjust_bd_df, spreadsheet_token, bd_sheet_id, to_char=False)
            self.feishu_sheet.write_df_replace(audit_count_df, spreadsheet_token, sheet_id, to_char=False)

            return {
                'file_name': sheet_title,
                'url': url
            }
        except ValueError as e:
            print(f"数据验证失败: {e}, 抛出空数据。")

    def send_card_logic(self, card: Dict[str, Any], sheet):
        res = {
            'title': '风控部周报数据',
            **sheet,
            'description': '数据请见下方链接附件'
        }
        self.feishu_robot.send_msg_card(data=res, card_id=self.card_id, version_name='1.0.0')


dag = RiskWeekly().create_dag()
