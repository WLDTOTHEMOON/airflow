delete from ads.ads_ks_estimated_pnl where 1 = 1;

insert into ads.ads_ks_estimated_pnl (
    order_date, date_diff_n, anchor_name, valid_estimated_income, profit_rate,
    predict_income, predict_income_belong_org
)
with cps_lst as (
    select
        anchor_name
        ,order_trade_amount * (commission_rate + coalesce(service_rate, 0)) * 0.9 origin_estimated_income
        ,estimated_income + coalesce(estimated_service_income, 0) estimated_income
        ,cps_order_status
        ,date(update_time) update_date
        ,date(order_create_time) order_create_date
    from (
        select
            o_id
            ,account_id
            ,order_trade_amount
            ,commission_rate
            ,service_rate
            ,estimated_income 
            ,estimated_service_income 
            ,cps_order_status
            ,update_time
            ,order_create_time
        from dwd.dwd_ks_cps_order dkco
        where order_create_time between concat(current_date - interval 180 day, ' 00:00:00') and concat(current_date - interval 15 day, ' 23:59:59')
            and settlement_biz_type != '聚力计划'
            and o_id not in (
                select o_id
                from ods.ods_cps_order_fake
                union 
                select o_id
                from dwd.dwd_ks_recreation
                union
                select o_id
                from dwd.dwd_ks_leader_commission_income
            )
    ) src
    inner join (
        select
            account_id
            ,anchor_name
        from dim.dim_ks_account_info dkai
        where line = '直播电商'
            or line = '其它'
            or line is null
    ) ai on src.account_id = ai.account_id
),
profit as (
    select
        timestampdiff(day, src.order_create_date, update_date) date_diff
        ,src.anchor_name anchor_name
        ,src.order_create_date order_create_date
        ,update_date
        ,estimated_income actual_income
        ,origin_estimated_income - sum(invalid_estimated_income) over(partition by src.order_create_date, src.anchor_name order by update_date asc) valid_estimated_income
    from (
        select
            update_date
            ,order_create_date
            ,anchor_name
            ,sum(case when cps_order_status = '已失效' then origin_estimated_income else 0 end) invalid_estimated_income
        from cps_lst
        group by update_date, order_create_date, anchor_name
    )src
    left join (
        select
            order_create_date
            ,anchor_name
            ,sum(origin_estimated_income) origin_estimated_income
            ,sum(estimated_income) estimated_income
        from
            cps_lst
        group by order_create_date, anchor_name
    )total on src.order_create_date = total.order_create_date and src.anchor_name = total.anchor_name
),
profit_rate as (
    select 
        date_diff
        ,anchor_name
        ,case when sum(valid_estimated_income) = 0 then null else sum(actual_income) / sum(valid_estimated_income) end profit_rate
    from (	
        select
            date_diff
            ,anchor_name
            ,valid_estimated_income
            ,actual_income
            ,sum(valid_estimated_income) over(partition by date_diff, anchor_name) valid_sum
        from profit
    ) src
    group by date_diff, anchor_name
),
profit_rate_backup as (
    select 
        date_diff
        ,case when sum(valid_estimated_income) = 0 then null else sum(actual_income) / sum(valid_estimated_income) end profit_rate_backup
    from profit
    group by date_diff
),
current_status as (
    select
        anchor_name
        ,order_date
        ,timestampdiff(day, order_date, current_date) date_diff
        ,sum(estimated_income + coalesce(estimated_service_income, 0)) valid_estimated_income
    from dws.dws_ks_big_tbl
    where order_date between date_format(current_date - interval 1 day, '%Y-%m-01') and current_date - interval 1 day
    group by anchor_name, order_date
)
select 
    src.order_date order_date
    ,date_diff_n
    ,src.anchor_name anchor_name
    ,valid_estimated_income
    ,profit_rate
    ,predict_income
    ,predict_income * coalesce(spec.organization_commission_rate, info.organization_commission_rate) predict_income_belong_org
from (
    select
        order_date
        ,cs.date_diff date_diff_n
        ,cs.anchor_name
        ,valid_estimated_income
        ,coalesce(profit_rate, profit_rate_backup) profit_rate
        ,valid_estimated_income * coalesce(profit_rate,profit_rate_backup) predict_income
    from (
        select *
        from current_status
    ) cs
    left join (
        select *
        from profit_rate
    ) pr on cs.anchor_name = pr.anchor_name and  cs.date_diff = pr.date_diff
    left join (
        select *
        from profit_rate_backup
    ) prb on cs.date_diff = prb.date_diff
) src
inner join (
    select 
        anchor_name
        ,organization_commission_rate
    from dim.dim_ks_account_info dai 
    where line = '直播电商'
        or line = '其它'
        or line is null
    group by 1, 2
) info on src.anchor_name = info.anchor_name
left join (
    select 
        order_date
        ,anchor_name
        ,organization_commission_rate
    from (
        select
            order_date
            ,account_id
            ,organization_commission_rate
        from ods.ods_special_allocation
    ) src
    left join (
        select account_id, anchor_name
        from dim.dim_ks_account_info 
    ) info on src.account_id = info.account_id
) spec on src.anchor_name = spec.anchor_name and src.order_date = spec.order_date;