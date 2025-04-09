delete from dws.dws_ks_slice_slicer
where order_date >= date_format(%(order_create_time)s - interval 4 hour, '%%Y-%%m-%%d');


insert into dws.dws_ks_slice_slicer (
    order_date, port, account_id, author_id, item_id, item_title,
    origin_gmv, origin_order_number, final_gmv, final_order_number,
    send_order_number, estimated_income, estimated_service_income,
    send_order_number_yesterday
)
select
    date(order_create_time - interval 4 hour) order_date
    ,'A' port
    ,src.anchor_id account_id
    ,'-' author_id
    ,item_id
    ,item_title
    ,sum(order_trade_amount) origin_gmv
    ,count(o_id) origin_order_number
    ,sum(if(cps_order_status = '已失效', 0, order_trade_amount)) final_gmv        
    ,count(if(cps_order_status = '已失效', null, o_id)) final_order_number
    ,sum(if(cps_order_status != '已失效' and send_status = '已发货', 1, 0)) send_order_number
    ,sum(estimated_income) estimated_income
    ,sum(estimated_service_income) estimated_service_income
    ,sum(if(send_time between date_format(current_timestamp - interval 1 day, '%%Y-%%m-%%d 10:00:00') and date_format(current_timestamp, '%%Y-%%m-%%d 10:00:00'), 1, 0)) send_order_number_yesterday
from (
    select 
        anchor_id
        ,start_time
        ,end_time
    from ods.ods_fs_slice_account
    -- A端和主播共有账号
    where anchor_id not in ('173119688')
        and anchor_id not in (
            select account_id 
            from dim.dim_ks_account_info
            where anchor_name = '樊欣羽'
        )
) src
inner join(
    select 
        account_id anchor_id
        ,o_id
        ,item_id
        ,item_title
        ,order_trade_amount
        ,cps_order_status
        ,send_status
        ,send_time
        ,coalesce(estimated_income, 0) estimated_income
        ,coalesce(estimated_service_income,0) estimated_service_income
        ,order_create_time
    from dwd.dwd_ks_cps_order dco 
    where order_create_time >= date_format(%(order_create_time)s - interval 4 hour, '%%Y-%%m-%%d 04:00:00')
        and o_id not in ( select o_id from ods.ods_cps_order_recreation )
) income on src.anchor_id = income.anchor_id and income.order_create_time between src.start_time and src.end_time
group by 1, 2, 3, 4, 5, 6
union all
-- B，C端剪手
select 
    date(order_create_time - interval 4 hour) order_date
    ,case
        when activity_id = '5084317902' then 'C'
        when activity_id in ('5084323902','5142199902','4920930902','6701551902','6244252902', '7469588902') then 'B'
        else '未知端口'
    end port
    ,account_id
    ,'-' author_id
    ,item_id
    ,item_title
    ,sum(order_trade_amount) origin_gmv
    ,count(o_id) origin_order_number
    ,sum(if(cps_order_status = '已失效', 0, order_trade_amount)) final_gmv        
    ,count(if(cps_order_status = '已失效', null, o_id)) final_order_number
    ,sum(if(cps_order_status != '已失效' and send_status = '已发货', 1, 0)) send_order_number
    ,sum(estimated_income) estimated_income
    ,sum(estimated_service_income) estimated_service_income
    ,sum(if(send_time between date_format(current_timestamp - interval 1 day, '%%Y-%%m-%%d 10:00:00') and date_format(current_timestamp, '%%Y-%%m-%%d 10:00:00'), 1, 0)) send_order_number_yesterday
from (
    select 
        promotion_id account_id
        ,o_id
        ,item_id
        ,item_title
        ,order_trade_amount
        ,cps_order_status
        ,send_status
        ,send_time
        ,0 estimated_income
        ,coalesce(estimated_service_income,0) estimated_service_income
        ,order_create_time
        ,activity_id
    from dwd.dwd_ks_leader_order
    where order_create_time >= date_format(%(order_create_time)s - interval 4 hour, '%%Y-%%m-%%d 04:00:00')
        and activity_id in (
            '5084323902','5142199902','4920930902','6701551902','6244252902', '5084317902', '7469588902'
        )
        and promotion_id not in ( select anchor_id from ods.ods_fs_slice_account )
) src
group by 1, 2, 3, 4, 5, 6