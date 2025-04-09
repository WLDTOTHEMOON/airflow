delete from dws.dws_ks_slice_recreation
where order_date >= (select date_format(min(order_create_time), '%%Y-%%m-%%d') from ods.ods_ks_cps_order where update_time between %(begin_time)s and %(end_time)s);


insert into dws.dws_ks_clice_recreation (
    order_date, port, account_id, author_id, item_id, item_title,
    origin_gmv, origin_order_number, final_gmv, final_order_number,
    send_order_number, estimated_income, estimated_service_income,
    send_order_number_yesterday
)
select 
    date(order_create_time - interval 4 hour) order_date
    ,'A' port
    ,account_id
    ,author_id
    ,item_id
    ,item_title
    ,sum(order_trade_amount) origin_gmv
    ,count(o_id) origin_order_number
    ,sum(if(cps_order_status = '已失效', 0, order_trade_amount)) final_gmv        
    ,count(if(cps_order_status = '已失效', null, o_id)) final_order_number
    ,sum(if(cps_order_status != '已失效' and send_status = '已发货', 1, 0)) send_order_number
    ,sum(coalesce(anchor_keep_estimated_income, 0)) + sum(coalesce(leader_share_estimated_income, 0)) estimated_income
    ,sum(coalesce(leader_origin_estimated_service_income, 0)) + sum(coalesce(leader_keep_estimated_service_income, 0)) estimated_service_income
    ,sum(if(send_time between date_format(current_timestamp - interval 1 day, '%%Y-%%m-%%d 10:00:00') and date_format(current_timestamp, '%%Y-%%m-%%d 10:00:00'), 1, 0)) send_order_number_yesterday
from (
    select 
        account_id
        ,order_create_time
        ,author_id
        ,item_id
        ,item_title
        ,order_trade_amount
        ,o_id
        ,cps_order_status
        ,send_status
        ,send_time
        ,anchor_keep_estimated_income
        ,leader_origin_estimated_service_income
        ,leader_share_estimated_income
        ,leader_keep_estimated_service_income
    from dwd.dwd_ks_leader_commission_income 
    where order_create_time >= (select date_format(min(order_create_time), '%%Y-%%m-%%d 04:00:00') from ods.ods_ks_cps_order where update_time between %(begin_time)s and %(end_time)s)
) src
inner join (
    select 
        anchor_id
        ,start_time
        ,end_time
    from ods.ods_fs_slice_account
) info on src.author_id = info.anchor_id and src.order_create_time between info.start_time and info.end_time
group by 1, 2, 3, 4, 5, 6
union all 
-- C端二创
select 
    date(order_create_time - interval 4 hour) order_date
    ,'C' port
    ,account_id
    ,author_id
    ,item_id
    ,item_title
    ,sum(order_trade_amount) origin_gmv
    ,count(o_id) origin_order_number
    ,sum(if(cps_order_status = '已失效', 0, order_trade_amount)) final_gmv        
    ,count(if(cps_order_status = '已失效', null, o_id)) final_order_number
    ,sum(if(cps_order_status != '已失效' and send_status = '已发货', 1, 0)) send_order_number
    ,sum(coalesce(anchor_keep_estimated_income, 0)) + sum(coalesce(leader_share_estimated_income, 0)) estimated_income
    ,sum(coalesce(leader_origin_estimated_service_income, 0)) + sum(coalesce(leader_keep_estimated_service_income, 0)) estimated_service_income
    ,sum(if(send_time between date_format(current_timestamp - interval 1 day, '%%Y-%%m-%%d 10:00:00') and date_format(current_timestamp, '%%Y-%%m-%%d 10:00:00'), 1, 0)) send_order_number_yesterday
from (
    select 
        account_id
        ,order_create_time
        ,author_id
        ,item_id
        ,item_title
        ,order_trade_amount
        ,o_id
        ,cps_order_status
        ,send_status
        ,send_time
        ,anchor_keep_estimated_income
        ,leader_origin_estimated_service_income
        ,0 leader_share_estimated_income
        ,leader_keep_estimated_service_income
    from dwd.dwd_ks_leader_commission_income 
    where order_create_time >= (select date_format(min(order_create_time), '%%Y-%%m-%%d 04:00:00') from ods.ods_ks_cps_order where update_time between %(begin_time)s and %(end_time)s)
) src
left join (
    select 
        anchor_id 
    from ods.ods_fs_slice_account
) info on src.author_id = info.anchor_id
where info.anchor_id is null
group by 1, 2, 3, 4, 5, 6
union all 
-- B端二创
select 
    date(order_create_time - interval 4 hour) order_date
    ,'B' port
    ,account_id
    ,'-' author_id -- 由于B端不过自己团长，无法获取创作者ID
    ,item_id
    ,item_title
    ,sum(order_trade_amount) origin_gmv
    ,count(src.o_id) origin_order_number
    ,sum(if(cps_order_status = '已失效', 0, order_trade_amount)) final_gmv        
    ,count(if(cps_order_status = '已失效', null, src.o_id)) final_order_number
    ,sum(if(cps_order_status != '已失效' and send_status = '已发货', 1, 0)) send_order_number
    ,sum(estimated_income) estimated_income
    ,sum(estimated_service_income) estimated_service_income
    ,sum(if(send_time between date_format(current_timestamp - interval 1 day, '%%Y-%%m-%%d 10:00:00') and date_format(current_timestamp, '%%Y-%%m-%%d 10:00:00'), 1, 0)) send_order_number_yesterday
from (
    select 
        account_id
        ,o_id
        ,item_id
        ,item_title
        ,cps_order_status
        ,coalesce(estimated_income, 0) estimated_income
        ,coalesce(estimated_service_income, 0) estimated_service_income
        ,order_trade_amount
        ,order_create_time
        ,send_status
        ,send_time
    from dwd.dwd_ks_recreation dkr 
    where order_create_time >= (select date_format(min(order_create_time), '%%Y-%%m-%%d 04:00:00') from ods.ods_ks_cps_order where update_time between %(begin_time)s and %(end_time)s)
) src
left join (
	select o_id 
	from dwd.dwd_ks_leader_commission_income
) lci on src.o_id = lci.o_id
where lci.o_id is null
group by 1, 2, 3, 4, 5, 6;