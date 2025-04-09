delete from dws.dws_ks_slice_mcn
where order_date >= (select date_format(min(order_create_time), '%%Y-%%m-%%d') from ods.ods_ks_cps_order where update_time between %(begin_time)s and %(end_time)s);

insert into dws.dws_ks_slice_mcn (
    order_date, port, account_id, mcn_id, item_id, item_title, origin_gmv, origin_order_number,
    final_gmv, final_order_number, send_order_number, estimated_income, estimated_service_income, send_order_number_yesterday
)
select 
    date(order_create_time - interval 4 hour) order_date
    ,'A' port
    ,src.anchor_id account_id
    ,mcn.mcn_id mcn_id
    ,item_id
    ,item_title
    ,sum(order_trade_amount) origin_gmv
    ,count(cps.o_id) origin_order_number
    ,sum(if(cps_order_status = '已失效', 0, order_trade_amount)) final_gmv
    ,count(if(cps_order_status = '已失效', null, cps.o_id)) final_order_number
    ,sum(if(cps_order_status != '已失效' and send_status = '已发货', 1, 0)) send_order_number
    ,sum(estimated_income) estimated_income
    ,sum(if(cps_order_status = '已失效', 0, estimated_service_income)) estimated_service_income
    ,sum(if(send_time between date_format(current_timestamp - interval 1 day, '%%Y-%%m-%%d 10:00:00') and date_format(current_timestamp, '%%Y-%%m-%%d 10:00:00'), 1, 0)) send_order_number_yesterday
from (
    select 
	    anchor_id
	    ,start_time 
	    ,end_time 
    from ods.ods_fs_slice_account
    where anchor_id in (
    	select account_id 
    	from dim.dim_ks_anchor_info
    	where line = '切片' 
    		and anchor_name = '樊欣羽'
    )
) src
inner join (
    select 
        account_id 
        ,o_id
        ,item_id
        ,item_title
        ,order_trade_amount
        ,cps_order_status
        ,send_status
        ,send_time
        ,estimated_income
        ,order_create_time
    from dwd.dwd_ks_cps_order dkco 
    where order_create_time >= (select date_format(min(order_create_time), '%%Y-%%m-%%d 04:00:00') from ods.ods_ks_cps_order where update_time between %(begin_time)s and %(end_time)s)
) cps on src.anchor_id = cps.account_id  and cps.order_create_time between src.start_time and src.end_time
left join (
	select
		o_id
		,mcn_id
		,estimated_service_income
	from dwd.dwd_ks_mcn_order
) mcn on cps.o_id = mcn.o_id
group by 1, 2, 3, 4, 5, 6
union all
select 
    date(order_create_time - interval 4 hour) order_date
    ,'B' port
    ,src.account_id
    ,mcn.mcn_id mcn_id
    ,item_id
    ,item_title
    ,sum(order_trade_amount) origin_gmv
    ,count(cps.o_id) origin_order_number
    ,sum(if(cps_order_status = '已失效', 0, order_trade_amount)) final_gmv
    ,count(if(cps_order_status = '已失效', null, cps.o_id)) final_order_number
    ,sum(if(cps_order_status != '已失效' and send_status = '已发货', 1, 0)) send_order_number
    ,0 estimated_income -- B端佣金不归属于公司，写死0
    ,sum(if(cps_order_status = '已失效', 0, estimated_service_income)) estimated_service_income
    ,sum(if(send_time between date_format(current_timestamp - interval 1 day, '%%Y-%%m-%%d 10:00:00') and date_format(current_timestamp, '%%Y-%%m-%%d 10:00:00'), 1, 0)) send_order_number_yesterday
from (
    select 
        account_id 
    from dim.dim_ks_anchor_info dkai 
    where line = '切片' and anchor_name = '樊欣羽'
            and account_id not in (select anchor_id from ods.ods_slice_account osa)
) src
inner join (
    select 
        account_id 
        ,o_id
        ,item_id
        ,item_title
        ,order_trade_amount
        ,cps_order_status
        ,send_status
        ,send_time
        ,estimated_income
        ,order_create_time
    from dwd.dwd_ks_cps_order dkco 
    where order_create_time >= (select date_format(min(order_create_time), '%%Y-%%m-%%d 04:00:00') from ods.ods_ks_cps_order where update_time between %(begin_time)s and %(end_time)s)
) cps on src.account_id = cps.account_id 
left join (
    select
		o_id
		,mcn_id
		,estimated_service_income
	from dwd.dwd_ks_mcn_order
) mcn on cps.o_id = mcn.o_id
group by 1, 2, 3, 4, 5, 6;