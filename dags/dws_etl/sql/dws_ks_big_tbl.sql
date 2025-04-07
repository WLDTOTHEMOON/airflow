insert into dws.dws_ks_big_tbl (
	order_date, account_id, anchor_name, item_id, item_title, first_sell_at, item_category, product_id,
	product_name, bd_name, product_submit_at, origin_gmv, origin_order_number, origin_item_number, final_gmv, final_order_number,
	final_item_number, send_order_number, estimated_income, estimated_service_income, settlement_gmv, settlement_order_number,
	settlement_income, settlement_service_income, send_order_number_yesterday, income
)
select 
    order_date
    ,account_id
    ,anchor_name
    ,src.item_id item_id
    ,src.item_title item_title
    ,first_sell_at
    ,item_category
    ,product_id
    ,product_name
    ,bd_name
    ,product_submit_at
    ,origin_gmv
    ,origin_order_number
    ,origin_item_number
    ,final_gmv
    ,final_order_number
    ,final_item_number
    ,send_order_number
    ,estimated_income
    ,estimated_service_income
    ,settlement_gmv
    ,settlement_order_number
    ,settlement_income
    ,settlement_service_income
    ,send_order_number_yesterday
    ,income
from (
    select 
        src.order_date order_date
        ,src.account_id account_id
        ,coalesce(info.anchor_name, '未归属于主播账号') anchor_name
        ,src.item_id item_id
        ,src.item_title item_title
        ,src.first_sell_at first_sell_at
        ,origin_gmv
        ,origin_order_number
        ,origin_item_number
        ,final_gmv
        ,final_order_number
        ,final_item_number
        ,send_order_number
        ,estimated_income
        ,estimated_service_income
        ,settlement_gmv
        ,settlement_order_number
        ,settlement_income
        ,settlement_service_income
        ,send_order_number_yesterday
        ,(coalesce(estimated_income, 0) + coalesce(estimated_service_income, 0)) * coalesce(spec.organization_commission_rate, info.organization_commission_rate) income
    from (
        select
            order_date
            ,account_id
            ,item_id
            ,item_title
            ,min(order_create_time) first_sell_at
            ,sum(order_trade_amount) origin_gmv
            ,count(src.o_id) origin_order_number
            ,sum(item_num) origin_item_number
            ,sum(if(cps_order_status = '已失效', 0, order_trade_amount)) final_gmv
            ,count(if(cps_order_status = '已失效', null, src.o_id)) final_order_number
            ,sum(if(cps_order_status = '已失效', 0, item_num)) final_item_number
            ,count(if(cps_order_status != '已失效' and send_status = '已发货', src.o_id, null)) send_order_number
            ,count(if(
                send_time between date_format(current_timestamp - interval 1 day, '%%Y-%%m-%%d 04:00:00') and date_format(current_timestamp, '%%Y-%%m-%%d 04:00:00'),
                src.o_id, null
            )) send_order_number_yesterday
            ,sum(estimated_income) estimated_income
            ,sum(estimated_service_income) estimated_service_income
            ,sum(if(cps_order_status = '已结算', order_trade_amount, 0)) settlement_gmv
            ,count(if(cps_order_status = '已结算', src.o_id, null)) settlement_order_number
            ,sum(if(cps_order_status = '已结算', settlement_amount , 0)) settlement_income
            ,sum(if(cps_order_status = '已结算', leader_settlement_amount , 0)) settlement_service_income
        from (
        	select 
        		date(order_create_time - interval 4 hour) order_date
        		,o_id
        		,account_id
        		,item_id
        		,item_title
        		,order_create_time
        		,item_num
        		,cps_order_status
        		,order_trade_amount
        		,send_time 
        		,estimated_income
        		,estimated_service_income
        		,settlement_amount
        		,leader_settlement_amount 
        		,send_status
    		from dwd.dwd_ks_cps_order cps
            where order_create_time >= (select date_format(min(order_create_time), '%%Y-%%m-%%d 04:00:00') from ods.ods_ks_cps_order where update_time between %(begin_time)s and %(end_time)s)
        ) src
        left join (
        	select o_id
        	from dwd.dwd_ks_recreation
        ) recreation on src.o_id = recreation.o_id
        left join (
        	select o_id 
        	from dwd.dwd_ks_leader_commission_income
        ) lci on src.o_id = lci.o_id
        where recreation.o_id is null
            and lci.o_id is null
        group by order_date
            ,account_id
            ,item_id
            ,item_title
    ) src 
    inner join (
        select 
            account_id
            ,anchor_name
            ,organization_commission_rate
        from dim.dim_ks_account_info dai 
        where line = '直播电商'
            or line = '其它'
            or line is null
    ) info on src.account_id = info.account_id
    left join (
        select 
            order_date
            ,account_id
            ,organization_commission_rate
        from ods.ods_special_allocation
    ) spec on src.account_id = spec.account_id and src.order_date = spec.order_date
) src
left join (
    select 
        item_id
        ,product_id
        ,product_name
        ,item_category
        ,bd_name
        ,product_submit_at
        ,start_date
        ,end_date	
    from dwd.dwd_ks_item_belong
) item_belong on src.item_id = item_belong.item_id and src.order_date between item_belong.start_date and item_belong.end_date
on duplicate key update 
	first_sell_at = values(first_sell_at),
	item_category = values(item_category),
	product_id = values(product_id),
	product_name = values(product_name),
	bd_name = values(bd_name),
	product_submit_at = values(product_submit_at),
	origin_gmv = values(origin_gmv),
	origin_order_number = values(origin_order_number),
	origin_item_number = values(origin_item_number),
	final_gmv = values(final_gmv),
	final_order_number = values(final_order_number),
	final_item_number = values(final_item_number),
	send_order_number = values(send_order_number),
	estimated_income = values(estimated_income),
	estimated_service_income = values(estimated_service_income),
	settlement_gmv = values(settlement_gmv),
	settlement_order_number = values(settlement_order_number),
	settlement_income = values(settlement_income),
	settlement_service_income = values(settlement_service_income),
	send_order_number_yesterday = values(send_order_number_yesterday),
	income = values(income);