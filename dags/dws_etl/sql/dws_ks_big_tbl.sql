delete from dws.dws_ks_ec_2hourly
where order_date between %(start_date)s and %(end_date)s

insert into dws.dws_ks_big_tbl (
    order_date, account_id, anchor_name, item_id, item_title, first_sell_at, item_category,
    product_id, product_name, bd_name, product_submit_at, origin_gmv, origin_order_number, origin_item_number,
    final_gmv, final_order_number, final_item_number, send_order_number, 
    estimated_income, estimated_service_income, settlement_gmv, settlement_order_number,
    settlement_income, settlement_service_income, send_order_number_yesterday, income
)
select 
    order_date
    ,account_id
    ,anchor_name
    ,src.item_id item_id
    ,src.item_title item_title
    ,first_sell_at
    ,if(item_category is not null, item_category, cp.path) item_category
    ,product_id
    ,product_name
    ,if(item_belong.bd_name is null and product_id is null, item_belong_check.bd_name, item_belong.bd_name) bd_name
    ,item_belong.pro_submit_at product_submit_at
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
        ,item_info.item_category_name item_category
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
            date(order_create_time - interval 4 hour) order_date
            ,account_id
            ,item_id
            ,item_title
            ,min(order_create_time) first_sell_at
            ,sum(order_trade_amount) origin_gmv
            ,count(o_id) origin_order_number
            ,sum(item_num) origin_item_number
            ,sum(if(cps_order_status = '已失效', 0, order_trade_amount)) final_gmv
            ,count(if(cps_order_status = '已失效', null, o_id)) final_order_number
            ,sum(if(cps_order_status = '已失效', 0, item_num)) final_item_number
            ,count(if(cps_order_status != '已失效' and send_status = '已发货', o_id, null)) send_order_number
            ,count(if(
                send_time between date_format(current_timestamp - interval 1 day, '%Y-%m-%d 04:00:00') and date_format(current_timestamp, '%Y-%m-%d 04:00:00'),
                o_id, null
            )) send_order_number_yesterday
            ,sum(estimated_income) estimated_income
            ,sum(estimated_service_income) estimated_service_income
            ,sum(if(cps_order_status = '已结算', order_trade_amount, 0)) settlement_gmv
            ,count(if(cps_order_status = '已结算', o_id, null)) settlement_order_number
            ,sum(if(cps_order_status = '已结算', settlement_amount , 0)) settlement_income
            ,sum(if(cps_order_status = '已结算', leader_settlement_amount , 0)) settlement_service_income
        from dwd.dwd_ks_cps_order
        where o_id not in ( select o_id from dwd.dwd_ks_recreation )
            and o_id not in ( select o_id from dwd.dwd_ks_leader_commission_income )
            and order_create_time between 
                if(hour(current_timestamp) = 4, '2024-10-01 04:00:00', date_format(current_timestamp - interval 1 month, '%Y-%m-01 04:00:00')) and current_timestamp
        group by date(order_create_time - interval 4 hour)
            ,account_id
            ,item_id
            ,item_title
    ) src 
    inner join (
        select 
            account_id
            ,anchor_name
            ,organization_commission_rate
        from dim.dim_ks_anchor_info dai 
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
    left join (
        select 
            item_id
            ,item_category_name 
        from (
            select
                *
                ,row_number() over(partition by item_id order by item_apply_time desc) rn
            from dwd.dwd_activity_item_list
        ) tmp
        where rn = 1
    ) item_info on src.item_id = item_info.item_id
) src
left join (
    select 
        src.item_id item_id
        ,src.product_id product_id
        ,pro.name product_name
        ,substring_index(pro.category COLLATE utf8mb4_general_ci, ',', -1) product_category
        ,sup.bd_name bd_name
        ,pro.created_at pro_submit_at
        ,coalesce(sup.start_date, '2000-01-01') start_date
        ,coalesce(sup.end_date, '2099-12-31') end_date	
    from (
        select 
            item_id
            ,product_id
        from (
            select
                item_id
                ,product_id
                ,row_number() over(partition by item_id order by by_anchor, updated_at desc) rn
            from dwd.dwd_platform_links
            where status = '通过'
        ) tmp
        where rn = 1
    ) src
    left join (
        select 
            id
            ,supplier_id
            ,name
            ,class category
            ,created_at
        from dwd.dwd_platform_products_bd
        union all
        select 
            id
            ,null supplier_id
            ,name
            ,class category
            ,created_at
        from dwd.dwd_platform_products_anchor
    ) pro on src.product_id = pro.id
    left join (
        select 
            supplier_id
            ,start_date
            ,end_date
            ,bd_id
            ,bd_name
        from dwd.dwd_platform_suppliers_belong
    ) sup on pro.supplier_id = sup.supplier_id
) item_belong on src.item_id = item_belong.item_id and src.order_date between item_belong.start_date and item_belong.end_date
left join (
    select *
    from ods.ods_item_category_path
) cp on item_belong.product_category = cp.category_id
left join (
    select item_id, item_title, max(bd_name) bd_name
    from ods.ods_item_belong oib 
    where order_date >= '2023-11-01'
    group by item_id, item_title
) item_belong_check on src.item_id = item_belong_check.item_id and src.item_title = item_belong_check.item_title;