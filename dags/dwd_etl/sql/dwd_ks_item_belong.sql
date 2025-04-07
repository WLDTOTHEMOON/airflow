delete from dwd.dwd_ks_item_belong where 1 = 1;

insert into dwd.dwd_ks_item_belong
select 
	src.item_id item_id
    ,src.start_date start_date
    ,src.end_date end_date
    ,product_id
    ,product_name
    ,if(item_info.item_category is not null, item_info.item_category, cp.path) item_category
    ,if(src.bd_name is null and product_id is null, item_belong_check.bd_name, src.bd_name) bd_name
    ,src.pro_submit_at product_submit_at
from (
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
	        from dwd.dwd_pf_links
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
	    from dwd.dwd_pf_products_bd
	    union all
	    select 
	        id
	        ,null supplier_id
	        ,name
	        ,class category
	        ,created_at
	    from dwd.dwd_pf_products_anchor
	) pro on src.product_id = pro.id
	left join (
	    select 
	        supplier_id
	        ,start_date
	        ,end_date
	        ,bd_id
	        ,bd_name
	    from dwd.dwd_pf_suppliers_belong
	) sup on pro.supplier_id = sup.supplier_id
) src
left join (
    select 
        item_id
        ,item_category_name item_category
    from (
        select
            *
            ,row_number() over(partition by item_id order by item_apply_time desc) rn
        from dwd.dwd_ks_activity_item_list
    ) tmp
    where rn = 1
) item_info on src.item_id = item_info.item_id
left join (
    select *
    from ods.ods_item_category_path -- 快手类目code，不进行更新
) cp on src.product_category = cp.category_id
left join (
    select item_id, max(bd_name) bd_name
    from ods.ods_fs_links
    where order_date >= '2023-11-01'
    group by item_id
) item_belong_check on src.item_id = item_belong_check.item_id;