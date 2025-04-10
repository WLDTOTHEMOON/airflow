delete from ads.ads_bd_products_reviews_daily where 1 = 1;

insert into ads.ads_bd_products_reviews_daily
select
    target_id
    ,cat.category_name product_category
    ,brand product_brand
    ,pro.name product_name
    ,audit_type
    ,substring_index(act, '_', -1) roles
    ,substring_index(act, '_', 1) act
    ,result
    ,advice
    ,row_number() over(partition by substring_index(act, '_', -1),target_id order by src.created_at) audit_num
    ,src.created_at audit_time
    ,sup.name ec_name
    ,sup.supplier_name supplier_name
    ,sup.supplier_mobile supplier_mobile
    ,sup.bd_name bd_name
    ,audit_user_name
    ,pro.created_at submit_time
from (
    select
        target
        ,target_id
        ,audit_type
        ,act
        ,result
        ,advice
        ,created_at
        ,audit_user_name
    from dwd.dwd_pf_reviews
    where target = 'product'
        and audit_user_name not in ('方涌超', '张澜', '管理员', '雷江玲', '赵乙都', '张小卓')
) src
inner join (
    select
        id
        ,name
        ,substring_index(class,',', 1) class
        ,brand
        ,supplier_id
        ,user_id
        ,created_at
    from dwd.dwd_pf_products_bd
) pro on src.target_id = pro.id
left join (
    select 
        id
        ,name
        ,supplier_id
        ,supplier_name
        ,supplier_mobile
        ,bd_id
        ,bd_name
        ,bd_mobile
    from dwd.dwd_pf_suppliers
) sup on pro.supplier_id = sup.id
left join (
    select category_id, category_name
    from ods.ods_item_category oic 
) cat on pro.class collate utf8mb4_0900_ai_ci = cat.category_id;