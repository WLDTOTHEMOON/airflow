delete from ads.ads_anchor_select_reviews_daily where 1 = 1;
    
insert into ads.ads_anchor_select_reviews_daily
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
    ,row_number() over(partition by substring_index(act, '_', -1), target_id order by src.created_at) audit_num
    ,src.created_at audit_time
    ,ec_name
    ,sup_users.name supplier_name
    ,sup_users.mobile supplier_mobile
    ,if(substring_index(act, '_', -1) = '特批', anchor_users.name, anchor_live_users.name) anchor_name
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
        and audit_user_name not in ('方涌超', '张澜', '管理员', '雷江玲', '赵乙都')
) src
inner join (
    select
        id
        ,name
        ,substring_index(class,',', 1) class
        ,brand
        ,user_id
        ,anchor_live
        ,anchor
        ,ec_name
        ,created_at
    from dwd.dwd_pf_products_anchor
) pro on src.target_id = pro.id
left join (
    select id, name, mobile
    from dwd.dwd_pf_users
) sup_users on pro.user_id = sup_users.id
left join (
    select id, name, mobile
    from dwd.dwd_pf_users
--     where all_role like '%主播%'
) anchor_live_users on pro.anchor_live = anchor_live_users.id
left join (
    select id, name, mobile
    from dwd.dwd_pf_users
) anchor_users on pro.anchor = anchor_users.id
left join (
    select category_id, category_name
    from ods.ods_item_category oic 
) cat on pro.class collate utf8mb4_0900_ai_ci = cat.category_id;