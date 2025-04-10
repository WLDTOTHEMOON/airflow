delete from ads.ads_suppliers_reviews_daily where 1 = 1;

insert into ads.ads_suppliers_reviews_daily
select
    target_id
    ,type
    ,ec_category
    ,sup.name ec_name
    ,substring_index(act, '_', -1) roles
    ,substring_index(act, '_', 1) act
    ,result
    ,advice
    ,row_number() over(partition by substring_index(act, '_', -1), target_id order by src.created_at) audit_num
    ,src.created_at audit_time
    ,supplier_name
    ,supplier_mobile
    ,bd_name
    ,audit_user_name
    ,sup.created_at submit_time
from (
    select
        target
        ,target_id
        ,act
        ,result
        ,advice
        ,created_at
        ,audit_user_name
    from dwd.dwd_pf_reviews
    where target = 'supplier'
        and audit_user_name not in ('方涌超', '张澜', '管理员', '雷江玲', '赵乙都')
) src
inner join (
    select
        id
        ,name
        ,class
        ,supplier_name
        ,supplier_mobile
        ,bd_name
        ,created_at
        ,type
    from dwd.dwd_pf_suppliers
) sup on src.target_id = sup.id
left join (
    select 
        supplier_id
        ,group_concat(name) ec_category
    from (
        select supplier_id, class_id
        from ods.ods_pf_supplier_class sc 
    ) src
    left join (
        select id, name
        from ods.ods_pf_tree
    ) tree on src.class_id = tree.id
    group by src.supplier_id
) sup_cls on sup.id = sup_cls.supplier_id;