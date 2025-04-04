-- 备注行
insert into dwd.dwd_pf_links
select 
    src.id id
    ,user_id
    ,if(by_anchor = 1, src.product_id, anchor_sel.product_id) product_id
    ,activity_id
    ,item_id
    ,case status 
        when 0 then '待审核'
        when 1 then '通过'
        when 2 then '不通过'
        else '未知状态'
    end status
    ,by_anchor
    ,updated_at 
    ,created_at 
from (
    select 
        id
        ,user_id
        ,select_product_id
        ,product_id
        ,activity_id
        ,item_id
        ,status
        ,by_anchor
        ,updated_at 
        ,created_at 
    from ods.ods_platform_links opl 
) src
left join (
    select id, product_id
    from ods.ods_platform_anchor_select_products opasp 
) anchor_sel on src.select_product_id = anchor_sel.id
on duplicate key update
    user_id = values(user_id)
    ,product_id = values(product_id)
    ,activity_id = values(activity_id)
    ,item_id = values(item_id)
    ,status = values(status)
    ,by_anchor = values(by_anchor)
    ,updated_at = values(updated_at)
    ,created_at = values(created_at);