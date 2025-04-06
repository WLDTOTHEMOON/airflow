-- 备注行
insert into dwd.dwd_pf_suppliers
select 
    src.id id
    ,src.name name
    ,code
    ,class
    ,case type
        when 'dealer' then '经销商'
        when 'manufacturer' then '生产商'
        else '未知类型'
    end type
    ,address
    ,contact
    ,src.mobile mobile
    ,website
    ,email
    ,group_s wechat_group
    ,case src.status 
        when 0 then '待审核'
        when 1 then '初审不通过'
        when 2 then '初审通过'
        when 3 then '复审不通过'
        when 4 then '复审不通过_不可再次提交'
        when 5 then '审核通过'
        when 13 then '复审不通过_退回初审'
        when 14 then '退回'
        when -1 then '主动撤回编辑中'
        else '未知状态'
    end status
    ,user_id supplier_id
    ,sup_user.name supplier_name
    ,sup_user.mobile supplier_mobile
    ,case sup_user.status 
        when 1 then '正常'
        when 2 then '停用'
        else '未知状态'
    end supllier_status
    ,commerce bd_id
    ,bd_user.name bd_name
    ,bd_user.mobile bd_mobile
    ,case bd_user.status 
        when 1 then '正常'
        when 2 then '停用'
        else '未知状态'
    end bd_status
    ,updated_at
    ,created_at
from (
    select
        id
        ,user_id 
        ,name
        ,code
        ,class
        ,type
        ,commerce
        ,address 
        ,contact
        ,mobile
        ,website 
        ,email
        ,group_s
        ,status
        ,updated_at 
        ,created_at 
    from ods.ods_pf_suppliers
    where updated_at between %(begin_time)s and %(end_time)s
) src
left join (
    select id, name, mobile, status
    from ods.ods_pf_users
) sup_user on src.user_id = sup_user.id
left join (
    select id, name, mobile, status
    from ods.ods_pf_users
) bd_user on src.commerce = bd_user.id
on duplicate key update
    name = values(name)
    ,code = values(code)
    ,class = values(class)
    ,type = values(type)
    ,address = values(address)
    ,contact = values(contact)
    ,mobile = values(mobile)
    ,website = values(website)
    ,email = values(email)
    ,wechat_group = values(wechat_group)
    ,status = values(status)
    ,supplier_id = values(supplier_id)
    ,supplier_name = values(supplier_name)
    ,supplier_mobile = values(supplier_mobile)
    ,supplier_status = values(supplier_status)
    ,bd_id = values(bd_id)
    ,bd_name = values(bd_name)
    ,bd_mobile = values(bd_mobile)
    ,bd_status = values(bd_status)
    ,updated_at = values(updated_at)
    ,created_at = values(created_at);