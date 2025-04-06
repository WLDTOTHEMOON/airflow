insert into dwd.dwd_pf_reviews (
	id, target, target_id, audit_type, audit_user_id, audit_user_name,
	act, result, advice, annex, updated_at, created_at 
)
select
    src.id id
    ,`target`
    ,target_id
    ,case 
        when special = 1 then '特批'
        when special = 0 or special is null then '常规'
        else '未知'
    end audit_type
    ,user_id audit_user_id
    ,coalesce(audit_user.name, '未知审核人') audit_user_name
    ,case act
        when 0 then '初审_招商|初审专员'
        when 1 then '复审_质控'
        when 2 then '复审_商务负责人|总经办'
        when 3 then '终审_成本'
        when 4 then '入库'
        when 5 then '主播选品_出库'
        when 6 then '主播选品_归还' 
        when 7 then '链接审核'
        when 8 then '迁移数据时，招商品与样品匹配不上时，设为退回'
        when 9 then '仓库上下架，这时候的result：1-上架|0-下架'
        else '未知操作'
    end act
    ,case result
        when 0 then '待审核 | 初审中 | 特批-初审中'
        when 1 then '不通过'
        when 2 then '通过'
        when 3 then '不通过'
        when 4 then '不通过（不可再次提交）'
        when 5 then '通过'
        when 6 then '待成本审核 - 在成本审核不通过后，供应商可以选择只修改机制，从而无需再次【初审】和【复审】，直接到达【成本终审】（6是个意外）'
        when 7 then '不通过'
        when 8 then '通过'
        when 9 then '不通过'
        when 10 then '通过'
        when 11 then '通过'
        when 12 then '不通过'
        when 13 then '不通过_打回初审'
        when 14 then '退回'
        when 15 then '不通过（不可再次提交）'
        when 16 then '特批不通过'
        when 17 then '特批不通过'
        when 18 then '特批上报'
        when 19 then '特批中（总经办）'
        when -1 then '编辑中'
	    else '未知状态'
	end result
    ,advice
    ,annex
    ,updated_at 
    ,created_at 
from (
    select *
    from ods.ods_pf_reviews
    where updated_at between %(begin_time)s and %(end_time)s
) src
left join (
    select id, name
    from ods.ods_pf_users
) audit_user on src.user_id = audit_user.id
on duplicate key update
    `target` = values(`target`)
    ,target_id = values(target_id)
    ,audit_type = values(audit_type)
    ,audit_user_id = values(audit_user_id)
    ,audit_user_name = values(audit_user_name)
    ,act = values(act)
    ,result = values(result)
    ,advice = values(advice)
    ,annex = values(annex)
    ,updated_at = values(updated_at)
    ,created_at = values(created_at);
