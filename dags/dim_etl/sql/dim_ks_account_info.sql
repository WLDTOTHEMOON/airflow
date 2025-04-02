delete from dim.dim_ks_account_info where 1 = 1;

insert into dim.dim_ks_account_info (
    account_id, account_status, anchor_name, anchor_status, line, anchor_type,
    group_leader, specific_bd, specific_op, anchor_commission_rate, organization_commission_rate,
    other_commission_rate, other_commission_belong, open_id, update_at
)
select 
    account_id
    ,case src.status
        when 1 then '已授权'
        when 0 then '未授权'
    end account_status
    ,an.name anchor_name
    ,case info.status
        when 1 then '合作中'
        when 2 then '解约中'
        when 3 then '已解约'
    end anchor_status
    ,case info.line
        when 0 then '特殊主播'
        when 1 then '直播电商'
        when 2 then '切片'
        when 3 then '其它'
    end line
    ,anchor_type
    ,gl.name group_leader
    ,sb.name specific_bd
    ,so.name specific_op
    ,anchor_commission_rate
    ,organization_commission_rate
    ,other_commission_rate
    ,ocb.name other_commission_belong
    ,open_id
    ,if(src.update_at >= info.updated_at, src.update_at, info.updated_at) update_at
from (
    select 
        account_id
        ,anchor_id
        ,open_id
        ,status
        ,update_at
    from ods.ods_pf_account_info
) src
left join (
    select 
        anchor_id
        ,anchor_type
        ,group_leader
        ,specific_bd
        ,specific_op
        ,anchor_commission_rate
        ,organization_commission_rate
        ,other_commission_rate
        ,other_commission_belong
        ,status
        ,line
        ,updated_at
    from ods.ods_pf_anchor_info
) info on src.anchor_id = info.anchor_id
left join (
    select id, name
    from ods.ods_pf_users
) an on info.anchor_id = an.id
left join (
    select id, name
    from ods.ods_pf_users
) gl on info.group_leader = gl.id
left join (
    select id, name
    from ods.ods_pf_users
) sb on info.specific_bd = sb.id
left join (
    select id, name
    from ods.ods_pf_users
) so on info.specific_op = so.id
left join (
    select id, name
    from ods.ods_pf_users
) ocb on info.other_commission_belong = ocb.id;