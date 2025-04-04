delete from dwd.dwd_pf_suppliers_belong where 1 = 1;

insert into dwd.dwd_pf_suppliers_belong
select 
    src.id supplier_id
    ,coalesce(begin_time, '2000-01-01') start_date
    ,coalesce(over_time, '2099-12-31') end_date
    ,commerce bd_id
    ,name bd_name
from (
    select
        s.id
        ,commerce
        ,date_format(lv.created_at, '%Y-%m-%d') begin_time
        ,null over_time
    from ods.ods_pf_suppliers s
    left join(
        select
            content
            ,handover_id
            ,created_at
        from(
            select
                content
                ,handover_id
                ,created_at
                ,row_number() over (partition by content order by created_at desc) rn
            from ods.ods_pf_handover h
        ) lst
        where rn = 1
    ) lv on s.id = lv.content and s.commerce = lv.handover_id
    union all
    select
        src.id
        ,handoff_id
        ,case 
            when rn = 1 then null
            when rn > 1 then date_format(last_over_time, '%Y-%m-%d')
        end begin_time
        ,date_format(created_at - interval 1 day, '%Y-%m-%d') over_time
    from(
        select
            s.id
            ,s.name
            ,commerce
            ,handover_id
            ,handoff_id
            ,hd.created_at
            ,hd.updated_at
            ,lag(hd.created_at) over (partition by s.id order by hd.created_at asc) last_over_time
            ,row_number() over (partition by s.id order by hd.created_at asc) rn
        from ods.ods_pf_suppliers s
        inner join(
            select
                *
            from ods.ods_pf_handover h
        ) hd on s.id = hd.content
    ) src
) src
left join (
    select id, name
    from ods.ods_pf_users
) usr on src.commerce = usr.id