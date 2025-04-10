delete from ads.ads_sale_target_2hourly;

insert into ads.ads_sale_target_2hourly (
    year, month, quarter, anchor_name, group_leader, line,
    origin_target, origin_finish, final_target, final_finish
)
select 
    src.year year
    ,src.month month
    ,case
        when src.month between 1 and 3 then 1
        when src.month between 4 and 6 then 2
        when src.month between 7 and 9 then 3
        when src.month between 10 and 12 then 4
    end quarter
    ,src.anchor_name anchor_name
    ,case
        when anchor_status = '已解约' then '解约主播'
        when group_leader is null then '解约主播'
        else group_leader
    end group_leader
    ,if(anchor_status = '已解约', '', coalesce(line, '')) line
    ,origin_target
    ,coalesce(ec.origin_gmv, slice.origin_gmv, 0) origin_finish
    ,final_target
    ,coalesce(ec.final_gmv, slice.final_gmv, 0) final_finish
from (
    select 
        substr(month, 1, 4) year
        ,substr(month, 6, 7) month
        ,anchor anchor_name
        ,target*10000 origin_target
        ,target_final*10000 final_target
    from ods.ods_gmv_target ogt 
) src
left join (
    select 
        anchor_name 
        ,group_leader 
        ,line 
        ,anchor_status
    from dim.dim_ks_account_info dkai 
    group by 1, 2, 3, 4
) info on src.anchor_name = info.anchor_name
left join (
    select 
        substr(order_date, 1, 4) year
        ,substr(order_date, 6, 2) month
        ,anchor_name 
        ,sum(origin_gmv) origin_gmv
        ,sum(final_gmv) final_gmv
    from dws.dws_ks_big_tbl
    group by 1, 2, 3
) ec on src.year = ec.year and src.month = ec.month and src.anchor_name = ec.anchor_name
left join (
    select
        year
        ,month
        ,anchor_name
        ,sum(origin_gmv) origin_gmv
        ,sum(final_gmv) final_gmv
    from (
        select 
            substr(order_date, 1, 4) year
            ,substr(order_date, 6, 2) month
            ,'刘海州' anchor_name 
            ,sum(origin_gmv) origin_gmv
            ,sum(final_gmv) final_gmv
        from dws.dws_ks_slice_recreation
        group by 1, 2, 3
        union all
        select 
            substr(order_date, 1, 4) year
            ,substr(order_date, 6, 2) month
            ,'刘海州' anchor_name 
            ,sum(origin_gmv) origin_gmv
            ,sum(final_gmv) final_gmv
        from dws.dws_ks_slice_mcn
        group by 1, 2, 3
        union all
        select 
            substr(order_date, 1, 4) year
            ,substr(order_date, 6, 2) month
            ,'刘海州' anchor_name 
            ,sum(origin_gmv) origin_gmv
            ,sum(final_gmv) final_gmv
        from dws.dws_ks_slice_slicer
        group by 1, 2, 3
    ) tmp
    group by 1, 2, 3
) slice on src.year = slice.year and src.month = slice.month and src.anchor_name = slice.anchor_name;