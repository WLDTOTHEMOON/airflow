insert into dwd.dwd_ks_leader_commission_income (
        o_id, 
        account_id, 
        author_id, 
        author_name, 
        buyer_open_id, 
        order_trade_amount, 
        cps_order_status, 
        order_create_time, 
        pay_time, 
        send_time, 
        recv_time, 
        settlement_success_time,
        send_status, 
        item_id, 
        item_title, 
        activity_id, 
        seller_id,
        seller_nick_name,
        item_num, 
        commission_rate,
        service_rate,
        anchor_keep_estimated_income,
        leader_origin_estimated_service_income,
        leader_share_estimated_income,
        leader_keep_estimated_service_income,
        settlement_biz_type
    )
select 
    src.o_id o_id
    ,cps.anchor_id account_id
    ,author_id
    ,author_name
    ,buyer_open_id
    ,order_trade_amount / 100 order_trade_amount
    ,case cps_order_status
        when 30 then '已付款'
        when 40 then '已发货'
        when 50 then '已收货'
        when 60 then '已结算'
        when 80 then '已失效'
        else '未知状态'
    end cps_order_status
    ,order_create_time
    ,pay_time
    ,send_time
    ,recv_time
    ,settlement_success_time
    ,case send_status
        when 0 then '未发货'
        when 1 then '已发货'
        else '未知状态'
    end send_status
    ,item_id
    ,item_title
    ,activity_id
    ,seller_id
    ,seller_nick_name
    ,num item_num
    ,commission_rate / 1000 commission_rate
    ,leader.service_rate service_rate -- 该值可能为空，说明对应订单的产品链接没有经过团长
    ,coalesce(cps.estimated_income / 100, 0) anchor_keep_estimated_income -- 达人账号保留佣金收入：一般为0，特殊情况，小仙女保留0.5
    ,coalesce(leader.estimated_service_income, 0) leader_origin_estimated_service_income -- 团长账号原始佣金收入：
    ,0 leader_share_estimated_income -- 废弃字段，剪手为内部剪手时该值才有意义，但是内部剪手不会被团长出让佣金
    ,coalesce(src.estimated_service_income, 0) leader_keep_estimated_service_income -- 团长出让后剩余佣金收入：出让给公司剪手0，出让给外部剪手根据实际情况不一样
    ,case settlement_biz_type
        when 1 then '快分销'
        when 2 then '聚力计划'
        else '未知'
    end settlement_biz_type
from (
    select 
        o_id
        ,author_id
        ,author_name
        ,estimated_income estimated_service_income
    from ods.ods_crawler_leader_commission_income
    where order_create_time >= (select min(order_create_time) from ods.ods_ks_cps_order where update_time between %(begin_time)s and %(end_time)s)
) src
inner join (
    select *
    from ods.ods_ks_cps_order oco 
) cps on src.o_id = cps.o_id
left join (
    select
        o_id
        ,activity_id
        ,regimental_promotion_rate / 1000 service_rate
        ,regimental_promotion_amount / 100000 estimated_service_income
        ,settlement_amount / 100 leader_settlement_amount
    from ods.ods_ks_leader_order
) leader on src.o_id = leader.o_id
on duplicate key update
    account_id = values(account_id)
    ,author_id = values(author_id)
    ,author_name = values(author_name)
    ,buyer_open_id = values(buyer_open_id)
    ,order_trade_amount = values(order_trade_amount)
    ,cps_order_status = values(cps_order_status)
    ,order_create_time = values(order_create_time)
    ,pay_time = values(pay_time)
    ,send_time = values(send_time)
    ,recv_time = values(recv_time)
    ,settlement_success_time = values(settlement_success_time)
    ,send_status = values(send_status)
    ,item_id = values(item_id)
    ,item_title = values(item_title)
    ,item_num = values(item_num)
    ,commission_rate = values(commission_rate)
    ,service_rate = values(service_rate)
    ,activity_id = values(activity_id)
    ,seller_id = values(seller_id)
    ,seller_nick_name = values(seller_nick_name)
    ,anchor_keep_estimated_income = values(anchor_keep_estimated_income)
    ,leader_origin_estimated_service_income = values(leader_origin_estimated_service_income)
    ,leader_share_estimated_income = values(leader_share_estimated_income)
    ,leader_keep_estimated_service_income = values(leader_keep_estimated_service_income)
    ,settlement_biz_type = values(settlement_biz_type);