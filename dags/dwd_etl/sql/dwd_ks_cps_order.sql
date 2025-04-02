insert into dwd.dwd_ks_cps_order (
    update_time, create_time, o_id, account_id, buyer_open_id, order_trade_amount, 
    cps_order_status, order_create_time, pay_time, send_time, recv_time, settlement_success_time,
    settlement_amount, leader_settlement_amount, send_status, item_id, item_title, activity_id,
    seller_id, seller_nick_name, item_num, commission_rate, service_rate, estimated_income,
    estimated_service_income, settlement_biz_type
)
select
    update_time
    ,create_time
    ,src.o_id o_id
    ,src.anchor_id account_id
    ,buyer_open_id
    ,order_trade_amount / 100 order_trade_amount
    ,case cps_order_status
        when 30 then '已付款'
        when 50 then '已收货'
        when 60 then '已结算'
        when 80 then '已失效'
        when 40 then '已发货'
        else '未知状态'
    end cps_order_status
    ,order_create_time
    ,pay_time
    ,send_time
    ,recv_time
    ,settlement_success_time
    ,settlement_amount / 100 settlement_amount
    ,leader_settlement_amount
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
    ,leader.service_rate service_rate
    ,estimated_income / 100 estimated_income
    ,estimated_service_income
    ,case settlement_biz_type
        when 1 then '快分销'
        when 2 then '聚力计划'
        else '未知'
    end settlement_biz_type
from (
    select *
    from ods.ods_ks_cps_order
    where update_time between %(begin_time)s and %(end_time)s
) src
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
    update_time = values(update_time)
    ,create_time = values(create_time)
    ,account_id = values(account_id)
    ,buyer_open_id = values(buyer_open_id)
    ,order_trade_amount = values(order_trade_amount)
    ,cps_order_status = values(cps_order_status)
    ,order_create_time = values(order_create_time)
    ,pay_time = values(pay_time)
    ,send_time = values(send_time)
    ,recv_time = values(recv_time)
    ,settlement_success_time = values(settlement_success_time)
    ,settlement_amount = values(settlement_amount)
    ,leader_settlement_amount = values(leader_settlement_amount)
    ,send_status = values(send_status)
    ,item_id = values(item_id)
    ,item_title = values(item_title)
    ,item_num = values(item_num)
    ,commission_rate = values(commission_rate)
    ,service_rate = values(service_rate)
    ,estimated_income = values(estimated_income)
    ,estimated_service_income = values(estimated_service_income)
    ,activity_id = values(activity_id)
    ,seller_id = values(seller_id)
    ,seller_nick_name = values(seller_nick_name)
    ,settlement_biz_type = values(settlement_biz_type);