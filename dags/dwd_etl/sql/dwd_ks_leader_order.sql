insert into dwd.dwd_ks_leader_order (
    create_time, update_time, o_id, activity_id, promotion_id, promotion_nickname,
    promotion_type, buyer_open_id, order_create_time, cps_order_status,
    service_rate, estimated_service_income, settlement_amount, settlement_success_time,
    recv_time, item_id, item_title, send_time, send_status, order_trade_amount,
    expend_regimental_settle_amount, expend_estimate_settle_amount, 
    expend_regimental_promotion_rate, excitation_income, service_income
)
select
    create_time
    ,update_time
    ,o_id
    ,activity_id
    ,promotion_id
    ,promotion_nickname 
    ,case promotion_type
        when 0 then '无'
        when 1 then '达人推广'
        when 2 then '团长推广'
        else '未知类型'
    end promotion_type
    ,buyer_open_id 
    ,order_create_time
    ,case cps_order_status
        when 30 then '已付款'
        when 50 then '已收货'
        when 60 then '已结算'
        when 80 then '已失效'
        when 40 then '已发货'
        else '未知状态'
    end cps_order_status
    ,regimental_promotion_rate / 1000 service_rate
    ,regimental_promotion_amount / 100000 estimated_service_income
    ,settlement_amount / 100 settlement_amount
    ,settlement_success_time
    ,recv_time
    ,item_id
    ,item_title
    ,send_time
    ,case send_status
        when 0 then '未发货'
        when 1 then '已发货'
        else '未知状态'
    end send_status
    ,order_trade_amount / 100 order_trade_amount
    ,expend_regimental_settle_amount / 1000 expend_regimental_settle_amount
    ,expend_estimate_settle_amount / 1000 expend_estimate_settle_amount 
    ,expend_regimental_promotion_rate / 1000 expend_regimental_promotion_rate
    ,excitation_income 
    ,service_income 
from ods.ods_ks_leader_order
where update_time between %(begin_time)s and %(end_time)s
on duplicate key update
    update_time = values(update_time)
    ,create_time = values(create_time)
    ,activity_id = values(activity_id)
    ,promotion_id = values(promotion_id)
    ,promotion_nickname = values(promotion_nickname)
    ,promotion_type = values(promotion_type)
    ,buyer_open_id = values(buyer_open_id)
    ,order_create_time = values(order_create_time)
    ,cps_order_status = values(cps_order_status)
    ,service_rate = values(service_rate)
    ,estimated_service_income = values(estimated_service_income)
    ,settlement_amount = values(settlement_amount)
    ,settlement_success_time = values(settlement_success_time)
    ,recv_time = values(recv_time)
    ,item_id = values(item_id)
    ,item_title = values(item_title)
    ,send_time = values(send_time)
    ,send_status = values(send_status)
    ,order_trade_amount = values(order_trade_amount)
    ,expend_regimental_settle_amount = values(expend_regimental_settle_amount)
    ,expend_estimate_settle_amount = values(expend_estimate_settle_amount)
    ,expend_regimental_promotion_rate = values(expend_regimental_promotion_rate)
    ,excitation_income = values(excitation_income)
    ,service_income = values(service_income);
