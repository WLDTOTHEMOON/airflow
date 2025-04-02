insert into dwd.dwd_ks_activity_item_list 
select
    activity_id
    ,item_id
    ,item_title
    ,item_commission_rate / 1000 item_commission_rate
    ,investment_promotion_rate / 1000 investment_promotion_rate
    ,investment_promotion_remise_rate / 1000 investment_promotion_remise_rate
    ,seller_id
    ,seller_nick_name
    ,shop_id
    ,pre_activity_id
    ,item_apply_time
    ,item_price / 100 item_price
    ,item_volume
    ,base_order_amount
    ,case base_order_status
        when 0 then '未达成'
        when 1 then '已达成'
        else '未知状态'
    end base_order_status
    ,case item_audit_status
        when 1 then '待审核'
        when 2 then '审核通过'
        when 3 then '审核拒绝'
        when 5 then '已失效'
    end item_audit_status
    ,reason
    ,cooperate_begin_time
    ,cooperate_end_time
    ,case identity
        when 1 then '商家报名'
        when 2 then '团长报名'
        else '未知'
    end identity
    ,case item_status
        when 1 then '正常'
        else '非正常'
    end item_status
    ,item_category_name
    ,item_leaf_category_id
    ,substring_index(item_category_name, '>', 1) item_category_level_first
    ,substring_index(item_category_name, '>', -1) item_category_level_last
    ,item_stock
    ,item_img_url
    ,disabled_msg
    ,contact
    ,disabled
    ,contact_user_type
from ods.ods_ks_activity_item_list
on duplicate key update
    item_commission_rate = values(item_commission_rate)
    ,investment_promotion_rate = values(investment_promotion_rate)
    ,investment_promotion_remise_rate = values(investment_promotion_remise_rate)
    ,seller_nick_name = values(seller_nick_name)
    ,shop_id = values(shop_id)
    ,item_apply_time = values(item_apply_time)
    ,item_price = values(item_price)
    ,item_volume = values(item_volume) 
    ,base_order_amount = values(base_order_amount)
    ,base_order_status = values(base_order_status)
    ,item_audit_status = values(item_audit_status)
    ,reason = values(reason)
    ,cooperate_begin_time = values(cooperate_begin_time)
    ,cooperate_end_time = values(cooperate_end_time)
    ,identity = values(identity)
    ,item_status = values(item_status)
    ,item_category_name = values(item_category_name)
    ,item_leaf_category_id = values(item_leaf_category_id)
    ,item_category_level_first = values(item_category_level_first)
    ,item_category_level_last = values(item_category_level_last)
    ,item_stock = values(item_stock)
    ,item_img_url = values(item_img_url)
    ,disabled_msg = values(disabled_msg)
    ,contact = values(contact)
    ,disabled = values(disabled)
    ,contact_user_type = values(contact_user_type);