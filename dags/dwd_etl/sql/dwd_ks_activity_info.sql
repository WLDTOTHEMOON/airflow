insert into dwd.dwd_ks_activity_info
select 
    activity_id
    ,activity_user_id
    ,activity_user_nick_name
    ,activity_title
    ,case activity_type
        when 1 then '普通活动'
        when 2 then '专属活动'
        when 3 then '合作招商'
        else '其他'
    end activity_type
    ,activity_begin_time
    ,activity_end_time
    ,case activity_status
        when 1 then '未发布'
        when 2 then '已发布'
        when 3 then '推广中'
        when 4 then '活动失效'
        else '其他'
    end activity_status
    ,apply_item_count
    ,wait_audit_item_count
    ,audit_pass_item_count
    ,min_investment_promotion_rate
    ,min_item_commission_rate
    ,contact
    ,pre_exclusive_activity_sign_type
    ,seller_apply_url
    ,leader_apply_url
    ,exists_activity_id
    ,promoter_id
    ,tips
    ,base_order_amount
    ,base_order_status
    ,cooperate_begin_time
    ,cooperate_end_time
from ods.ods_ks_activity_info
on duplicate key update 
    activity_user_id = values(activity_user_id),
    activity_user_nick_name = values(activity_user_nick_name),
    activity_title = values(activity_title),
    activity_type = values(activity_type),
    activity_begin_time = values(activity_begin_time),
    activity_end_time = values(activity_end_time),
    activity_status = values(activity_status),
    apply_item_count = values(apply_item_count),
    wait_audit_item_count = values(wait_audit_item_count),
    audit_pass_item_count = values(audit_pass_item_count),
    min_investment_promotion_rate = values(min_investment_promotion_rate),
    min_item_commission_rate = values(min_item_commission_rate),
    contact = values(contact),
    pre_exclusive_activity_sign_type = values(pre_exclusive_activity_sign_type),
    seller_apply_url = values(seller_apply_url),
    leader_apply_url = values(leader_apply_url),
    exists_activity_id = values(exists_activity_id),
    promoter_id = values(promoter_id),
    tips = values(tips),
    base_order_amount = values(base_order_amount),
    base_order_status = values(base_order_status),
    cooperate_begin_time = values(cooperate_begin_time),
    cooperate_end_time = values(cooperate_end_time);
