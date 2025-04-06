insert into dwd.dwd_pf_products_anchor
select 
    id 
    ,user_id
    ,name
    ,class
    ,spec
    ,brand
    ,proDate pro_date 
    ,expDate exp_date 
    ,batchId batch_id 
    ,mfr_license 
    ,mfrName mfr_name
    ,ec_name
    ,ec_mobile
    ,ec_license
    ,ec_contract
    ,anchor_live 
    ,live_time
    ,machine
    ,priceSale price_sale
    ,comRatio com_ratio
    ,settle_date
    ,settle_way
    ,urlReal url_real 
    ,urlOuter url_outer 
    ,urlInner url_inner
    ,thirdTest third_test 
    ,licensePro license_pro 
    ,trademark
    ,certAuth cert_auth 
    ,certFiling cert_filing 
    ,annex
    ,case status
        when 0 then '待审核 | 初审中 | 特批-初审中'
        when 1 then '初审不通过'
        when 2 then '初审通过 | 复审中 | 特批-复审中'
        when 3 then '复审不通过'
        when 4 then '复审不通过（不可再次提交）'
        when 5 then '复审通过 | 成本审核中'
        when 6 then '待成本审核 - 在成本审核不通过后，供应商可以选择只修改机制，从而无需再次【初审】和【复审】，直接到达【成本终审】（6是个意外）'
        when 7 then '特批-复审不通过'
        when 8 then '特批-审核通过 | 成本审核中'
        when 9 then '成本-不通过'
        when 10 then '成本-通过 | 待入库'
        when 11 then '入库'
        when 12 then '不允许入库'
        when 13 then '复审不通过 - 退回初审，包括招商商品和主播自采'
        when 14 then '退回 - 把已通过的退回，编辑后重新审核'
        when -1 then '编辑中，这是老平台的数据'
        else '未知状态'
    end status 
    ,special 
    ,anchor
    ,remark
    ,ref_id
    ,updated_at 
    ,created_at 
from ods.ods_pf_products
where updated_at between %(begin_time)s and %(end_time)s
    and by_anchor = 1
on duplicate key update
    user_id = values(user_id),
    name = values(name),
    class = values(class),
    spec = values(spec),
    brand = values(brand),
    pro_date = values(pro_date),
    exp_date = values(exp_date),
    batch_id = values(batch_id),
    mfr_name = values(mfr_name),
    mfr_license = values(mfr_license),
    ec_name = values(ec_name),
    ec_mobile = values(ec_mobile),
    ec_license = values(ec_license),
    ec_contract = values(ec_contract),
    anchor_live = values(anchor_live),
    live_time = values(live_time),
    machine = values(machine),
    price_sale = values(price_sale),
    com_ratio = values(com_ratio),
    settle_date = values(settle_date),
    settle_way = values(settle_way),
    url_real = values(url_real),
    url_outer = values(url_outer),
    url_inner = values(url_inner),
    third_test = values(third_test),
    license_pro = values(license_pro),
    trademark = values(trademark),
    cert_auth = values(cert_auth),
    cert_filing = values(cert_filing),
    annex = values(annex),
    status = values(status),
    special = values(special),
    anchor = values(anchor),
    remark = values(remark),
    ref_id = values(ref_id),
    updated_at = values(updated_at),
    created_at = values(created_at);