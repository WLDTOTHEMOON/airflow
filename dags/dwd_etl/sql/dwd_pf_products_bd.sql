insert into dwd.dwd_pf_products_bd
select 
    id
    ,user_id
    ,supplier_id 
    ,brand
    ,name 
    ,class
    ,mfr_license 
    ,mfrName mfr_name
    ,spec
    ,specUnit spec_unit 
    ,filingId filing_id 
    ,batchId batch_id 
    ,proDate pro_date 
    ,expDate exp_date 
    ,number
    ,priceNormal price_normal
    ,priceSale price_sale
    ,comRatio com_ratio
    ,saleText sale_text 
    ,detail 
    ,urlCover url_cover 
    ,urlCard url_card 
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
    ,machine 
    ,inner_number 
    ,inner_category 
    ,wh_count 
    ,is_sale
    ,standers 
    ,special
    ,anchor
    ,remark
    ,updated_at
    ,created_at
from ods.ods_pf_products
where updated_at between %(begin_time)s and %(end_time)s
    and by_anchor = 0
on duplicate key update
    user_id = values(user_id),
    supplier_id = values(supplier_id),
    brand = values(brand),
    name = values(name),
    class = values(class),
    mfr_license = values(mfr_license),
    mfr_name = values(mfr_name),
    spec = values(spec),
    spec_unit = values(spec_unit),
    filing_id = values(filing_id),
    batch_id = values(batch_id),
    pro_date = values(pro_date),
    exp_date = values(exp_date),
    number = values(number),
    price_normal = values(price_normal),
    price_sale = values(price_sale),
    com_ratio = values(com_ratio),
    sale_text = values(sale_text),
    detail = values(detail),
    url_cover = values(url_cover),
    url_card = values(url_card),
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
    machine = values(machine),
    inner_number = values(inner_number),
    inner_category = values(inner_category),
    wh_count = values(wh_count),
    is_sale = values(is_sale),
    standers = values(standers),
    special = values(special),
    anchor = values(anchor),
    remark = values(remark),
    updated_at = values(updated_at),
    created_at = values(created_at);