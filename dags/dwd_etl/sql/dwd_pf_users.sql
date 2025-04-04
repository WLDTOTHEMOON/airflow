-- 备注行
insert into dwd.dwd_pf_users
select 
    id
    ,name
    ,mobile
    ,nickname
    ,avatar
    ,pwd 
    ,case status
    	when 1 then '正常'
    	when 0 then '停用'
    	else '未知状态'
    end status
    ,updated_at 
    ,created_at 
from (
    select *
    from ods.ods_pf_users
) src
on duplicate key update
    name = values(name)
    ,mobile = values(mobile)
    ,nickname = values(nickname)
    ,avatar = values(avatar)
    ,pwd = values(pwd)
    ,status = values(status)
    ,updated_at = values(updated_at)
    ,created_at = values(created_at);