CREATE TABLE HOME_GROUP_DEPT
  (
     DEPT_SID        NVARCHAR(50) NOT NULL,-- 主键     
     GROUP_SID       NVARCHAR(50) NULL,    -- 物业集团SID
     DEPT_NAME       NVARCHAR(50) NULL,    -- 部门名称   
     SORT_INDEX      INT NULL,             -- 排序号    
     PARENT_DEPT_SID NVARCHAR(50) NULL,    -- 上级部门   
     REMARK          NVARCHAR(200) NULL,   -- 备注     
     CREATEDBY       NVARCHAR(50) NULL,    -- 创建用户SID
     CREATED_ON      DATETIME NULL,        -- 创建时间   
     MODIFIEDBY      NVARCHAR(50) NULL,    -- 修改用户SID
     MODIFIED_ON     DATETIME NULL,        -- 修改时间   
     CONSTRAINT PK_HOME_GROUP_DEPT PRIMARY KEY(DEPT_SID )
  )
  
  
  
  with A as(
  select a.OWNER_SID,a.OWNER_NO ,a.GROUP_SID,a. DEPT_SID,b.DEPT_SID ,b.GROUP_SID 
  ,b.DEPT_NAME,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX
  from HOME_OWNER a 
  left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
  where DEPT_SID in('4e034f7d-3e75-4397-91e8-0ef16d66c7b9')
  union all 
  select a.OWNER_SID,a.OWNER_NO ,a.GROUP_SID,a. DEPT_SID,b.DEPT_SID ,b.GROUP_SID 
  ,b.DEPT_NAME,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX
  from HOME_OWNER a 
  left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
 inner join A on b.DEPT_SID=A.DEPT_SID
  
  )
  select * from A
  
  
  
 DEPT_SID(子集ID)   PARENT_DEPT_SID(上级ID)  
 a                        b
 b                        c
 c                        d
 d                        e
 
 
 
 
 
 
 

select c.APARTMENT_NAME 项目,d.TYPE_NAME 类型,a.CREATED_ON 发帖时间,SUBSTRING(CONVERT(varchar(100),a.CREATED_ON, 108),1,5) 发帖时点,
 datediff(mi,a.CREATED_ON,getdate())/60 未回复时长,
datediff(mi,a.CREATED_ON,getdate())/3600 未回复天数 ,
b.FAMILY_NAME 发帖人,a.POST_CONTENT 发帖内容,e.COMMENT_CONTENT 回复内容,
e.CREATED_ON 回复时间
from HOME_NEIGHBOR_POST a 
left join HOME_NEIGHBOR_COMMENT e on a.POST_SID = e.POST_SID
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY 
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
where d.TYPE_NAME in('咨询物业')
--and b.OWNER_NO like ('%一期%')
and a.CREATED_ON >='20170101'
--and a.CREATED_ON <'20170401'
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and b.OWNER_TYPE=1--类型为业主
and e.CREATED_ON is null
--and convert(char(8),a.CREATED_ON,108)>='08:30:00' and  convert(char(8),a.CREATED_ON,108)<'18:00:00'--1
order by SUBSTRING(CONVERT(varchar(100),a.CREATED_ON, 108),1,5) 




select a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC, 
(CASE a.SERVICE_STATUS
                 WHEN '0' THEN
                  '等待确认付款'
                 WHEN '1' THEN
                  '待派单'
                 WHEN '2' THEN
                  '已派单' 
                 wHEN '3' THEN
                  '已撤消' 
                 WHEN '20' THEN
                  '已转发'
                 WHEN '21' THEN
                  '已退回' 
                 WHEN '22' THEN
                  '处理中'
                 WHEN '3' THEN
                  '撤消' 
                 WHEN '4' THEN
                  '处理完成，待评价' 
                 WHEN '6' THEN
                  '流程结束'
                 WHEN '9' THEN
                  '已关闭' 
                 ELSE
                  ''
               END) AS 服务提报状态,a.CREATED_ON 呼叫时间,
a.PROCESS_TIME 处理时间,b.CATEGORY_NAME 服务类型,c.APARTMENT_NAME 小区名称,a.RESPONSE_TIME 响应时间
from HOME_SERVICE_MAIN  a
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID 
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where APARTMENT_NAME NOT IN('幸福家园')
--and e.OWNER_NO like ('%一期%')
and b.CATEGORY_NAME not in('巡检','家政服务')
and a.CREATED_ON >='20170101'--当月提报
and a.CREATED_ON <'20170226'
and a.RESPONSE_TIME is null


