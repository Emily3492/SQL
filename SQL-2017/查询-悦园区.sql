--JOY_PARK
-----------悦园区查询
---20170316悦园区埋点完善



--用户原始表
--悦园区对应的悦服务用户,悦服务用户无对应小区,根据权限表中部门区分项目
select * from HOME_OWNER  b  
left join HOME_APARTMENT c on b.APARTMENT_SID =c.APARTMENT_SID
where  OWNER_TYPE not LIKE ('%1%')--工作人员


--悦园区用户表
select * from HOME_OWNER  b  
left join HOME_APARTMENT c on b.APARTMENT_SID =c.APARTMENT_SID
where OWNER_TYPE LIKE ('%1%')--业主




--权限表提取
--匹配悦服务工作人员所在部门的时候需要
select t1.是否启用,t1.用户类型,t1.事业部,t1.区域,t1.地区类型,t1.园区项目,t1.岗位,t1.事业部+'-'+t1.区域+'-'+t1.地区类型+'-'+t1.园区项目+'-'+t1.岗位 事业部汇总
,t1.排序号,t1.房号,t1.姓名,t1.手机号码
 from(
  select  a.OWNER_SID,a.OWNER_NO 房号,a.OWNER_NAME 姓名,a.OWNER_PHONE 手机号码,a.OWNER_STATUS 是否启用,
a.GROUP_SID,a. DEPT_SID,a.OWNER_TYPE 用户类型
  ,b.DEPT_NAME 岗位,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX 排序号,c.DEPT_NAME 园区项目,e.DEPT_NAME 地区类型,
f.DEPT_NAME 区域,g.DEPT_NAME 事业部,d.GROUP_NAME 集团名称
  from HOME_OWNER a
  left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID
left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID
left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
--where  a.OWNER_SID in('0000c52a-8688-46ef-a4b2-4f90a449cedd')
where g.DEPT_NAME in('事业三部')
and a.OWNER_STATUS=1)t1





--测试日志生成
select  top 10 t1.* from(select a.CREATED_ON,a.CONTENT,b.OWNER_NAME ,a.OWNER_SID,b.OWNER_NO,b.APARTMENT_SID,c.APARTMENT_NAME
from Home_OwnerLog a
left join HOME_OWNER b on a.OWNER_SID=b.OWNER_SID
left join HOME_APARTMENT c on b.APARTMENT_SID =c.APARTMENT_SID
where a.SYSTEM_TYPE = 0--0悦园区,1悦服务
and b.OWNER_TYPE =1--1业主
and a.created_on >='20170323'
and b.OWNER_NAME in('王一艳'))t1
--and a.CONTENT not like '%进入主界面%'--剔除只进入首页的，跳出率=只进入首页的次数/总访问次数
order by t1.CREATED_ON desc





--巡检单数
--事业三部的数据需园区后台+悦嘉家后台两个数据汇总
select t1.小区名称,t1.服务类型, count(t1.SERVICE_SID) 提报单数 from
(
select a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME ,a.SERVICE_NO,a.SERVICE_DESC,
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
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,a.TYPE_NAME 服务内容,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where APARTMENT_NAME NOT IN('幸福家园')
and a.CREATED_ON >='20170201'
and a.CREATED_ON <'20170301'
and b.CATEGORY_NAME  in('巡检')
--and day(a.CREATED_ON)!=16
--and day(a.CREATED_ON)!=17
--and day(a.CREATED_ON)!=18
--and day(a.CREATED_ON)!=19
)t1
group by t1.小区名称,t1.服务类型
order by count(t1.SERVICE_SID) desc
--order by c.APARTMENT_NAME ,a.CREATED_ON desc
-- HOME_SERVICE_CATEGORY :CATEGORY_SID,CATEGORY_NAME
--HOME_SERVICE_PRO :CATEGORY_SID





--部分事业三部巡检数据在园区数据库中跑
--巡检提报ID数（事业三部）
--事业三部的数据需园区后台+悦嘉家后台两个数据汇总(_浙大科技园、阿里巴巴滨江园区)
select t1.小区名称,count(distinct(t1.OWNER_SID)) 提报ID数 from
(
select d.OWNER_SID,a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,
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
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_OWNER d on d.OWNER_SID = a.CREATEDBY
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where APARTMENT_NAME NOT IN('幸福家园')
and a.CREATED_ON >='20170101'
and a.CREATED_ON <'20170201'
and b.CATEGORY_NAME  in('巡检')
and day(a.CREATED_ON)!=16
and day(a.CREATED_ON)!=17
and day(a.CREATED_ON)!=18
and day(a.CREATED_ON)!=19
)t1
group by t1.小区名称
order by count(distinct(t1.OWNER_SID))desc
-- HOME_SERVICE_CATEGORY :CATEGORY_SID,CATEGORY_NAME
--HOME_SERVICE_PRO :CATEGORY_SID



--浙大科技园、阿里巴巴滨江园区两个项目取详细表去重求巡检提报ID数(悦园区+悦嘉家后台)
--事业三部的数据需园区后台+悦嘉家后台两个数据汇总
select distinct(t1.OWNER_SID) 用户ID,t1.小区名称
 from(
select d.OWNER_SID,a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,
a.CREATED_ON 呼叫时间,
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_OWNER d on d.OWNER_SID = a.CREATEDBY
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where a.CREATED_ON >='20170201'
and APARTMENT_NAME  IN('浙大科技园','阿里巴巴滨江园区')--这两个项目取详细表与园区版后台去重
and a.CREATED_ON <'20170301'
and b.CATEGORY_NAME  in('巡检')
--and day(a.CREATED_ON)!=16--2017年1月份剔除16-19日数据，这四天派单系统不能及时派单
--and day(a.CREATED_ON)!=17
--and day(a.CREATED_ON)!=18
--and day(a.CREATED_ON)!=19
)t1



--截止20170101-20170116小区巡检工作人员提报单数、处理单数（待匹配至工作人员表中）
--事业三部的数据需园区后台+悦嘉家后台两个数据汇总
select t2.小区名称,t2.OWNER_NAME 姓名,t2.OWNER_PHONE 手机号码,t2.服务类型,count(t2.呼叫时间) 提报单数,count(t2.处理时间) 处理单数 from (
select t1.小区名称,t1.OWNER_NAME,t1.OWNER_PHONE,t1.服务类型,t1.TYPE_NAME,t1.SERVICE_DESC,t1.呼叫时间,t1.处理时间,t1.rn from(
select a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,d.OWNER_NAME,d.OWNER_PHONE,
ROW_NUMBER() over (partition by c.APARTMENT_NAME,b.CATEGORY_NAME ,d.OWNER_NAME order by c.APARTMENT_NAME,b.CATEGORY_NAME ) as rn,
a.CREATED_ON 呼叫时间,b.CATEGORY_NAME 服务类型,
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY 服务类型ID,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_OWNER d on a.CREATEDBY = d.OWNER_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where  b.CATEGORY_NAME like('%巡检%')
and a.CREATED_ON >='20170201'
and a.CREATED_ON <'20170301')t1)t2
group by t2.小区名称,t2.OWNER_NAME,t2.OWNER_PHONE,t2.服务类型



--事业三部权限表提取
--匹配工作人员提报巡检及响应情况时需要
select t1.是否启用,t1.用户类型,t1.事业部,t1.区域,t1.地区类型,t1.园区项目,t1.岗位,t1.事业部+'-'+t1.区域+'-'+t1.地区类型+'-'+t1.园区项目+'-'+t1.岗位 事业部汇总
,t1.排序号,t1.房号,t1.姓名,t1.手机号码
 from(
  select  a.OWNER_SID,a.OWNER_NO 房号,a.OWNER_NAME 姓名,a.OWNER_PHONE 手机号码,a.OWNER_STATUS 是否启用,
a.GROUP_SID,a. DEPT_SID,a.OWNER_TYPE 用户类型
  ,b.DEPT_NAME 岗位,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX 排序号,c.DEPT_NAME 园区项目,e.DEPT_NAME 地区类型,
f.DEPT_NAME 区域,g.DEPT_NAME 事业部,d.GROUP_NAME 集团名称
  from HOME_OWNER a
  left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID
left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID
left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
--where  a.OWNER_SID in('0000c52a-8688-46ef-a4b2-4f90a449cedd')
where g.DEPT_NAME in('事业三部')
and a.OWNER_STATUS=1)t1





--20170217之前的数据计算响应及时率用此代码（新算法）
--工作时间段（8:30-18:00）提报的单子，15min内响应为及时响应；
--其他时间段提报的单子，9点之前响应为及时响应。
select t7.小区名称,sum(t7.及时单数) 响应及时 from
(select t1.小区名称,count(t1.SERVICE_SID) 及时单数 from
(
select t2.* from(
select a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,
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
               END) AS 服务提报状态,a.CREATED_ON 呼叫时间,a.CREATED_ON+1 处理不超时,dateadd(minute,+15,a.CREATED_ON) 响应不超时,
d.CREATED_ON 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_SERVICE_HIST d on d.SERVICE_SID=a.SERVICE_SID
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where APARTMENT_NAME NOT IN('幸福家园')
and d.HIST_TYPE = 2
--and e.OWNER_NO like ('%一期%')
and b.CATEGORY_NAME in('巡检')
and a.CREATED_ON >='20170227'--当月提报
and a.CREATED_ON <'20170306'
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
--and day(a.CREATED_ON)!=16
--and day(a.CREATED_ON)!=17
--and day(a.CREATED_ON)!=18
--and day(a.CREATED_ON)!=19
and convert(char(8),a.CREATED_ON,108)>='08:30:00' and  convert(char(8),a.CREATED_ON,108)<'18:00:00'--1
)t2
where t2.响应时间 <= t2.响应不超时
--where t2.处理时间 >= t2.处理不超时
)t1
group by t1.小区名称
union all
select t3.小区名称,count(t3.SERVICE_SID) 及时单数 from
(
select t4.* from(
select a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,
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
               END) AS 服务提报状态,a.CREATED_ON 呼叫时间,a.CREATED_ON+1 处理不超时,dateadd(minute,+15,a.CREATED_ON) 响应不超时,
d.CREATED_ON 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_SERVICE_HIST d on d.SERVICE_SID=a.SERVICE_SID
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where APARTMENT_NAME NOT IN('幸福家园')
and d.HIST_TYPE = 2
and b.CATEGORY_NAME in('巡检')
--and e.OWNER_NO like ('%一期%')
and a.CREATED_ON >='20170227'--当月提报
and a.CREATED_ON <'20170306'
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
--and day(a.CREATED_ON)!=16
--and day(a.CREATED_ON)!=17
--and day(a.CREATED_ON)!=18
--and day(a.CREATED_ON)!=19
and convert(char(8),a.CREATED_ON,108)>='18:00:00' and  convert(char(8),a.CREATED_ON,108)<'24:00:00'--11
)t4
where CAST(t4.响应时间 AS DATE)= CAST(t4.呼叫时间 AS DATE)
or(CAST(t4.响应时间 AS DATE)= CAST(t4.呼叫时间+1 AS DATE) and convert(char(8),t4.响应时间,108)<'09:00:00' )--11)
)t3
group by t3.小区名称
union ALL
select t5.小区名称,count(t5.SERVICE_SID) 及时单数 from
(
select t6.* from(
select a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,
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
               END) AS 服务提报状态,a.CREATED_ON 呼叫时间,a.CREATED_ON+1 处理不超时,dateadd(minute,+15,a.CREATED_ON) 响应不超时,
d.CREATED_ON 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_SERVICE_HIST d on d.SERVICE_SID=a.SERVICE_SID
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where APARTMENT_NAME NOT IN('幸福家园')
and d.HIST_TYPE = 2
--and e.OWNER_NO like ('%一期%')
and a.CREATED_ON >='20170227'--当月提报
and a.CREATED_ON <'20170306'
and b.CATEGORY_NAME in('巡检')
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
--and day(a.CREATED_ON)!=16
--and day(a.CREATED_ON)!=17
--and day(a.CREATED_ON)!=18
--and day(a.CREATED_ON)!=19
and convert(char(8),a.CREATED_ON,108)>='00:00:00' and  convert(char(8),a.CREATED_ON,108)<'08:30:00'--11
)t6
where convert(char(8),t6.响应时间,108)<'09:00:00' --111
and CAST(t6.响应时间 AS DATE)= CAST(t6.呼叫时间 AS DATE)--111
)t5
group by t5.小区名称)t7
group by t7.小区名称
order by sum(t7.及时单数) desc