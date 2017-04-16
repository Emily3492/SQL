-----------悦服务查询
--切换到园区之后，数据存在了HOME_PARK园区数据库中
--悦服务账号只面向工作人员,该无对应小区，但可以根据权限表中部门得到工作人员归属的项目
--悦嘉家：JOY_HOME
--悦园区:JOY_PARK
--Home_owner表下owner_type1='1'且创建时间为空的用户，时间全部刷成2015年12月12日。
--20170330以后工作人员创建时间均正常
--20170320悦服务埋点完善



--pyspark取值原始表(工作人员,部门,role_name)

select REPLACE(t3.dept,' ','-') dept_1,t3.OWNER_TAG,t3.created_on,t3.OWNER_SID,t3.OWNER_NO ,t3.OWNER_NAME ,--t3.USER_SID,t3.ROLE_NAME,t3.ROLE_SID,
t3.OWNER_PHONE ,t3.OWNER_STATUS ,t3.OWNER_TYPE
  ,t3.GROUP_NAME
from(
select t1.*,
ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' '+isnull(t1.d,'') +' '+isnull(t1.f,'')) dept
from(select a.OWNER_TAG,
a.created_on,a.OWNER_SID,a.OWNER_NO ,a.OWNER_NAME ,--k.USER_SID,m.ROLE_NAME,m.ROLE_SID,
a.OWNER_PHONE ,a.OWNER_STATUS ,a.GROUP_SID,a.DEPT_SID,a.OWNER_TYPE
,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX ,d.GROUP_NAME ,n.DEPT_NAME g,g.DEPT_NAME a,f.DEPT_NAME b,e.DEPT_NAME c,c.DEPT_NAME d,b.DEPT_NAME f
  from HOME_OWNER a
--left join HOME_USER_ROLE k on k.USER_SID=a.OWNER_SID
--left join HOME_GROUP_ROLE m on k.ROLE_SID=m.ROLE_SID
  left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID
left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID
left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID
left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
--where  a.OWNER_SID in('0000c52a-8688-46ef-a4b2-4f90a449cedd')
where  a.OWNER_TYPE in('2','3','4','5')--工作人员--工作人员
--and a.created_on >='20170226'
--and a.CREATED_ON  <='20170326'
--and m.ROLE_NAME like('%工程专员%')--or b.DEPT_NAME like('%高级经理%')
and a.OWNER_STATUS=1
)t1)t3

--剔除重复,按部门汇总时需要,按小区汇总不需要





--权限表按等级顺序分裂
IF NOT OBJECT_ID('f_GetStr') IS NULL
    DROP FUNCTION [f_GetStr]
GO
CREATE FUNCTION dbo.f_GetStr(
    @s varchar(8000),      --包含多个数据项的字符串
    @pos int,             --要获取的数据项的位置
    @split varchar(10)     --数据分隔符
)RETURNS varchar(1000)
AS
BEGIN
    IF @s IS NULL RETURN(NULL)
    DECLARE @splitlen int
    SELECT @splitlen=LEN(@split+'a')-2
    WHILE @pos>1 AND CHARINDEX(@split,@s+@split)>0
        SELECT @pos=@pos-1,
            @s=STUFF(@s,1,CHARINDEX(@split,@s+@split)+@splitlen,'')
    RETURN(ISNULL(LEFT(@s,CHARINDEX(@split,@s+@split)-1),''))
END
GO
SELECT 
    dbo.f_GetStr(t3.部门,1,'-') F1,
    dbo.f_GetStr(t3.部门,2,'-') F2,
    dbo.f_GetStr(t3.部门,3,'-') F3,
    dbo.f_GetStr(t3.部门,4,'-') F4,
    dbo.f_GetStr(t3.部门,5,'-') F5,
    dbo.f_GetStr(t3.部门,6,'-') F6
from(
select REPLACE(t2.dept,' ','-') 部门,t2.OWNER_TAG 岗位,t2.是否启用,t2.用户类型,t2.排序号,t2.房号,t2.姓名,t2.手机号码 from(
select t1.owner_SID, t1.是否启用,t1.用户类型,ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' ' +isnull(t1.d,'') +' '+isnull(t1.f,'')) dept
,t1.排序号,t1.房号,t1.姓名,t1.手机号码,t1.rn,t1.OWNER_TAG
 from(select ROW_NUMBER() over (partition by a.OWNER_SID order by a.OWNER_SID desc ) as rn,
a.OWNER_SID,a.OWNER_NO 房号,a.OWNER_NAME 姓名,a.OWNER_PHONE 手机号码,
(CASE a.OWNER_STATUS
                 WHEN '0' THEN
                  '否'
                 WHEN '1' THEN
                  '是'
                 ELSE
                  ''
               END) AS 是否启用,
a.GROUP_SID,a.DEPT_SID,
(CASE a.OWNER_TYPE
                 WHEN '1' THEN
                  '小区用户'
                 WHEN '2' THEN
                  '小区服务人员'
                 WHEN '3' THEN
                  '集团服务人员'
                 WHEN '4' THEN
                  '小区管理员'
                 WHEN '5' THEN
                  '集团管理员'
                 ELSE
                  ''
               END) AS 用户类型,n.DEPT_NAME g
  ,b.DEPT_NAME f,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX 排序号,c.DEPT_NAME d,e.DEPT_NAME c,
f.DEPT_NAME b,g.DEPT_NAME a,d.GROUP_NAME 集团名称,a.OWNER_TAG
  from HOME_OWNER a
  left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID
left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID
left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID
left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
--where  a.OWNER_SID in('0000c52a-8688-46ef-a4b2-4f90a449cedd')
where  a.OWNER_TYPE in('2','3','4','5')--工作人员
and a.OWNER_STATUS=1
)t1)t2)t3
--order by t3.部门 
--where t3.部门 like ('%事业三部%') 




----事业三部悦服务账户数
----园区巡检汇总需要
select t4.F5 项目,count(distinct(t4.owner_SID)) 悦服务账户数 from (
SELECT 
    dbo.f_GetStr(t3.部门,1,'-') F1,
    dbo.f_GetStr(t3.部门,2,'-') F2,
    dbo.f_GetStr(t3.部门,3,'-') F3,
    dbo.f_GetStr(t3.部门,4,'-') F4,
    dbo.f_GetStr(t3.部门,5,'-') F5,
    dbo.f_GetStr(t3.部门,6,'-') F6,*
from(
select REPLACE(t2.dept,' ','-') 部门,t2.OWNER_TAG 岗位,t2.是否启用,t2.用户类型,t2.排序号,t2.房号,t2.姓名,t2.手机号码 ,t2.owner_SID from(
select t1.owner_SID, t1.是否启用,t1.用户类型,ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' ' +isnull(t1.d,'') +' '+isnull(t1.f,'')) dept
,t1.排序号,t1.房号,t1.姓名,t1.手机号码,t1.rn,t1.OWNER_TAG
 from(select ROW_NUMBER() over (partition by a.OWNER_SID order by a.OWNER_SID desc ) as rn,
a.OWNER_SID,a.OWNER_NO 房号,a.OWNER_NAME 姓名,a.OWNER_PHONE 手机号码,
(CASE a.OWNER_STATUS
                 WHEN '0' THEN
                  '否'
                 WHEN '1' THEN
                  '是'
                 ELSE
                  ''
               END) AS 是否启用,
a.GROUP_SID,a.DEPT_SID,
(CASE a.OWNER_TYPE
                 WHEN '1' THEN
                  '小区用户'
                 WHEN '2' THEN
                  '小区服务人员'
                 WHEN '3' THEN
                  '集团服务人员'
                 WHEN '4' THEN
                  '小区管理员'
                 WHEN '5' THEN
                  '集团管理员'
                 ELSE
                  ''
               END) AS 用户类型,n.DEPT_NAME g
  ,b.DEPT_NAME f,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX 排序号,c.DEPT_NAME d,e.DEPT_NAME c,
f.DEPT_NAME b,g.DEPT_NAME a,d.GROUP_NAME 集团名称,a.OWNER_TAG
  from HOME_OWNER a
  left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID
left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID
left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID
left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
--where  a.OWNER_SID in('0000c52a-8688-46ef-a4b2-4f90a449cedd')
where  a.OWNER_TYPE in('2','3','4','5')--工作人员
and a.OWNER_STATUS=1
and a.created_on<'20170405'
)t1)t2)t3
--order by t3.部门 
where t3.部门 like ('%事业三部%'))t4 group by t4.F5
order by count(distinct(t4.owner_SID)) desc




--权限表提取
--匹配悦服务工作人员所在部门的时候需要
--Home_owner表下owner_type1='1'且创建时间为空的用户，时间全部刷成2015年12月12日。
--20170330以后工作人员创建时间均正常

--拥有权限的小区 t1.apartment_name
select * from(
select REPLACE(t2.dept,' ','-') 部门,t2.owner_SID,t2.是否启用,t2.用户类型,t2.排序号,t2.房号,t2.姓名,t2.手机号码 from(
select t1.owner_SID,t1.apartment_name, t1.是否启用,t1.用户类型,ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' ' +isnull(t1.d,'') +' '+isnull(t1.f,'')) dept
,t1.排序号,t1.房号,t1.姓名,t1.手机号码,t1.rn
 from(select ROW_NUMBER() over (partition by a.OWNER_SID order by a.OWNER_SID desc ) as rn,
a.OWNER_SID,a.OWNER_NO 房号,a.OWNER_NAME 姓名,a.OWNER_PHONE 手机号码,a.OWNER_STATUS 是否启用,
a.GROUP_SID,a.DEPT_SID,a.OWNER_TYPE 用户类型,j.apartment_name
  ,b.DEPT_NAME f,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX 排序号,n.DEPT_NAME g,c.DEPT_NAME d,e.DEPT_NAME c,
f.DEPT_NAME b,g.DEPT_NAME a,d.GROUP_NAME 集团名称
  from HOME_OWNER a
    left join HOME_GROUP_USER_APARTMENT i on i.USER_SID=a.OWNER_SID
  left join HOME_APARTMENT j on i.APARTMENT_SID=j.APARTMENT_SID
  left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID
left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID
left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID
--where  a.OWNER_SID in('0000c52a-8688-46ef-a4b2-4f90a449cedd')
where a.OWNER_TYPE not LIKE ('%1%')--工作人员
and a.OWNER_STATUS=1)t1
--where t1.rn=1--拥有多个小区权限，只取第一条(按ID去重)
)t2)t3
--where t3.部门 like('%事业三部%')




--悦服务用户无对应小区,根据拥有的小区汇总,存在HOME_GROUP_USER_APARTMENT 表中
--悦嘉家：JOY_HOME
--悦园区:JOY_PARK
select * from HOME_OWNER  b  
  left join HOME_GROUP_USER_APARTMENT i on i.USER_SID=a.OWNER_SID
  left join HOME_APARTMENT j on i.APARTMENT_SID=j.APARTMENT_SID
where  OWNER_TYPE not LIKE ('%1%')--工作人员



--测试日志生成
select  top 10 t1.* from(select a.CREATED_ON,a.CONTENT,b.OWNER_NAME ,a.OWNER_SID,b.OWNER_NO--,j.APARTMENT_SID,j.APARTMENT_NAME
from Home_OwnerLog a
left join HOME_OWNER b on a.OWNER_SID=b.OWNER_SID
  left join HOME_GROUP_USER_APARTMENT i on i.USER_SID=a.OWNER_SID
 -- left join HOME_APARTMENT j on i.APARTMENT_SID=j.APARTMENT_SID
where a.SYSTEM_TYPE = 1--0悦嘉家\悦园区,1悦服务
and b.OWNER_TYPE != 1--1业主
and a.created_on >='20170323'
and b.OWNER_NAME in('王一艳'))t1
--and a.CONTENT not like '%进入主界面%'--剔除只进入首页的，跳出率=只进入首页的次数/总访问次数
order by t1.CREATED_ON desc



--不需要,按拥有小区汇总是有重复数据的
--按拥有权限小区汇总的用户数，独立访客ID数，点击次数
--一个人有多个小区的权限,算作多个用户(按权限小区汇总,几次权限算几次)
--目前悦园区尚未推广,只需从joy_home中取值，不管joy_park
--剔除体验小区、幸福家园

select t22.拥有小区,t22.悦服务用户数,t11.独立访客ID数,t11.点击次数,
round(cast(t11.独立访客ID数 as float)/cast(t22.悦服务用户数 as float),2) 活跃度, 
round(cast(t11.点击次数 as float)/cast(t22.悦服务用户数 as float),2) 人均点击次数
 from(
select t2.拥有小区,count(t2.owner_SID) 悦服务用户数 from(
select * from(
select REPLACE(t3.dept,' ','-') 部门,* from(
select t1.created_on,
t1.owner_SID,t1.拥有小区, t1.是否启用,t1.用户类型,
ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' ' +isnull(t1.d,'') +' '+isnull(t1.f,'')) dept
,t1.排序号,t1.房号,t1.姓名,t1.手机号码,t1.rn
from(select ROW_NUMBER() over (partition by a.OWNER_SID order by a.OWNER_SID desc ) as rn,
j.APARTMENT_NAME 拥有小区,a.created_on,a.OWNER_SID,a.OWNER_NO 房号,a.OWNER_NAME 姓名,
a.OWNER_PHONE 手机号码,a.OWNER_STATUS 是否启用,a.GROUP_SID,a.DEPT_SID,a.OWNER_TYPE 用户类型
,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX 排序号,d.GROUP_NAME 集团名称,n.DEPT_NAME g,
b.DEPT_NAME f,c.DEPT_NAME d,e.DEPT_NAME c,f.DEPT_NAME b,g.DEPT_NAME a
  from HOME_OWNER a
  left join HOME_GROUP_USER_APARTMENT i on i.USER_SID=a.OWNER_SID
  left join HOME_APARTMENT j on i.APARTMENT_SID=j.APARTMENT_SID
  left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID
left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID
left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID
left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
--where  a.OWNER_SID in('0000c52a-8688-46ef-a4b2-4f90a449cedd')
where a.OWNER_TYPE not LIKE ('%1%')--工作人员
--and a.created_on >='20170226'
and a.CREATED_ON  <='20170326'
and a.OWNER_STATUS=1)t1 where t1.拥有小区 not in ('幸福家园','体验小区')
 where t1.rn=1--剔除重复,按部门汇总时需要,按小区汇总不需要
)t3)t4
--where t4.部门 like('%事业三部%')
)t2
group by t2.拥有小区
)t22
left join 
(select t2.APARTMENT_NAME 拥有小区,count(distinct(t2.OWNER_SID)) 独立访客ID数 ,count(t2.OWNER_SID) 点击次数 from(
select * from(
select REPLACE(t3.dept,' ','-') 部门,* from(
select ROW_NUMBER() over (partition by t1.APARTMENT_NAME order by t1.rn desc) as rnn,
 t1.SYSTEM_TYPE,t1.CREATED_ON,t1.CONTENT,t1.OWNER_SID,t1.apartment_name,t1.rn,
t1.OWNER_STATUS,t1.OWNER_TYPE,t1.g,t1.a,t1.b,t1.c,t1.d,t1.f,
ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' ' +isnull(t1.d,'') +' '+isnull(t1.f,'')) dept,
t1.SORT_INDEX,t1.OWNER_NO,t1.OWNER_NAME,t1.OWNER_PHONE
from(select ROW_NUMBER() over (partition by j.APARTMENT_NAME,h.OWNER_SID order by j.APARTMENT_NAME,h.OWNER_SID desc) as rn,
h.SYSTEM_TYPE,h.CREATED_ON,j.APARTMENT_NAME,
a.OWNER_PHONE,a.OWNER_STATUS,a.GROUP_SID,a.DEPT_SID,a.OWNER_TYPE,
h.CONTENT,h.OWNER_SID,a.OWNER_NO,a.OWNER_NAME,n.DEPT_NAME g,
b.DEPT_NAME f,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX,c.DEPT_NAME d,
e.DEPT_NAME c,f.DEPT_NAME b,g.DEPT_NAME a,d.GROUP_NAME 
from Home_OwnerLog h
left join HOME_OWNER a on h.OWNER_SID=a.OWNER_SID
  left join HOME_GROUP_USER_APARTMENT i on i.USER_SID=a.OWNER_SID
  left join HOME_APARTMENT j on i.APARTMENT_SID=j.APARTMENT_SID
left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID 
left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID
left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID
 left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
where a.OWNER_TYPE not LIKE ('%1%')--工作人员
and h.CREATED_ON>='20170226'
and h.CREATED_ON<'20170326'
and a.OWNER_STATUS=1
and h.SYSTEM_TYPE = 1--0悦嘉家\悦园区,1悦服务
and  a.OWNER_TYPE in('2','3','4','5')--工作人员
)t1 
)t3)t4
--where t4.部门 like('%事业三部%')
)t2 
group by t2.APARTMENT_NAME 
where t2.APARTMENT_NAME  not in ('幸福家园','体验小区')
)as t11
on t22.拥有小区=t11.拥有小区
order by round(cast(t11.独立访客ID数 as float)/cast(t22.悦服务用户数 as float),2) desc





--按详细部门汇总的用户数，独立访客ID数，点击次数
--去重

select t22.部门,t22.悦服务用户数,t11.独立访客ID数,t11.点击次数,
round(cast(t11.独立访客ID数 as float)/cast(t22.悦服务用户数 as float),2) 活跃度, 
round(cast(t11.点击次数 as float)/cast(t22.悦服务用户数 as float),2) 人均点击次数
from(
select t2.部门,count(t2.owner_SID) 悦服务用户数 from(
select * from(
select REPLACE(t3.dept,' ','-') 部门,* from(
select t1.created_on,
t1.owner_SID,t1.是否启用,t1.用户类型,
ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' ' +isnull(t1.d,'') +' '+isnull(t1.f,'')) dept
,t1.排序号,t1.房号,t1.姓名,t1.手机号码,t1.rn
from(select ROW_NUMBER() over (partition by a.OWNER_SID order by a.OWNER_SID desc ) as rn,
a.created_on,a.OWNER_SID,a.OWNER_NO 房号,a.OWNER_NAME 姓名,
a.OWNER_PHONE 手机号码,a.OWNER_STATUS 是否启用,a.GROUP_SID,a.DEPT_SID,a.OWNER_TYPE 用户类型
,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX 排序号,d.GROUP_NAME 集团名称,n.DEPT_NAME g,
b.DEPT_NAME f,c.DEPT_NAME d,e.DEPT_NAME c,f.DEPT_NAME b,g.DEPT_NAME a
  from HOME_OWNER a
  left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID
left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID
left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID
left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
--where  a.OWNER_SID in('0000c52a-8688-46ef-a4b2-4f90a449cedd')
where a.OWNER_TYPE not LIKE ('%1%')--工作人员
--and a.created_on >='20170226'
and a.CREATED_ON  <='20170326'
and a.OWNER_STATUS=1)t1
 where t1.rn=1--剔除重复,按部门汇总时需要,按小区汇总不需要
)t3)t4
--where t4.部门 like('%事业三部%')
)t2
group by t2.部门
)t22
left join 
(select t2.部门,count(distinct(t2.OWNER_SID)) 独立访客ID数 ,count(t2.OWNER_SID) 点击次数
from(
select * from(
select REPLACE(t4.dept,' ','-') 部门,* from(
select 
 t1.SYSTEM_TYPE,t1.CREATED_ON,t1.CONTENT,t1.OWNER_SID,t1.rn,
t1.OWNER_STATUS,t1.OWNER_TYPE,t1.a,t1.b,t1.c,t1.d,t1.f,
ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' ' +isnull(t1.d,'') +' '+isnull(t1.f,'')) dept,
t1.SORT_INDEX,t1.OWNER_NO,t1.OWNER_NAME,t1.OWNER_PHONE
from(select ROW_NUMBER() over (partition by h.OWNER_SID order by h.OWNER_SID desc) as rn,
h.SYSTEM_TYPE,h.CREATED_ON,
a.OWNER_PHONE,a.OWNER_STATUS,a.GROUP_SID,a.DEPT_SID,a.OWNER_TYPE,
h.CONTENT,h.OWNER_SID,a.OWNER_NO,a.OWNER_NAME,n.DEPT_NAME g,
b.DEPT_NAME f,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX,c.DEPT_NAME d,
e.DEPT_NAME c,f.DEPT_NAME b,g.DEPT_NAME a,d.GROUP_NAME 
from Home_OwnerLog h
left join HOME_OWNER a on h.OWNER_SID=a.OWNER_SID
left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID
 left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID 
left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID
left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
where a.OWNER_TYPE not LIKE ('%1%')--工作人员
and h.CREATED_ON>='20170101'
and h.CREATED_ON<'20170226'
and a.OWNER_STATUS=1
and h.SYSTEM_TYPE = 1--0悦嘉家\悦园区,1悦服务
and  a.OWNER_TYPE in('2','3','4','5')--工作人员
)t1
--where t1.rn=1
)t4)t5
--where t5.部门 like('%事业三部%')
)t2 
group by t2.部门)as t11
on t22.部门=t11.部门
order by round(cast(t11.独立访客ID数 as float)/cast(t22.悦服务用户数 as float),2) desc






--按详细部门汇总的独立访客ID数及点击次数

select  t2.部门,count(distinct(t2.OWNER_SID)) 独立访客ID数 ,count(t2.OWNER_SID) 点击次数
from(
select * from(
select REPLACE(t4.dept,' ','-') 部门,* from(
select 
 t1.SYSTEM_TYPE,t1.CREATED_ON,t1.CONTENT,t1.OWNER_SID,t1.apartment_name,t1.rn,
t1.OWNER_STATUS,t1.OWNER_TYPE,t1.a,t1.b,t1.c,t1.d,t1.f,
ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' ' +isnull(t1.d,'') +' '+isnull(t1.f,'')) dept,
t1.SORT_INDEX,t1.OWNER_NO,t1.OWNER_NAME,t1.OWNER_PHONE
from(select ROW_NUMBER() over (partition by h.OWNER_SID order by h.OWNER_SID desc) as rn,
h.SYSTEM_TYPE,h.CREATED_ON,
a.OWNER_PHONE,a.OWNER_STATUS,a.GROUP_SID,a.DEPT_SID,a.OWNER_TYPE,
h.CONTENT,h.OWNER_SID,a.OWNER_NO,a.OWNER_NAME,n.DEPT_NAME g,
b.DEPT_NAME f,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX,c.DEPT_NAME d,
e.DEPT_NAME c,f.DEPT_NAME b,g.DEPT_NAME a,d.GROUP_NAME 
from Home_OwnerLog h
left join HOME_OWNER a on h.OWNER_SID=a.OWNER_SID
left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID 
left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID 
left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID
left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
where a.OWNER_TYPE not LIKE ('%1%')--工作人员
and h.CREATED_ON>='20170101'
and h.CREATED_ON<'20170326'
and a.OWNER_STATUS=1
and h.SYSTEM_TYPE = 1--0悦嘉家\悦园区,1悦服务
and  a.OWNER_TYPE in('2','3','4','5')--工作人员
)t1
where t1.rn=1
)t4)t5
--where t5.部门 like('%事业三部%')
)t2 
group by t2.部门
order by count(distinct(t2.OWNER_SID)) desc 





--悦服务按拥有小区汇总的工作人员ID数

--目前只统计悦嘉家对应的，所以不需要去JOY_PARK取值
--根据拥有的项目(权限)进行分组汇总，一个用户拥有多个项目，在各个项目均算一次。
--悦服务要去悦园区和悦嘉家两个数据库中看
--JOY_HOME
--JOY_PARK
--Home_owner表下owner_type1='1'且创建时间为空的用户，时间全部刷成2015年12月12日。
--20170330以后工作人员创建时间均正常
--注意：where t1.rn=1--剔除重复,按部门汇总时需要,按小区汇总不需要

select t2.拥有小区,count(t2.owner_SID) 悦服务用户数 from(
select * from(
select REPLACE(t3.dept,' ','-') 部门,* from(
select t1.created_on,
t1.owner_SID,t1.拥有小区, t1.是否启用,t1.用户类型,
ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' ' +isnull(t1.d,'') +' '+isnull(t1.f,'')) dept
,t1.排序号,t1.房号,t1.姓名,t1.手机号码,t1.rn
from(select ROW_NUMBER() over (partition by a.OWNER_SID order by a.OWNER_SID desc ) as rn,
j.APARTMENT_NAME 拥有小区,a.created_on,a.OWNER_SID,a.OWNER_NO 房号,a.OWNER_NAME 姓名,
a.OWNER_PHONE 手机号码,a.OWNER_STATUS 是否启用,a.GROUP_SID,a.DEPT_SID,a.OWNER_TYPE 用户类型
,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX 排序号,d.GROUP_NAME 集团名称,n.DEPT_NAME g,
b.DEPT_NAME f,c.DEPT_NAME d,e.DEPT_NAME c,f.DEPT_NAME b,g.DEPT_NAME a
  from HOME_OWNER a
  left join HOME_GROUP_USER_APARTMENT i on i.USER_SID=a.OWNER_SID
  left join HOME_APARTMENT j on i.APARTMENT_SID=j.APARTMENT_SID
  left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID
left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID
left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID
left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
--where  a.OWNER_SID in('0000c52a-8688-46ef-a4b2-4f90a449cedd')
where a.OWNER_TYPE not LIKE ('%1%')--工作人员
--and a.created_on >='20170226'
and a.CREATED_ON  <='20170326'
and a.OWNER_STATUS=1)t1
-- where t1.rn=1--剔除重复,按部门汇总时需要,按小区汇总不需要
)t3)t4
--where t4.部门 like('%事业三部%')
)t2
group by t2.拥有小区
order by count(t2.owner_SID) desc




--按部门汇总的悦服务用户ID数
select t2.部门,count(t2.owner_SID) 悦服务用户数 from(
select * from(
select REPLACE(t3.dept,' ','-') 部门,* from(
select t1.created_on,
t1.owner_SID,t1.拥有小区, t1.是否启用,t1.用户类型,
ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' ' +isnull(t1.d,'') +' '+isnull(t1.f,'')) dept
,t1.排序号,t1.房号,t1.姓名,t1.手机号码,t1.rn
from(select ROW_NUMBER() over (partition by a.OWNER_SID order by a.OWNER_SID desc ) as rn,
a.created_on,a.OWNER_SID,a.OWNER_NO 房号,a.OWNER_NAME 姓名,
a.OWNER_PHONE 手机号码,a.OWNER_STATUS 是否启用,a.GROUP_SID,a.DEPT_SID,a.OWNER_TYPE 用户类型
,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX 排序号,d.GROUP_NAME 集团名称,n.DEPT_NAME g,
b.DEPT_NAME f,c.DEPT_NAME d,e.DEPT_NAME c,f.DEPT_NAME b,g.DEPT_NAME a
  from HOME_OWNER a
  left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID
left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID
left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID
left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
--where  a.OWNER_SID in('0000c52a-8688-46ef-a4b2-4f90a449cedd')
where a.OWNER_TYPE not LIKE ('%1%')--工作人员
--and a.created_on >='20170226'
and a.CREATED_ON  <='20170326'
and a.OWNER_STATUS=1)t1
 where t1.rn=1--剔除重复,按部门汇总时需要,按小区汇总不需要
)t3)t4
--where t4.部门 like('%事业三部%')
)t2
group by t2.部门
order by count(t2.owner_SID) desc





---对应到个人的悦服务点击次数
--按照用户表左连接

select t5.部门,t5.岗位,t5.排序号,t5.房号,t5.姓名,t5.手机号码,t5.是否启用,t5.用户类型,t6.点击次数 from
(select * from(
select REPLACE(t3.dept,' ','-') 部门,* from(
select t1.created_on,t1.OWNER_TAG 岗位,
t1.owner_SID,t1.拥有小区, t1.是否启用,t1.用户类型,
ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' ' +isnull(t1.d,'') +' '+isnull(t1.f,'')) dept
,t1.排序号,t1.房号,t1.姓名,t1.手机号码,t1.rn
from(select ROW_NUMBER() over (partition by a.OWNER_SID order by a.OWNER_SID desc ) as rn,
a.created_on,a.OWNER_SID,a.OWNER_NO 房号,a.OWNER_NAME 姓名,a.OWNER_TAG ,
a.OWNER_PHONE 手机号码,a.OWNER_STATUS 是否启用,a.GROUP_SID,a.DEPT_SID,
(CASE a.OWNER_TYPE
                 WHEN '2' THEN
                  '小区服务人员'
                 WHEN '3' THEN
                  '集团服务人员'
                 WHEN '4' THEN
                  '小区管理员'
                 WHEN '5' THEN
                  '集团管理员'
                 ELSE
                  ''
               END) AS 用户类型
-- 1 - 小区用户   2 - 小区服务人员  3 - 集团服务人员   4 - 小区管理员   5 - 集团管理员
,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX 排序号,d.GROUP_NAME 集团名称,n.DEPT_NAME g,
b.DEPT_NAME f,c.DEPT_NAME d,e.DEPT_NAME c,f.DEPT_NAME b,g.DEPT_NAME a
  from HOME_OWNER a
  left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID
left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID
left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID
left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
--where  a.OWNER_SID in('0000c52a-8688-46ef-a4b2-4f90a449cedd')
where a.OWNER_TYPE not LIKE ('%1%')--工作人员
--and a.created_on >='20170226'
and a.CREATED_ON  <='20170326'
and a.OWNER_STATUS=1)t1
 where t1.rn=1--剔除重复,按部门汇总时需要,按小区汇总不需要
)t3)t4
--where t4.部门 like('%事业三部%')
)t5
left JOIN
(select REPLACE(dept,' ','-') 部门,t2.OWNER_SID,t2.OWNER_NAME 用户名,t2.OWNER_PHONE 手机号码,t2.OWNER_STATUS 用户状态,t2.OWNER_TYPE 用户类型,t2.rn 点击次数 from(
select  ROW_NUMBER() over (partition by t1.OWNER_SID order by t1.rn desc) as rnn,
t1.log_created,t1.SYSTEM_TYPE,t1.CREATED_ON,t1.CONTENT,t1.OWNER_SID,t1.apartment_name,t1.rn,
t1.OWNER_STATUS,t1.OWNER_TYPE,t1.a,t1.b,t1.c,t1.d,t1.f,ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' ' +isnull(t1.d,'') +' '+isnull(t1.f,'')) dept,
t1.SORT_INDEX,t1.OWNER_NO,t1.OWNER_NAME,t1.OWNER_PHONE
from(
select ROW_NUMBER() over (partition by h.OWNER_SID order by h.OWNER_SID desc) as rn,
a.created_on,h.SYSTEM_TYPE,h.CREATED_ON log_created,
h.CONTENT,h.OWNER_SID,a.OWNER_NO,a.OWNER_NAME,
a.OWNER_PHONE,a.OWNER_STATUS,a.GROUP_SID,a.DEPT_SID,a.OWNER_TYPE,n.DEPT_NAME g,
b.DEPT_NAME f,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX,c.DEPT_NAME d,
e.DEPT_NAME c,f.DEPT_NAME b,g.DEPT_NAME a,d.GROUP_NAME from Home_OwnerLog h
left join HOME_OWNER a on h.OWNER_SID=a.OWNER_SID 
left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID 
left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID 
left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID
left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
where a.OWNER_TYPE not LIKE ('%1%')--工作人员
--and g.DEPT_NAME in('事业三部')
and a.OWNER_STATUS=1
and h.SYSTEM_TYPE = 1--0悦嘉家\悦园区,1悦服务
and  a.OWNER_TYPE in('2','3','4','5')--工作人员
and h.created_on >='20170101'
and h.created_on <'20170226'
--and a.OWNER_NAME in('王一艳')
)t1
)t2
)t6 on t5.owner_SID=t6.owner_SID
order by t6.点击次数 desc




--按部门岗位汇总以此为准
--对应到各部门层级的点击ID数、点击次数汇总
----------按层级汇总的悦服务使用情况
---------------------------------------------------------------------------
--更改F1....F6部门层级,连接a.OWNER_TAG岗位,得到各个层级下的各岗位的账户数、活跃度、点击次数、点击ID数、人均、日均点击数等

IF NOT OBJECT_ID('f_GetStr') IS NULL
    DROP FUNCTION [f_GetStr]
GO
CREATE FUNCTION dbo.f_GetStr(
    @s varchar(8000),      --包含多个数据项的字符串
    @pos int,             --要获取的数据项的位置
    @split varchar(10)     --数据分隔符
)RETURNS varchar(1000)
AS
BEGIN
    IF @s IS NULL RETURN(NULL)
    DECLARE @splitlen int
    SELECT @splitlen=LEN(@split+'a')-2
    WHILE @pos>1 AND CHARINDEX(@split,@s+@split)>0
        SELECT @pos=@pos-1,
            @s=STUFF(@s,1,CHARINDEX(@split,@s+@split)+@splitlen,'')
    RETURN(ISNULL(LEFT(@s,CHARINDEX(@split,@s+@split)-1),''))
END
GO
----上面是函数创建，创建后直接跑下面的导出数据，否则加上函数代码导不出数据
--下面代码为按区聚合
select t9.部门,t9.部门1,t9.OWNER_TAG 岗位,t9.悦服务用户数,t10.独立访客ID数,round(convert(float,t10.独立访客ID数)/convert(float,t9.悦服务用户数),2) 活跃度,
t10.点击次数,t10.人均点击次数,t10.日均点击次数,t10.日人均点击次数 from 
(select REPLACE(t8.dept1,' ','-') 部门, REPLACE(t8.dept2,' ','-') 部门1,t8.OWNER_TAG ,t8.悦服务用户数  from(
select Rtrim(isnull(t3.F1,'')+' '+isnull(t3.F2,'')+' '+isnull(t3.F3,'')+' '+isnull(t3.OWNER_TAG,'')) dept1, 
Rtrim(isnull(t3.F1,'')+' '+isnull(t3.F2,'')+' '+isnull(t3.F3,'')) dept2,
t3.F1,t3.F2,t3.F3,t3.OWNER_TAG,count(t3.owner_SID) 悦服务用户数 
from( 
SELECT 
    dbo.f_GetStr(t4.部门,1,'-') F1,
    dbo.f_GetStr(t4.部门,2,'-') F2,
    dbo.f_GetStr(t4.部门,3,'-') F3,
    dbo.f_GetStr(t4.部门,4,'-') F4,
    dbo.f_GetStr(t4.部门,5,'-') F5,
    dbo.f_GetStr(t4.部门,6,'-') F6,*
from(
select REPLACE(t2.dept,' ','-') 部门,t2.OWNER_TAG ,t2.是否启用,t2.用户类型,t2.排序号,t2.房号,t2.姓名,t2.手机号码,t2.owner_SID from(
select t1.owner_SID, t1.是否启用,t1.用户类型,ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' ' +isnull(t1.d,'') +' '+isnull(t1.f,'')) dept
,t1.排序号,t1.房号,t1.姓名,t1.手机号码,t1.rn,t1.OWNER_TAG
 from(select ROW_NUMBER() over (partition by a.OWNER_SID order by a.OWNER_SID desc ) as rn,
a.OWNER_SID,a.OWNER_NO 房号,a.OWNER_NAME 姓名,a.OWNER_PHONE 手机号码,
(CASE a.OWNER_STATUS
                 WHEN '0' THEN
                  '否'
                 WHEN '1' THEN
                  '是'
                 ELSE
                  ''
               END) AS 是否启用,
a.GROUP_SID,a.DEPT_SID,
(CASE a.OWNER_TYPE
                 WHEN '1' THEN
                  '小区用户'
                 WHEN '2' THEN
                  '小区服务人员'
                 WHEN '3' THEN
                  '集团服务人员'
                 WHEN '4' THEN
                  '小区管理员'
                 WHEN '5' THEN
                  '集团管理员'
                 ELSE
                  ''
               END) AS 用户类型,n.DEPT_NAME g
  ,b.DEPT_NAME f,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX 排序号,c.DEPT_NAME d,e.DEPT_NAME c,
f.DEPT_NAME b,g.DEPT_NAME a,d.GROUP_NAME 集团名称,a.OWNER_TAG
  from HOME_OWNER a
  left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID
left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID
left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID
left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
--where  a.OWNER_SID in('0000c52a-8688-46ef-a4b2-4f90a449cedd')
where  a.OWNER_TYPE in('2','3','4','5')--工作人员
and a.OWNER_STATUS=1
and a.created_on <'20170401'
)t1)t2)t4)t3
group by t3.F1,t3.F2,t3.F3,t3.OWNER_TAG  
)t8)t9
--order by t8.悦服务用户数 desc
--where t3.部门 like ('%事业三部%') 
left join 
(select REPLACE(t7.dept1,' ','-') 部门, t7.独立访客ID数,t7.点击次数,t7.人均点击次数,t7.日均点击次数,t7.日人均点击次数
from(
select Rtrim(isnull(t5.F1,'')+' '+isnull(t5.F2,'')+' '+isnull(t5.F3,'')+' '+isnull(t5.OWNER_TAG,'')) dept1,
t5.F1,t5.F2,t5.F3,t5.OWNER_TAG,--,t5.F4,t5.F5
count(distinct(t5.OWNER_SID)) 独立访客ID数 ,count(t5.OWNER_SID) 点击次数,
round(convert(float,count(t5.OWNER_SID)/count(distinct(t5.OWNER_SID))),2 ) 人均点击次数,
round(convert(float,count(t5.OWNER_SID))/12,2)日均点击次数,
round(convert(float,count(t5.OWNER_SID)/count(distinct(t5.OWNER_SID)))/12, 2)日人均点击次数
from(
SELECT 
    dbo.f_GetStr(t6.部门,1,'-') F1,
    dbo.f_GetStr(t6.部门,2,'-') F2,
    dbo.f_GetStr(t6.部门,3,'-') F3,
    dbo.f_GetStr(t6.部门,4,'-') F4,
    dbo.f_GetStr(t6.部门,5,'-') F5,
    dbo.f_GetStr(t6.部门,6,'-') F6,* 
from(
select REPLACE(t4.dept,' ','-') 部门,* from(
select T1.OWNER_TAG,
 t1.SYSTEM_TYPE,t1.CREATED_ON,t1.CONTENT,t1.OWNER_SID,t1.rn,
t1.OWNER_STATUS,t1.OWNER_TYPE,t1.a,t1.b,t1.c,t1.d,t1.f,
ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' ' +isnull(t1.d,'') +' '+isnull(t1.f,'')) dept,
t1.SORT_INDEX,t1.OWNER_NO,t1.OWNER_NAME,t1.OWNER_PHONE
from
(select A.OWNER_TAG,ROW_NUMBER() over (partition by h.OWNER_SID order by h.OWNER_SID desc) as rn,
h.SYSTEM_TYPE,h.CREATED_ON,
a.OWNER_PHONE,a.OWNER_STATUS,a.GROUP_SID,a.DEPT_SID,a.OWNER_TYPE,
h.CONTENT,h.OWNER_SID,a.OWNER_NO,a.OWNER_NAME,n.DEPT_NAME g,
b.DEPT_NAME f,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX,c.DEPT_NAME d,
e.DEPT_NAME c,f.DEPT_NAME b,g.DEPT_NAME a,d.GROUP_NAME 
from Home_OwnerLog h
left join HOME_OWNER a on h.OWNER_SID=a.OWNER_SID
left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID
 left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID 
left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID
left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
where a.OWNER_TYPE in('2','3','4','5')--工作人员
and h.CREATED_ON>='20170320'
and h.CREATED_ON<'20170401'
and a.OWNER_STATUS=1
and h.SYSTEM_TYPE = 1--0悦嘉家\悦园区,1悦服务
)t1
--where t1.rn=1
)t4)t6)t5
--where t5.部门 like('%事业三部%')
group by t5.F1,t5.F2,t5.F3,t5.OWNER_TAG--,t5.F4,t5.F5
)t7)t10
--order by t7.独立访客ID数 desc
on t9.部门=t10.部门
order by t10.日人均点击次数 desc




—------按事业部聚合
--对应到各部门层级的点击ID数、点击次数汇总
----------按层级汇总的悦服务使用情况

select t9.部门,t9.部门1,t9.OWNER_TAG 岗位,t9.悦服务用户数,t10.独立访客ID数,round(convert(float,t10.独立访客ID数)/convert(float,t9.悦服务用户数),2) 活跃度,
t10.点击次数,t10.人均点击次数,t10.日均点击次数,t10.日人均点击次数 from 
(select REPLACE(t8.dept1,' ','-') 部门, REPLACE(t8.dept2,' ','-') 部门1,t8.OWNER_TAG ,t8.悦服务用户数  from(
select Rtrim(isnull(t3.F1,'')+' '+isnull(t3.F2,'')+' '+isnull(t3.OWNER_TAG,'')) dept1, 
Rtrim(isnull(t3.F1,'')+' '+isnull(t3.F2,'')) dept2,
t3.F1,t3.F2,t3.OWNER_TAG,count(t3.owner_SID) 悦服务用户数 
from( 
SELECT 
    dbo.f_GetStr(t4.部门,1,'-') F1,
    dbo.f_GetStr(t4.部门,2,'-') F2,
    dbo.f_GetStr(t4.部门,3,'-') F3,
    dbo.f_GetStr(t4.部门,4,'-') F4,
    dbo.f_GetStr(t4.部门,5,'-') F5,
    dbo.f_GetStr(t4.部门,6,'-') F6,*
from(
select REPLACE(t2.dept,' ','-') 部门,t2.OWNER_TAG ,t2.是否启用,t2.用户类型,t2.排序号,t2.房号,t2.姓名,t2.手机号码,t2.owner_SID from(
select t1.owner_SID, t1.是否启用,t1.用户类型,ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' ' +isnull(t1.d,'') +' '+isnull(t1.f,'')) dept
,t1.排序号,t1.房号,t1.姓名,t1.手机号码,t1.rn,t1.OWNER_TAG
 from(select ROW_NUMBER() over (partition by a.OWNER_SID order by a.OWNER_SID desc ) as rn,
a.OWNER_SID,a.OWNER_NO 房号,a.OWNER_NAME 姓名,a.OWNER_PHONE 手机号码,
(CASE a.OWNER_STATUS
                 WHEN '0' THEN
                  '否'
                 WHEN '1' THEN
                  '是'
                 ELSE
                  ''
               END) AS 是否启用,
a.GROUP_SID,a.DEPT_SID,
(CASE a.OWNER_TYPE
                 WHEN '1' THEN
                  '小区用户'
                 WHEN '2' THEN
                  '小区服务人员'
                 WHEN '3' THEN
                  '集团服务人员'
                 WHEN '4' THEN
                  '小区管理员'
                 WHEN '5' THEN
                  '集团管理员'
                 ELSE
                  ''
               END) AS 用户类型,n.DEPT_NAME g
  ,b.DEPT_NAME f,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX 排序号,c.DEPT_NAME d,e.DEPT_NAME c,
f.DEPT_NAME b,g.DEPT_NAME a,d.GROUP_NAME 集团名称,a.OWNER_TAG
  from HOME_OWNER a
  left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID
left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID
left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID
left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
--where  a.OWNER_SID in('0000c52a-8688-46ef-a4b2-4f90a449cedd')
where  a.OWNER_TYPE in('2','3','4','5')--工作人员
and a.OWNER_STATUS=1
and a.created_on <'20170401'
)t1)t2)t4)t3
group by t3.F1,t3.F2,t3.OWNER_TAG  
)t8)t9
--order by t8.悦服务用户数 desc
--where t3.部门 like ('%事业三部%') 
left join 
(select REPLACE(t7.dept1,' ','-') 部门, t7.独立访客ID数,t7.点击次数,t7.人均点击次数,t7.日均点击次数,t7.日人均点击次数
from(
select Rtrim(isnull(t5.F1,'')+' '+isnull(t5.F2,'')+' '+isnull(t5.OWNER_TAG,'')) dept1,
t5.F1,t5.F2,t5.OWNER_TAG,--,t5.F4,t5.F5
count(distinct(t5.OWNER_SID)) 独立访客ID数 ,count(t5.OWNER_SID) 点击次数,
round(convert(float,count(t5.OWNER_SID)/count(distinct(t5.OWNER_SID))),2 ) 人均点击次数,
round(convert(float,count(t5.OWNER_SID))/12,2)日均点击次数,
round(convert(float,count(t5.OWNER_SID)/count(distinct(t5.OWNER_SID)))/12, 2)日人均点击次数
from(
SELECT 
    dbo.f_GetStr(t6.部门,1,'-') F1,
    dbo.f_GetStr(t6.部门,2,'-') F2,
    dbo.f_GetStr(t6.部门,3,'-') F3,
    dbo.f_GetStr(t6.部门,4,'-') F4,
    dbo.f_GetStr(t6.部门,5,'-') F5,
    dbo.f_GetStr(t6.部门,6,'-') F6,* 
from(
select REPLACE(t4.dept,' ','-') 部门,* from(
select T1.OWNER_TAG,
 t1.SYSTEM_TYPE,t1.CREATED_ON,t1.CONTENT,t1.OWNER_SID,t1.rn,
t1.OWNER_STATUS,t1.OWNER_TYPE,t1.a,t1.b,t1.c,t1.d,t1.f,
ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' ' +isnull(t1.d,'') +' '+isnull(t1.f,'')) dept,
t1.SORT_INDEX,t1.OWNER_NO,t1.OWNER_NAME,t1.OWNER_PHONE
from
(select A.OWNER_TAG,ROW_NUMBER() over (partition by h.OWNER_SID order by h.OWNER_SID desc) as rn,
h.SYSTEM_TYPE,h.CREATED_ON,
a.OWNER_PHONE,a.OWNER_STATUS,a.GROUP_SID,a.DEPT_SID,a.OWNER_TYPE,
h.CONTENT,h.OWNER_SID,a.OWNER_NO,a.OWNER_NAME,n.DEPT_NAME g,
b.DEPT_NAME f,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX,c.DEPT_NAME d,
e.DEPT_NAME c,f.DEPT_NAME b,g.DEPT_NAME a,d.GROUP_NAME 
from Home_OwnerLog h
left join HOME_OWNER a on h.OWNER_SID=a.OWNER_SID
left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID
 left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID 
left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID
left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
where a.OWNER_TYPE in('2','3','4','5')--工作人员
and h.CREATED_ON>='20170320'
and h.CREATED_ON<'20170401'
and a.OWNER_STATUS=1
and h.SYSTEM_TYPE = 1--0悦嘉家\悦园区,1悦服务
)t1
--where t1.rn=1
)t4)t6)t5
--where t5.部门 like('%事业三部%')
group by t5.F1,t5.F2,t5.OWNER_TAG--,t5.F4,t5.F5
)t7)t10
--order by t7.独立访客ID数 desc
on t9.部门=t10.部门
order by t10.独立访客ID数 desc



















--去除空格
字符前的空格，用ltrim(string1)
字符后的空格，用rtrim(string1)
字符中的空格，用replace(搜索string1, ' ', ' ')





----------按层级汇总的悦服务账号数
---------------------------------------------------------------------------
--更改F1....F6部门层级,连接a.OWNER_TAG岗位,得到各个层级下的各岗位的账户数

IF NOT OBJECT_ID('f_GetStr') IS NULL
    DROP FUNCTION [f_GetStr]
GO
CREATE FUNCTION dbo.f_GetStr(
    @s varchar(8000),      --包含多个数据项的字符串
    @pos int,             --要获取的数据项的位置
    @split varchar(10)     --数据分隔符
)RETURNS varchar(1000)
AS
BEGIN
    IF @s IS NULL RETURN(NULL)
    DECLARE @splitlen int
    SELECT @splitlen=LEN(@split+'a')-2
    WHILE @pos>1 AND CHARINDEX(@split,@s+@split)>0
        SELECT @pos=@pos-1,
            @s=STUFF(@s,1,CHARINDEX(@split,@s+@split)+@splitlen,'')
    RETURN(ISNULL(LEFT(@s,CHARINDEX(@split,@s+@split)-1),''))
END
GO
----上面是函数创建，创建后直接跑下面的导出数据，否则加上函数代码导不出数据
select REPLACE(t8.dept1,' ','-') 部门,t8.OWNER_TAG 岗位, t8.悦服务用户数  from(
select Rtrim(isnull(t3.F1,'')+' '+isnull(t3.F2,'')+' '+isnull(t3.F3,'') ) dept1,
t3.F1,t3.F2,t3.F3,t3.OWNER_TAG,count(t3.owner_SID) 悦服务用户数 
from( 
SELECT 
    dbo.f_GetStr(t4.部门,1,'-') F1,
    dbo.f_GetStr(t4.部门,2,'-') F2,
    dbo.f_GetStr(t4.部门,3,'-') F3,
    dbo.f_GetStr(t4.部门,4,'-') F4,
    dbo.f_GetStr(t4.部门,5,'-') F5,
    dbo.f_GetStr(t4.部门,6,'-') F6,*
from(
select REPLACE(t2.dept,' ','-') 部门,t2.OWNER_TAG ,t2.是否启用,t2.用户类型,t2.排序号,t2.房号,t2.姓名,t2.手机号码,t2.owner_SID from(
select t1.owner_SID, t1.是否启用,t1.用户类型,ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' ' +isnull(t1.d,'') +' '+isnull(t1.f,'')) dept
,t1.排序号,t1.房号,t1.姓名,t1.手机号码,t1.rn,t1.OWNER_TAG
 from(select ROW_NUMBER() over (partition by a.OWNER_SID order by a.OWNER_SID desc ) as rn,
a.OWNER_SID,a.OWNER_NO 房号,a.OWNER_NAME 姓名,a.OWNER_PHONE 手机号码,
(CASE a.OWNER_STATUS
                 WHEN '0' THEN
                  '否'
                 WHEN '1' THEN
                  '是'
                 ELSE
                  ''
               END) AS 是否启用,
a.GROUP_SID,a.DEPT_SID,
(CASE a.OWNER_TYPE
                 WHEN '1' THEN
                  '小区用户'
                 WHEN '2' THEN
                  '小区服务人员'
                 WHEN '3' THEN
                  '集团服务人员'
                 WHEN '4' THEN
                  '小区管理员'
                 WHEN '5' THEN
                  '集团管理员'
                 ELSE
                  ''
               END) AS 用户类型,n.DEPT_NAME g
  ,b.DEPT_NAME f,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX 排序号,c.DEPT_NAME d,e.DEPT_NAME c,
f.DEPT_NAME b,g.DEPT_NAME a,d.GROUP_NAME 集团名称,a.OWNER_TAG
  from HOME_OWNER a
  left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID
left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID
left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID
left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
--where  a.OWNER_SID in('0000c52a-8688-46ef-a4b2-4f90a449cedd')
where  a.OWNER_TYPE in('2','3','4','5')--工作人员
and a.OWNER_STATUS=1
)t1)t2)t4)t3
group by t3.F1,t3.F2,t3.F3,t3.OWNER_TAG ,ltrim(isnull(t3.F1,'')+' '+isnull(t3.F2,'')+' '+isnull(t3.F3,'')) 
)t8
order by t8.悦服务用户数 desc
--where t3.部门 like ('%事业三部%') 




----------按层级汇总的悦服务使用情况
---------------------------------------------------------------------------
--更改F1....F6部门层级,连接a.OWNER_TAG岗位,得到各个层级下的各岗位的点击次数、点击ID数、人均、日均点击数等

IF NOT OBJECT_ID('f_GetStr') IS NULL
    DROP FUNCTION [f_GetStr]
GO
CREATE FUNCTION dbo.f_GetStr(
    @s varchar(8000),      --包含多个数据项的字符串
    @pos int,             --要获取的数据项的位置
    @split varchar(10)     --数据分隔符
)RETURNS varchar(1000)
AS
BEGIN
    IF @s IS NULL RETURN(NULL)
    DECLARE @splitlen int
    SELECT @splitlen=LEN(@split+'a')-2
    WHILE @pos>1 AND CHARINDEX(@split,@s+@split)>0
        SELECT @pos=@pos-1,
            @s=STUFF(@s,1,CHARINDEX(@split,@s+@split)+@splitlen,'')
    RETURN(ISNULL(LEFT(@s,CHARINDEX(@split,@s+@split)-1),''))
END
GO

----上面是函数创建，创建后直接跑下面的导出数据，否则加上函数代码导不出数据
select REPLACE(t7.dept1,' ','-') 部门,t7.OWNER_TAG, t7.独立访客ID数,t7.点击次数,t7.人均点击次数,t7.日均点击次数,t7.日人均点击次数 from(
select Rtrim(isnull(t5.F1,'')+' '+isnull(t5.F2,'')+' '+isnull(t5.F3,'')) dept1,
t5.F1,t5.F2,t5.F3,t5.OWNER_TAG,--,t5.F4,t5.F5
count(distinct(t5.OWNER_SID)) 独立访客ID数 ,count(t5.OWNER_SID) 点击次数,count(t5.OWNER_SID)/count(distinct(t5.OWNER_SID)) 人均点击次数,count(t5.OWNER_SID)/15 日均点击次数,count(t5.OWNER_SID)/count(distinct(t5.OWNER_SID))/15 日人均点击次数
from(
SELECT 
    dbo.f_GetStr(t6.部门,1,'-') F1,
    dbo.f_GetStr(t6.部门,2,'-') F2,
    dbo.f_GetStr(t6.部门,3,'-') F3,
    dbo.f_GetStr(t6.部门,4,'-') F4,
    dbo.f_GetStr(t6.部门,5,'-') F5,
    dbo.f_GetStr(t6.部门,6,'-') F6,* 
from(
select REPLACE(t4.dept,' ','-') 部门,* from(
select T1.OWNER_TAG,
 t1.SYSTEM_TYPE,t1.CREATED_ON,t1.CONTENT,t1.OWNER_SID,t1.rn,
t1.OWNER_STATUS,t1.OWNER_TYPE,t1.a,t1.b,t1.c,t1.d,t1.f,
ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' ' +isnull(t1.d,'') +' '+isnull(t1.f,'')) dept,
t1.SORT_INDEX,t1.OWNER_NO,t1.OWNER_NAME,t1.OWNER_PHONE
from
(select A.OWNER_TAG,ROW_NUMBER() over (partition by h.OWNER_SID order by h.OWNER_SID desc) as rn,
h.SYSTEM_TYPE,h.CREATED_ON,
a.OWNER_PHONE,a.OWNER_STATUS,a.GROUP_SID,a.DEPT_SID,a.OWNER_TYPE,
h.CONTENT,h.OWNER_SID,a.OWNER_NO,a.OWNER_NAME,n.DEPT_NAME g,
b.DEPT_NAME f,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX,c.DEPT_NAME d,
e.DEPT_NAME c,f.DEPT_NAME b,g.DEPT_NAME a,d.GROUP_NAME 
from Home_OwnerLog h
left join HOME_OWNER a on h.OWNER_SID=a.OWNER_SID
left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID
 left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID 
left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID
left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
where a.OWNER_TYPE not LIKE ('%1%')--工作人员
and h.CREATED_ON>='20170320'
and h.CREATED_ON<'20170405'
and a.OWNER_STATUS=1
and h.SYSTEM_TYPE = 1--0悦嘉家\悦园区,1悦服务
and  a.OWNER_TYPE in('2','3','4','5')--工作人员
)t1
--where t1.rn=1
)t4)t6)t5 group by t5.F1,t5.F2,t5.F3,t5.OWNER_TAG,ltrim(isnull(t5.F1,'')+' '+isnull(t5.F2,'')+' '+isnull(t5.F3,''))
)t7






删除存储过程
use JOY_HOME
go
DROP PROCEDURE [sp_getTreeById]

删除函数
DROP FUNCTION [f_getParent]



--构造一个递归函数
CREATE FUNCTION [f_getParent](@id VARCHAR(100)) 
  RETURNS @re TABLE(DEPT_SID VARCHAR(100),PARENT_DEPT_SID  VARCHAR(100),level VARCHAR(100)) 
  AS 
  begin 
  declare @level VARCHAR(100) 
  set @level = 1 
  declare @pid VARCHAR(100) 
  select @pid = PARENT_DEPT_SID from HOME_GROUP_DEPT tb where DEPT_SID = @id 
  insert @re 
  select DEPT_SID, PARENT_DEPT_SID ,@level from HOME_GROUP_DEPT tb where DEPT_SID = @pid 
  while @@rowcount > 0   
  begin 
  set @level = @level + 1 
  select @pid = PARENT_DEPT_SID from HOME_GROUP_DEPT tb where DEPT_SID = @pid 
  insert @re 
  select DEPT_SID,PARENT_DEPT_SID,@level from HOME_GROUP_DEPT tb where DEPT_SID = @pid 
  end 
  return 
  end
--调用递归函数
  select * from f_getParent('0083e945-ebd2-4487-ae2b-114deb9027f2')

  
  

-------构造一个分裂函数

IF NOT OBJECT_ID('f_GetStr') IS NULL
    DROP FUNCTION [f_GetStr]
GO
CREATE FUNCTION dbo.f_GetStr(
    @s varchar(8000),      --包含多个数据项的字符串
    @pos int,             --要获取的数据项的位置
    @split varchar(10)     --数据分隔符
)RETURNS varchar(1000)
AS
BEGIN
    IF @s IS NULL RETURN(NULL)
    DECLARE @splitlen int
    SELECT @splitlen=LEN(@split+'a')-2
    WHILE @pos>1 AND CHARINDEX(@split,@s+@split)>0
        SELECT @pos=@pos-1,
            @s=STUFF(@s,1,CHARINDEX(@split,@s+@split)+@splitlen,'')
    RETURN(ISNULL(LEFT(@s,CHARINDEX(@split,@s+@split)-1),''))
END
GO


--调用分裂函数
SELECT 
    dbo.f_GetStr(t6.部门,1,'-') F1,
    dbo.f_GetStr(t6.部门,2,'-') F2,
    dbo.f_GetStr(t6.部门,3,'-') F3,
    dbo.f_GetStr(t6.部门,4,'-') F4,
    dbo.f_GetStr(t6.部门,5,'-') F5,
    dbo.f_GetStr(t6.部门,6,'-') F6,* 
from(
select REPLACE(t4.dept,' ','-') 部门......






---------------------------------------------------------------------
--埋点日志分类
---------------------------------------------------------------------
根据登入-登出之间的时间间隔，可以得到用户的登陆时长。

首页--登入：登录账号,回到APP
登出--登出：离开APP(切换至后台)
注销--左侧功能条-注销：注销账号
访客验证--验证访客 --访客验证-编号验证
                   --访客验证-二维码验证
--首页底部图片：底部banner
--任务大厅-我的任务
--任务大厅-任务大厅
--任务大厅-特别关注


--%或者mod取余数
select T1.A%3 FROM(
SELECT  count(create_time) A
from user
)T1

