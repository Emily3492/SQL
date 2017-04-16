﻿**************************注意
--HOME_NEIGHNORHOOD_INVESTIGATION          调查主表
-- HOME_VOTE_ANSWER                        业主调查答案表
-- HOME_VOTE_HIST                          业主调查历史表 --不用
-- HOME_VOTE_ITEM                          业主调查题目表
-- HOME_NEIGHBORHOOD_LOG:                  调查 活动 话题记录表
-- HOME_NOTICE:                            园区公告表    （悦嘉家 和悦服务 共用）
HOME_NEIGHNORHOOD_INVESTIGATION 的 REFID 和 HOME_VOTE_ITEM 的VOTE_SID 相关联
select * from HOME_NEIGHNORHOOD_INVESTIGATION  a where a.TITLE ='悦嘉家APP用户使用调查'
select * from HOME_NEIGHNORHOOD_INVESTIGATION  a where a.REFID = '929065f2-3322-4d42-8536-98168add6fed'
select * from HOME_VOTE_ITEM a where a.VOTE_SID = '929065f2-3322-4d42-8536-98168add6fed'

select * from HOME_VOTE_ANSWER a where a.VOTE_ITEM_SID = '04d84a97-04b3-4524-b06b-eb6816f118e1'




--调查表
--悦嘉家后台-互动管理主表对应：HOME_NEIGHNORHOOD_INVESTIGATION , 悦嘉家后台-互动调查1
--悦嘉家后台-发调查主表对应：HOME_VOTE,悦嘉家后台-发调查(无法得到参与人ID,弃用)2,将1替换成2取在这里发布的调查对应的答案数据
--对应到每道题目个选项的选择数量
--剔除悦悦、姜瑜晶、幸福家园小区的调查数据，同一个人参与两次只统计第一次。
--c.VOTE_ANSWER_QTY 按照答案选中数量-题目 降序排列,取最大值(假设答案选中数量为累计值)
select t2.标题,t2.CONTENT 内容, t2.调查题目类型,t2.序号,t2.调查题目,t2.答案内容,t2.答案选中数量 from(
SELECT t1.标题,t1.CONTENT, t1.调查题目类型,t1.活动状态,t1.序号,t1.调查题目,t1.答案内容,t1.答案选中数量
,t1.OWNER_NAME,t1.APARTMENT_NAME ,t1.rn,t1.rnn,
ROW_NUMBER() over (partition by t1.调查题目,t1.答案内容 order by t1.调查题目,t1.答案内容,t1.答案选中数量 desc  ) as rn1
from(
select g.OWNER_NAME 创建人,a.REFID 主键,a.TITLE 标题,a.MEDIA_SID 图片ID,a.SUMARY 摘要,a.CONTENT,a.STARTDATE,a.DEADLINE,
(CASE a.STATUS
                 WHEN '1' THEN
                  '草稿'
                 WHEN '2' THEN
                  '发布'
                 wHEN '3' THEN
                  '屏蔽'
                 WHEN '4' THEN
                  '过期'
                 ELSE
                  ''
               END) AS 活动状态,
 b.VOTE_ITEM 调查题目,b.VOTE_ITEM_INDEX 序号,
 (CASE b.VOTE_ITEM_TYPE
                 WHEN '0' THEN
                  '单选题'
                 WHEN '1' THEN
                  '多选题'
                 WHEN '2' THEN
                  '填空题'
                 ELSE
                  ''
               END) AS 调查题目类型,
 b.VOTE_ITEM_QTY 答题数量,b.CREATEDBY 题目创建用户ID,b.CREATED_ON 题目时间,
 c.VOTE_ANSWER 答案内容 ,c.VOTE_ANSWER_QTY 答案选中数量,c.CREATEDBY 答案创建用户,c.CREATED_ON 答案创建时间,
ROW_NUMBER() over (partition by a.TITLE,e.OWNER_SID  order by a.TITLE,e.OWNER_SID  ) as rn,
ROW_NUMBER() over (partition by b.VOTE_ITEM,c.VOTE_ANSWER,e.OWNER_SID order by  e.OWNER_SID, b.VOTE_ITEM_INDEX,c.VOTE_ANSWER ) as rnn,
e.OWNER_NO,e.OWNER_NAME,e.OWNER_PHONE,e.FAMILY_NAME,f.APARTMENT_NAME
from HOME_NEIGHNORHOOD_INVESTIGATION a
left join HOME_OWNER g on a.AUTHORID=g.OWNER_SID
left join HOME_VOTE_ITEM b on a.REFID=b.VOTE_SID
left join HOME_VOTE_ANSWER c on b.VOTE_ITEM_SID=c.VOTE_ITEM_SID
left join HOME_NEIGHBORHOOD_LOG d on a.REFID=d.REFID
left join HOME_OWNER e on d.OWNER_SID =e.OWNER_SID
left join HOME_APARTMENT f on f.APARTMENT_SID=e.APARTMENT_SID
where a.TITLE like('悦嘉家APP用户使用调查')
and d.STATUS like('%2%')
and d.TYPE like('%3%')
and e.OWNER_TYPE like('%1%')
and f.APARTMENT_NAME not like  ('%幸福家园%')
and e.FAMILY_NAME not like  ('%悦悦%')
and e.OWNER_NAME not like  ('%姜瑜晶%')
and g.OWNER_NAME like('%刘邦操%')
and a.STARTDATE >='20170116'
--and b.VOTE_ITEM_TYPE=1
)t1
where t1.rnn=1)t2
where t2.rn1=1
order by t2.序号,t2.答案选中数量 desc


--关联vote表待调整

select t2.标题,t2.序号,t2.调查题目,t2.答案内容,t2.答案选中数量 from(
SELECT t1.标题, t1.序号,t1.调查题目,t1.答案内容,t1.答案选中数量
,t1.OWNER_NAME,t1.APARTMENT_NAME ,t1.rn,t1.rnn,
ROW_NUMBER() over (partition by t1.调查题目,t1.答案内容 order by t1.调查题目,t1.答案内容,t1.答案选中数量 desc  ) as rn1
from(
select g.OWNER_NAME 创建人,a.VOTE_SID 主键,a. VOTE_SUBJECT 标题,
 b.VOTE_ITEM 调查题目,b.VOTE_ITEM_INDEX 序号,
 b.VOTE_ITEM_QTY 答题数量,b.CREATEDBY 题目创建用户ID,b.CREATED_ON 题目时间,
 c.VOTE_ANSWER 答案内容 ,c.VOTE_ANSWER_QTY 答案选中数量,c.CREATEDBY 答案创建用户,c.CREATED_ON 答案创建时间,
ROW_NUMBER() over (partition by a.VOTE_SUBJECT,e.OWNER_SID  order by a.VOTE_SUBJECT,e.OWNER_SID  ) as rn,
ROW_NUMBER() over (partition by b.VOTE_ITEM,c.VOTE_ANSWER,e.OWNER_SID order by  e.OWNER_SID, b.VOTE_ITEM_INDEX,c.VOTE_ANSWER ) as rnn,
e.OWNER_NO,e.OWNER_NAME,e.OWNER_PHONE,e.FAMILY_NAME,f.APARTMENT_NAME
from HOME_VOTE a
left join HOME_OWNER g on a.CREATEDBY=g.OWNER_SID
left join HOME_VOTE_ITEM b on a.VOTE_SID=b.VOTE_SID
left join HOME_VOTE_ANSWER c on b.VOTE_ITEM_SID=c.VOTE_ITEM_SID
left join HOME_NEIGHBORHOOD_LOG d on a.VOTE_SID=d.REFID
left join HOME_OWNER e on d.OWNER_SID =e.OWNER_SID
left join HOME_APARTMENT f on f.APARTMENT_SID=e.APARTMENT_SID
where a.VOTE_SUBJECT like('悦嘉家APP用户使用调查')
--and d.STATUS like('%2%')
--and d.TYPE like('%3%')
--and e.OWNER_TYPE like('%1%')
--and f.APARTMENT_NAME not like  ('%幸福家园%')
--and e.FAMILY_NAME not like  ('%悦悦%')
---and e.OWNER_NAME not like  ('%姜瑜晶%')
--and g.OWNER_NAME like('%刘邦操%')
--and a.CREATED_ON >='20170116'
--and b.VOTE_ITEM_TYPE=1
)t1
where t1.rnn=1)t2
where t2.rn1=1
order by t2.序号,t2.答案选中数量 desc



--邻居圈入口
--浏览/参与或报名次数，ID数
select t1.APARTMENT_NAME 项目,count(distinct(t1.OWNER_SID)) ID数,count(t1.OWNER_SID) 次数 from
(
select d.OWNER_SID,a.REFID 主键,a.TITLE 标题,a.MEDIA_SID 图片ID,a.SUMARY 摘要,a.CONTENT,a.STARTDATE,a.DEADLINE,
(CASE a.STATUS
                 WHEN '1' THEN
                  '草稿'
                 WHEN '2' THEN
                  '发布'
                 wHEN '3' THEN
                  '屏蔽'
                 WHEN '4' THEN
                  '过期'
                 ELSE
                  ''
               END) AS 活动状态,
 e.OWNER_NO,e.OWNER_NAME,e.OWNER_PHONE,e.FAMILY_NAME,f.APARTMENT_NAME
from HOME_NEIGHBORHOOD_LOG d
left join HOME_NEIGHNORHOOD_INVESTIGATION a on a.REFID=d.REFID
left join HOME_OWNER e on d.OWNER_SID =e.OWNER_SID
left join HOME_APARTMENT f on f.APARTMENT_SID=e.APARTMENT_SID
where d.STATUS=1 -- 活动状态(1:浏览 2:报名或已参与 )
and d.TYPE=3
and a.TITLE like('悦嘉家APP用户使用调查')
and e.OWNER_TYPE like('%1%')
and f.APARTMENT_NAME not like  ('%幸福家园%')
and e.FAMILY_NAME not like  ('%悦悦%')
and e.OWNER_NAME not like  ('%姜瑜晶%')
)t1
group by t1.APARTMENT_NAME
order by count(distinct(t1.OWNER_SID)) desc



--公告入口的调查浏览次数
select a.NOTICE_SUBJECT 类型,b.APARTMENT_NAME 项目, sum(a.BROWSE_QTY) 浏览次数
from HOME_NOTICE a
left join HOME_APARTMENT b on b.APARTMENT_SID=a.APARTMENT_SID
--left join HOME_IMAGEALL c on c.NOTICE_SID =a.NOTICE_SID
where a.NOTICE_SUBJECT like('%悦嘉家APP使用调查%')
--and b.APARTMENT_NAME in('%江滨花园%')
--and CREATED_ON ='20170308'
 GROUP BY a.NOTICE_SUBJECT,b.APARTMENT_NAME
order by sum(a.BROWSE_QTY) desc



--调查参与评分的ID数
select t1.APARTMENT_NAME ,count(t1.rn) from (
select a.REFID 主键,a.TITLE 标题,a.MEDIA_SID 图片ID,a.SUMARY 摘要,a.CONTENT,a.STARTDATE,a.DEADLINE,
(CASE a.STATUS
                 WHEN '1' THEN
                  '草稿'
                 WHEN '2' THEN
                  '发布'
                 wHEN '3' THEN
                  '屏蔽'
                 WHEN '4' THEN
                  '过期'
                 ELSE
                  ''
               END) AS 活动状态,
 b.VOTE_ITEM 调查题目,b.VOTE_ITEM_INDEX 序号,
 (CASE b.VOTE_ITEM_TYPE
                 WHEN '0' THEN
                  '单选题'
                 WHEN '1' THEN
                  '多选题'
                 WHEN '2' THEN
                  '填空题'
                 ELSE
                  ''
               END) AS 调查题目类型,
 b.VOTE_ITEM_QTY 答题数量,b.CREATEDBY 题目创建用户ID,b.CREATED_ON 题目时间,
 c.VOTE_ANSWER 答案内容 ,c.VOTE_ANSWER_QTY 答案选中数量,c.CREATEDBY 答案创建用户,c.CREATED_ON 答案创建时间,
ROW_NUMBER() over (partition by a.TITLE,e.OWNER_SID  order by a.TITLE,e.OWNER_SID  ) as rn
 ,e.OWNER_NO,e.OWNER_NAME,e.OWNER_PHONE,e.FAMILY_NAME,f.APARTMENT_NAME
from
HOME_NEIGHNORHOOD_INVESTIGATION a
left join HOME_VOTE_ITEM b on a.REFID=b.VOTE_SID
left join HOME_VOTE_ANSWER c on b.VOTE_ITEM_SID=c.VOTE_ITEM_SID
left join HOME_NEIGHBORHOOD_LOG d on a.REFID=d.REFID
left join HOME_OWNER e on d.OWNER_SID =e.OWNER_SID
left join HOME_APARTMENT f on f.APARTMENT_SID=e.APARTMENT_SID
where a.TITLE like('悦嘉家APP用户使用调查')
and d.STATUS=2
and d.TYPE=3
and e.OWNER_TYPE like('%1%')
and f.APARTMENT_NAME not like  ('%幸福家园%')
and FAMILY_NAME not like  ('%悦悦%')
and e.OWNER_NAME not like  ('%姜瑜晶%')
)t1
where rn =1--参与人数
group by t1.APARTMENT_NAME
order by count(t1.rn) desc




--物业服务中心房号，1月18号部分项目修订房号信息为“项目名...物业...”，剔除工作人员房号时，剔除 like'%物业%'

-->=,<
--每月注册ID数
select str(6) month,count(distinct(a.OWNER_SID)) sum_owner
from HOME_OWNER a
where  a.CREATED_ON <'20160626'
and a.OWNER_type = 1
union all
select str(7) month,count(distinct(a.OWNER_SID)) sum_owner
from HOME_OWNER a
where a.CREATED_ON <'20160726'
and a.OWNER_type = 1
union all
select str(8) month,count(distinct(a.OWNER_SID)) sum_owner
from HOME_OWNER a
where a.CREATED_ON <'20160826'
and a.OWNER_type = 1
union all
select str(9) month,count(distinct(a.OWNER_SID)) sum_owner
from HOME_OWNER a
where a.CREATED_ON <'20160926'
and a.OWNER_type = 1
union all
select str(10) month,count(distinct(a.OWNER_SID)) sum_owner
from HOME_OWNER a
where  a.CREATED_ON <'20161026'
and a.OWNER_type = 1
union all
select str(11) month,count(distinct(a.OWNER_SID)) sum_owner
from HOME_OWNER a
where  a.CREATED_ON <'20161126'
and a.OWNER_type = 1

--下载量
select count(distinct(OWNER_SID)) from HOME_OWNER
where OWNER_TYPE=1
and FAMILY_NAME not in ('悦悦')
and OWNER_NO not like '%物业%'

--取用户表
select OWNER_SID,CREATED_ON,APARTMENT_SID,OWNER_NAME, OWNER_TYPE from HOME_OWNER
where OWNER_TYPE=1
 --and FAMILY_NAME not in ('悦悦')
 --and OWNER_NO not like '%物业%'

--取单月日志表
select OWNER_SID,SYSTEM_TYPE,CREATED_ON,CONTENT from Home_OwnerLog
where SYSTEM_TYPE=0
and CREATED_ON >='2016-09-26'
and CREATED_ON <'2016-10-26'

--用户表关联10月日志
select a.CREATED_ON id_created,b.CREATED_ON log_created,a.OWNER_SID,a.APARTMENT_SID,a.OWNER_NAME, a.OWNER_TYPE ,b.CONTENT
from HOME_OWNER a
left join Home_OwnerLog b on a.OWNER_SID = b.OWNER_SID
where a.OWNER_TYPE=1
 and a.FAMILY_NAME not in ('悦悦')
 and a.OWNER_NO not like '%物业%'
--and b.CREATED_ON >='2016-09-26'
--and b.CREATED_ON <'2016-10-26'
and b.SYSTEM_TYPE=0
order by a.OWNER_SID



--取日志表


         注册ID户   APP独立访客数   活跃度
第一周     100         10            10%
第二周     200         20            10%
第三周     300         30            10%
第四周     400         40            10%


6991

--日志
--悦思悦想-拨打咨询热线
--房屋租售-拨打看房热线

--悦嘉家注册用户原始表
SELECT  a.OWNER_SID,a.CREATED_ON 注册时间,b.APARTMENT_NAME 项目名称,a.OWNER_PHONE 帐号,a.FAMILY_NAME 昵称,a.OWNER_NAME 真实姓名,a.OWNER_NO 房号,
(CASE a.OWNER_CATEGORY
                 WHEN '0' THEN
                  '业主'
                 WHEN '1' THEN
                  '租户'
                 WHEN '2' THEN
                  '家属'
                 ELSE
                  ''
               END) AS 租户类型,
(CASE a.OWNER_STATUS
                 WHEN '0' THEN
                  '否'
                 WHEN '1' THEN
                  '是'
                 ELSE
                  ''
               END) AS 是否启用,
(CASE c.VERIFICATION_TAG
                 WHEN '0' THEN
                  '未申请'
                 WHEN '1' THEN
                  '待验证'
                 WHEN '2' THEN
                  '已验证'
                 WHEN '3' THEN
                  '验证未通过'
                 ELSE
                  ''
               END) AS 验证状态,
c.CREATED_ON 验证时间
          FROM HOME_OWNER AS a
          left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
          left join HOME_OWNER_VERIFICATION c on a.OWNER_SID = c.OWNER_SID
         WHERE  a.CREATED_ON < '20170101 12:00:01'
        -- and a.OWNER_NO not like '%物业服务中心%'--门禁数据不剔除
           and a.CREATED_ON  >= '20161201'
         -- and  a.CREATED_ON < '2016-10-23'
        -- and b.APARTMENT_NAME not in ('普升福邸','蓝爵国际','体验小区','幸福家园','房屋租售中心')
         and b.APARTMENT_NAME not in ('幸福家园','体验小区','金橡臻园')
          and a.OWNER_type = 1--业主
		  and a.OWNER_STATUS=1
order by b.APARTMENT_NAME,a.CREATED_ON




--悦园区原始表
--注册园区字段后台有错误，现在对应的是apartment_sid(可切换登陆的小区),待更正（应该对应REGIN_OWNER_APARTMENT_SID字段）
select * from (
select a.REGION,a.SEX,a.OWNER_SID,a.CREATED_ON 注册时间,
d.APARTMENT_NAME 注册园区待调整,b.APARTMENT_NAME 登陆账户所在小区,
a.OWNER_PHONE 帐号,a.FAMILY_NAME 昵称,a.OWNER_NAME 真实姓名,
(CASE a.OWNER_STATUS
                 WHEN '0' THEN
                  '否'
                 WHEN '1' THEN
                  '是'
                 ELSE
                  ''
               END) AS 是否启用,a.OWNER_CATEGORY,
  (select g.APARTMENT_NAME +',' FROM HOME_OWNER_APARTMENT f
  left join HOME_APARTMENT g on f.APARTMENT_SID = g.APARTMENT_SID
  WHERE a.OWNER_SID = f.OWNER_SID FOR XML PATH('')) AS 拥有园区
  from HOME_OWNER AS a
  left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
  left join HOME_APARTMENT d on a.REGIN_OWNER_APARTMENT_SID = d.APARTMENT_SID--注册园区，待调整
  where a.OWNER_STATUS=1
  )t1
--where t1.拥有园区 is not null




--剔除物业服务中心后的用户表
SELECT  a.OWNER_SID,a.CREATED_ON 注册时间,b.APARTMENT_NAME 项目名称,a.OWNER_PHONE 帐号,a.FAMILY_NAME 昵称,a.OWNER_NAME 真实姓名,a.OWNER_NO 房号,
(CASE a.OWNER_CATEGORY
                 WHEN '0' THEN
                  '业主'
                 WHEN '1' THEN
                  '租户'
                 WHEN '2' THEN
                  '家属'
                 ELSE
                  ''
               END) AS 租户类型,
(CASE a.OWNER_STATUS
                 WHEN '0' THEN
                  '否'
                 WHEN '1' THEN
                  '是'
                 ELSE
                  ''
               END) AS 是否启用,
(CASE c.VERIFICATION_TAG
                 WHEN '0' THEN
                  '未申请'
                 WHEN '1' THEN
                  '待验证'
                 WHEN '2' THEN
                  '已验证'
                 WHEN '3' THEN
                  '验证未通过'
                 ELSE
                  ''
               END) AS 验证状态,
c.CREATED_ON 验证时间
          FROM HOME_OWNER AS a
          left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
          left join HOME_OWNER_VERIFICATION c on a.OWNER_SID = c.OWNER_SID
         WHERE   b.APARTMENT_NAME not in ('幸福家园','体验小区','金橡臻园','竹径茶语')
          and a.OWNER_type = 1--业主
and a.OWNER_NO not like '%物业%'
order by b.APARTMENT_NAME,a.CREATED_ON



--新增用户ID按天汇总，按月汇总及历史ID数（在SQL中进行取数）
--20160901之前的用户注册ID数为6072
select t2.data,sum(t1.new)+6569 as  all_sum
from (select t1.data, count(t1.OWNER_SID) new --,ROW_NUMBER() over (partition by t1.data order by t1.data ) as rn
          from (select b.OWNER_NAME,b.FAMILY_NAME,b.CREATED_ON,b.OWNER_NO,b.OWNER_SID,CAST(b.CREATED_ON AS DATE) data
          from HOME_OWNER b
                 where b.OWNER_TYPE = 1 --内部计算活跃度不剔除，计算项目绩效时剔除
                  -- and b.FAMILY_NAME not in ('悦悦')
                  -- and b.OWNER_NO not like '%物业%'
                    and b.APARTMENT_NAME not in ('幸福家园','体验小区')
                   and b.CREATED_ON >= '20160826' --历史数据and b.CREATED_ON <'20161001'
                   and b.CREATED_ON < '20160926') t1
         group by t1.data) t1,
         (select t1.data,
               count(t1.OWNER_SID) new --,ROW_NUMBER() over (partition by t1.data order by t1.data ) as rn
          from (select b.OWNER_NAME, b.FAMILY_NAME,b.CREATED_ON, b.OWNER_NO,b.OWNER_SID,CAST(b.CREATED_ON AS DATE) data
                  from HOME_OWNER b
                     where b.OWNER_TYPE = 1 --内部计算活跃度不剔除，计算项目绩效时剔除
                  -- and b.FAMILY_NAME not in ('悦悦')
                  -- and b.OWNER_NO not like '%物业%'
                    and b.APARTMENT_NAME not in ('幸福家园','体验小区')
                      and b.CREATED_ON >='20160826' --历史数据and b.CREATED_ON <'20161001'
                   and b.CREATED_ON < '20160926') t1
         group by t1.data) t2
         where  t1.data<=t2.data
         group by t2.data
order by t2.data

 格式如下：
    日期       截止该日OWNER_SID用户数(历史加新增)
 10月1 日           5600
 10月2 日           5723
 ...                ...
 10月31日           5891



 --按小区汇总的用户ID数
 select yy.APARTMENT_NAME 小区名称,COUNT(yy.OWNER_SID) 注册ID数
FROM (
SELECT  a.OWNER_NO,a.OWNER_SID ,b.APARTMENT_NAME
          FROM HOME_OWNER AS a
          left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
         WHERE  a.CREATED_ON >= '20170101 12:00:00'
        -- and a.OWNER_NO not like '%物业服务中心%'--门禁数据不剔除
                -- and a.OWNER_NO  like ('%一期%')--东方福邸一期、二期分别统计
           and a.CREATED_ON  < '20170226'
        -- and b.APARTMENT_NAME not in ('普升福邸','蓝爵国际','体验小区','幸福家园','房屋租售中心')
         and b.APARTMENT_NAME not in ('幸福家园','体验小区','金橡臻园')
               and a.OWNER_type like('%1%')--业主
) as yy
group by yy.APARTMENT_NAME
order by COUNT(yy.OWNER_SID) desc



--注册ID数,悦嘉家
select count(distinct(t1.OWNER_SID)) from(
select *
from HOME_OWNER a
where a.CREATED_ON >='20160926'
and a.CREATED_ON < '20161026'
and a.OWNER_type = 1--悦嘉家
) t1


--注册ID数，悦服务
select count(distinct(owner_sid)) from(
select * from HOME_OWNER where OWNER_TYPE in('2','3','4','5')
and owner_phone not in ('89738060','15906680522')
and OWNER_STATUS = 1 --1启用，0停用
)t1



--房号小区唯一用户数。（历史去重）
--select t2.APARTMENT_NAME 小区名称,count(t2.rn) 安装户数 FROM(
select * from(
select t2.APARTMENT_NAME 小区名称,count(t2.rn) 安装户数 FROM(
select t1.* from (
SELECT  a.OWNER_type,a.CREATED_ON,a.OWNER_NO,a.OWNER_SID ,b.APARTMENT_NAME, ROW_NUMBER() over (partition by b.APARTMENT_NAME,a.OWNER_NO order by a.CREATED_ON,b.APARTMENT_NAME ) as rn
          FROM HOME_OWNER AS a
   left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
     WHERE a.OWNER_type like('%1%')
             -- and a.OWNER_NO not like '%物业服务中心%'--门禁数据不剔除
     -- and a.OWNER_NO  like ('%一期%')--东方福邸一期、二期分别统计
    -- and b.APARTMENT_NAME  in ('东方润园')
 -- and b.APARTMENT_NAME not in ('幸福家园','体验小区','金橡臻园')
)t1
where t1.rn =1
and t1.CREATED_ON >= '2017-01-01 12:00:00'
and t1.CREATED_ON < '2017-02-20'
)t2
group by t2.APARTMENT_NAME
)t3
order by t3.安装户数 desc


--第二天起登陆过的ID
select t1.APARTMENT_NAME ,count(distinct(t1.log_id)) from (
select a.OWNER_SID ,b.OWNER_SID log_id,a.CREATED_ON OWNER,b.CREATED_ON,c.APARTMENT_NAME from HOME_OWNER a
left join HOME_APARTMENT c on c.APARTMENT_SID = a.APARTMENT_SID
left join Home_OwnerLog b on a.OWNER_SID = b.OWNER_SID
where a.OWNER_TYPE=1
and b.SYSTEM_TYPE=0
and CAST(b.CREATED_ON AS DATE)> CAST(a.CREATED_ON AS DATE)
and c.APARTMENT_NAME in ('东方润园')
---and b.created_on>='20161228'
)t1
GROUP BY  t1.APARTMENT_NAME


 --门禁访客ID数
 select t1.APARTMENT_NAME,count(distinct(t1.OWNER_SID)) 门禁访客ID数
from(
select a.OWNER_SID,b.OWNER_NO,b.APARTMENT_SID,c.APARTMENT_NAME
from Home_OwnerLog a
left join HOME_OWNER b on a.OWNER_SID=b.OWNER_SID
left join HOME_APARTMENT c on b.APARTMENT_SID =c.APARTMENT_SID
where a.SYSTEM_TYPE = 0
and b.OWNER_TYPE =1
and a.CREATED_ON >='20170117'
and a.CREATED_ON <'20170217'
--and b.OWNER_NO not like '%物业服务%'--内部计算活跃度不剔除，计算项目绩效时剔除
and (a.CONTENT like '%一键开门%')--更改限制条件''，or改为and,提取多级节点下细化日活数据
)t1
group by t1.APARTMENT_NAME
order by count(distinct(t1.OWNER_SID)) desc




--小区门禁申请
 select a.SHEET_SID,a.CREATED_ON 申请时间,b.USER_SID 用户ID,b.DOOR_SID,c.DOOR_SID,
c.DOOR_NAME 门名称,c.APARTMENT_SID,d.APARTMENT_SID,d.APARTMENT_NAME 小区名称,
(CASE a.FLAG
                 WHEN '0' THEN
                  '待审'
                 WHEN '1' THEN
                  '审核通过'
                 WHEN '2' THEN
                  '驳回'
                 ELSE
                  ''
               END) AS 单据状态
from HOME_UD_SHEET a
         left join HOME_UDITEM_SHEET b
                on a.SHEET_SID = b.SHEET_SID
         left join HOME_APARTMENT_DOOR c
                on b.DOOR_SID = c.DOOR_SID
         left join HOME_APARTMENT d
                on c.APARTMENT_SID = d.APARTMENT_SID
                where d.APARTMENT_NAME not in('幸福家园')
                and a.remark  is null
order by a.CREATED_ON desc


--未申请门禁的业主
SELECT  a. OWNER_PHONE,a.OWNER_NAME,a.FAMILY_NAME,a.OWNER_NO,a.OWNER_SID ,b.APARTMENT_NAME
          FROM HOME_OWNER AS a
          left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
          LEFT JOIN HOME_UD_SHEET c on c.CREATEDBY=a.OWNER_SID
         WHERE  c.SHEET_SID is null
         and (a.FAMILY_NAME not like'%客服%' or a.FAMILY_NAME not like'%悦悦%')
      and a.OWNER_NO not like '%物业服务中心%'--门禁数据不剔除
       and a.OWNER_type = 1--业主
       order by b.APARTMENT_NAME


--申请了门禁的业主
SELECT  c.SHEET_SID 申请单ID,a. OWNER_PHONE,a.OWNER_NAME,a.FAMILY_NAME,a.OWNER_NO,a.OWNER_SID ,b.APARTMENT_NAME
          FROM HOME_OWNER AS a
          left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
          LEFT JOIN HOME_UD_SHEET c on c.CREATEDBY=a.OWNER_SID
         WHERE  c.SHEET_SID is not null
 --        and (a.FAMILY_NAME not like'%客服%' or a.FAMILY_NAME not like'%悦悦%')
 --     and a.OWNER_NO not like '%物业%'--门禁数据不剔除
       and a.OWNER_type = 1--业主
       order by b.APARTMENT_NAME


--门禁申请ID数
select t3.小区名称,count(t3.rn) 申请用户ID数
from (
select t2.* from (
select t1.*,ROW_NUMBER() over (partition by t1.小区名称,t1.用户ID order by t1.申请时间,t1.小区名称 desc ) as rn
from (
select a.SHEET_SID,a.CREATED_ON 申请时间,b.USER_SID 用户ID,b.DOOR_SID 门ID,c.DOOR_SID,
c.DOOR_NAME 门名称,c.APARTMENT_SID 小区ID,d.APARTMENT_SID,d.APARTMENT_NAME 小区名称,
(CASE a.FLAG
                 WHEN '0' THEN
                  '待审'
                 WHEN '1' THEN
                  '审核通过'
                 WHEN '2' THEN
                  '驳回'
                 ELSE
                  ''
               END) AS 单据状态
from HOME_UD_SHEET a
         left join HOME_UDITEM_SHEET b
                on a.SHEET_SID = b.SHEET_SID
         left join HOME_APARTMENT_DOOR c
                on b.DOOR_SID = c.DOOR_SID
         left join HOME_APARTMENT d
                on c.APARTMENT_SID = d.APARTMENT_SID
where d.APARTMENT_NAME not in('幸福家园')
--and a.CREATED_ON >= '20161009'
and a.CREATED_ON < '20161017'
and a.remark  is null
)t1
--order by t1.申请时间,t1.小区名称  desc
)t2
where t2.rn = 1
)t3
group by t3.小区名称
order by count(t3.rn) desc

--门禁申请户数
select t3.小区名称,count(t3.rn) 申请用户数
from (
select t2.* from (
select t1.*,ROW_NUMBER() over (partition by t1.小区名称,t1.房号 order by t1.小区名称,t1.房号 desc ) as rn
from (
select a.SHEET_SID,a.CREATED_ON 申请时间,b.USER_SID 用户ID,b.DOOR_SID 门ID,c.DOOR_SID,
c.DOOR_NAME 门名称,c.APARTMENT_SID 小区ID,d.APARTMENT_SID,d.APARTMENT_NAME 小区名称,e.OWNER_NO 房号,e.OWNER_NAME,a.REMARK,
(CASE a.FLAG
                 WHEN '0' THEN
                  '待审'
                 WHEN '1' THEN
                  '审核通过'
                 WHEN '2' THEN
                  '驳回'
                 ELSE
                  ''
               END) AS 单据状态
from HOME_UD_SHEET a
         left join HOME_UDITEM_SHEET b
                on a.SHEET_SID = b.SHEET_SID
         left join HOME_APARTMENT_DOOR c
                on b.DOOR_SID = c.DOOR_SID
         left join HOME_APARTMENT d
                on c.APARTMENT_SID = d.APARTMENT_SID
         left join HOME_OWNER e
                on b.USER_SID = e.OWNER_SID
where d.APARTMENT_NAME not in('幸福家园','体验小区')
and e.OWNER_NO not like ('%物业中心%')
and a.remark  is null
and a.CREATED_ON >='20161009'
and a.CREATED_ON < '20161017'
)t1
--order by t1.申请时间,t1.小区名称  desc
)t2
where t2.rn = 1
)t3
group by t3.小区名称

--新增注册用户户数对应的申请门禁户数
select t2.APARTMENT_NAME,count(t2.rn) 新增注册用户户数对应的申请门禁户数 FROM(
select t1.* from (
SELECT  a.OWNER_NO,a.OWNER_SID ,c.SHEET_SID 申请单,b.APARTMENT_NAME, ROW_NUMBER() over (partition by b.APARTMENT_NAME,a.OWNER_NO order by a.CREATED_ON,b.APARTMENT_NAME ) as rn
          FROM HOME_OWNER AS a
          left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
          left join HOME_UDITEM_SHEET c on a.OWNER_SID = c.USER_SID
          left join HOME_UD_SHEET d
         on c.SHEET_SID = d.SHEET_SID
         WHERE a.CREATED_ON >= '20161113'
         and  a.CREATED_ON < '20161122'
and d.CREATED_ON >= '20161113'
         and  d.CREATED_ON < '20161122'
--and a.OWNER_NO like('%一期%')
        --and b.APARTMENT_NAME in ('东方郡','东方福邸','江滨花园','绿野春天','依山郡','银爵世纪','擎天半岛')
          and a.OWNER_type like( '%1%')
          and d.remark  is null
)t1
where t1.rn =1
)t2
group by t2.APARTMENT_NAME


--门禁使用ID数
select t3.APARTMENT_NAME,t3.Y_month,count(t3.rn) 门禁使用ID数 from (
select t2.* from (
select t1.*,ROW_NUMBER() over (partition by t1.APARTMENT_NAME,t1.USER_SID order by t1.APARTMENT_NAME,t1.USER_SID desc)rn
from (
select c.APARTMENT_NAME,b.APARTMENT_SID BM,b.OWNER_SID,b.OWNER_NO,b.OWNER_NAME,c.APARTMENT_SID,
convert(varchar(7),a.CREATED_ON,120) Y_month,
 a.DoorLog_SID,a.USER_SID,a.DOOR_SID,a.Content,a.OPENTIME
from HOME_Blue_User_DoorLog a
left join HOME_OWNER b
      on a.USER_SID = b.OWNER_SID
      left join HOME_APARTMENT c
      on b.APARTMENT_SID = c.APARTMENT_SID
where c.APARTMENT_NAME not in('幸福家园')
and b.OWNER_TYPE like('%1%')
--and b.OWNER_NO like('%一期%')
and a.OPENTIME >= '20170101'
and a.OPENTIME < '20170226'
)t1
)t2
where t2.rn=1
)t3
group by t3.APARTMENT_NAME,t3.Y_month
order by t3.APARTMENT_NAME,t3.Y_month,count(t3.rn) desc


--开门次数
select t1.APARTMENT_NAME,,t1.Y_month,count(t1.DoorLog_SID) 开门次数 from (
select c.APARTMENT_NAME,b.APARTMENT_SID,b.OWNER_SID,b.OWNER_NO,b.OWNER_NAME,
 a.DoorLog_SID,a.USER_SID,a.DOOR_SID,a.Content,a.OPENTIME
from HOME_Blue_User_DoorLog a
left join HOME_OWNER b
      on a.USER_SID = b.OWNER_SID
      left join HOME_APARTMENT c
      on b.APARTMENT_SID = c.APARTMENT_SID
where c.APARTMENT_NAME not in('幸福家园')
--and a.OPENTIME >= '20161009'
and a.OPENTIME < '20170119'
and b.OWNER_TYPE like('%1%')
--order by a.OPENTIME desc
)t1
group by t1.APARTMENT_NAME
order by count(t1.DoorLog_SID) desc



--各项目月均开门次数
select t1.APARTMENT_NAME,t1.Y_month,count(t1.DoorLog_SID) 开门次数 from (
select c.APARTMENT_NAME,b.APARTMENT_SID,b.OWNER_SID,b.OWNER_NO,b.OWNER_NAME,
 a.DoorLog_SID,a.USER_SID,a.DOOR_SID,a.Content,a.OPENTIME,convert(varchar(7),a.CREATED_ON,120) Y_month
from HOME_Blue_User_DoorLog a
left join HOME_OWNER b
      on a.USER_SID = b.OWNER_SID
      left join HOME_APARTMENT c
      on b.APARTMENT_SID = c.APARTMENT_SID
where c.APARTMENT_NAME not in('幸福家园')
--and a.OPENTIME >= '20161009'
and a.OPENTIME < '20170313'
and b.OWNER_TYPE like('%1%')
--order by a.OPENTIME desc
)t1
group by t1.APARTMENT_NAME,t1.Y_month
order by t1.APARTMENT_NAME,t1.Y_month,count(t1.DoorLog_SID) desc



--对应到人/门的开门次数
--对应到人的开门次数,(注释掉t1.DOOR_NAME)
select t1.APARTMENT_NAME 项目,t1.OWNER_NO 房号,t1.OWNER_NAME 姓名,count(t1.DoorLog_SID) 开门次数 --,t1.DOOR_NAME 门名称
from (
select d.DOOR_NAME,c.APARTMENT_NAME,b.APARTMENT_SID BM,b.OWNER_SID,b.OWNER_NO,b.OWNER_NAME,c.APARTMENT_SID,
 a.DoorLog_SID,a.USER_SID,a.DOOR_SID,a.Content,a.OPENTIME
from HOME_Blue_User_DoorLog a
left join HOME_APARTMENT_DOOR d on a.DOOR_SID=d.DOOR_SID
left join HOME_OWNER b
      on a.USER_SID = b.OWNER_SID
      left join HOME_APARTMENT c
      on b.APARTMENT_SID = c.APARTMENT_SID
where c.APARTMENT_NAME not in('幸福家园')
--and a.OPENTIME >= '20161009'
--and a.OPENTIME < '20170119'
and b.OWNER_TYPE like('%1%')
--order by a.OPENTIME desc
)t1
group by t1.APARTMENT_NAME,t1.OWNER_NO,t1.OWNER_NAME--,t1.DOOR_NAME
order by count(t1.DoorLog_SID) desc



--按项目按时点汇总的开门次数
--按时点汇总的开门次数(注释掉t1.APARTMENT_NAME)
select t1.开门时点,count(t1.DoorLog_SID) 开门次数 --,t1.APARTMENT_NAME 项目
from (
select d.DOOR_NAME,c.APARTMENT_NAME,b.APARTMENT_SID BM,b.OWNER_SID,b.OWNER_NO,b.OWNER_NAME,c.APARTMENT_SID,CONVERT(varchar(2), a.OPENTIME,8) 开门时点,
 a.DoorLog_SID,a.USER_SID,a.DOOR_SID,a.Content,a.OPENTIME
from HOME_Blue_User_DoorLog a
left join HOME_APARTMENT_DOOR d on a.DOOR_SID=d.DOOR_SID
left join HOME_OWNER b
      on a.USER_SID = b.OWNER_SID
      left join HOME_APARTMENT c
      on b.APARTMENT_SID = c.APARTMENT_SID
where c.APARTMENT_NAME not in('幸福家园')
--and a.OPENTIME >= '20161009'
--and a.OPENTIME < '20170119'
and b.OWNER_TYPE like('%1%')
--order by a.OPENTIME desc
)t1
group by t1.开门时点--t1.APARTMENT_NAME
order by count(t1.DoorLog_SID) desc--t1.APARTMENT_NAME




 --用户验证ID数据计数（以最上面ID数为准）
select t3.小区名称,COUNT(t3.rn) 验证用户数
from
(
select t2.* from
(
select t1.*,ROW_NUMBER() over (partition by t1.小区名称,t1.业主ID order by t1.小区名称,t1.创建时间 desc) as rn,
(CASE t1.VERIFICATION_TAG
                 WHEN '0' THEN
                  '未申请'
                 WHEN '1' THEN
                  '待验证'
                 WHEN '2' THEN
                  '已验证'
                 WHEN '3' THEN
                  '验证未通过'
                 ELSE
                  '未申请'
               END) AS 验证标识
from
(
SELECT  c.APARTMENT_NAME 小区名称,a.OWNER_NO 房号,a.OWNER_NAME 业主真实名字,a.OWNER_PHONE 业主手机号码, b.CREATED_ON 创建时间 ,a.OWNER_SID 业主ID,a.FAMILY_NAME 昵称,
(CASE a.OWNER_CATEGORY
                 WHEN '0' THEN
                  '业主'
                 WHEN '1' THEN
                  '租户'
                 WHEN '2' THEN
                  '家属'
                 ELSE
                  ''
               END) AS 业主类型
,ISNULL(b.VERIFICATION_TAG,'0') AS VERIFICATION_TAG,
b.CREATED_ON AS VERIFICATION_TIME
 -- 业主类型（0：业主，1：租户，2：家属）
   FROM HOME_OWNER A
   LEFT JOIN HOME_OWNER_VERIFICATION b ON  A.OWNER_SID=b.OWNER_SID
   left join HOME_APARTMENT as c on a.APARTMENT_SID = c.APARTMENT_SID
   WHERE A.OWNER_TYPE='1' --and b.VERIFICATION_TAG in (0, 1, 2,3)
)t1
)t2
where t2.rn = 1
and t2.创建时间 >= '2016-08-26'
and t2.创建时间 < '2016-10-22'
and t2.小区名称 not in ('普升福邸','蓝爵国际','体验小区','幸福家园','房屋租售中心')
--and t2.小区名称 in ('普升福邸','蓝爵国际')
)t3
group by t3.小区名称
order by COUNT(t3.rn) desc



 --用户验证户数计数（有问题，以上面为准）
select t3.小区名称,COUNT(t3.rn) 小区房号唯一的验证户数
from
(
select t2.* from
(
select t1.*,ROW_NUMBER() over (partition by t1.小区名称,t1.房号 order by t1.小区名称,t1.创建时间 desc) as rn,
(CASE t1.VERIFICATION_TAG
                 WHEN '0' THEN
                  '未申请'
                 WHEN '1' THEN
                  '待验证'
                 WHEN '2' THEN
                  '已验证'
                 WHEN '3' THEN
                  '验证未通过'
                 ELSE
                  '未申请'
               END) AS 验证标识
from
(
SELECT  c.APARTMENT_NAME 小区名称,a.OWNER_NO 房号,a.OWNER_NAME 业主真实名字,a.OWNER_PHONE 业主手机号码, b.CREATED_ON 创建时间 ,a.FAMILY_NAME 昵称,
(CASE a.OWNER_CATEGORY
                 WHEN '0' THEN
                  '业主'
                 WHEN '1' THEN
                  '租户'
                 WHEN '2' THEN
                  '家属'
                 ELSE
                  ''
               END) AS 业主类型
,ISNULL(b.VERIFICATION_TAG,'0') AS VERIFICATION_TAG,
b.CREATED_ON AS VERIFICATION_TIME
 -- 业主类型（0：业主，1：租户，2：家属）
   FROM HOME_OWNER A
   LEFT JOIN HOME_OWNER_VERIFICATION b ON  A.OWNER_SID=b.OWNER_SID
   left join HOME_APARTMENT as c on a.APARTMENT_SID = c.APARTMENT_SID
   WHERE A.OWNER_TYPE='1' --and b.VERIFICATION_TAG in (0, 1, 2,3)
)t1
)t2
where t2.rn = 1
and t2.创建时间 >= '2016-10-23'
and t2.创建时间 < '2016-10-24'
and t2.小区名称 not in ('普升福邸','蓝爵国际','体验小区','幸福家园','房屋租售中心')
--and t2.小区名称 in ('普升福邸','蓝爵国际')
)t3
group by t3.小区名称
order by COUNT(t3.rn) desc

-- right outer join



--论坛数据提取
--每个月份当月发帖数据、点赞数据、回复数据、公告数据、指南数据、总发布、同比上月增长值、活跃度*完成度；
--活跃度=（发帖总量-投诉帖）/安装户数，完成度=实际安装户数/目标安装户数

select COUNT(DISTINCT(yy.OWNER_SID)) 截止9月26日总用户数
  FROM (SELECT  a.OWNER_SID
          FROM HOME_OWNER AS a
         WHERE a.CREATED_ON < '20160926'
          and a.OWNER_type = 1
) as yy

select  COUNT(DISTINCT(a.OWNER_SID)) 九月独立访客数
 from Home_OwnerLog as a
left join HOME_OWNER as b
     on a.OWNER_SID = b.OWNER_SID
   where --a.OWNER_SID is not NULL--APP独立访客数
--a.CONTENT like '%邻居圈%'
a.CONTENT like '%物业服务%'
   --and
and a.CREATED_ON >= '20160916'
 and a.CREATED_ON < '20160926'
   and a.CONTENT not like '%园区公告%'
   and a.CONTENT not like'%小区公告%'



--帖子图片数原始关联表
   select a.POST_TYPE ,a.POST_CONTENT,a.POST_IMAGES,a.CREATED_ON,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME ,b.OWNER_NAME,b.FAMILY_NAME ,b.OWNER_TYPE
from HOME_NEIGHBOR_POST a
LEFT join HOME_OWNER b
on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
where a.POST_IMAGES is not null
and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and b.OWNER_TYPE in('1')--用户类型为业主
and b.FAMILY_NAME not in('悦悦')



--总发帖数量
select * from (
select t1.APARTMENT_NAME 小区名称,count(t1.TYPE_SID) 发帖数 from(
select a.POST_TYPE ,a.POST_CONTENT,a.CREATED_ON,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_POST a
LEFT join HOME_OWNER b
on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
where a.CREATED_ON >='20170226'
and a.CREATED_ON <'20170326'
and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and b.OWNER_TYPE like('%1%')--类型为业主
--and b.OWNER_NO like ('%一期%')
)t1
group by t1.APARTMENT_NAME
)t2
order by t2.发帖数 desc


--总回复数量
select * from (
select t2.APARTMENT_NAME ,count(t2.COMMENT_SID) 回复帖数 from(
select t1.*,c.APARTMENT_NAME  from
(
select a.COMMENT_SID ,a.POST_SID,a.COMMENT_CONTENT,a.CREATED_ON,b.POST_SID 主帖ID,b.APARTMENT_SID,b.POST_TYPE,d.TYPE_NAME
from HOME_NEIGHBOR_COMMENT a
left join HOME_NEIGHBOR_POST b on  a.POST_SID = b.POST_SID
left join HOME_OWNER e on b.CREATEDBY =e.OWNER_SID
left join HOME_NEIGHBOR_POST_TYPE d on b.POST_TYPE = d.TYPE_SID
where a.CREATED_ON >='20170226'
and a.CREATED_ON <'20170326'
and b.POST_OKFLAG like('%1%')--剔除屏蔽帖
--and e.OWNER_NO like ('%一期%')
--and d.TYPE_NAME in('')--发帖类型
)t1
left join  HOME_APARTMENT c on t1.APARTMENT_SID = c.APARTMENT_SID
)t2
group by t2.APARTMENT_NAME
)t3
order by t3.回复帖数 desc



--点赞数据
select * from (
select t2.APARTMENT_NAME ,count(t2.LIKE_SID) 点赞帖数 from(
select t1.*,c.APARTMENT_NAME  from
(
select b.POST_SID 主帖ID,b.APARTMENT_SID,b.POST_TYPE,d.TYPE_NAME,a.LIKE_SID,a.POST_SID,a.CREATED_ON,b.POST_CONTENT
from HOME_NEIGHBOR_LIKE a
left join HOME_NEIGHBOR_POST b on  a.POST_SID = b.POST_SID
left join HOME_OWNER e on b.CREATEDBY =e.OWNER_SID
left join HOME_NEIGHBOR_POST_TYPE d on b.POST_TYPE = d.TYPE_SID
where a.CREATED_ON >='20170101'
and a.CREATED_ON <'20170226'
and b.POST_OKFLAG like('%1%')--剔除屏蔽帖
--and e.OWNER_NO like('%一期%')
--and d.TYPE_NAME in('')
)t1
left join  HOME_APARTMENT c on t1.APARTMENT_SID = c.APARTMENT_SID
)t2
group by t2.APARTMENT_NAME
)t3
order by t3.点赞帖数 desc


--公告数据
--无法区分东方福邸一期、二期
select * from (
select t1.APARTMENT_NAME 小区名称,count(t1.NOTICE_SID) 公告数据 from(
select a.NOTICE_SUBJECT,a.NOTICE_CONTENT,a.NOTICE_SID,a.APARTMENT_SID,a.CREATED_ON,b.APARTMENT_NAME
from HOME_NOTICE a
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
where a.FLAG = 0--悦嘉家公告
--and e.OWNER_NO like('%二期%')--无法区分东方福邸一期、二期
and a.CREATED_ON >='20170101'
and a.CREATED_ON <'20170226'
)t1
group by t1.APARTMENT_NAME )t2
order by t2.公告数据 desc


--指南数据
--无法区分东方福邸一期、二期
select * from (
select t1.APARTMENT_NAME ,count(t1.GUIDE_SID) 指南数据
from(
select a.GUIDE_SID,a.GUIDE_TITLE,a.GUIDE_CONTENT,a.CREATED_ON,a.APARTMENT_SID,b.APARTMENT_NAME
from HOME_SERVICE_GUIDE a
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
where  a.CREATED_ON >='20170101'
and a.CREATED_ON <'20170226'
--and e.OWNER_NO like('%一期%')--无法区分东方福邸一期、二期
)t1
group by t1.APARTMENT_NAME
)t2
order by t2.指南数据 desc


--咨询物业帖子明细
select b.FAMILY_NAME,f.FAMILY_NAME,a.POST_TYPE ,a.POST_CONTENT,a.CREATED_ON,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_POST a
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
left join HOME_NEIGHBOR_COMMENT e on a.POST_SID = e.POST_SID
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
where a.CREATED_ON >='20161025'
and a.CREATED_ON <'20161126'
and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and d.TYPE_NAME in('咨询物业')
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
--and (f.FAMILY_NAME  like'%客服%' or f.FAMILY_NAME  like'%管家%')
and b.OWNER_TYPE=1--类型为业主




--提单类型：
--公共维修
--家政服务
--入室维修
--送水
--投诉
--巡检


--10月提报总单数
select t1.小区名称,count(t1.SERVICE_SID) 提报总单数 from
(
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
               END) AS 服务提报状态,a.CREATED_ON 呼叫时间,
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_OWNER d on a.CREATEDBY=d.OWNER_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where APARTMENT_NAME NOT IN('幸福家园')
--and d.OWNER_NO like ('%一期%')
and a.CREATED_ON >='20170101'
and b.CATEGORY_NAME  in('巡检')
--and a.ROOM_NO is not null --巡检下此字段不为空,为业主提报的单子）
and a.CREATED_ON <'20170116'
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
)t1
group by t1.小区名称
order by count(t1.SERVICE_SID) desc
--order by c.APARTMENT_NAME ,a.CREATED_ON desc
-- HOME_SERVICE_CATEGORY :CATEGORY_SID,CATEGORY_NAME
--HOME_SERVICE_PRO :CATEGORY_SID




--提报总单数不含巡检、家政服务
select t1.小区名称,count(t1.SERVICE_SID) 提报总单数
from(
select d.OWNER_NAME,a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,
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
               END) AS 服务提报状态,a.CREATED_ON 呼叫时间,b.CATEGORY_NAME 服务类型,
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY 服务类型ID,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_OWNER d on a.CREATEDBY=d.OWNER_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where c.APARTMENT_NAME NOT IN('幸福家园')
--and c.APARTMENT_NAME IN('东方润园')
--and d.OWNER_NO like ('%一期%')
and b.CATEGORY_NAME not in('巡检','家政服务')
--and a.ROOM_NO is not null --巡检下此字段不为空,为业主提报的单子）
--and a.CREATED_ON >'20161025'
and a.CREATED_ON <'20161126'
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
)t1
group by t1.小区名称
order by count(t1.SERVICE_SID)  desc


--各提单类型小区分布情况
select t2.小区名称,t2.服务类型,count(distinct(t2.SERVICE_SID)) 提报单数 from(
select t1.*,ROW_NUMBER() over (partition by t1.服务类型,t1.TYPE_NAME  order by t1.rn desc ) as rnn
from(
select a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,
ROW_NUMBER() over (partition by c.APARTMENT_NAME,b.CATEGORY_NAME,a.TYPE_NAME order by c.APARTMENT_NAME,b.CATEGORY_NAME,a.TYPE_NAME ) as rn,
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
               END) AS 服务提报状态,a.CREATED_ON 呼叫时间,b.CATEGORY_NAME 服务类型,
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY 服务类型ID,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_OWNER d on a.CREATEDBY=d.OWNER_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where c.APARTMENT_NAME NOT IN('幸福家园')
and c.APARTMENT_NAME not IN('阿里巴巴滨江园区','北师大附中','财富金融中心','崇文实验学校','富越香溪（待开放）','古墩印象城购物中心','海运国际大厦','恒基小区','恒生大厦','华三通信','建行省分行','金橡臻园','体验小区','天城国际','乐佳国际','天恒大厦','西溪科创园','萧山财政局','玉皇山南基金小镇（二期）','浙大科技园','幸福家园')
and c.APARTMENT_NAME not like'%银泰%' and c.APARTMENT_NAME not like'%大厦%'
--and d.OWNER_NO like ('%一期%')
--and b.CATEGORY_NAME not in('巡检','家政服务')
--and a.ROOM_NO is not null --巡检下此字段不为空,为业主提报的单子）
and a.CREATED_ON >='20160101'
and a.CREATED_ON <'20160201'
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
)t1
)t2
group by t2.小区名称,t2.服务类型
order by t2.服务类型, count(distinct(t2.SERVICE_SID)) desc




--各提单类型小区分布情况（细分）
select t2.小区名称,t2.服务类型,t2.TYPE_NAME,t2.rn 提单数 from(
select t1.*,ROW_NUMBER() over (partition by t1.服务类型,t1.TYPE_NAME  order by t1.rn desc ) as rnn
from(
select d.owner_no,a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,
ROW_NUMBER() over (partition by c.APARTMENT_NAME,b.CATEGORY_NAME,a.TYPE_NAME order by c.APARTMENT_NAME,b.CATEGORY_NAME,a.TYPE_NAME ) as rn,
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
               END) AS 服务提报状态,a.CREATED_ON 呼叫时间,b.CATEGORY_NAME 服务类型,
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY 服务类型ID,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_OWNER d on a.CREATEDBY=d.OWNER_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where c.APARTMENT_NAME NOT IN('幸福家园')
and c.APARTMENT_NAME not IN('阿里巴巴滨江园区','北师大附中','财富金融中心','崇文实验学校','富越香溪（待开放）','古墩印象城购物中心','海运国际大厦','恒基小区','恒生大厦','华三通信','建行省分行','体验小区','天城国际','乐佳国际','天恒大厦','西溪科创园','萧山财政局','玉皇山南基金小镇（二期）','浙大科技园','幸福家园')
and c.APARTMENT_NAME not like'%银泰%' and c.APARTMENT_NAME not like'%大厦%'
--and b.CATEGORY_NAME not in('巡检','家政服务')
--and a.ROOM_NO is not null --巡检下此字段不为空,为业主提报的单子）
--and d.owner_no like('%二期%')
and a.CREATED_ON >='20170101'
and a.CREATED_ON <'20170326'
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
)t1
)t2
where t2.rnn=1
order by t2.服务类型,t2.小区名称, t2.rn desc




--流程关闭提单数：a.SERVICE_STATUS：4,6,9
--服务提报状态：（0:等待确认付款    1：呼叫，2：物业响应--已派单   20：转发，21：退回 22:处理中  3：撤消，4：处理完成，待评价，6：流程结束，9：关闭）
select * from (
select t1.小区名称,count(t1.SERVICE_SID) 流程关闭提单数 from(
select a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,
 a.SERVICE_STATUS,a.CREATED_ON 呼叫时间,a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where APARTMENT_NAME NOT IN('幸福家园')
and  a.SERVICE_STATUS in(6,4,9)
and a.CREATED_ON >='20160101'
and a.CREATED_ON <'20170101'
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
--and b.CATEGORY_NAME not in('巡检','家政服务')
--and a.ROOM_NO is not null --巡检下此字段不为空,为业主提报的单子）
)t1
group by t1.小区名称
)t2
order by t2.流程关闭提单数 desc


--提单状态分布
select * from(
select t3.* from(
select t2.*,ROW_NUMBER() over (partition by t2.SERVICE_STATUS,t2.小区名称  order by t2.rn desc ) as rnn
from(
select t1.小区名称,t1.SERVICE_STATUS,ROW_NUMBER() over (partition by t1.小区名称,t1.SERVICE_STATUS  order by t1.小区名称,t1.SERVICE_STATUS ) as rn from(
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
               END) AS SERVICE_STATUS,a.CREATED_ON 呼叫时间,a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where c.APARTMENT_NAME NOT IN('幸福家园','北师大附中','财富金融中心','崇文实验学校','风华新语','富越香溪（待开放）','古墩印象城购物中心','恒基小区','华三通信','金橡臻园','钱塘航空大厦','体验小区','天城国际','天恒大厦','萧山财政局','荀庄','玉皇山南基金小镇（二期）','浙大科技园')
and c.APARTMENT_NAME NOT like ('%银泰%')
and a.CREATED_ON >='20160101'
and a.CREATED_ON <'20170101'
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
--and b.CATEGORY_NAME in('家政服务')
)t1
)t2
)t3
where t3.rnn=1
)t4
order by t4.SERVICE_STATUS,t4.rn desc


--提单状态名称字典表
select*from  HOME_DICTIONARY



--代收快递数（按小区、日汇总）
select t3.小区名称,t3.data 日期,t3.rn 代收快递数 from(
select t2.*,ROW_NUMBER() over (partition by t2.小区名称,t2.data order by t2.rn desc) as rnn from
(select t1.小区名称,t1.data,ROW_NUMBER() over (partition by t1.小区名称,t1.data order by t1.小区名称,t1.data desc) as rn--,count(t1.EXPRESS_SID)
from
(select a.EXPRESS_SID,b.APARTMENT_NAME 小区名称,c.OWNER_NAME 用户名称,a.ROOM_NO 房号,a.EXPRESS_COMPANY 快递公司,a.EXPRESS_NO 快递单号,a.EXPRESS_PHONE 联系电话,a.CREATED_ON 代收时间,a.MODIFIED_ON 领取时间,
(CASE a.EXPRESS_STATUS
                 WHEN '2' THEN
                  '已到站'
                 WHEN '3' THEN
                  '已领取'
                 ELSE
                  ''
               END) AS 快递状态,CAST(a.CREATED_ON AS DATE) data
from HOME_EXPRESS a left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
left join HOME_OWNER c on a.OWNER_SID = c.OWNER_SID
where a.created_on <'20161212'
--and c.owner_no like('%二期%'))t1
)t2)t3
where t3.rnn=1
order by t3.小区名称,t3.data


--按小区汇总，代收快递数
select * from(
select t4.小区名称,sum(t4.代收快递数) 代收快递数 from(
select t3.小区名称,t3.data 日期,t3.rn 代收快递数 from(
select t2.*,ROW_NUMBER() over (partition by t2.小区名称,t2.data order by t2.rn desc) as rnn from
(select t1.小区名称,t1.data,ROW_NUMBER() over (partition by t1.小区名称,t1.data order by t1.小区名称,t1.data desc) as rn--,count(t1.EXPRESS_SID)
from
(select a.EXPRESS_SID,b.APARTMENT_NAME 小区名称,c.OWNER_NAME 用户名称,a.ROOM_NO 房号,a.EXPRESS_COMPANY 快递公司,a.EXPRESS_NO 快递单号,a.EXPRESS_PHONE 联系电话,a.CREATED_ON 代收时间,a.MODIFIED_ON 领取时间,
(CASE a.EXPRESS_STATUS
                 WHEN '2' THEN
                  '已到站'
                 WHEN '3' THEN
                  '已领取'
                 ELSE
                  ''
               END) AS 快递状态,CAST(a.CREATED_ON AS DATE) data
from HOME_EXPRESS a left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
left join HOME_OWNER c on a.OWNER_SID = c.OWNER_SID
where a.created_on <'20170326'
--and c.owner_no like('%二期%')
and a.created_on >='20170226')t1
)t2)t3
where t3.rnn=1
--order by t3.小区名称,t3.data
)t4
group by t4.小区名称
)t5
order by t5.代收快递数 desc



--提单详情表
select a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC 服务提报描述,
a.SERVICE_STATUS,a.PROCESS_DESC 处理描述 ,
a.PROCESS_USER 处理人员,a.PROCESS_TIME 处理时间,a.CREATEDBY  提报用户SID,a.CREATED_ON  提报时间,a.ASSIGN_FROM 指派人员SID,
a.ASSIGN_TO 被指派人员SID,a.ASSIGN_TIME 派单时间,a.EVALUATION_ITEM1,a.EVALUATION_ITEM2,a.EVALUATION_ITEM3,a.REMARK 备注
 from home_service_main a
 where a.CREATED_ON >='20170101'
order by a.CREATED_ON desc




--巡检提单提报情况详细表,测试
select t1.APARTMENT_NAME 项目名称,t1.CATEGORY_NAME,t1.TYPE_NAME, t1.提报用户SID from(
select d.owner_name ,a.SERVICE_SID,c.CATEGORY_NAME,b.APARTMENT_NAME,a.TYPE_SID,a.SERVICE_NO,a.SERVICE_DESC 服务提报描述,
a.SERVICE_STATUS,a.PROCESS_DESC 处理描述 ,A.TYPE_NAME,
a.PROCESS_USER 处理人员,a.PROCESS_TIME 处理时间,a.CREATEDBY  提报用户SID,a.CREATED_ON  提报时间,a.ASSIGN_FROM 指派人员SID,
a.ASSIGN_TO 被指派人员SID,a.RESPONSE_TIME 派单时间,a.EVALUATION_ITEM1,a.EVALUATION_ITEM2,a.EVALUATION_ITEM3,a.REMARK 备注
 from home_service_main a
left join HOME_OWNER d on d.OWNER_SID =a.CREATEDBY
left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
left join HOME_SERVICE_CATEGORY c on a.SERVICE_CATEGORY = c.CATEGORY_SID
 where a.CREATED_ON >='20170215'
and b.APARTMENT_NAME  like('%幸福家园%')
order by a.CREATED_ON desc
--and d.owner_name like('%杨%')
--and a.CREATED_ON <'20170201'
--and c.CATEGORY_NAME like('%巡检%')
)t1
order by t1.提报时间 desc



--及时率增加一个：悦服务下巡检的单子在数据库中的room_no不为空（业主提报）
--响应及时单数（以此为准，新算法，20170112）
--工作时间段（8:30-18:00）提报的单子，15min内响应为及时响应；
--其他时间段提报的单子，9点之前响应为及时响应。
select t7.小区名称,sum(t7.及时单数) from
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
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where APARTMENT_NAME NOT IN('幸福家园')
and b.CATEGORY_NAME in('巡检')
and a.CREATED_ON >='20170101'--当月提报
and a.CREATED_ON <'20170201'
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
and day(a.CREATED_ON)!=16
and day(a.CREATED_ON)!=17
and day(a.CREATED_ON)!=18
and day(a.CREATED_ON)!=19
--and e.OWNER_NO like ('%一期%')
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
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
and b.CATEGORY_NAME in('巡检')
and a.CREATED_ON >='20170101'--当月提报
and a.CREATED_ON <'20170201'
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
and day(a.CREATED_ON)!=16
and day(a.CREATED_ON)!=17
and day(a.CREATED_ON)!=18
and day(a.CREATED_ON)!=19
--and e.OWNER_NO like ('%一期%')
and convert(char(8),a.CREATED_ON,108)>='18:00:00' and  convert(char(8),a.CREATED_ON,108)<'24:00:00'--11
)t4
where t4.响应时间<t4.呼叫时间+1--11
and convert(char(8),t4.响应时间,108)<'09:00:00' --11
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
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where APARTMENT_NAME NOT IN('幸福家园')
and b.CATEGORY_NAME in('巡检')
and a.CREATED_ON >='20170101'--当月提报
and a.CREATED_ON <'20170201'
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
and day(a.CREATED_ON)!=16
and day(a.CREATED_ON)!=17
and day(a.CREATED_ON)!=18
and day(a.CREATED_ON)!=19
--and e.OWNER_NO like ('%一期%')
and convert(char(8),a.CREATED_ON,108)>='00:00:00' and  convert(char(8),a.CREATED_ON,108)<'08:30:00'--11
)t6
where convert(char(8),t6.响应时间,108)<'09:00:00' --111
and CAST(t6.响应时间 AS DATE)= CAST(t6.呼叫时间 AS DATE)--111
)t5
group by t5.小区名称)t7
group by t7.小区名称
order by sum(t7.及时单数) desc





--及时率增加一个：悦服务下巡检的单子在数据库中的room_no不为空（业主提报）
--20170217之前的数据计算响应及时率用此代码（新算法）
--工作时间段（8:30-18:00）提报的单子，15min内响应为及时响应；
--其他时间段提报的单子，9点之前响应为及时响应。
select t7.小区名称,sum(t7.及时单数) 响应及时 from
(select t1.小区名称,count(t1.SERVICE_SID) 及时单数 from
(
select t2.* from(
select a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,a.room_no,
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
and b.CATEGORY_NAME not in('巡检','家政服务')
--and a.ROOM_NO is not null --巡检下此字段不为空,为业主提报的单子）
--and 
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
and b.CATEGORY_NAME not in('巡检','家政服务')
--and a.ROOM_NO is not null --巡检下此字段不为空,为业主提报的单子）
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
and b.CATEGORY_NAME not in('巡检','家政服务')
--and a.ROOM_NO is not null --巡检下此字段不为空,为业主提报的单子）
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




--超时提单详情
select t7.小区名称,t7.服务类型,t7.TYPE_NAME 提报详情,t7.SERVICE_NO 提单号,t7.SERVICE_DESC 备注,t7.服务提报状态,t7.呼叫时间,t7.响应时间,t7.处理时间
 from(
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
and b.CATEGORY_NAME not in('巡检','家政服务')
--and a.ROOM_NO is not null --巡检下此字段不为空,为业主提报的单子）
and a.CREATED_ON >='20170307'--当月提报
and a.CREATED_ON <'20170308'
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
--and day(a.CREATED_ON)!=16
--and day(a.CREATED_ON)!=17
--and day(a.CREATED_ON)!=18
--and day(a.CREATED_ON)!=19
and convert(char(8),a.CREATED_ON,108)>='08:30:00' and  convert(char(8),a.CREATED_ON,108)<'18:00:00'--1
)t2
where t2.响应时间 > t2.响应不超时
--where t2.处理时间 >= t2.处理不超时
union all
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
and b.CATEGORY_NAME not in('巡检','家政服务')
--and a.ROOM_NO is not null --巡检下此字段不为空,为业主提报的单子）
--and e.OWNER_NO like ('%一期%')
and a.CREATED_ON >='20170307'--当月提报
and a.CREATED_ON <'20170308'
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
--and day(a.CREATED_ON)!=16x
--and day(a.CREATED_ON)!=17
--and day(a.CREATED_ON)!=18
--and day(a.CREATED_ON)!=19
and convert(char(8),a.CREATED_ON,108)>='18:00:00' and  convert(char(8),a.CREATED_ON,108)<'24:00:00'--11
)t4
where (CAST(t4.响应时间 AS DATE)= CAST(t4.呼叫时间+1 AS DATE) and  convert(char(8),t4.响应时间,108)>'09:00:00')
or(CAST(t4.响应时间 AS DATE)> CAST(t4.呼叫时间+1 AS DATE))
union ALL
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
and a.CREATED_ON >='20170307'--当月提报
and a.CREATED_ON <'20170308'
and b.CATEGORY_NAME not in('巡检','家政服务')
--and a.ROOM_NO is not null --巡检下此字段不为空,为业主提报的单子）
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
--and day(a.CREATED_ON)!=16
--and day(a.CREATED_ON)!=17
--and day(a.CREATED_ON)!=18
--and day(a.CREATED_ON)!=19
and convert(char(8),a.CREATED_ON,108)>='00:00:00' and  convert(char(8),a.CREATED_ON,108)<'08:30:00'--11
)t6
where convert(char(8),t6.响应时间,108)>'09:00:00' --111
and CAST(t6.响应时间 AS DATE)= CAST(t6.呼叫时间 AS DATE)--111
)t7




--处理及时单数，不含巡检和家政服务（旧算法）
select t1.小区名称,count(t1.SERVICE_SID) 处理及时单数 from
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
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where APARTMENT_NAME NOT IN('幸福家园')
and a.CREATED_ON >='20170101'
and a.CREATED_ON <'20170201'
and b.CATEGORY_NAME not in('巡检','家政服务')
--and a.ROOM_NO is not null --巡检下此字段不为空,为业主提报的单子）
--and day(a.CREATED_ON)!=16
--and day(a.CREATED_ON)!=17
--and day(a.CREATED_ON)!=18
--and day(a.CREATED_ON)!=19
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
--and e.OWNER_NO like ('%一期%')
--and a.PROCESS_TIME >='20170101'--当月处理
--and a.PROCESS_TIME<'20170116'
)t2
--where t2.响应时间 <= t2.响应不超时
where t2.处理时间 <= t2.处理不超时
)t1
group by t1.小区名称
order by count(t1.SERVICE_SID) desc




--按小区汇总提报单数、响应单数、处理单数
--不含巡检/家政
select t1.小区名称,count(t1.呼叫时间) 提报单数,count(t1.响应时间) 响应单数,count(t1.处理时间) 处理单数 from
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
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where APARTMENT_NAME NOT IN('幸福家园')
and a.CREATED_ON >='20170101'
and a.CREATED_ON <'20170226'
and b.CATEGORY_NAME not in('巡检','家政服务')
--and a.ROOM_NO is not null --巡检下此字段不为空,为业主提报的单子）
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
--and day(a.CREATED_ON)!=16
--and day(a.CREATED_ON)!=17
--and day(a.CREATED_ON)!=18
--and day(a.CREATED_ON)!=19
--and e.OWNER_NO like ('%一期%')
--and a.PROCESS_TIME >='20170101'--当月处理
--and a.PROCESS_TIME<'20170116'
)t2
)t1
group by t1.小区名称
order by count(t1.SERVICE_SID) desc




--平均时长数据
--不含巡检/家政
--select t2.小区名称,t2.总响应时长/总单数 平均响应时长 from(
select t2.小区名称,t2.总单数,t2.总处理时长/总单数 平均处理时长 from(
--select t1.小区名称,count(t1.SERVICE_SID) 总单数,sum(t1.响应时长) 总响应时长 from
select t1.小区名称,count(t1.SERVICE_SID) 总单数,sum(t1.处理时长) 总处理时长 from
(
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
--DATEDIFF( mi, a.CREATED_ON, a.RESPONSE_TIME)/60.0 响应时长 ,
DATEDIFF( mi, a.CREATED_ON, a.PROCESS_TIME )/60.0 处理时长 ,
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where APARTMENT_NAME NOT IN('幸福家园')
and b.CATEGORY_NAME not in('巡检','家政服务')
--and a.ROOM_NO is not null --巡检下此字段不为空,为业主提报的单子）
and a.CREATED_ON >='20170101'
and a.CREATED_ON <'20170226'
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
--and e.OWNER_NO like ('%一期%')
and convert(char(8),a.CREATED_ON,108)>='08:30:00' and  convert(char(8),a.CREATED_ON,108)<'18:00:00'--只计算工作时间段提报单子的时长
--and CAST(a.CREATED_ON AS DATE)= CAST(a.RESPONSE_TIME AS DATE)--当天响应
--and and CAST(a.CREATED_ON AS DATE)= CAST(a.PROCESS_TIME  AS DATE)--当天处理
)t1
group by t1.小区名称
)t2




--响应<=1h,处理<=24h
--select t1.小区名称,count(t1.SERVICE_SID) from
select count(t1.SERVICE_SID) from
(
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
DATEDIFF( mi, a.CREATED_ON, a.RESPONSE_TIME)/60.0 响应时长 ,
--DATEDIFF( mi, a.CREATED_ON, a.PROCESS_TIME )/60.0 处理时长 ,
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where APARTMENT_NAME NOT IN('幸福家园')
and a.CREATED_ON <='20161121'
--and a.CREATED_ON >='20160926'
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
--and convert(char(8),a.CREATED_ON,108)>='08:30:00' and  convert(char(8),a.CREATED_ON,108)<'18:00:00'--计算响应时长,只计算工作时间段提报的
)t1
where t1.响应时长 <= 1
--group by t1.小区名称



--巡检单数
--阿里巴巴滨江园区和浙大科技园需要再从JOY_PARK中取值
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
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
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




--巡检提报ID数（事业三部）
--阿里巴巴滨江园区和浙大科技园需要再从JOY_PARK中取值
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
and a.CREATED_ON >='20170301'
and a.CREATED_ON <'20170401'
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
and b.CATEGORY_NAME  in('巡检')
--and day(a.CREATED_ON)!=16
--and day(a.CREATED_ON)!=17
--and day(a.CREATED_ON)!=18
--and day(a.CREATED_ON)!=19
)t1
group by t1.小区名称
order by count(distinct(t1.OWNER_SID))desc
-- HOME_SERVICE_CATEGORY :CATEGORY_SID,CATEGORY_NAME
--HOME_SERVICE_PRO :CATEGORY_SID



--浙大科技园、阿里巴巴滨江园区两个项目取详细表去重求巡检提报ID数(悦园区+悦嘉家后台)
--阿里巴巴滨江园区和浙大科技园需要再从JOY_PARK中取值
select distinct(t1.OWNER_SID) 用户ID,t1.小区名称
 from(
select d.OWNER_SID,a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,
a.CREATED_ON 呼叫时间,
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_OWNER d on d.OWNER_SID = a.CREATEDBY
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where a.CREATED_ON >='20170301'
--and APARTMENT_NAME  IN('浙大科技园','阿里巴巴滨江园区')--这两个项目取详细表与园区版后台去重
and a.CREATED_ON <'20170401'
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
and b.CATEGORY_NAME  in('巡检')
--and day(a.CREATED_ON)!=16--2017年1月份剔除16-19日数据，这四天派单系统不能及时派单
--and day(a.CREATED_ON)!=17
--and day(a.CREATED_ON)!=18
--and day(a.CREATED_ON)!=19
)t1



--截止20170101-20170116小区巡检工作人员提报单数、处理单数（待匹配至工作人员表中）
--阿里巴巴滨江园区和浙大科技园需要再从JOY_PARK中取值
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
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
and a.CREATED_ON <'20170301')t1)t2
group by t2.小区名称,t2.OWNER_NAME,t2.OWNER_PHONE,t2.服务类型



--事业三部权限表提取
--阿里巴巴滨江园区和浙大科技园需要再从JOY_PARK中取值
--匹配工作人员提报巡检及响应情况时需要
--注意：目前权限表工作人员对应的创建时间为空，所以不能限制时间,空的已经全部刷成2015年12月12日
--20170330以后工作人员创建时间均正常
--拥有权限的小区t1.apartment_name
select * from(
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
where t3.部门 like ('%事业三部%') 



--待修正，响应人员(RESPONSE_USER在2017年3月23日之后有数据)
--截至20170118工作人员响应单数
--剔除巡检
select t2.小区名称,t2.OWNER_NAME 姓名,t2.family_name,t2.OWNER_PHONE 手机号码,count(t2.响应时间) 响应单数 from (
select t1.小区名称,t1.OWNER_NAME,t1.family_name,t1.OWNER_PHONE,t1.服务类型,t1.TYPE_NAME,t1.SERVICE_DESC,t1.呼叫时间,t1.响应时间,t1.处理时间,t1.rn from(
select a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,d.OWNER_NAME,d.OWNER_PHONE,d.family_name,
ROW_NUMBER() over (partition by c.APARTMENT_NAME,b.CATEGORY_NAME ,d.OWNER_NAME order by c.APARTMENT_NAME,b.CATEGORY_NAME ) as rn,
a.CREATED_ON 呼叫时间,b.CATEGORY_NAME 服务类型,
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY 服务类型ID,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_OWNER d on a.ASSIGN_FROM = d.OWNER_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where  b.CATEGORY_NAME not like('%巡检%')
and c.apartment_name not in('幸福家园')
--and d.family_name like '%客服%'
--and a.CREATED_ON >='20170101'
and a.RESPONSE_TIME <'20170119')t1)t2
group by t2.小区名称,t2.OWNER_NAME,t2.OWNER_PHONE,t2.family_name
order by count(t2.响应时间) desc




--包括服务类型下细分内容的提单数

select t1.小区名称,t1.服务类型,t1.服务内容, count(t1.SERVICE_SID) 提报单数 from
(
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
               END) AS 服务提报状态,a.CREATED_ON 呼叫时间,
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,a.TYPE_NAME 服务内容,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_OWNER e on e.OWNER_SID = a.CREATEDBY
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where c.APARTMENT_NAME NOT IN('幸福家园','海运国际大厦','恒生大厦','建行省分行','西溪科创园','北师大附中','财富金融中心','崇文实验学校','乐佳国际','培训园区',
'富越香溪（待开放）','古墩印象城购物中心','恒基小区','华三通信','金橡臻园','钱塘航空大厦','体验小区','天城国际','天恒大厦','萧山财政局','联合新苑',
'荀庄','玉皇山南基金小镇（二期）','浙大科技园')
and c.APARTMENT_NAME not like ('%银泰%')
--and e.OWNER_NO like('%一期%')--东方福邸一期、二期分别统计
and a.CREATED_ON >='20170101'
and a.CREATED_ON <'20170226')t1
group by t1.小区名称,t1.服务类型,t1.服务内容
order by t1.服务类型




--根据后台数据，响应时长超过15分钟为超时，处理时长超过1440分钟,即24小时为超时。
--响应时长=响应时间-创建时间，即响应时长>=创建时间+15分钟为超时
--处理时长=处理时间-创建时间，即处理时长>=创建时间+1DAY为超时

--部分投诉巡检提报单数
select t3.小区名称,t3.服务类型,t3.TYPE_NAME 类型,t3.截止20161109总单数
 from (
select t2.*, ROW_NUMBER() over (partition by t2.小区名称,t2.服务类型,t2.TYPE_NAME  order by t2.截止20161109总单数 desc) as rnn from
(
select t1.小区名称,t1.服务类型,t1.TYPE_NAME,
ROW_NUMBER() over (partition by t1.小区名称,t1.服务类型,t1.TYPE_NAME order by t1.小区名称,t1.服务类型,t1.TYPE_NAME desc) as 截止20161109总单数
 from(
select a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,
a.CREATED_ON 呼叫时间,
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where APARTMENT_NAME NOT IN('幸福家园')
and b.CATEGORY_NAME in('投诉','巡检')
--and b.CATEGORY_NAME in('巡检')
and (a.TYPE_NAME  like '%安保%'
or a.TYPE_NAME like '%停车%'
or a.TYPE_NAME  like '%装修%'
or a.TYPE_NAME  like '%停车%'
or a.TYPE_NAME  like '%小区%')
--[投诉]1.安保；2.停车；3.装修；[巡检]4.停车问题；5.小区安全；6.装修问题
)t1
)t2
)t3
where t3.rnn=1
order by t3.小区名称,t3.截止20161109总单数 desc



--部分投诉、巡检汇总截止20161109
select t3.小区名称,t3.服务类型,t3.截止20161109总单数
 from (
select t2.*, ROW_NUMBER() over (partition by t2.小区名称,t2.服务类型  order by t2.截止20161109总单数 desc) as rnn from
(
select t1.小区名称,t1.服务类型,t1.TYPE_NAME,
ROW_NUMBER() over (partition by t1.小区名称,t1.服务类型 order by t1.小区名称,t1.服务类型,t1.TYPE_NAME desc) as 截止20161109总单数
 from(
select a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,
a.CREATED_ON 呼叫时间,
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,c.APARTMENT_NAME 小区名称
from HOME_SERVICE_MAIN  a
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where APARTMENT_NAME NOT IN('幸福家园')
and b.CATEGORY_NAME in('投诉','巡检')
--and b.CATEGORY_NAME in('巡检')
and (a.TYPE_NAME  like '%安保%'
or a.TYPE_NAME like '%停车%'
or a.TYPE_NAME  like '%装修%'
or a.TYPE_NAME  like '%停车%'
or a.TYPE_NAME  like '%小区%')
--[投诉]1.安保；2.停车；3.装修；[巡检]4.停车问题；5.小区安全；6.装修问题
)t1
)t2
)t3
where t3.rnn=1
order by t3.服务类型,t3.截止20161109总单数,t3.小区名称 desc


 --满意度原始表
SELECT  hs.apartment_sid,  ha.apartment_name,  hs.PROCESS_TIME,hs.REMARK,hs.PROCESS_USER ,a.OWNER_NAME,
evaluation_item1,
evaluation_item2 ,
evaluation_item3  , evaluation_item1+evaluation_item2+evaluation_item3
from  home_service_main  hs  left  join  home_apartment  ha  on  hs.apartment_sid  =  ha.apartment_sid
left join home_owner a on hs.PROCESS_USER =a.owner_sid
where  (service_status  =  6  or  service_status  =  9)
and  SERVICE_CATEGORY  in(select CATEGORY_SID from HOME_SERVICE_CATEGORY where CATEGORY_NAME not in('巡检','家政服务'))
and    hs.apartment_sid  in  (select APARTMENT_SID from HOME_APARTMENT)

--满意度原始表测试
SELECT  hs.apartment_sid,  ha.apartment_name,  hs.PROCESS_TIME,hs.REMARK,hs.PROCESS_USER ,a.OWNER_NAME,
evaluation_item1,
evaluation_item2 ,
evaluation_item3  , evaluation_item1+evaluation_item2+evaluation_item3
from  home_service_main  hs  left  join  home_apartment  ha  on  hs.apartment_sid  =  ha.apartment_sid
left join home_owner a on hs.PROCESS_USER =a.owner_sid
where  (service_status  =  6  or  service_status  =  9)
and  SERVICE_CATEGORY  in(select CATEGORY_SID from HOME_SERVICE_CATEGORY where CATEGORY_NAME not in('巡检','家政服务'))
and    hs.apartment_sid  in  (select APARTMENT_SID from HOME_APARTMENT)
and hs.PROCESS_TIME is not null
and 12<=evaluation_item1+evaluation_item2+evaluation_item3
and evaluation_item1+evaluation_item2+evaluation_item3 <15
--and hs.PROCESS_TIME>='20161126'
--and hs.PROCESS_TIME<'20161226'
--and hs.remark not like'%自动好评%'



--十月份项目满意度数据（截止20161027）（剔除巡检+家政）
--1差评，2中评，3好评
--剔除7天自动好评
select  A1.APARTMENT_SID,A1.APARTMENT_NAME,A1.grade,A1.total*100 /a2.total  满意度,A1.groupCase,a1.total  as  selftotal,a2.total  as  alltotal  from  (
                --满意度
                select    APARTMENT_SID  ,APARTMENT_NAME,  'evaluation_item3'  as  groupCase,grade,COUNT(1)  total  FROM  (
                SELECT  hs.apartment_sid,  ha.apartment_name,
                  case  when  evaluation_item3  <  3  then  1
                  when  evaluation_item3  =  3  then  2
                  when  evaluation_item3  >  3  then  3
                  else  0  end  as  grade
                  from  home_service_main  hs  left  join  home_apartment  ha  on  hs.apartment_sid  =  ha.apartment_sid
                  where  (service_status  =  6  or  service_status  =  9)
                                  and  SERVICE_CATEGORY  in(select CATEGORY_SID from HOME_SERVICE_CATEGORY where CATEGORY_NAME not in('巡检','家政服务'))
                                  and  evaluation_item3  is  not  null
                                  and    hs.apartment_sid  in  (select APARTMENT_SID from HOME_APARTMENT
)and hs.PROCESS_TIME>='20160101' and hs.PROCESS_TIME<'20170101'and hs.REMARK not like '%自动好评%'--取月周期满意度，因评价时间大部分为空，所以月周期取处理时间
)  b
                  group  by  apartment_sid,APARTMENT_NAME,  grade
 ) A1

         left join (
        --满意度
         select  APARTMENT_SID,apartment_name,groupCase,COUNT(1) total FROM (
         SELECT hs.apartment_sid, ha.apartment_name, 'evaluation_item3' as groupCase,
         case when evaluation_item3 < 3 then 1
         when evaluation_item3 = 3 then 2
         when evaluation_item3 > 3 then 3
         else 0 end as grade
         from home_service_main hs left join home_apartment ha on hs.apartment_sid = ha.apartment_sid
         where (service_status = 6 or service_status = 9)
                 and SERVICE_CATEGORY in(select CATEGORY_SID from HOME_SERVICE_CATEGORY where CATEGORY_NAME not in('巡检','家政服务'))
                 and evaluation_item3 is not null
                 and  hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT )
and hs.PROCESS_TIME>='20160101' and hs.PROCESS_TIME<'20170101'--取月周期满意度，因评价时间大部分为空，所以月周期取处理时间
) c
         group by apartment_sid,apartment_name,groupCase
       ) A2
         on A1.apartment_sid = A2.apartment_sid and a1.groupCase=a2.groupCase
         where A1.grade =3--筛选出好评的
order by A1.apartment_sid;

--满意度评价表--（剔除家政、巡检）
--grade列 为 满意度-解决速度-服务态度，1差评；2中评；3好评
--item3满意度；item2解决速度；item1服务态度
--selftotal 差/中/好评个数
--alltotal  评价总数
select  A1.APARTMENT_SID,A1.APARTMENT_NAME,A1.grade,A1.total*100 /a2.total  total,A1.groupCase,a1.total  as  selftotal,a2.total  as  alltotal  from  (
                --满意度
                select    APARTMENT_SID  ,APARTMENT_NAME,  'evaluation_item3'  as  groupCase,grade,COUNT(1)  total  FROM  (
                SELECT  hs.apartment_sid,  ha.apartment_name,
                  case  when  evaluation_item3  <  3  then  1
                  when  evaluation_item3  =  3  then  2
                  when  evaluation_item3  >  3  then  3
                  else  0  end  as  grade
                  from  home_service_main  hs  left  join  home_apartment  ha  on  hs.apartment_sid  =  ha.apartment_sid
                  where  (service_status  =  6  or  service_status  =  9)
                                  and  SERVICE_CATEGORY  in(select CATEGORY_SID from HOME_SERVICE_CATEGORY where CATEGORY_NAME not in('巡检','家政服务')  )
                                  and  evaluation_item3  is  not  null
                                  and    hs.apartment_sid  in  (select APARTMENT_SID from HOME_APARTMENT)
and hs.PROCESS_TIME>='20161126' and hs.PROCESS_TIME<'20161226' --取月周期满意度，因评价时间大部分为空，所以月周期取处理时间
)  b
                  group  by  apartment_sid,APARTMENT_NAME,  grade
                --解决速度
                  union  all
                  select    APARTMENT_SID  ,APARTMENT_NAME,  'evaluation_item2'  as  groupCase,grade,COUNT(1)  total  FROM  (
                  SELECT  hs.apartment_sid,  ha.apartment_name,
                  case  when  evaluation_item2  <  3  then  1
                  when  evaluation_item2  =  3  then  2
                  when  evaluation_item2  >  3  then  3
                  else  0  end  as  grade
                  from  home_service_main  hs  left  join  home_apartment  ha  on  hs.apartment_sid  =  ha.apartment_sid
                  where  (service_status  =  6  or  service_status  =  9)
                                  and  SERVICE_CATEGORY  in(select CATEGORY_SID from HOME_SERVICE_CATEGORY where CATEGORY_NAME not in('巡检','家政服务') )
                                  and  evaluation_item2  is  not  null
                                  and    hs.apartment_sid  in  (select APARTMENT_SID from HOME_APARTMENT)
and hs.PROCESS_TIME>='20161126' and hs.PROCESS_TIME<'20161226'--取月周期满意度，因评价时间大部分为空，所以月周期取处理时间
)  b
                  group  by  apartment_sid,APARTMENT_NAME,  grade
                --服务态度
                  union  all
                  select    APARTMENT_SID  ,APARTMENT_NAME,  'evaluation_item1'  as  groupCase,grade,COUNT(1)  total  FROM  (
                  SELECT  hs.apartment_sid,  ha.apartment_name,
                  case  when  evaluation_item1  <  3  then  1
                  when  evaluation_item1  =  3  then  2
                  when  evaluation_item1  >  3  then  3
                  else  0  end  as  grade
                  from  home_service_main  hs  left  join  home_apartment  ha  on  hs.apartment_sid  =  ha.apartment_sid
                  where  (service_status  =  6  or  service_status  =  9)
                                  and  SERVICE_CATEGORY  in(select CATEGORY_SID from HOME_SERVICE_CATEGORY where CATEGORY_NAME not in('巡检','家政服务')  )
                 and evaluation_item1 is not null
                 and  hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)
and hs.PROCESS_TIME>='20161126' and hs.PROCESS_TIME<'20161226' --取月周期满意度，因评价时间大部分为空，所以月周期取处理时间
)
 b
         group by apartment_sid,APARTMENT_NAME, grade  ) A1

         left join (
        --满意度
         select  APARTMENT_SID,apartment_name,groupCase,COUNT(1) total FROM (
         SELECT hs.apartment_sid, ha.apartment_name, 'evaluation_item3' as groupCase,
         case when evaluation_item3 < 3 then 1
         when evaluation_item3 = 3 then 2
         when evaluation_item3 > 3 then 3
         else 0 end as grade
         from home_service_main hs left join home_apartment ha on hs.apartment_sid = ha.apartment_sid
         where (service_status = 6 or service_status = 9)
                 and SERVICE_CATEGORY in(select CATEGORY_SID from HOME_SERVICE_CATEGORY where CATEGORY_NAME not in('巡检','家政服务'))
                 and evaluation_item3 is not null
                 and  hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)
and hs.PROCESS_TIME>='20161126' and hs.PROCESS_TIME<'20161226'--取月周期满意度，因评价时间大部分为空，所以月周期取处理时间
) c
         group by apartment_sid,apartment_name,groupCase
        --解决速度
         union all
         select  APARTMENT_SID,apartment_name,groupCase,COUNT(1) total  FROM (
         SELECT hs.apartment_sid, ha.apartment_name, 'evaluation_item2' as groupCase,
         case when evaluation_item2 < 3 then 1
         when evaluation_item2 = 3 then 2
         when evaluation_item2 > 3 then 3
         else 0 end as grade
         from home_service_main hs left join home_apartment ha on hs.apartment_sid = ha.apartment_sid
         where (service_status = 6 or service_status = 9)
                and SERVICE_CATEGORY in(select CATEGORY_SID from HOME_SERVICE_CATEGORY where CATEGORY_NAME not in('巡检','家政服务'))
                 and evaluation_item2 is not null
                 and  hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)
and hs.PROCESS_TIME>='20161126' and hs.PROCESS_TIME<'20161226' --取月周期满意度，因评价时间大部分为空，所以月周期取处理时间
) c
         group by apartment_sid,apartment_name,groupCase
        --服务态度
         union all
         select  APARTMENT_SID,apartment_name,groupCase,COUNT(1) total  FROM (
         SELECT hs.apartment_sid, ha.apartment_name, 'evaluation_item1' as groupCase,
         case when evaluation_item1 < 3 then 1
         when evaluation_item1 = 3 then 2
         when evaluation_item1 > 3 then 3
         else 0 end as grade
         from home_service_main hs left join home_apartment ha on hs.apartment_sid = ha.apartment_sid
         where (service_status = 6 or service_status = 9)
                 and SERVICE_CATEGORY in(select CATEGORY_SID from HOME_SERVICE_CATEGORY where CATEGORY_NAME not in('巡检','家政服务'))
                 and evaluation_item1 is not null
                 and  hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)
and hs.PROCESS_TIME>='20161126' and hs.PROCESS_TIME<'20161226' --取月周期满意度，因评价时间大部分为空，所以月周期取处理时间
) c  --从小区表中找到对应小区APARTMENT_SID代入
         group by apartment_sid,apartment_name,groupCase  ) A2
         on A1.apartment_sid = A2.apartment_sid and a1.groupCase=a2.groupCase where A1.grade =3--筛选出好评的
order by A2.groupCase,A1.apartment_sid;




--总满意度（未剔除巡检、家政）
select t1.groupCase,sum(t1.selftotal),sum(t1.alltotal),sum(t1.selftotal)*1.0/sum(t1.alltotal) rate from
(
select  A1.APARTMENT_SID,A1.APARTMENT_NAME,A1.grade,A1.total*100 /a2.total  total,A1.groupCase,a1.total  as  selftotal,a2.total  as  alltotal  from  (
                --满意度
                select    APARTMENT_SID  ,APARTMENT_NAME,  'evaluation_item3'  as  groupCase,grade,COUNT(1)  total  FROM  (
                SELECT  hs.apartment_sid,  ha.apartment_name,
                  case  when  evaluation_item3  <  3  then  1
                  when  evaluation_item3  =  3  then  2
                  when  evaluation_item3  >  3  then  3
                  else  0  end  as  grade
                  from  home_service_main  hs  left  join  home_apartment  ha  on  hs.apartment_sid  =  ha.apartment_sid
                  where  (service_status  =  6  or  service_status  =  9)
                                  and  SERVICE_CATEGORY  in(select CATEGORY_SID from HOME_SERVICE_CATEGORY   )
                                  and  evaluation_item3  is  not  null
                                  and    hs.apartment_sid  in  (select APARTMENT_SID from HOME_APARTMENT)
and hs.PROCESS_TIME>='20160101' and hs.PROCESS_TIME<'20170101'--取月周期满意度，因评价时间大部分为空，所以月周期取处理时间
)  b
                  group  by  apartment_sid,APARTMENT_NAME,  grade
                --解决速度
                  union  all
                  select    APARTMENT_SID  ,APARTMENT_NAME,  'evaluation_item2'  as  groupCase,grade,COUNT(1)  total  FROM  (
                  SELECT  hs.apartment_sid,  ha.apartment_name,
                  case  when  evaluation_item2  <  3  then  1
                  when  evaluation_item2  =  3  then  2
                  when  evaluation_item2  >  3  then  3
                  else  0  end  as  grade
                  from  home_service_main  hs  left  join  home_apartment  ha  on  hs.apartment_sid  =  ha.apartment_sid
                  where  (service_status  =  6  or  service_status  =  9)
                                  and  SERVICE_CATEGORY  in(select CATEGORY_SID from HOME_SERVICE_CATEGORY  )
                                  and  evaluation_item2  is  not  null
                                  and    hs.apartment_sid  in  (select APARTMENT_SID from HOME_APARTMENT)
and hs.PROCESS_TIME>='20160101' and hs.PROCESS_TIME<'20170101'--取月周期满意度，因评价时间大部分为空，所以月周期取处理时间
)  b
                  group  by  apartment_sid,APARTMENT_NAME,  grade
                --服务态度
                  union  all
                  select    APARTMENT_SID  ,APARTMENT_NAME,  'evaluation_item1'  as  groupCase,grade,COUNT(1)  total  FROM  (
                  SELECT  hs.apartment_sid,  ha.apartment_name,
                  case  when  evaluation_item1  <  3  then  1
                  when  evaluation_item1  =  3  then  2
                  when  evaluation_item1  >  3  then  3
                  else  0  end  as  grade
                  from  home_service_main  hs  left  join  home_apartment  ha  on  hs.apartment_sid  =  ha.apartment_sid
                  where  (service_status  =  6  or  service_status  =  9)
                                  and  SERVICE_CATEGORY  in(select CATEGORY_SID from HOME_SERVICE_CATEGORY   )
                 and evaluation_item1 is not null
                 and  hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)
and hs.PROCESS_TIME>='20160101' and hs.PROCESS_TIME<'20170101'--取月周期满意度，因评价时间大部分为空，所以月周期取处理时间
)
 b
         group by apartment_sid,APARTMENT_NAME, grade  ) A1

         left join (
        --满意度
         select  APARTMENT_SID,apartment_name,groupCase,COUNT(1) total FROM (
         SELECT hs.apartment_sid, ha.apartment_name, 'evaluation_item3' as groupCase,
         case when evaluation_item3 < 3 then 1
         when evaluation_item3 = 3 then 2
         when evaluation_item3 > 3 then 3
         else 0 end as grade
         from home_service_main hs left join home_apartment ha on hs.apartment_sid = ha.apartment_sid
         where (service_status = 6 or service_status = 9)
                 and SERVICE_CATEGORY in(select CATEGORY_SID from HOME_SERVICE_CATEGORY )
                 and evaluation_item3 is not null
                 and  hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)
and hs.PROCESS_TIME>='20160101' and hs.PROCESS_TIME<'20170101'--取月周期满意度，因评价时间大部分为空，所以月周期取处理时间
) c
         group by apartment_sid,apartment_name,groupCase
        --解决速度
         union all
         select  APARTMENT_SID,apartment_name,groupCase,COUNT(1) total  FROM (
         SELECT hs.apartment_sid, ha.apartment_name, 'evaluation_item2' as groupCase,
         case when evaluation_item2 < 3 then 1
         when evaluation_item2 = 3 then 2
         when evaluation_item2 > 3 then 3
         else 0 end as grade
         from home_service_main hs left join home_apartment ha on hs.apartment_sid = ha.apartment_sid
         where (service_status = 6 or service_status = 9)
                and SERVICE_CATEGORY in(select CATEGORY_SID from HOME_SERVICE_CATEGORY )
                 and evaluation_item2 is not null
                 and  hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)
and hs.PROCESS_TIME>='20160101' and hs.PROCESS_TIME<'20170101'--取月周期满意度，因评价时间大部分为空，所以月周期取处理时间
) c
         group by apartment_sid,apartment_name,groupCase
        --服务态度
         union all
         select  APARTMENT_SID,apartment_name,groupCase,COUNT(1) total  FROM (
         SELECT hs.apartment_sid, ha.apartment_name, 'evaluation_item1' as groupCase,
         case when evaluation_item1 < 3 then 1
         when evaluation_item1 = 3 then 2
         when evaluation_item1 > 3 then 3
         else 0 end as grade
         from home_service_main hs left join home_apartment ha on hs.apartment_sid = ha.apartment_sid
         where (service_status = 6 or service_status = 9)
                 and SERVICE_CATEGORY in(select CATEGORY_SID from HOME_SERVICE_CATEGORY )
                 and evaluation_item1 is not null
                 and  hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)
and hs.PROCESS_TIME>='20160101' and hs.PROCESS_TIME<'20170101'--取月周期满意度，因评价时间大部分为空，所以月周期取处理时间
) c  --从小区表中找到对应小区APARTMENT_SID代入
         group by apartment_sid,apartment_name,groupCase  ) A2
         on A1.apartment_sid = A2.apartment_sid and a1.groupCase=a2.groupCase where A1.grade =3--筛选出好评的
--order by A2.groupCase,A1.apartment_sid;
)t1
group by t1.groupCase


--总满意度（剔除巡检、家政）
select t1.groupCase,sum(t1.selftotal),sum(t1.alltotal),sum(t1.selftotal)*1.0/sum(t1.alltotal) rate from
(
select  A1.APARTMENT_SID,A1.APARTMENT_NAME,A1.grade,A1.total*100 /a2.total  total,A1.groupCase,a1.total  as  selftotal,a2.total  as  alltotal  from  (
                --满意度
                select    APARTMENT_SID  ,APARTMENT_NAME,  'evaluation_item3'  as  groupCase,grade,COUNT(1)  total  FROM  (
                SELECT  hs.apartment_sid,  ha.apartment_name,
                  case  when  evaluation_item3  <  3  then  1
                  when  evaluation_item3  =  3  then  2
                  when  evaluation_item3  >  3  then  3
                  else  0  end  as  grade
                  from  home_service_main  hs  left  join  home_apartment  ha  on  hs.apartment_sid  =  ha.apartment_sid
                  where  (service_status  =  6  or  service_status  =  9)
                                  and  SERVICE_CATEGORY  in(select CATEGORY_SID from HOME_SERVICE_CATEGORY where CATEGORY_NAME not in('巡检','家政服务')  )
                                  and  evaluation_item3  is  not  null
                                  and    hs.apartment_sid  in  (select APARTMENT_SID from HOME_APARTMENT)
and hs.PROCESS_TIME>='20160101' and hs.PROCESS_TIME<'20170101'--取月周期满意度，因评价时间大部分为空，所以月周期取处理时间
)  b
                  group  by  apartment_sid,APARTMENT_NAME,  grade
                --解决速度
                  union  all
                  select    APARTMENT_SID  ,APARTMENT_NAME,  'evaluation_item2'  as  groupCase,grade,COUNT(1)  total  FROM  (
                  SELECT  hs.apartment_sid,  ha.apartment_name,
                  case  when  evaluation_item2  <  3  then  1
                  when  evaluation_item2  =  3  then  2
                  when  evaluation_item2  >  3  then  3
                  else  0  end  as  grade
                  from  home_service_main  hs  left  join  home_apartment  ha  on  hs.apartment_sid  =  ha.apartment_sid
                  where  (service_status  =  6  or  service_status  =  9)
                                  and  SERVICE_CATEGORY  in(select CATEGORY_SID from HOME_SERVICE_CATEGORY where CATEGORY_NAME not in('巡检','家政服务') )
                                  and  evaluation_item2  is  not  null
                                  and    hs.apartment_sid  in  (select APARTMENT_SID from HOME_APARTMENT)
and hs.PROCESS_TIME>='20160101' and hs.PROCESS_TIME<'20170101'--取月周期满意度，因评价时间大部分为空，所以月周期取处理时间
)  b
                  group  by  apartment_sid,APARTMENT_NAME,  grade
                --服务态度
                  union  all
                  select    APARTMENT_SID  ,APARTMENT_NAME,  'evaluation_item1'  as  groupCase,grade,COUNT(1)  total  FROM  (
                  SELECT  hs.apartment_sid,  ha.apartment_name,
                  case  when  evaluation_item1  <  3  then  1
                  when  evaluation_item1  =  3  then  2
                  when  evaluation_item1  >  3  then  3
                  else  0  end  as  grade
                  from  home_service_main  hs  left  join  home_apartment  ha  on  hs.apartment_sid  =  ha.apartment_sid
                  where  (service_status  =  6  or  service_status  =  9)
                                  and  SERVICE_CATEGORY  in(select CATEGORY_SID from HOME_SERVICE_CATEGORY where CATEGORY_NAME not in('巡检','家政服务')  )
                 and evaluation_item1 is not null
                 and  hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)
and hs.PROCESS_TIME>='20160101' and hs.PROCESS_TIME<'20170101'--取月周期满意度，因评价时间大部分为空，所以月周期取处理时间
)
 b
         group by apartment_sid,APARTMENT_NAME, grade  ) A1

         left join (
        --满意度
         select  APARTMENT_SID,apartment_name,groupCase,COUNT(1) total FROM (
         SELECT hs.apartment_sid, ha.apartment_name, 'evaluation_item3' as groupCase,
         case when evaluation_item3 < 3 then 1
         when evaluation_item3 = 3 then 2
         when evaluation_item3 > 3 then 3
         else 0 end as grade
         from home_service_main hs left join home_apartment ha on hs.apartment_sid = ha.apartment_sid
         where (service_status = 6 or service_status = 9)
                 and SERVICE_CATEGORY in(select CATEGORY_SID from HOME_SERVICE_CATEGORY where CATEGORY_NAME not in('巡检','家政服务'))
                 and evaluation_item3 is not null
                 and  hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)
and hs.PROCESS_TIME>='20160101' and hs.PROCESS_TIME<'20170101'--取月周期满意度，因评价时间大部分为空，所以月周期取处理时间
) c
         group by apartment_sid,apartment_name,groupCase
        --解决速度
         union all
         select  APARTMENT_SID,apartment_name,groupCase,COUNT(1) total  FROM (
         SELECT hs.apartment_sid, ha.apartment_name, 'evaluation_item2' as groupCase,
         case when evaluation_item2 < 3 then 1
         when evaluation_item2 = 3 then 2
         when evaluation_item2 > 3 then 3
         else 0 end as grade
         from home_service_main hs left join home_apartment ha on hs.apartment_sid = ha.apartment_sid
         where (service_status = 6 or service_status = 9)
                and SERVICE_CATEGORY in(select CATEGORY_SID from HOME_SERVICE_CATEGORY where CATEGORY_NAME not in('巡检','家政服务'))
                 and evaluation_item2 is not null
                 and  hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)
and hs.PROCESS_TIME>='20160101' and hs.PROCESS_TIME<'20170101'--取月周期满意度，因评价时间大部分为空，所以月周期取处理时间
) c
         group by apartment_sid,apartment_name,groupCase
        --服务态度
         union all
         select  APARTMENT_SID,apartment_name,groupCase,COUNT(1) total  FROM (
         SELECT hs.apartment_sid, ha.apartment_name, 'evaluation_item1' as groupCase,
         case when evaluation_item1 < 3 then 1
         when evaluation_item1 = 3 then 2
         when evaluation_item1 > 3 then 3
         else 0 end as grade
         from home_service_main hs left join home_apartment ha on hs.apartment_sid = ha.apartment_sid
         where (service_status = 6 or service_status = 9)
                 and SERVICE_CATEGORY in(select CATEGORY_SID from HOME_SERVICE_CATEGORY where CATEGORY_NAME not in('巡检','家政服务'))
                 and evaluation_item1 is not null
                 and  hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)
and hs.PROCESS_TIME>='20160101' and hs.PROCESS_TIME<'20170101'--取月周期满意度，因评价时间大部分为空，所以月周期取处理时间
) c  --从小区表中找到对应小区APARTMENT_SID代入
         group by apartment_sid,apartment_name,groupCase  ) A2
         on A1.apartment_sid = A2.apartment_sid and a1.groupCase=a2.groupCase where A1.grade =3--筛选出好评的
--order by A2.groupCase,A1.apartment_sid;
)t1
group by t1.groupCase




--满意度（综合服务态度、解决速度、满意度）个人(python或者以此为准)
select a1.apartment_name 项目,a1.OWNER_NAME 姓名,a1.FAMILY_NAME 昵称,a1.rn 评价数,a2.rn 好评数,round(convert(float,a2.rn)/convert(float,a1.rn),2) 好评度,
a3.rn 剔除后评价数,a4.rn 剔除后好评数,
 round(convert(float,a4.rn)/convert(float,a3.rn),2) 剔除后好评度 from
(select * from (
select t2.PROCESS_USER,t2.apartment_name,t2.OWNER_NAME ,t2.FAMILY_NAME,t2.rn,
ROW_NUMBER() over (partition by t2.apartment_name,t2.OWNER_NAME,t2.FAMILY_NAME,t2.OWNER_PHONE order by t2.apartment_name,t2.OWNER_NAME,t2.rn desc ) as rnn from(
select t1.apartment_name,t1.OWNER_NAME,t1.OWNER_PHONE,t1.PROCESS_USER,t1.FAMILY_NAME,
ROW_NUMBER() over (partition by t1.OWNER_NAME,t1.FAMILY_NAME,t1.OWNER_PHONE order by t1.OWNER_NAME,t1.FAMILY_NAME,t1.OWNER_PHONE ) as rn from(
SELECT ha.apartment_name,hs.PROCESS_TIME,hs.REMARK,hs.PROCESS_USER,a.OWNER_SID,a.OWNER_NAME,a.FAMILY_NAME,a.OWNER_PHONE,
evaluation_item1,evaluation_item2,evaluation_item3  , evaluation_item1+evaluation_item2+evaluation_item3 总分
from home_service_main hs
left join home_owner a on hs.PROCESS_USER=a.owner_sid
left join home_apartment ha on hs.apartment_sid=ha.apartment_sid
where  (service_status  =  6  or  service_status  =  9)
and SERVICE_CATEGORY in(select CATEGORY_SID
from HOME_SERVICE_CATEGORY
where CATEGORY_NAME not in('巡检','家政服务'))and
 hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)and hs.PROCESS_TIME is not null
--and evaluation_item1+evaluation_item2+evaluation_item3>=12
and hs.PROCESS_TIME>='20160101'
and hs.PROCESS_TIME<'20170101'
--and a.OWNER_NO like ('%一期%')
)t1)t2)t3
where rnn=1
)a1
left join (
select * from (
select t2.PROCESS_USER,t2.apartment_name,t2.OWNER_NAME ,t2.FAMILY_NAME,t2.rn,
ROW_NUMBER() over (partition by t2.apartment_name,t2.OWNER_NAME,t2.FAMILY_NAME,t2.OWNER_PHONE order by t2.apartment_name,t2.OWNER_NAME,t2.rn desc ) as rnn from(
select t1.apartment_name,t1.OWNER_NAME,t1.OWNER_PHONE,t1.PROCESS_USER,t1.FAMILY_NAME,
ROW_NUMBER() over (partition by t1.OWNER_NAME,t1.FAMILY_NAME,t1.OWNER_PHONE order by t1.OWNER_NAME,t1.FAMILY_NAME,t1.OWNER_PHONE ) as rn from(
SELECT ha.apartment_name,hs.PROCESS_TIME,hs.REMARK,hs.PROCESS_USER,a.OWNER_SID,a.OWNER_NAME,a.FAMILY_NAME,a.OWNER_PHONE,
evaluation_item1,evaluation_item2,evaluation_item3  , evaluation_item1+evaluation_item2+evaluation_item3 总分
from home_service_main hs
left join home_owner a on hs.PROCESS_USER=a.owner_sid
left join home_apartment ha on hs.apartment_sid=ha.apartment_sid
where  (service_status  =  6  or  service_status  =  9)
and SERVICE_CATEGORY in(select CATEGORY_SID
from HOME_SERVICE_CATEGORY
where CATEGORY_NAME not in('巡检','家政服务'))and
 hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)and hs.PROCESS_TIME is not null
and evaluation_item1+evaluation_item2+evaluation_item3>=12
and hs.PROCESS_TIME>='20160101'
and hs.PROCESS_TIME<'20170101'
--and a.OWNER_NO like ('%一期%')
)t1)t2)t3
where t3.rnn=1
)a2 on a1.PROCESS_USER=a2.PROCESS_USER
left join 
(select * from (
select t2.PROCESS_USER,t2.apartment_name,t2.OWNER_NAME ,t2.FAMILY_NAME,t2.rn,
ROW_NUMBER() over (partition by t2.apartment_name,t2.OWNER_NAME,t2.FAMILY_NAME,t2.OWNER_PHONE order by t2.apartment_name,t2.OWNER_NAME,t2.rn desc ) as rnn from(
select t1.apartment_name,t1.OWNER_NAME,t1.OWNER_PHONE,t1.PROCESS_USER,t1.FAMILY_NAME,
ROW_NUMBER() over (partition by t1.OWNER_NAME,t1.FAMILY_NAME,t1.OWNER_PHONE order by t1.OWNER_NAME,t1.FAMILY_NAME,t1.OWNER_PHONE ) as rn from(
SELECT ha.apartment_name,hs.PROCESS_TIME,hs.REMARK,hs.PROCESS_USER,a.OWNER_SID,a.OWNER_NAME,a.FAMILY_NAME,a.OWNER_PHONE,
evaluation_item1,evaluation_item2,evaluation_item3  , evaluation_item1+evaluation_item2+evaluation_item3 总分
from home_service_main hs
left join home_owner a on hs.PROCESS_USER=a.owner_sid
left join home_apartment ha on hs.apartment_sid=ha.apartment_sid
where  (service_status  =  6  or  service_status  =  9)
and SERVICE_CATEGORY in(select CATEGORY_SID
from HOME_SERVICE_CATEGORY
where CATEGORY_NAME not in('巡检','家政服务'))and
 hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)and hs.PROCESS_TIME is not null
and evaluation_item1+evaluation_item2+evaluation_item3 <15  --如果总分小于15,则不考虑自动好评的条件
or(not evaluation_item1+evaluation_item2+evaluation_item3 <15 and hs.REMARK not like ('%自动好评%'))
--and evaluation_item1+evaluation_item2+evaluation_item3>=12
and hs.PROCESS_TIME>='20160101'
and hs.PROCESS_TIME<'20170101'
--and a.OWNER_NO like ('%一期%')
)t1)t2)t3
where rnn=1
)a3 on a2.PROCESS_USER=a3.PROCESS_USER
left join
(
select * from (
select t2.PROCESS_USER,t2.apartment_name,t2.OWNER_NAME ,t2.FAMILY_NAME,t2.rn,
ROW_NUMBER() over (partition by t2.apartment_name,t2.OWNER_NAME,t2.FAMILY_NAME,t2.OWNER_PHONE order by t2.apartment_name,t2.OWNER_NAME,t2.rn desc ) as rnn from(
select t1.apartment_name,t1.OWNER_NAME,t1.OWNER_PHONE,t1.PROCESS_USER,t1.FAMILY_NAME,
ROW_NUMBER() over (partition by t1.OWNER_NAME,t1.FAMILY_NAME,t1.OWNER_PHONE order by t1.OWNER_NAME,t1.FAMILY_NAME,t1.OWNER_PHONE ) as rn from(
SELECT ha.apartment_name,hs.PROCESS_TIME,hs.REMARK,hs.PROCESS_USER,a.OWNER_SID,a.OWNER_NAME,a.FAMILY_NAME,a.OWNER_PHONE,
evaluation_item1,evaluation_item2,evaluation_item3  , evaluation_item1+evaluation_item2+evaluation_item3 总分
from home_service_main hs
left join home_owner a on hs.PROCESS_USER=a.owner_sid
left join home_apartment ha on hs.apartment_sid=ha.apartment_sid
where  (service_status  =  6  or  service_status  =  9)
and SERVICE_CATEGORY in(select CATEGORY_SID
from HOME_SERVICE_CATEGORY
where CATEGORY_NAME not in('巡检','家政服务'))and
 hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)and hs.PROCESS_TIME is not null
 and evaluation_item1+evaluation_item2+evaluation_item3>=12
and evaluation_item1+evaluation_item2+evaluation_item3 <15  --如果总分小于15,则不考虑自动好评的条件
or(not evaluation_item1+evaluation_item2+evaluation_item3 <15 and hs.REMARK not like ('%自动好评%'))

and hs.PROCESS_TIME>='20160101'
and hs.PROCESS_TIME<'20170101'
--and a.OWNER_NO like ('%一期%')
)t1)t2)t3
where rnn=1)a4 on a3.PROCESS_USER=a4.PROCESS_USER
where a1.OWNER_NAME not like('%悦嘉家%')
and a1.apartment_name not in ('幸福家园','体验小区')
order by a3.rn desc





--满意度（综合服务态度、解决速度、满意度）项目(python或者以此为准)
select a1.apartment_name 项目,a1.rn 评价数,a2.rn 好评数,round(convert(float,a2.rn)/convert(float,a1.rn),2) 好评度,
a3.rn 剔除后评价数,a4.rn 剔除后好评数,
 round(convert(float,a4.rn)/convert(float,a3.rn),2) 剔除后好评度 from
(select * from (
select t2.apartment_name,t2.rn,
ROW_NUMBER() over (partition by t2.apartment_name order by t2.apartment_name,t2.rn desc ) as rnn from(
select t1.apartment_name,t1.OWNER_NAME,t1.OWNER_PHONE,t1.PROCESS_USER,t1.FAMILY_NAME,
ROW_NUMBER() over (partition by t1.apartment_name order by t1.apartment_name ) as rn from(
SELECT ha.apartment_name,hs.PROCESS_TIME,hs.REMARK,hs.PROCESS_USER,a.OWNER_SID,a.OWNER_NAME,a.FAMILY_NAME,a.OWNER_PHONE,
evaluation_item1,evaluation_item2,evaluation_item3  , evaluation_item1+evaluation_item2+evaluation_item3 总分
from home_service_main hs
left join home_owner a on hs.PROCESS_USER=a.owner_sid
left join home_apartment ha on hs.apartment_sid=ha.apartment_sid
where  (service_status  =  6  or  service_status  =  9)
and SERVICE_CATEGORY in(select CATEGORY_SID
from HOME_SERVICE_CATEGORY
where CATEGORY_NAME not in('巡检','家政服务'))and
 hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)and hs.PROCESS_TIME is not null
--and evaluation_item1+evaluation_item2+evaluation_item3>=12
and hs.PROCESS_TIME>='20160101'
and hs.PROCESS_TIME<'20170101'
--and a.OWNER_NO like ('%一期%')
)t1)t2)t3
where rnn=1
)a1
left join (
select * from (
select t2.apartment_name,t2.rn,
ROW_NUMBER() over (partition by t2.apartment_name order by t2.apartment_name,t2.rn desc ) as rnn from(
select t1.apartment_name,t1.OWNER_NAME,t1.OWNER_PHONE,t1.PROCESS_USER,t1.FAMILY_NAME,
ROW_NUMBER() over (partition by t1.apartment_name order by t1.apartment_name ) as rn from(
SELECT ha.apartment_name,hs.PROCESS_TIME,hs.REMARK,hs.PROCESS_USER,a.OWNER_SID,a.OWNER_NAME,a.FAMILY_NAME,a.OWNER_PHONE,
evaluation_item1,evaluation_item2,evaluation_item3  , evaluation_item1+evaluation_item2+evaluation_item3 总分
from home_service_main hs
left join home_owner a on hs.PROCESS_USER=a.owner_sid
left join home_apartment ha on hs.apartment_sid=ha.apartment_sid
where  (service_status  =  6  or  service_status  =  9)
and SERVICE_CATEGORY in(select CATEGORY_SID
from HOME_SERVICE_CATEGORY
where CATEGORY_NAME not in('巡检','家政服务'))and
 hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)and hs.PROCESS_TIME is not null
and evaluation_item1+evaluation_item2+evaluation_item3>=12
and hs.PROCESS_TIME>='20160101'
and hs.PROCESS_TIME<'20170101'
--and a.OWNER_NO like ('%一期%')
)t1)t2)t3
where t3.rnn=1
)a2 on a1.apartment_name=a2.apartment_name
left join 
(select * from (
select t2.apartment_name,t2.rn,
ROW_NUMBER() over (partition by t2.apartment_name order by t2.apartment_name,t2.rn desc ) as rnn from(
select t1.apartment_name,t1.OWNER_NAME,t1.OWNER_PHONE,t1.PROCESS_USER,t1.FAMILY_NAME,
ROW_NUMBER() over (partition by t1.apartment_name order by t1.apartment_name ) as rn from(
SELECT ha.apartment_name,hs.PROCESS_TIME,hs.REMARK,hs.PROCESS_USER,a.OWNER_SID,a.OWNER_NAME,a.FAMILY_NAME,a.OWNER_PHONE,
evaluation_item1,evaluation_item2,evaluation_item3  , evaluation_item1+evaluation_item2+evaluation_item3 总分
from home_service_main hs
left join home_owner a on hs.PROCESS_USER=a.owner_sid
left join home_apartment ha on hs.apartment_sid=ha.apartment_sid
where  (service_status  =  6  or  service_status  =  9)
and SERVICE_CATEGORY in(select CATEGORY_SID
from HOME_SERVICE_CATEGORY
where CATEGORY_NAME not in('巡检','家政服务'))and
 hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)and hs.PROCESS_TIME is not null
and evaluation_item1+evaluation_item2+evaluation_item3 <15  --如果总分小于15,则不考虑自动好评的条件
or(not evaluation_item1+evaluation_item2+evaluation_item3 <15 and hs.REMARK not like ('%自动好评%'))
--and evaluation_item1+evaluation_item2+evaluation_item3>=12
and hs.PROCESS_TIME>='20160101'
and hs.PROCESS_TIME<'20170101'
--and a.OWNER_NO like ('%一期%')
)t1)t2)t3
where rnn=1
)a3 on a2.apartment_name=a3.apartment_name
left join
(
select * from (
select t2.apartment_name,t2.rn,
ROW_NUMBER() over (partition by t2.apartment_name order by t2.apartment_name,t2.rn desc ) as rnn from(
select t1.apartment_name,t1.OWNER_NAME,t1.OWNER_PHONE,t1.PROCESS_USER,t1.FAMILY_NAME,
ROW_NUMBER() over (partition by t1.apartment_name order by t1.apartment_name ) as rn from(
SELECT ha.apartment_name,hs.PROCESS_TIME,hs.REMARK,hs.PROCESS_USER,a.OWNER_SID,a.OWNER_NAME,a.FAMILY_NAME,a.OWNER_PHONE,
evaluation_item1,evaluation_item2,evaluation_item3  , evaluation_item1+evaluation_item2+evaluation_item3 总分
from home_service_main hs
left join home_owner a on hs.PROCESS_USER=a.owner_sid
left join home_apartment ha on hs.apartment_sid=ha.apartment_sid
where  (service_status  =  6  or  service_status  =  9)
and SERVICE_CATEGORY in(select CATEGORY_SID
from HOME_SERVICE_CATEGORY
where CATEGORY_NAME not in('巡检','家政服务'))and
 hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)and hs.PROCESS_TIME is not null
 and evaluation_item1+evaluation_item2+evaluation_item3>=12
and evaluation_item1+evaluation_item2+evaluation_item3 <15  --如果总分小于15,则不考虑自动好评的条件
or(not evaluation_item1+evaluation_item2+evaluation_item3 <15 and hs.REMARK not like ('%自动好评%'))
and hs.PROCESS_TIME>='20160101'
and hs.PROCESS_TIME<'20170101'
--and a.OWNER_NO like ('%一期%')
)t1)t2)t3
where rnn=1)a4 on a3.apartment_name=a4.apartment_name
where a1.apartment_name not in ('幸福家园','体验小区')
order by a3.rn desc





-- SELECT * FROM HOME_APARTMENT

--select* from  HOME_SERVICE_CATEGORY
--评价取自HOME_SERVICE_CATEGORY中有的类型，SID






--APP安装量
select t3.小区名称,t3.rn 安装量 from
(
select t2.*, ROW_NUMBER() over (partition by t2.小区名称 order by t2.rn desc) as rnn from
(
select t1.*, ROW_NUMBER() over (partition by t1.小区名称 order by t1.小区名称 desc) as rn from
(
select b.APARTMENT_NAME 小区名称,a.DEVICE_TYPE 设备类型,a.DEVICE_TOKEN 设备TOKEN,
(CASE  a.DEVICE_APP
 WHEN '10'
 THEN '安卓业主版'
 WHEN '11'
 THEN '安卓物业版'
 WHEN '20'
 THEN 'ios业主版'
 WHEN '21'
 THEN 'app store业主大客户版本'
  WHEN '22'
 THEN '物业大客户版本'
 WHEN '23'
 THEN 'app store物业'
 ELSE '' END)
 AS APP标识,
a.DEVICE_APP APP版本,
a.DEVICE_LNG 设备纬度,
a.DEVICE_LAT 设备经度,
a.CREATED_ON 创建时间
from HOME_DEVICE a
left join HOME_APARTMENT b
     on a.APARTMENT_SID=b.APARTMENT_SID
--where a.CREATED_ON >= '20160801'限制时间
)t1
)t2
)t3
where t3.rnn=1 order by t3.rn desc


--投诉帖数量
  select t3.小区名称,t3.rn 投诉帖数量 from
(
select t2.*, ROW_NUMBER() over (partition by t2.小区名称 order by t2.rn desc) as rnn from
(
select t1.*, ROW_NUMBER() over (partition by t1.小区名称 order by t1.小区名称 desc) as rn from
(
select gg.小区名称,gg.发帖分类名称,gg.创建时间,gg.修改时间,gg.业主房号,gg.发帖内容,gg.回复内容,gg.发帖人昵称,gg.发帖人,gg.回复人昵称,gg.发帖人手机号码 from
(select ROW_NUMBER() over (partition by a.POST_SID order by a.CREATED_ON) as ROWNUM,a.CREATED_ON 创建时间,a.MODIFIED_ON 修改时间,e.APARTMENT_NAME 小区名称, d.TYPE_NAME 发帖分类名称,g.OWNER_NO 业主房号,f.ROOM_NO 房号,a.POST_CONTENT 发帖内容,b.COMMENT_CONTENT 回复内容,
 g.OWNER_PHONE 发帖人手机号码,g.FAMILY_NAME 发帖人昵称,c.FAMILY_NAME 回复人昵称 ,c.REMARK 备注,
g.OWNER_NAME 发帖人,
--c.GROUP_SID 集团SID,c.DEPT_SID 物业人员部门SID,a.CREATEDBY 创建用户SID,b.AT_OWNER 回复用户SID,a.MODIFIEDBY 修改用户SID,a.POST_TYPE 发帖分类,a.APARTMENT_SID 小区SID,g.APARTMENT_TAG 房号标识,
(CASE  g.REGISTER_TYPE
 WHEN '0'
 THEN '普通注册'
 WHEN '1'
 THEN '邀请码注册'
 ELSE '' END)
 AS 注册类型,
(CASE  g.OWNER_STATUS
 WHEN '0'
 THEN '停用'
 WHEN '1'
 THEN '启用'
 ELSE '' END)
 AS 用户状态,
(CASE a.POST_TAG
 WHEN '0'
 THEN '否'
 WHEN '1'
 THEN '是'
 ELSE '' END)
 AS 是否保存到草稿箱,
(CASE  g.OWNER_TYPE
 WHEN '1'
 THEN '小区用户'
 WHEN '2'
 THEN '小区服务人员'
 WHEN '3'
 THEN '集团服务人员'
 WHEN '4'
 THEN '小区管理员'
 WHEN '5'
 THEN '集团管理员'
 ELSE '' END)
 AS 发帖用户类型,
(CASE  g.OWNER_CATEGORY
 WHEN '0'
 THEN '业主'
 WHEN '1'
 THEN '租户'
 WHEN '2'
 THEN '家属'
 ELSE '' END)
 AS 业主类型
from
 (HOME_NEIGHBOR_POST
 as a
 left join HOME_NEIGHBOR_COMMENT as b
  on a.POST_SID = b.POST_SID)
 left join HOME_OWNER as c
  on b.CREATEDBY = c.OWNER_SID
 left join HOME_OWNER as g
  on a.CREATEDBY = g.OWNER_SID
 left join HOME_NEIGHBOR_POST_TYPE as d
  on a.POST_TYPE=d.TYPE_SID
 left join HOME_APARTMENT as e
  on a.APARTMENT_SID = e.APARTMENT_SID
 left join HOME_ROOM as f
  on g.OWNER_PHONE = f.OWNER_PHONE
 where g.OWNER_TYPE in  ('1')
and c.FAMILY_NAME like '%客服%'
and b.COMMENT_CONTENT like '%业主您好%'
and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and g.OWNER_NO not like '%物业%'
and (a.POST_CONTENT not like '%赞%'and a.POST_CONTENT not like '%感谢%'and a.POST_CONTENT not like '%建议%' and a.POST_CONTENT not like '%帮%'
and
 a.POST_CONTENT not like '%表扬%'and a.POST_CONTENT not like '%给力%'and a.POST_CONTENT not like '%可能%'and a.POST_CONTENT not like '%求租%'
and a.POST_CONTENT not like '%推荐%'and a.POST_CONTENT not like '%不知%'and a.POST_CONTENT not like '%请%'and a.POST_CONTENT not like '%希望%')
and (a.POST_CONTENT like '%吗%' or a.POST_CONTENT like '%物业%' or a.POST_CONTENT like '%？%' or a.POST_CONTENT like '%！%'or a.POST_CONTENT like '%垃圾%'
or a.POST_CONTENT like '%装修%' or a.POST_CONTENT like '%坏%' or a.POST_CONTENT like '%维修%' or a.POST_CONTENT like '%脏%' or a.POST_CONTENT like '%绿化%'
or a.POST_CONTENT like '%清理%'or a.POST_CONTENT like '%修理%'or a.POST_CONTENT like '%差%'or a.POST_CONTENT like '%草%')
and a.CREATED_ON < '20161126'--筛选时间
and a.CREATED_ON >= '20161025'
)gg
where gg.ROWNUM = 1
)t1
)t2
)t3
where rnn=1 order by t3.rn desc

--论坛数据提取 方法2
 select a.CREATED_ON 创建时间,a.MODIFIED_ON 修改时间,e.APARTMENT_NAME 小区名称, d.TYPE_NAME 发帖分类名称,g.OWNER_NO 业主房号,f.ROOM_NO 房号,a.POST_CONTENT 发帖内容,
 a.POST_TAG 是否保存到草稿箱0为否,g.OWNER_PHONE 发帖人手机号码,g.FAMILY_NAME 发帖人昵称,c.FAMILY_NAME 回复人昵称,g.OWNER_STATUS 用户状态0停用 ,c.REMARK 备注,
g.OWNER_CATEGORY 业主类型,g.OWNER_TYPE 用户类型 ,g.REGISTER_TYPE 注册类型,g.OWNER_NAME 发帖人
--c.GROUP_SID 集团SID,c.DEPT_SID 物业人员部门SID,a.CREATEDBY 创建用户SID,b.AT_OWNER 回复用户SID,a.MODIFIEDBY 修改用户SID,a.POST_TYPE 发帖分类,a.APARTMENT_SID 小区SID,g.APARTMENT_TAG 房号标识,
 --  OWNER_TYPE用户类型 * 1 - 小区用户   2 - 小区服务人员  3 - 集团服务人员   4 - 小区管理员   5 - 集团管理员
--g.OWNER_CATEGORY 业主类型（0：业主，1：租户，2：家属）
----注册类型： 0= 普通注册 1=邀请码注册
from
 (HOME_NEIGHBOR_POST
 as a
 left join
(SELECT MAX(COMMENT_SID) as COMMENT_SID ,MAX(POST_SID) as POST_SID  ,MAX(cast( COMMENT_CONTENT as varchar)) as COMMENT_CONTENT ,MAX(COMMENT_OWNER) as COMMENT_OWNER,MAX(AT_OWNER) as AT_OWNER  ,MAX(CREATEDBY) as CREATEDBY,  MAX(CREATED_ON) as CREATED_ON
FROM
HOME_NEIGHBOR_COMMENT GROUP  BY post_sid ) as b
  on a.POST_SID = b.POST_SID)
 left join HOME_OWNER as c
  on b.CREATEDBY = c.OWNER_SID
 left join HOME_OWNER as g
  on a.CREATEDBY = g.OWNER_SID
 left join HOME_NEIGHBOR_POST_TYPE as d
  on a.POST_TYPE=d.TYPE_SID
 left join HOME_APARTMENT as e
  on a.APARTMENT_SID = e.APARTMENT_SID
 left join HOME_ROOM as f
  on g.OWNER_PHONE = f.OWNER_PHONE
 where  g.OWNER_TYPE in  ('1')
--c.OWNER_NO not like '%物业%中心%'
 and   c.FAMILY_NAME like '%客服%'

 
 
 
 
 --日志数据
 select  row_number() over(partition by column  a1,b1 order by ..  ) rn
 from  table a ,table b  where ....  ---这样取出来的就是 根据a1，b1分组 的数据
rank() over... 是排名
截取日cast('1980-12-17' as datetime)
select distinct xx.二级 from (...)xx--取出字段中不一样的值

--用户访问路径长短
select sum(t11.APP访问次数),sum(t11.APP独立访客ID数),sum(t11.APP访问次数)/sum(t11.APP独立访客ID数)
from(
select t4.APARTMENT_NAME 小区名称,t4.APP访问次数,t3.APP独立访客ID数,t4.APP访问次数/t3.APP独立访客ID数 用户访问路径长度 from
(select t2.APARTMENT_NAME,count(t2.OWNER_SID) APP访问次数
from(
select a.OWNER_SID,b.OWNER_NO,b.APARTMENT_SID,c.APARTMENT_NAME
from Home_OwnerLog a
left join HOME_OWNER b on a.OWNER_SID=b.OWNER_SID
left join HOME_APARTMENT c on b.APARTMENT_SID =c.APARTMENT_SID
where a.SYSTEM_TYPE = 0--内部计算活跃度不剔除，计算项目绩效时剔除
and b.OWNER_TYPE =1--内部计算活跃度不剔除，计算项目绩效时剔除
and a.CREATED_ON >='20161025'
and a.CREATED_ON <'20161126'
)t2
group by t2.APARTMENT_NAME)t4
left join
(select t1.APARTMENT_NAME,count(distinct(t1.OWNER_SID)) APP独立访客ID数
from(
select a.OWNER_SID,b.OWNER_NO,b.APARTMENT_SID,c.APARTMENT_NAME
from Home_OwnerLog a
left join HOME_OWNER b on a.OWNER_SID=b.OWNER_SID
left join HOME_APARTMENT c on b.APARTMENT_SID =c.APARTMENT_SID
where a.SYSTEM_TYPE = 0--内部计算活跃度不剔除，计算项目绩效时剔除
and b.OWNER_TYPE =1--内部计算活跃度不剔除，计算项目绩效时剔除
and a.CREATED_ON >='20161025'
and a.CREATED_ON <'20161126'
)t1
group by t1.APARTMENT_NAME)t3
on t3.APARTMENT_NAME=t4.APARTMENT_NAME
--order by t4.APP访问次数/t3.APP独立访客ID数 desc
)t11


   --APP独立访客ID数(以此为准)，APP总点击用 非户数
select t1.APARTMENT_NAME,count(distinct(t1.OWNER_SID)) APP独立访客ID数
from(
select a.OWNER_SID,b.OWNER_NO,b.APARTMENT_SID,c.APARTMENT_NAME
from Home_OwnerLog a
left join HOME_OWNER b on a.OWNER_SID=b.OWNER_SID
left join HOME_APARTMENT c on b.APARTMENT_SID =c.APARTMENT_SID
where a.SYSTEM_TYPE = 0--内部计算活跃度不剔除，计算项目绩效时剔除
and b.OWNER_TYPE like('%1%')--内部计算活跃度不剔除，计算项目绩效时剔除
and a.CREATED_ON >='20170101'
and a.CREATED_ON <'20170226'
--and b.OWNER_NO  like '%二期%'--东方福邸一期、二期分别统计
--and b.OWNER_NO not like '%物业服务%'--内部计算活跃度不剔除，计算项目绩效时剔除
   --and (a.CONTENT like '%邻居圈%'or a.CONTENT like '%浏览帖子%') --更改限制条件''，or改为and,提取多级节点下细化日活数据
)t1
group by t1.APARTMENT_NAME
order by count(distinct(t1.OWNER_SID)) desc




--一次性跑出按月按小区的独立访客ID数
select t1.Y_month,t1.月,t1.APARTMENT_NAME,count(distinct(t1.OWNER_SID)) 月APP独立访客ID数
from(
select * from(--,ROW_NUMBER() over (partition by t3.APARTMENT_NAME,t3.Y_month order by t3.APARTMENT_NAME,t3.rn_M desc ) as rn_1 from(
select  *,ROW_NUMBER() over (partition by t2.APARTMENT_NAME,t2.Y_month,t2.OWNER_SID order by t2.APARTMENT_NAME,t2.Y_month,t2.CREATED_ON ) as rn from(
select  cast(year(a.CREATED_ON) as varchar)+'年'+cast(month(a.CREATED_ON) as varchar)+'月' 月,
a.CREATED_ON, convert(varchar(7),a.CREATED_ON,120) Y_month,
a.OWNER_SID,b.OWNER_NO,b.APARTMENT_SID,c.APARTMENT_NAME
from Home_OwnerLog a
left join HOME_OWNER b on a.OWNER_SID=b.OWNER_SID
left join HOME_APARTMENT c on b.APARTMENT_SID =c.APARTMENT_SID
where a.SYSTEM_TYPE = 0--内部计算活跃度不剔除，计算项目绩效时剔除
and a.CREATED_ON >='20160101'
and a.CREATED_ON <'20170317'
--and b.OWNER_NO  like '%二期%'--东方福邸一期、二期分别统计
and b.OWNER_NO not like '%物业%'--内部计算活跃度不剔除，计算项目绩效时剔除
   --and (a.CONTENT like '%邻居圈%'or a.CONTENT like '%浏览帖子%') --更改限制条件''，or改为and,提取多级节点下细化日活数据
)t2
)t3 
)t1
group by t1.Y_month,t1.月,t1.APARTMENT_NAME
order by t1.Y_month,t1.APARTMENT_NAME,count(distinct(t1.OWNER_SID)) 




--测试日志生成
select  top 10 t1.* from(select a.CREATED_ON,a.CONTENT,b.OWNER_NAME ,a.OWNER_SID,b.OWNER_NO,b.APARTMENT_SID,c.APARTMENT_NAME
from Home_OwnerLog a
left join HOME_OWNER b on a.OWNER_SID=b.OWNER_SID
left join HOME_APARTMENT c on b.APARTMENT_SID =c.APARTMENT_SID
where a.SYSTEM_TYPE = 0--0悦嘉家,1悦服务
and b.OWNER_TYPE = 1--1业主
and a.created_on >='20170302'
and b.OWNER_NAME in('王一艳'))t1
--and a.CONTENT not like '%进入主界面%'--剔除只进入首页的，跳出率=只进入首页的次数/总访问次数
order by t1.CREATED_ON desc




--计算跳出率
--计数，日志数
--对null的处理可以使用isnull(值,0）的方式来处理
--select isnull(收入,0)   当收入为null时将计为0
--概念解析
--跳出率=日志中只访问首页的次数/总访问次数  #跳出率越低越好

select count(OWNER_SID) drump from Home_OwnerLog a
where a.CONTENT like '%进入主界面%'
and a.CREATED_ON >= '2016-10-25'
and a.CREATED_ON <'2016-11-26'
and a.SYSTEM_TYPE = 0--悦嘉家
union all
select count(OWNER_SID) al from Home_OwnerLog a
where a.CREATED_ON >= '2016-10-25'
and a.SYSTEM_TYPE = 0--悦嘉家
and a.CREATED_ON < '2016-11-26'


--周人均启动次数
--人均启动次数=总启动次数/总点击ID数



--各级节点独立访客ID数（以此为准）
select sum(t2.邻居圈独立访客ID数) from(
select t1.APARTMENT_NAME,count(distinct(t1.OWNER_SID)) 邻居圈独立访客ID数
from(
select a.OWNER_SID,b.OWNER_NO,b.APARTMENT_SID,c.APARTMENT_NAME
from Home_OwnerLog a
left join HOME_OWNER b on a.OWNER_SID=b.OWNER_SID
left join HOME_APARTMENT c on b.APARTMENT_SID =c.APARTMENT_SID
where a.SYSTEM_TYPE = 0
and b.OWNER_TYPE =1
and a.CREATED_ON >='20160926'
and a.CREATED_ON <'20161026'
--and b.OWNER_NO not like '%物业服务%'--内部计算活跃度不剔除，计算项目绩效时剔除
and (a.CONTENT like '%邻居圈%'or a.CONTENT like '%浏览帖子%')--更改限制条件''，or改为and,提取多级节点下细化日活数据
)t1
group by t1.APARTMENT_NAME
)t2

 --设置条件筛选出含一级二级关键词的数据求row_number(得到各节点下按日汇总的点击量)，计数列即按功能汇总的点击量
 --只看邻居圈和物业服务
--一级：小区公告、点击邻居圈、邻居圈、物业服务、园区公告
--二级：表扬、车位租赁、公共维修、邻居圈发帖、入室维修、随便说说、跳蚤市场、投诉、物业服务、小区广场、园区公告、首页左侧弹出菜单、关于东方润园物业服务中心、家政服务、紧急通知、咨询物业、办事指南、邻里互助
--三级：...

--邻居圈、物业服务日活（参考）
  --一级：a.CONTENT like '%邻居圈%',a.CONTENT like '%物业服务%'
   --二级：邻居圈：a.CONTENT like '%小区广场%'
   --              a.CONTENT like '%随便说说%'
   --              a.CONTENT like '%邻里互助%'
   --              a.CONTENT like '%咨询物业%'
   --              a.CONTENT like '%跳蚤市场%'
   --              a.CONTENT like '%车位租赁%'
   --              a.CONTENT like '%紧急通知%'
   --二级：物业服务a.CONTENT like '%入室维修%'
   --              a.CONTENT like '%公共维修%'
   --              a.CONTENT like '%家电维修%'
   --              a.CONTENT like '%物业账单%'
   --              a.CONTENT like '%生活缴费%'
   --              a.CONTENT like '%投诉%'
   --              a.CONTENT like '%表扬%'
   --              a.CONTENT like '%建议%'

--需要计算活跃度的各级日志关键字  （以此为准）

   --邻居圈：邻居圈、浏览帖子
   --家政服务：家政服务、家政
   --房屋租售：房屋租售、房产、房屋
   --公告：公告、小区公告
   --悦购：团购、悦购
   --物业服务：物业服务
   --访客通行：访客通行、通行证
   --一键开门：一键开门
   --快递：我的快递、快递
   --送水：一键送水
   --悦思悦想：欢乐学


--一天内同一个人点击多次记1次，物业服务和邻居圈点击用户数。
select t1.创建日,sum(t1.日点击用户数) from--7-9月总独立用户数（计算活跃度的分子，活跃度=独立访客数/总用户数）
(
select zz.小区名,zz.内容,zz.一级,zz.二级,zz.三级,zz.系统,zz.a 创建日,zz.日点击用户数 from
(
select yy.*, ROW_NUMBER() over (partition by yy.a order by yy.日点击用户数 desc) as rnn from
(
SELECT *,row_number() over(partition by xx.a,xx.OWNER_SID order by xx.a desc)as 日点击用户数
  FROM (SELECT a.CONTENT 内容,
               parsename(REPLACE(REPLACE(a.CONTENT, ' ', '-'), '-', '.'), 3) 一级,
               parsename(REPLACE(REPLACE(a.CONTENT, ' ', '-'), '-', '.'), 2) 二级,
               parsename(REPLACE(REPLACE(a.CONTENT, ' ', '-'), '-', '.'), 1) 三级,
               a.OWNER_SID,
               b.OWNER_NAME 业主真实名称,
               b.OWNER_NO 业主房号,
               b.OWNER_PHONE 业主手机号码,
               b.FAMILY_NAME 业主昵称,
               (CASE b.OWNER_TYPE
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
               END) AS 用户类型,
               (CASE b.OWNER_CATEGORY
                 WHEN '0' THEN
                  '业主'
                 WHEN '1' THEN
                  '租户'
                 WHEN '2' THEN
                  '家属'
                 ELSE
                  ''
               END) AS 业主类型,
               (CASE a.SYSTEM_TYPE
                 WHEN '0' THEN
                  '悦嘉家'
                 WHEN '1' THEN
                  '悦服务'
                 WHEN '2' THEN
                  '看板'
                 WHEN '3' THEN
                  'JoyHome后台管理系统'
                 ELSE
                  ''
               END) AS 系统,
               a.DEVICE_Model 登陆设备型号,
               a.OS_Type 操作系统类型,
               a.CREATED_ON 创建时间,
               a.ModifyTime 最后修改时间,
               CAST(a.CREATED_ON AS DATE) a,
               a.REMARK 备注,c.APARTMENT_NAME 小区名
          FROM Home_OwnerLog AS a
          LEFT JOIN HOME_OWNER AS b ON a.OWNER_SID = b.OWNER_SID
          left join HOME_APARTMENT c on b.APARTMENT_SID = c.APARTMENT_SID
          where b.OWNER_TYPE = 1
     and a.SYSTEM_TYPE = 0
   and a.CONTENT LIKE '%家政%' )xx
)yy
) as zz
where rnn=1
and zz.创建时间 >='2016-12-18'
and zz.创建时间 <'2017-01-18'
--order by zz.创建时间 asc
)t1
group by t1.创建日



--用户数（唯一SID）,计算活跃度的分母
select count(t2.rn)  from (
select t1.*,ROW_NUMBER() over (partition by t1.用户SID order by t1.创建时间 desc) as rn
from (
select OWNER_SID 用户SID,OWNER_TYPE 用户类型,CREATED_ON 创建时间
 from HOME_OWNER
where OWNER_TYPE = 1
)t1
)t2
where t2.rn=1
and t2.创建时间 <'2016-09-25'

--10月份悦嘉家安装设备ID数
select count(distinct(t1.DEVICE_SID)) from(
select *
from HOME_DEVICE a
where a.CREATED_ON >='20161001'
and a.CREATED_ON <'20161031'
and DEVICE_APP in('10','20')
) t1

--用户ID对应设备ID是否为多对一
select * from (
select t1.APARTMENT_NAME,t1.OWNER_NO,t1.OWNER_NAME ,t1.FAMILY_NAME,t1.DEVICE_SID,t1.OWNER_PHONE,
ROW_NUMBER() over (partition by t1.DEVICE_SID order by t1.APARTMENT_NAME,t1.DEVICE_SID,t1.CREATED_ON) as rn from
(SELECT  a.OWNER_NO,a.OWNER_SID ,b.APARTMENT_NAME,c.DEVICE_SID,a.CREATED_ON,a.OWNER_NAME ,a.FAMILY_NAME,a.OWNER_PHONE
          FROM HOME_OWNER AS a
          left join HOME_DEVICE c on a.OWNER_SID = c.OWNER_SID
          left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
         WHERE a.CREATED_ON < '20161031'
          and a.CREATED_ON >= '20161001'
          and c.DEVICE_SID is not null
        -- and a.OWNER_NO not like '%物业服务中心%'--门禁数据不剔除
          and a.OWNER_type = 1--悦嘉家
)t1
)t2
--where t2.rn =1

--设备详细情况查询
select t1.created_on,t1.APARTMENT_NAME,t1.OWNER_NO,t1.OWNER_NAME ,t1.FAMILY_NAME,t1.DEVICE_SID,t1.OWNER_PHONE,t1.DEVICE_TYPE  ,
ROW_NUMBER() over (partition by t1.DEVICE_SID order by t1.APARTMENT_NAME,t1.DEVICE_SID,t1.CREATED_ON) as rn from
(SELECT  a.OWNER_NO,a.OWNER_SID ,b.APARTMENT_NAME,c.DEVICE_SID,a.CREATED_ON,a.OWNER_NAME ,a.FAMILY_NAME,a.OWNER_PHONE,c.DEVICE_TYPE
          FROM HOME_OWNER AS a
          left join HOME_DEVICE c on a.OWNER_SID = c.OWNER_SID
          left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
         WHERE a.CREATED_ON >= '20161201'
          --and b.APARTMENT_NAME  in('世纪新城')
          and c.DEVICE_SID is not null
        -- and a.OWNER_NO not like '%物业服务中心%'--门禁数据不剔除
          and a.OWNER_type = 1--悦嘉家
)t1
order by t1.APARTMENT_NAME,t1.created_on

--一个手机号码注册多个小区
SELECT * from(
select t1.OWNER_SID,t1.created_on,t1.APARTMENT_NAME,t1.OWNER_NO,t1.OWNER_NAME ,t1.FAMILY_NAME,t1.OWNER_PHONE ,
ROW_NUMBER() over (partition by t1.OWNER_PHONE order by t1.OWNER_PHONE,t1.APARTMENT_NAME,t1.CREATED_ON) as rn from
(SELECT  a.OWNER_NO,a.OWNER_SID ,b.APARTMENT_NAME,a.CREATED_ON,a.OWNER_NAME ,a.FAMILY_NAME,a.OWNER_PHONE--,c.DEVICE_TYPE,c.DEVICE_SID
          FROM HOME_OWNER AS a
          --left join HOME_DEVICE c on a.OWNER_SID = c.OWNER_SID
          left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
         WHERE
           b.APARTMENT_NAME not in('幸福家园','竹径茶语','房屋租售中心','恒基小区','体验小区')

        and a.OWNER_NO not like '%物业%'--门禁数据不剔除
       and  a.OWNER_type = 1--悦嘉家
and a.OWNER_PHONE is not null
)t1)t2
where t2.rn != 1
order by t2.OWNER_PHONE,t2.APARTMENT_NAME--,t2.created_on,t2.rn


--方式二
select * from(
select t1.*, ROW_NUMBER() over (partition by t1.DEVICE_SID order by t1.created_on desc) rn from
(select a.created_on,a.OWNER_SID,a.APARTMENT_SID,b.DEVICE_SID,c.APARTMENT_NAME,a.OWNER_PHONE,a.OWNER_NO,a.OWNER_NAME,a.FAMILY_NAME
 from  HOME_OWNER a
left join HOME_DEVICE b on a.OWNER_SID = b.OWNER_SID
left join HOME_APARTMENT c on  a.APARTMENT_SID=c.APARTMENT_SID
where OWNER_TYPE = 1
--and c.APARTMENT_NAME  in('富越香郡')
--and b.DEVICE_SID is not null
--and a.created_on>='20161130'
)t1
)t2
--where t2.rn=2


--12月份注册用户登录次数（截至20170104）
select t2.* from(
select t1.owner,t1.CREATED_ON,t1.OWNER_SID,t1.APARTMENT_NAME,t1.rn,row_number() over(partition by t1.APARTMENT_NAME,t1.OWNER_SID order by t1.APARTMENT_NAME,t1.rn desc) rnn
from(
select b.CREATED_ON owner,a.CREATED_ON,a.OWNER_SID,b.OWNER_NO,b.APARTMENT_SID,c.APARTMENT_NAME,row_number() over(partition by c.APARTMENT_NAME,a.OWNER_SID order by c.APARTMENT_NAME,a.OWNER_SID) rn
from Home_OwnerLog a
left join HOME_OWNER b on a.OWNER_SID=b.OWNER_SID
left join HOME_APARTMENT c on b.APARTMENT_SID =c.APARTMENT_SID
where a.SYSTEM_TYPE = 0--内部计算活跃度不剔除，计算项目绩效时剔除
and b.OWNER_TYPE =1--内部计算活跃度不剔除，计算项目绩效时剔除
and b.CREATED_ON >='20161201'
and b.CREATED_ON <'20170101 12:00:01'
)t1)t2
where t2.rnn=1


--登录次数（以此为准）
select t2.* from(
select t1.*,row_number() over(partition by t1.项目名称,t1.OWNER_SID order by t1.项目名称,t1.登陆次数 desc ) rnn
from(
SELECT  a.OWNER_SID ,a.CREATED_ON 注册时间,d.CREATED_ON 登陆时间,row_number() over(partition by b.APARTMENT_NAME,a.OWNER_SID order by b.APARTMENT_NAME,d.CREATED_ON,a.OWNER_SID desc) 登陆次数 ,
 b.APARTMENT_NAME 项目名称,a.OWNER_PHONE 帐号,a.OWNER_NAME 用户真实名称,a.FAMILY_NAME 昵称,a.OWNER_NO 房号,
(CASE a.OWNER_CATEGORY
                 WHEN '0' THEN
                  '业主'
                 WHEN '1' THEN
                  '租户'
                 WHEN '2' THEN
                  '家属'
                 ELSE
                  ''
               END) AS 租户类型,
(CASE a.OWNER_STATUS
                 WHEN '0' THEN
                  '否'
                 WHEN '1' THEN
                  '是'
                 ELSE
                  ''
               END) AS 是否启用,
(CASE c.VERIFICATION_TAG
                 WHEN '0' THEN
                  '未申请'
                 WHEN '1' THEN
                  '待验证'
                 WHEN '2' THEN
                  '已验证'
                 WHEN '3' THEN
                  '验证未通过'
                 ELSE
                  ''
               END) AS 验证状态,
c.CREATED_ON 验证时间
          FROM HOME_OWNER AS a
left join Home_OwnerLog d on d.OWNER_SID=a.OWNER_SID
          left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
          left join HOME_OWNER_VERIFICATION c on a.OWNER_SID = c.OWNER_SID
         WHERE  a.CREATED_ON < '20170101 12:00:01'
        -- and a.OWNER_NO not like '%物业服务中心%'--门禁数据不剔除
           and a.CREATED_ON  >= '20161201'
         -- and  a.CREATED_ON < '2016-10-23'
        -- and b.APARTMENT_NAME not in ('普升福邸','蓝爵国际','体验小区','幸福家园','房屋租售中心')
         and b.APARTMENT_NAME not in ('幸福家园','体验小区','荀庄','林语别墅','金橡臻园')
          and a.OWNER_type = 1--业主
--and d.CREATED_ON is not null--筛选出有登陆记录的
--and d.SYSTEM_TYPE = 0
)t1)t2
where t2.rnn=1
order by  t2.项目名称,t2.注册时间--,t2.登陆时间


--悦服务登录次数
select t2.* from(
select t1.*,row_number() over(partition by t1.项目名称,t1.OWNER_SID order by t1.项目名称,t1.登陆次数 desc ) rnn
from(
SELECT  a.OWNER_SID ,a.CREATED_ON 注册时间,d.CREATED_ON 登陆时间,row_number() over(partition by b.APARTMENT_NAME,a.OWNER_SID order by b.APARTMENT_NAME,d.CREATED_ON,a.OWNER_SID desc) 登陆次数 ,
 b.APARTMENT_NAME 项目名称,a.OWNER_PHONE 帐号,a.OWNER_NAME 用户真实名称,a.FAMILY_NAME 昵称,a.OWNER_NO 房号,
(CASE a.OWNER_STATUS
                 WHEN '0' THEN
                  '否'
                 WHEN '1' THEN
                  '是'
                 ELSE
                  ''
               END) AS 是否启用
          FROM Home_OwnerLog d
left join HOME_OWNER AS a on d.OWNER_SID=a.OWNER_SID
          left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
         WHERE  d.CREATED_ON < '20170223'
        -- and a.OWNER_NO not like '%物业服务中心%'--门禁数据不剔除
     --      and d.CREATED_ON  >= '20161201'
        --  and a.OWNER_type = 1--业主
--and d.CREATED_ON is not null--筛选出有登陆记录的
and d.SYSTEM_TYPE = 1 -- 系统 0=悦嘉家 1=悦服务 2=看板 3=JoyHome后台管理系统
)t1)t2
where t2.rnn=1
order by  t2.登陆时间 desc


2
分隔符分裂f_splitstr('1-2-3-4','-') 
replace  （'1-2-3-4 6',' ','-'）
先替换再分裂
select regexp_split_to_table('12-13-25-dajdha','-')
SQL server:
PARSENAME(REPLACE(t3.部门,'-','.'),1) as a,PARSENAME(REPLACE(t3.部门,'-','.'),2) as  a ,PARSENAME(REPLACE(t3.部门,'-','.'),3) as  a,
PARSENAME(REPLACE(t3.部门,'-','.'),4) as  a  ,PARSENAME(REPLACE(t3.部门,'-','.'),5) as  a,PARSENAME(REPLACE(t3.部门,'-','.'),6) as  a



 --将数字换成对应的名字
 (CASE  H.PAY_STATUS
 WHEN '02'
 THEN '审核'
 WHEN '03'
 THEN '退款完成'
 ELSE '' END)
 AS  PAY_STATUS_COPY

 --限制取数
 select top 10 * FROM
(select * from 表
 )xx

select top 3 xx.A FROM
(select a.CREATED_ON ,a.RESPONSE_TIME 响应时间,a.SERVICE_BOOKING_TIME 用户预约时间,PROCESS_TIME 处理时间,a.SERVICE_NO,
a.RESPONSE_USER, c.OWNER_NAME as responseUserName,
 a.PROCESS_USER, d.OWNER_NAME as ProcessUserName,
a.SERVICE_CATEGORY,e.CATEGORY_NAME,
b.APARTMENT_NAME
from HOME_SERVICE_MAIN as a
join HOME_APARTMENT as b
on a.APARTMENT_SID = b.APARTMENT_SID
  left join HOME_OWNER as c
  on a.RESPONSE_USER = c.OWNER_SID
  left join HOME_OWNER as d
  on a.PROCESS_USER = d.OWNER_SID
  left join HOME_SERVICE_CATEGORY as e on a.SERVICE_CATEGORY = e.CATEGORY_SID
where a.SERVICE_BOOKING_TIME != 'NULL'
and
select concat('11','22','33')b.APARTMENT_NAME !='幸福家园'
and b.APARTMENT_NAME !='体验小区';) xx

--查询当前时间，可加减年份
select year(getdate()),month(getdate()),day(getdate())；
select getdate()

--连接两个字段
select concat('11','22','33');

select (1+2)*2/6;
--截取前16个字段
select convert(char(10), '2002-06-05 15:42:52');
select convert(char(16), getdate());

--case...then...语句
select
CASE
WHEN 3= 1 THEN '等于1'
WHEN 3= 2 THEN '等于2'
WHEN 3= 3 THEN '等于3'
END;

/*取第一个不为空的值
coalesce函数：
coalesce (expr1, expr2, ..., exprn)
例子1：
select coalesce(null,null,3,4,5) from dual
例子2：
select coalesce(1,null,3,4,5) from dual
*/

--创建测试表
create table AggregationTable(Id int, [Name] varchar(10))
go
insert into AggregationTable
 select 1,'赵' union all
 select 2,'钱' union all
 select 1,'孙' union all
 select 1,'李' union all
 select 2,'周'
go;

select max(Id) from  AggregationTable;

select * from AggregationTable;
select min(Id) from  AggregationTable;

--创建自定义字符串聚合函数

Create FUNCTION AggregateString
(
 @Id int
)
RETURNS varchar(1024)
AS
BEGIN
 declare @Str varchar(1024)
 set @Str = ''
 select @Str = @Str + [Name] from AggregationTable
 where [Id] = @Id
 return @Str
END
GO;

select dbo.AggregateString(Id),Id from AggregationTable
group by Id ;

--删除表
DROP table AggregationTable;

--有表table1 , 四个int型 字段 F1,F2,F3,F4；现要查询得到四列中的最大值 , 并只需要返回最大值
select case when F12>F34 then F12 else F34 end as MaxNum from
02.(select case when F1>F2 then F1 else F2 end as F12,
03.case when F3>F4 then F3 else F4 end as F34 from table1) as t1 order by MaxNum desc

select case
when 2>1 then 2
else 1 end


--截至20170118客服回帖数排名
select * from (
select t2.小区名称,t2.回帖人,t2.react_name ,t2.rn ,
ROW_NUMBER() over (partition by t2.小区名称,t2.回帖人,t2.react_name order by t2.小区名称,t2.回帖人,t2.react_name,t2.rn desc ) as rnn
from(
select *
from(
select  a.POST_SID,ROW_NUMBER() over (partition by c.APARTMENT_NAME, f.OWNER_NAME,f.FAMILY_NAME order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,
a.CREATEDBY,b.FAMILY_NAME 发帖人,f.FAMILY_NAME react_name,f.OWNER_NAME 回帖人, a.POST_TYPE ,a.POST_CONTENT,
a.CREATED_ON,e.CREATED_ON react_time,c.APARTMENT_NAME 小区名称,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_POST a
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
left join HOME_NEIGHBOR_COMMENT e on a.POST_SID = e.POST_SID
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
left join HOME_APARTMENT g on g.APARTMENT_SID = f.APARTMENT_SID
where  a.CREATED_ON <'20170119'
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and f.FAMILY_NAME  like'%客服%'
and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and b.OWNER_TYPE=1--类型为业主
and c.APARTMENT_NAME not in ('幸福家园','体验小区','恒基小区','金橡臻园')
)t1
)t2)t3
where t3.rnn=1
order by t3.rn  desc



--咨询物业明细
8:30—18:00：1小时内响应；1
18：00—22:00：2小时内响应；2
22:00以后：次日9:00前响应:11,111

--不超时：1+2+11+111
--select A.APARTMENT_NAME,A.不超时1 from
--(
--select A.APARTMENT_NAME,A.不超时1 from
--(
--select t2.APARTMENT_NAME,count(t2.react_name) 不超时 from(
select * from (
select b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,SUBSTRING(CONVERT(varchar(100), a.CREATED_ON, 108),1,5) time,
dateadd(minute,+60,a.CREATED_ON) time1,
dateadd(minute,+120,a.CREATED_ON) time2,
a.CREATED_ON,e.CREATED_ON react_time,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_POST a
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
left join HOME_NEIGHBOR_COMMENT e on a.POST_SID = e.POST_SID
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
where d.TYPE_NAME in('咨询物业')
--and.CREATED_ON >='20161026'
--and a.CREATED_ON <'20161126'
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and (f.FAMILY_NAME  like'%客服%' or f.FAMILY_NAME  like'%管家%')
and b.OWNER_TYPE=1--类型为业主
and convert(char(8),a.CREATED_ON,108)>='08:30:00' and  convert(char(8),a.CREATED_ON,108)<'18:00:00'--1
--and convert(char(8),a.CREATED_ON,108)>='18:00:00' and  convert(char(8),a.CREATED_ON,108)<'22:00:00'--2
--and convert(char(8),a.CREATED_ON,108)>='22:00:00' and  convert(char(8),a.CREATED_ON,108)<'24:00:00'--11
--and convert(char(8),a.CREATED_ON,108)>='00:00:00' and  convert(char(8),a.CREATED_ON,108)<'08:30:00'--111
)t1
where t1.react_time<=time1--time1,time2
--where t1.react_time<t1.CREATED_ON+1--11
--and convert(char(8),t1.react_time,108)<'09:00:00' --11
--where convert(char(8),t1.react_time,108)<'09:00:00' --111
--and CAST(t1.react_time AS DATE)= CAST(t1.CREATED_ON AS DATE)--111
--)t2
--group by t2.APARTMENT_NAME)A



--各类型帖子分布情况
select * from(
select t2.TYPE_NAME,count(t2.CREATEDBY) 帖子数 from(
select *
from(
select a.POST_SID,ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,
a.CREATEDBY,b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,
a.CREATED_ON,e.CREATED_ON react_time,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_POST a
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
left join HOME_NEIGHBOR_COMMENT e on a.POST_SID = e.POST_SID
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
where a.CREATED_ON >='20170101'
--and d.TYPE_NAME in('咨询物业')
--and b.OWNER_NO like ('%一期%')
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and a.CREATED_ON <'20170226'
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and b.OWNER_TYPE=1--类型为业主
and c.APARTMENT_NAME not in ('幸福家园','体验小区','恒基小区','金橡臻园')
)t1
where t1.rn=1
)t2
group by t2.TYPE_NAME)t3
order by t3.帖子数 desc



--咨询物业帖子数
select * from(
select t2.APARTMENT_NAME 小区名称,count(t2.CREATEDBY) 咨询物业帖子数 from(
select *
from(
select a.POST_SID,ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,
a.CREATEDBY,b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,
a.CREATED_ON,e.CREATED_ON react_time,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_POST a
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
left join HOME_NEIGHBOR_COMMENT e on a.POST_SID = e.POST_SID
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
where d.TYPE_NAME in('咨询物业')
--and a.CREATED_ON >='20161026'
--and b.OWNER_NO like ('%一期%')
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and a.CREATED_ON <'20161222'
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and b.OWNER_TYPE=1--类型为业主
and c.APARTMENT_NAME not in ('幸福家园','体验小区','恒基小区','金橡臻园')
)t1
where t1.rn=1
)t2
group by t2.APARTMENT_NAME)t3
order by t3.咨询物业帖子数 desc




--咨询物业-客服回复帖子数
select * from (
select t2.APARTMENT_NAME 小区名称,count(t2.CREATEDBY) 回复帖子数 from(
select *
from(
select a.POST_SID,ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,
a.CREATEDBY,b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,
a.CREATED_ON,e.CREATED_ON react_time,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_POST a
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
left join HOME_NEIGHBOR_COMMENT e on a.POST_SID = e.POST_SID
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
where d.TYPE_NAME in('咨询物业')
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and a.CREATED_ON >='20160101'
and a.CREATED_ON <'20170326'
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and (f.FAMILY_NAME  like'%客服%' or f.FAMILY_NAME  like'%管家%')
--and b.OWNER_NO like ('%一期%')
and b.OWNER_TYPE=1--类型为业主
and c.APARTMENT_NAME not in ('幸福家园','体验小区','恒基小区','金橡臻园')
--order by a.CREATED_ON desc
)t1
where t1.rn=1
)t2
group by t2.APARTMENT_NAME)t3
order by t3.回复帖子数  desc





--咨询物业平均回复时长,当天or非当天
--1、2两个时段
select t2.小区名称,t2.总回复时长,t2.总回复数,t2.总回复时长/总回复数 平均回复时长 from(
select t1.小区名称,count(t1.react_time) 总回复数,sum(t1.回复时长) 总回复时长 from
(
select a.POST_SID,ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,
a.CREATEDBY,b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,
a.CREATED_ON,e.CREATED_ON react_time, DATEDIFF( mi, a.CREATED_ON, e.CREATED_ON)/60.0 回复时长,c.APARTMENT_NAME 小区名称,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_POST a
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
left join HOME_NEIGHBOR_COMMENT e on a.POST_SID = e.POST_SID
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
where d.TYPE_NAME in('咨询物业')
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and a.CREATED_ON >='20160101'
and a.CREATED_ON <'20170101'
--and b.owner_no like('%二期%')
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and (f.FAMILY_NAME  like'%客服%' or f.FAMILY_NAME  like'%管家%')
and b.OWNER_TYPE=1--类型为业主
and convert(char(8),a.CREATED_ON,108)>='08:30:00' and  convert(char(8),a.CREATED_ON,108)<'18:00:00'--1
--and convert(char(8),a.CREATED_ON,108)>='18:00:00' and  convert(char(8),a.CREATED_ON,108)<'22:00:00'--2
--and CAST(e.CREATED_ON AS DATE)= CAST(a.CREATED_ON AS DATE)--当天内回复
--order by a.CREATED_ON desc
)t1
where t1.rn=1
group by t1.小区名称)t2




--对应到客服/管家个人的回复时长
select t2.小区名称,t2.回复人,t2.OWNER_NAME 回复人姓名,t2.帖子类型,cast(AVG(t2.回复时长)as decimal(10,2)) 平均回帖时长 from(
select t1.小区名称,t1.FAMILY_NAME 发帖人,t1.react_name 回复人,t1.POST_CONTENT 帖子内容,t1.CREATED_ON 发帖时间,t1.TYPE_NAME 帖子类型,t1.回复时长,t1.OWNER_NAME
 from
(
select a.POST_SID,ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,
a.CREATEDBY,b.FAMILY_NAME,f.FAMILY_NAME react_name,f.OWNER_NAME,a.POST_TYPE ,a.POST_CONTENT,
a.CREATED_ON,e.CREATED_ON react_time, DATEDIFF( mi, a.CREATED_ON, e.CREATED_ON)/60.0 回复时长,c.APARTMENT_NAME 小区名称,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_POST a
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
left join HOME_NEIGHBOR_COMMENT e on a.POST_SID = e.POST_SID
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
where d.TYPE_NAME in('咨询物业')
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and a.CREATED_ON >='20160101'
and a.CREATED_ON <'20170101'
--and b.owner_no like('%二期%')
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and (f.FAMILY_NAME  like'%客服%' or f.FAMILY_NAME  like'%管家%')
and b.OWNER_TYPE=1--类型为业主
--and convert(char(8),a.CREATED_ON,108)>='08:30:00' and  convert(char(8),a.CREATED_ON,108)<'18:00:00'--1
--and convert(char(8),a.CREATED_ON,108)>='18:00:00' and  convert(char(8),a.CREATED_ON,108)<'22:00:00'--2
--and CAST(e.CREATED_ON AS DATE)= CAST(a.CREATED_ON AS DATE)--当天内回复
--order by a.CREATED_ON desc
)t1
where t1.rn=1)t2
group by t2.小区名称,t2.回复人,t2.帖子类型,t2.OWNER_NAME 
order by cast(AVG(t2.回复时长)as decimal(10,2)) 
--order by t1.回复时长 





--咨询物业-及时回复按小区汇总（新算法，分2时间段）
--工作时间段，当天回复
--其他时间段，24h内回复
select t9.小区名称,sum(t9.不超时) 咨询物业客服回复不超时 from
(select t2.APARTMENT_NAME 小区名称,count(t2.react_name) 不超时 from(
select * from (
select b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,e.COMMENT_CONTENT,
ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,
SUBSTRING(CONVERT(varchar(100), a.CREATED_ON, 108),1,5) time,
a.CREATED_ON,e.CREATED_ON react_time,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_COMMENT e
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
left join HOME_NEIGHBOR_POST a on a.POST_SID = e.POST_SID
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
where d.TYPE_NAME in('咨询物业')
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and a.CREATED_ON >='20160101'
and a.CREATED_ON <'20170101'
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and (f.FAMILY_NAME  like'%客服%' or f.FAMILY_NAME  like'%管家%')
and b.OWNER_TYPE=1--类型为业主
)t1
where t1.rn=1
and convert(char(8),t1.CREATED_ON,108)>='08:30:00' and  convert(char(8),t1.CREATED_ON,108)<'18:00:00'--1
and cast(t1.react_time as date)=cast(t1.CREATED_ON as date)
)t2
group by t2.APARTMENT_NAME
union all
select t6.APARTMENT_NAME 小区名称,count(t6.react_name) 不超时 from(
select * from (
select b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,e.COMMENT_CONTENT,
ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,
SUBSTRING(CONVERT(varchar(100), a.CREATED_ON, 108),1,5) time,
a.CREATED_ON,e.CREATED_ON react_time,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_COMMENT e
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
left join HOME_NEIGHBOR_POST a on a.POST_SID = e.POST_SID
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
where d.TYPE_NAME in('咨询物业')
and a.CREATED_ON >='20160101'
and a.CREATED_ON <'20170101'
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and (f.FAMILY_NAME  like'%客服%' or f.FAMILY_NAME  like'%管家%')
and b.OWNER_TYPE=1--类型为业主
)t5
where t5.rn=1
and convert(char(8),t5.CREATED_ON,108)>='18:00:00' and  convert(char(8),t5.CREATED_ON,108)<'24:00:00'--11
and t5.react_time<t5.CREATED_ON+1--11
)t6
group by t6.APARTMENT_NAME
union all
select t7.APARTMENT_NAME 小区名称,count(t7.react_name) 不超时 from(
select * from (
select b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,e.COMMENT_CONTENT,
ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,
SUBSTRING(CONVERT(varchar(100), a.CREATED_ON, 108),1,5) time,
a.CREATED_ON,e.CREATED_ON react_time,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_COMMENT e
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
left join HOME_NEIGHBOR_POST a on a.POST_SID = e.POST_SID
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
where d.TYPE_NAME in('咨询物业')
and a.CREATED_ON >='20160101'
and a.CREATED_ON <'20170101'
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and (f.FAMILY_NAME  like'%客服%' or f.FAMILY_NAME  like'%管家%')
and b.OWNER_TYPE=1--类型为业主
)t8
where t8.rn=1
and  convert(char(8),t8.CREATED_ON,108)<'08:30:00'--11
and t8.react_time<t8.CREATED_ON+1--11
)t7
group by t7.APARTMENT_NAME)t9
group by t9.小区名称
order by sum(t9.不超时) desc


--咨询物业-及时回复按小区汇总（新算法，当天回复单数）
select t2.APARTMENT_NAME 小区名称,count(t2.react_name) 咨询物业客服回复不超时 from(
select * from (
select b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,e.COMMENT_CONTENT,
ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,
SUBSTRING(CONVERT(varchar(100), a.CREATED_ON, 108),1,5) time,
a.CREATED_ON,e.CREATED_ON react_time,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_COMMENT e
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
left join HOME_NEIGHBOR_POST a on a.POST_SID = e.POST_SID
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
where d.TYPE_NAME in('咨询物业')
and a.CREATED_ON >='20160101'
and a.CREATED_ON <'20170101'
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and (f.FAMILY_NAME  like'%客服%' or f.FAMILY_NAME  like'%管家%')
and b.OWNER_TYPE=1--类型为业主
)t1
where t1.rn=1
and cast(t1.react_time as date)=cast(t1.CREATED_ON as date)
)t2
group by t2.APARTMENT_NAME
order by count(t2.react_name) desc



--咨询物业-及时回复按小区汇总（新算法，分时段同提单超时率）
--工作时间段：当天回复
--其他时间段：9点之前回复

select t9.小区名称,sum(t9.不超时) 咨询物业客服回复不超时 from
(select t2.APARTMENT_NAME 小区名称,count(t2.react_name) 不超时 from(
select * from (
select b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,e.COMMENT_CONTENT,
ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,
SUBSTRING(CONVERT(varchar(100), a.CREATED_ON, 108),1,5) time,
a.CREATED_ON,e.CREATED_ON react_time,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_COMMENT e
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
left join HOME_NEIGHBOR_POST a on a.POST_SID = e.POST_SID
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
where d.TYPE_NAME in('咨询物业')
and a.CREATED_ON >='20160101'
and a.CREATED_ON <'20170101'
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and (f.FAMILY_NAME  like'%客服%' or f.FAMILY_NAME  like'%管家%')
and b.OWNER_TYPE=1--类型为业主
and convert(char(8),a.CREATED_ON,108)>='08:30:00' and  convert(char(8),a.CREATED_ON,108)<'18:00:00'--1
)t1
where t1.rn=1
and cast(t1.react_time as date)=cast(t1.CREATED_ON as date)
)t2
group by t2.APARTMENT_NAME
union all
select t6.APARTMENT_NAME 小区名称,count(t6.react_name) 不超时 from(
select * from (
select b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,e.COMMENT_CONTENT,
ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,
SUBSTRING(CONVERT(varchar(100), a.CREATED_ON, 108),1,5) time,
a.CREATED_ON,e.CREATED_ON react_time,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_COMMENT e
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
left join HOME_NEIGHBOR_POST a on a.POST_SID = e.POST_SID
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
where d.TYPE_NAME in('咨询物业')
and a.CREATED_ON >='20160101'
and a.CREATED_ON <'20170101'
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and (f.FAMILY_NAME  like'%客服%' or f.FAMILY_NAME  like'%管家%')
and b.OWNER_TYPE=1--类型为业主
and convert(char(8),a.CREATED_ON,108)>='18:00:00' and  convert(char(8),a.CREATED_ON,108)<'24:00:00'--11
)t5
where t5.rn=1
and t5.react_time<t5.CREATED_ON+1--11
and convert(char(8),t5.react_time,108)<'09:00:00' --11
)t6
group by t6.APARTMENT_NAME
union all
select t8.APARTMENT_NAME 小区名称,count(t8.react_name) 不超时 from(
select * from (
select b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,e.COMMENT_CONTENT,
ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,
SUBSTRING(CONVERT(varchar(100), a.CREATED_ON, 108),1,5) time,
a.CREATED_ON,e.CREATED_ON react_time,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_COMMENT e
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
left join HOME_NEIGHBOR_POST a on a.POST_SID = e.POST_SID
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
where d.TYPE_NAME in('咨询物业')
and a.CREATED_ON >='20160101'
and a.CREATED_ON <'20170101'
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and (f.FAMILY_NAME  like'%客服%' or f.FAMILY_NAME  like'%管家%')
and b.OWNER_TYPE=1--类型为业主
and convert(char(8),a.CREATED_ON,108)<'08:30:00'--111
)t7
where t7.rn=1
and convert(char(8),t7.react_time,108)<'09:00:00' --111
and CAST(t7.react_time AS DATE)= CAST(t7.CREATED_ON AS DATE)--111
)t8
group by t8.APARTMENT_NAME
)t9
group by t9.小区名称
order by sum(t9.不超时) desc


--咨询物业-及时回复按小区汇总（新算法，分时段，以此为准）
--工作时间段8:30-18:00：当天回复
--其他时间段：12小时内回复

select t9.小区名称,sum(t9.不超时) 咨询物业客服回复不超时 from
(select t2.APARTMENT_NAME 小区名称,count(t2.react_name) 不超时 from(
select * from (
select b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,e.COMMENT_CONTENT,
ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,
SUBSTRING(CONVERT(varchar(100), a.CREATED_ON, 108),1,5) time,
a.CREATED_ON,e.CREATED_ON react_time,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_COMMENT e
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
left join HOME_NEIGHBOR_POST a on a.POST_SID = e.POST_SID
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
where d.TYPE_NAME in('咨询物业')
and a.CREATED_ON >='20160101'
and a.CREATED_ON <'20170101'
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
--and b.OWNER_NO like ('%一期%')
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and (f.FAMILY_NAME  like'%客服%' or f.FAMILY_NAME  like'%管家%')
and b.OWNER_TYPE=1--类型为业主
and convert(char(8),a.CREATED_ON,108)>='08:30:00' and  convert(char(8),a.CREATED_ON,108)<'18:00:00'--1
)t1
where t1.rn=1
and cast(t1.react_time as date)=cast(t1.CREATED_ON as date)
)t2
group by t2.APARTMENT_NAME
union all
select t6.APARTMENT_NAME 小区名称,count(t6.react_name) 不超时 from(
select * from (
select b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,e.COMMENT_CONTENT,
ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,
SUBSTRING(CONVERT(varchar(100), a.CREATED_ON, 108),1,5) time,
a.CREATED_ON,e.CREATED_ON react_time,
dateadd(hour,+12,a.CREATED_ON) time1,
c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_COMMENT e
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
left join HOME_NEIGHBOR_POST a on a.POST_SID = e.POST_SID
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
where d.TYPE_NAME in('咨询物业')
and a.CREATED_ON >='20160101'
and a.CREATED_ON <'20170101'
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
--and b.OWNER_NO like ('%一期%')
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and (f.FAMILY_NAME  like'%客服%' or f.FAMILY_NAME  like'%管家%')
and b.OWNER_TYPE=1--类型为业主
and convert(char(8),a.CREATED_ON,108)>='18:00:00' and  convert(char(8),a.CREATED_ON,108)<'24:00:00'--11
)t5
where t5.rn=1
and t5.react_time<t5.time1--11
)t6
group by t6.APARTMENT_NAME
union all
select t8.APARTMENT_NAME 小区名称,count(t8.react_name) 不超时 from(
select * from (
select b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,e.COMMENT_CONTENT,
ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,
SUBSTRING(CONVERT(varchar(100), a.CREATED_ON, 108),1,5) time,
dateadd(hour,+12,a.CREATED_ON) time1,
a.CREATED_ON,e.CREATED_ON react_time,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_COMMENT e
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
left join HOME_NEIGHBOR_POST a on a.POST_SID = e.POST_SID
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
where d.TYPE_NAME in('咨询物业')
and a.CREATED_ON >='20160101'
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
--and b.OWNER_NO like ('%一期%')
and a.CREATED_ON <'20170101'
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and (f.FAMILY_NAME  like'%客服%' or f.FAMILY_NAME  like'%管家%')
and b.OWNER_TYPE=1--类型为业主
)t8
where t8.rn=1
and  convert(char(8),t8.CREATED_ON,108)<'08:30:00'--11
and t8.react_time<t8.time1--11
)t8
group by t8.APARTMENT_NAME
)t9
group by t9.小区名称
order by sum(t9.不超时) desc





--咨询物业-及时回复按小区汇总（旧算法）
--8:30-18:00 1小时内回复
--18:00-22:00 2小时内回复
--其他时间段 9:00前回复

select t9.小区名称,sum(t9.不超时) 咨询物业客服回复不超时 from
(select t2.APARTMENT_NAME 小区名称,count(t2.react_name) 不超时 from(
select * from (
select ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,
b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,SUBSTRING(CONVERT(varchar(100), a.CREATED_ON, 108),1,5) time,
dateadd(minute,+60,a.CREATED_ON) time1,
dateadd(minute,+120,a.CREATED_ON) time2,
a.CREATED_ON,e.CREATED_ON react_time,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_POST a
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
left join HOME_NEIGHBOR_COMMENT e on a.POST_SID = e.POST_SID
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
where d.TYPE_NAME in('咨询物业')
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and a.CREATED_ON >='20160101'
and a.CREATED_ON <'20170101'
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and (f.FAMILY_NAME  like'%客服%' or f.FAMILY_NAME  like'%管家%')
and b.OWNER_TYPE=1--类型为业主
and convert(char(8),a.CREATED_ON,108)>='08:30:00' and  convert(char(8),a.CREATED_ON,108)<'18:00:00'--1
)t1
where t1.react_time<=time1--time1,time2
and t1.rn=1
)t2
group by t2.APARTMENT_NAME
union ALL
select t4.APARTMENT_NAME 小区名称,count(t4.react_name) 不超时 from(
select * from (
select ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,
b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,SUBSTRING(CONVERT(varchar(100), a.CREATED_ON, 108),1,5) time,
dateadd(minute,+60,a.CREATED_ON) time1,
dateadd(minute,+120,a.CREATED_ON) time2,
a.CREATED_ON,e.CREATED_ON react_time,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_POST a
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
left join HOME_NEIGHBOR_COMMENT e on a.POST_SID = e.POST_SID
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
where d.TYPE_NAME in('咨询物业')
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and a.CREATED_ON >='20160101'
and a.CREATED_ON <'20170101'
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and (f.FAMILY_NAME  like'%客服%' or f.FAMILY_NAME  like'%管家%')
and b.OWNER_TYPE=1--类型为业主
and convert(char(8),a.CREATED_ON,108)>='18:00:00' and  convert(char(8),a.CREATED_ON,108)<'22:00:00'--2
)t3
where t3.react_time<=time2--time1,time2
and t3.rn=1
)t4
group by t4.APARTMENT_NAME
union all
select t6.APARTMENT_NAME 小区名称,count(t6.react_name) 不超时 from(
select * from (
select ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,
b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,SUBSTRING(CONVERT(varchar(100), a.CREATED_ON, 108),1,5) time,
dateadd(minute,+60,a.CREATED_ON) time1,
dateadd(minute,+120,a.CREATED_ON) time2,
a.CREATED_ON,e.CREATED_ON react_time,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_POST a
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
left join HOME_NEIGHBOR_COMMENT e on a.POST_SID = e.POST_SID
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
where d.TYPE_NAME in('咨询物业')
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and a.CREATED_ON >='20160101'
and a.CREATED_ON <'20170101'
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and (f.FAMILY_NAME  like'%客服%' or f.FAMILY_NAME  like'%管家%')
and b.OWNER_TYPE=1--类型为业主
and convert(char(8),a.CREATED_ON,108)>='22:00:00' and  convert(char(8),a.CREATED_ON,108)<'24:00:00'--11
)t5
where CAST(t5.react_time AS DATE)= CAST(t5.CREATED_ON AS DATE)--11
or(CAST(t5.react_time AS DATE)= CAST(t5.CREATED_ON+1 AS DATE) and convert(char(8),t5.react_time,108)<'09:00:00' )--11
and t5.rn=1
)t6
group by t6.APARTMENT_NAME
union all
select t8.APARTMENT_NAME 小区名称,count(t8.react_name) 不超时 from(
select * from (
select ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,
b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,SUBSTRING(CONVERT(varchar(100), a.CREATED_ON, 108),1,5) time,
dateadd(minute,+60,a.CREATED_ON) time1,
dateadd(minute,+120,a.CREATED_ON) time2,
a.CREATED_ON,e.CREATED_ON react_time,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_POST a
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
left join HOME_NEIGHBOR_COMMENT e on a.POST_SID = e.POST_SID
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
where d.TYPE_NAME in('咨询物业')
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and a.CREATED_ON >='20160101'
and a.CREATED_ON <'20170101'
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and (f.FAMILY_NAME  like'%客服%' or f.FAMILY_NAME  like'%管家%')
and b.OWNER_TYPE=1--类型为业主
and  convert(char(8),a.CREATED_ON,108)<'08:30:00'--111
)t7
where convert(char(8),t7.react_time,108)<'09:00:00' --111
and CAST(t7.react_time AS DATE)= CAST(t7.CREATED_ON AS DATE)--111
and t7.rn=1
)t8
group by t8.APARTMENT_NAME
)t9
group by t9.小区名称





--提单超时未派单详情单
--实时健康度警告

select
 case when t1.未响应秒<3600 then
cast(FLOOR(LTRIM(t1.未响应秒%3600/60)) as varchar(10))+'分钟'
 when t1.未响应秒<86400 then
cast(FLOOR(LTRIM(t1.未响应秒%86400/3600)) as varchar(10))+'小时'+
cast(FLOOR(LTRIM(t1.未响应秒%3600/60)) as varchar(10))+'分钟'
ELSE
cast(FLOOR(LTRIM(t1.未响应秒/86400)) as varchar(10))+'天'+
cast(FLOOR(LTRIM(t1.未响应秒%86400/3600)) as varchar(10))+'小时'+
cast(FLOOR(LTRIM(t1.未响应秒%3600/60)) as varchar(10))+'分钟'
end 未响应时间,t1.*
 from(
select SUBSTRING(CONVERT(varchar(100),a.CREATED_ON, 108),1,5) 提报时点, datediff(mi,a.CREATED_ON,getdate())*60 未响应秒,
c.APARTMENT_NAME 小区名称,b.CATEGORY_NAME 服务类型,a.CREATED_ON 呼叫时间,a.RESPONSE_TIME 响应时间,
a.PROCESS_TIME 处理时间,e.OWNER_NO 提报人房号,e.OWNER_NAME 提报人姓名,e.FAMILY_NAME 提报人昵称,
a.TYPE_NAME 服务项目名称,a.SERVICE_NO 提单编号,a.SERVICE_DESC 备注,
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
               END) AS 服务提报状态,f.APARTMENT_NAME 创建人小区名称
from HOME_SERVICE_MAIN  a
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_APARTMENT f on e.APARTMENT_SID=f.APARTMENT_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where c.APARTMENT_NAME NOT IN('幸福家园','体验小区')
--and f.APARTMENT_NAME not in ('幸福家园','体验小区')
--and e.OWNER_NO like ('%一期%')
and b.CATEGORY_NAME not in('巡检','家政服务')
--and a.ROOM_NO is not null --巡检下此字段不为空,为业主提报的单子）
and a.CREATED_ON >='20170401'--当月提报
--and a.CREATED_ON <'20170401'
and a.RESPONSE_TIME is null
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
--and convert(char(8),a.CREATED_ON,108)>='08:30:00' and  convert(char(8),a.CREATED_ON,108)<'18:00:00'--1
--order by a.CREATED_ON,SUBSTRING(CONVERT(varchar(100),a.CREATED_ON, 108),1,5)
)t1 order by t1.呼叫时间




--咨询物业等超时未回复详情-具体到主帖
--剔除客服、物业、悦悦发帖
--实时健康度警告
select 
CASE 
when t2.回复时间 is null THEN
t2.未回复时长
else
t2.回复时长
end react_time
,t2.* from
(
select
case
 when t1.回复秒<3600 then
cast(FLOOR(LTRIM(t1.回复秒%3600/60)) as varchar(10))+'分钟'
 when t1.回复秒<86400 then
cast(FLOOR(LTRIM(t1.回复秒%86400/3600)) as varchar(10))+'小时'+
cast(FLOOR(LTRIM(t1.回复秒%3600/60)) as varchar(10))+'分钟'
ELSE
cast(FLOOR(LTRIM(t1.回复秒/86400)) as varchar(10))+'天'+
cast(FLOOR(LTRIM(t1.回复秒%86400/3600)) as varchar(10))+'小时'+
cast(FLOOR(LTRIM(t1.回复秒%3600/60)) as varchar(10))+'分钟'
end 回复时长
,
case
 when t1.秒<3600 then
cast(FLOOR(LTRIM(t1.秒%3600/60)) as varchar(10))+'分钟'
 when t1.秒<86400 then
cast(FLOOR(LTRIM(t1.秒%86400/3600)) as varchar(10))+'小时'+
cast(FLOOR(LTRIM(t1.秒%3600/60)) as varchar(10))+'分钟'
ELSE
cast(FLOOR(LTRIM(t1.秒/86400)) as varchar(10))+'天'+
cast(FLOOR(LTRIM(t1.秒%86400/3600)) as varchar(10))+'小时'+
cast(FLOOR(LTRIM(t1.秒%3600/60)) as varchar(10))+'分钟'
end 未回复时长
,t1.*
 from(
select a.POST_SID ,c.APARTMENT_NAME 项目,d.TYPE_NAME 类型,a.CREATED_ON 发帖时间,e.CREATED_ON 回复时间,
SUBSTRING(CONVERT(varchar(100),a.CREATED_ON, 108),1,5) 发帖时点,
 datediff(mi,a.CREATED_ON,e.CREATED_ON )*60 回复秒, datediff(mi,a.CREATED_ON,getdate() )*60 秒,
f.FAMILY_NAME 回帖人,
b.FAMILY_NAME 发帖人,a.POST_CONTENT 发帖内容,e.COMMENT_CONTENT 回复内容
from HOME_NEIGHBOR_POST a
left join HOME_NEIGHBOR_COMMENT e on a.POST_SID = e.POST_SID
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
LEFT join HOME_OWNER f on f.OWNER_SID = e.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
where a.CREATED_ON >'20170401'
and c.APARTMENT_NAME NOT IN('幸福家园','体验小区')--
and a.POST_OKFLAG like('%1%')--剔除已屏蔽的帖子(未保存至草稿箱,未屏蔽,屏蔽为0)
--and b.OWNER_NO like ('%一期%')   
and d.TYPE_NAME in('咨询物业')
--and a.CREATED_ON <'20170401'
and b.OWNER_NO not like('%物业%')
--and f.OWNER_NO  like('%物业%')
--and b.FAMILY_NAME not like('%客服%')
and e.CREATED_ON is null
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and b.OWNER_TYPE like('%1%')--类型为业主
--and convert(char(8),a.CREATED_ON,108)>='08:30:00' and  convert(char(8),a.CREATED_ON,108)<'18:00:00'--1
)t1 
--order by t1.发帖时间
)t2




--物业昵称不规范
--select distinct(t2.回帖人) from(
select
t1.*
 from(
select ROW_NUMBER() over (partition by c.APARTMENT_NAME,f.OWNER_SID order by c.APARTMENT_NAME,f.OWNER_SID  ) as rn,
 f.OWNER_PHONE ,f.OWNER_NAME,a.POST_SID ,c.APARTMENT_NAME 项目,d.TYPE_NAME 类型,a.CREATED_ON 发帖时间,e.CREATED_ON 回复时间,
SUBSTRING(CONVERT(varchar(100),a.CREATED_ON, 108),1,5) 发帖时点,
f.FAMILY_NAME 回帖人,
b.FAMILY_NAME 发帖人,a.POST_CONTENT 发帖内容,e.COMMENT_CONTENT 回复内容
from HOME_NEIGHBOR_POST a
left join HOME_NEIGHBOR_COMMENT e on a.POST_SID = e.POST_SID
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
LEFT join HOME_OWNER f on f.OWNER_SID = e.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
where 
--a.CREATED_ON >='20170301'
--and 
c.APARTMENT_NAME NOT IN('幸福家园','体验小区')--
and a.POST_OKFLAG like('%1%')--剔除已屏蔽的帖子(未保存至草稿箱,未屏蔽,屏蔽为0)
--and b.OWNER_NO like ('%一期%')
and d.TYPE_NAME in('咨询物业')
--and a.CREATED_ON <'20170401'
and b.OWNER_NO not like('%物业%')
and b.FAMILY_NAME not like('%客服%')
--and( f.FAMILY_NAME  not like('%客服%') or e.CREATED_ON is null)
and f.FAMILY_NAME  not like('%客服%')
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and f.FAMILY_NAME not in('悦悦')
and e.COMMENT_CONTENT like('%业主%')--业主您好
and b.OWNER_TYPE like('%1%')--类型为业主

--and convert(char(8),a.CREATED_ON,108)>='08:30:00' and  convert(char(8),a.CREATED_ON,108)<'18:00:00'--1
)t1 where t1.rn=1
order by t1.项目
--)t2







--按小区分，11月份及时关闭提单数
select t9.小区名称,sum(t9.及时关闭) 及时关闭 from(
select t1.小区名称, count(t1.SERVICE_SID) 及时关闭 from (
select * from(
select * , ROW_NUMBER() over (partition by t2.小区名称,t2.SERVICE_NO order by t2.小区名称,t2.SERVICE_NO,t2.rn desc) as rnn
 from (
select ROW_NUMBER() over (partition by a.SERVICE_SID  order by a.SERVICE_SID ,d.CREATED_ON ) as rn1,
a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,
a.CREATED_ON 呼叫时间,a.CREATED_ON+1 type1,a.CREATED_ON+7 type7,a.CREATED_ON+25 type25,d.CREATED_ON 关闭时间,
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,a.TYPE_NAME 服务内容,c.APARTMENT_NAME 小区名称,
ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.SERVICE_NO order by c.APARTMENT_NAME,a.SERVICE_NO,d.CREATED_ON ) as rn
from HOME_SERVICE_MAIN  a
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_SERVICE_HIST d on a.SERVICE_SID  = d.SERVICE_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where c.APARTMENT_NAME NOT IN('幸福家园','体验小区')
--and e.OWNER_NO like ('%一期%')
and b.CATEGORY_NAME  in('公共维修','投诉')
and (a.TYPE_NAME like '%安保投诉%' or a.TYPE_NAME like '%服务态度投诉%'or a.TYPE_NAME like '%绿化投诉%'or a.TYPE_NAME like '%清洁卫生投诉%')--1天
--and (a.TYPE_NAME like '%停车投诉%' or a.TYPE_NAME like '%报警设备%'or a.TYPE_NAME like '%道闸故障%'or a.TYPE_NAME like '%电梯故障%'or a.TYPE_NAME like '%健身设施故障%'or a.TYPE_NAME like '%其他设施设备故障%'or a.TYPE_NAME like '%弱电系统%'or a.TYPE_NAME like '%消防大类%'or a.TYPE_NAME like '%照明故障%')--7
--and a.TYPE_NAME like '%装修投诉%'--25
and ( a.SERVICE_STATUS=4 or a.SERVICE_STATUS=6 or  a.SERVICE_STATUS=9)
and ( d.HIST_TYPE=4 or d.HIST_TYPE=6 or  d.HIST_TYPE=9)
and a.CREATED_ON >='20170227'
and a.CREATED_ON <'20170306'
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
)t2
where t2.关闭时间 <= t2.type1
and t2.rn1=1)t3
where t3.rnn=1
)t1
group by t1.小区名称
union all
select t1.小区名称, count(t1.SERVICE_SID) 及时关闭 from (
select * from(
select * , ROW_NUMBER() over (partition by t2.小区名称,t2.SERVICE_NO order by t2.小区名称,t2.SERVICE_NO,t2.rn desc) as rnn
 from(
select  ROW_NUMBER() over (partition by a.SERVICE_SID  order by a.SERVICE_SID ,d.CREATED_ON ) as rn1,
a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,
a.CREATED_ON 呼叫时间,a.CREATED_ON+1 type1,a.CREATED_ON+7 type7,a.CREATED_ON+25 type25,d.CREATED_ON 关闭时间,
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,a.TYPE_NAME 服务内容,c.APARTMENT_NAME 小区名称,
ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.SERVICE_NO order by c.APARTMENT_NAME,a.SERVICE_NO,d.CREATED_ON ) as rn
from HOME_SERVICE_MAIN  a
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_SERVICE_HIST d on a.SERVICE_SID  = d.SERVICE_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where c.APARTMENT_NAME NOT IN('幸福家园','体验小区')
--and e.OWNER_NO like ('%一期%')
and b.CATEGORY_NAME  in('公共维修','投诉')
--and (a.TYPE_NAME like '%安保投诉%' or a.TYPE_NAME like '%服务态度投诉%'or a.TYPE_NAME like '%绿化投诉%'or a.TYPE_NAME like '%清洁卫生投诉%')--1天
and (a.TYPE_NAME like '%停车投诉%' or a.TYPE_NAME like '%报警设备%'or a.TYPE_NAME like '%道闸故障%'or a.TYPE_NAME like '%电梯故障%'or a.TYPE_NAME like '%健身设施故障%'or a.TYPE_NAME like '%其他设施设备故障%'or a.TYPE_NAME like '%弱电系统%'or a.TYPE_NAME like '%消防大类%'or a.TYPE_NAME like '%照明故障%')--7
--and a.TYPE_NAME like '%装修投诉%'--25
and ( a.SERVICE_STATUS=4 or a.SERVICE_STATUS=6 or  a.SERVICE_STATUS=9)
and ( d.HIST_TYPE=4 or d.HIST_TYPE=6 or  d.HIST_TYPE=9)
and a.CREATED_ON >='20170227'
and a.CREATED_ON <'20170306'
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
)t2
where t2.关闭时间 <= t2.type7
and t2.rn1=1)t3
where t3.rnn=1
)t1
group by t1.小区名称
union all
select t1.小区名称, count(t1.SERVICE_SID) 及时关闭 from (
select * from(
select * , ROW_NUMBER() over (partition by t2.小区名称,t2.SERVICE_NO order by t2.小区名称,t2.SERVICE_NO,t2.rn desc) as rnn
 from (
select
ROW_NUMBER() over (partition by a.SERVICE_SID  order by a.SERVICE_SID ,d.CREATED_ON ) as rn1,
a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC, 
a.CREATED_ON 呼叫时间,a.CREATED_ON+1 type1,a.CREATED_ON+7 type7,a.CREATED_ON+25 type25,d.CREATED_ON 关闭时间,
a.RESPONSE_TIME 响应时间,a.PROCESS_TIME 处理时间,a.SERVICE_CATEGORY,b.CATEGORY_NAME 服务类型,a.TYPE_NAME 服务内容,c.APARTMENT_NAME 小区名称,
ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.SERVICE_NO order by c.APARTMENT_NAME,a.SERVICE_NO,d.CREATED_ON ) as rn
from HOME_SERVICE_MAIN  a
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_SERVICE_HIST d on a.SERVICE_SID  = d.SERVICE_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where c.APARTMENT_NAME NOT IN('幸福家园','体验小区')
--and e.OWNER_NO like ('%一期%')
and b.CATEGORY_NAME  in('公共维修','投诉')
--and (a.TYPE_NAME like '%安保投诉%' or a.TYPE_NAME like '%服务态度投诉%'or a.TYPE_NAME like '%绿化投诉%'or a.TYPE_NAME like '%清洁卫生投诉%')--1天
--and (a.TYPE_NAME like '%停车投诉%' or a.TYPE_NAME like '%报警设备%'or a.TYPE_NAME like '%道闸故障%'or a.TYPE_NAME like '%电梯故障%'or a.TYPE_NAME like '%健身设施故障%'or a.TYPE_NAME like '%其他设施设备故障%'or a.TYPE_NAME like '%弱电系统%'or a.TYPE_NAME like '%消防大类%'or a.TYPE_NAME like '%照明故障%')--7
and a.TYPE_NAME like '%装修投诉%'--25
and ( a.SERVICE_STATUS=4 or a.SERVICE_STATUS=6 or  a.SERVICE_STATUS=9)
and ( d.HIST_TYPE=4 or d.HIST_TYPE=6 or  d.HIST_TYPE=9)
and a.CREATED_ON >='20170227'
and a.CREATED_ON <'20170306'
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
)t2
where t2.关闭时间 <= t2.type25
and t2.rn1=1
)t3
where t3.rnn=1
)t1
group by t1.小区名称
)t9
group by t9.小区名称
order by sum(t9.及时关闭) desc




--关闭提单数和考核的提单总数
--及时关闭只考核：家政服务、巡检
select t1.小区名称, count(t1.SERVICE_SID) 总关闭单数 from 
(select * from
(select ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.SERVICE_NO,a.SERVICE_SID order by c.APARTMENT_NAME,a.SERVICE_NO,a.SERVICE_SID  ) as rn,
e.OWNER_NO,a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,
a.SERVICE_DESC,a.SERVICE_STATUS,a.CREATED_ON ,d.CREATED_ON RESPONSE_TIME,
a.PROCESS_TIME ,a.SERVICE_CATEGORY,b.CATEGORY_NAME ,c.APARTMENT_NAME  小区名称
from HOME_SERVICE_MAIN a
 left join HOME_SERVICE_HIST d on d.SERVICE_SID=a.SERVICE_SID 
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID 
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID 
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID 
where c.APARTMENT_NAME NOT IN('幸福家园','体验小区') 
--and d.HIST_TYPE = 2 
--and ( a.SERVICE_STATUS=4 or a.SERVICE_STATUS=6 or  a.SERVICE_STATUS=9)--注销这一句则为考核提单总数
 --and b.CATEGORY_NAME not in('家政服务','巡检')
--and  e.OWNER_NO  like('%二期%')
--and a.CREATED_ON >='20170227'
--and a.CREATED_ON <'20170306'
and b.CATEGORY_NAME  in('公共维修','投诉')--及时关闭考核提数
AND a.SERVICE_STATUS NOT IN ('3') 
AND a.SERVICE_DESC NOT LIKE'%测%')t2 
where t2.rn=1
)t1
group by  t1.小区名称
order by count(t1.SERVICE_SID) desc




--平均关闭时长
select t1.APARTMENT_NAME 项目,count(t1.SERVICE_SID) 关闭单数,AVG(t1.lenth) 平均关闭时长 from(
select e.CREATED_ON close_time,DATEDIFF( mi, a.CREATED_ON, e.CREATED_ON)/60.0  lenth,e.HIST_TYPE,a.SERVICE_DESC,d.OWNER_NO,a.SERVICE_SID,a.APARTMENT_SID
,ROW_NUMBER() over (partition by a.SERVICE_SID  order by a.SERVICE_SID ,e.CREATED_ON ) as rn,
a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.CREATED_ON ,a.RESPONSE_TIME ,a.PROCESS_TIME ,a.SERVICE_CATEGORY,
b.CATEGORY_NAME ,c.APARTMENT_NAME,a.SERVICE_STATUS
from HOME_SERVICE_MAIN a
left join HOME_SERVICE_HIST e on a.SERVICE_SID  = e.SERVICE_SID
left join HOME_OWNER d on a.CREATEDBY= d.OWNER_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID where APARTMENT_NAME NOT IN('幸福家园')
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
and a.created_on>='20160101'
and a.created_on<'20170101'
and b.CATEGORY_NAME in ('投诉','公共维修')
and ( a.SERVICE_STATUS=4 or a.SERVICE_STATUS=6 or  a.SERVICE_STATUS=9)
and ( e.HIST_TYPE=4 or e.HIST_TYPE=6 or  e.HIST_TYPE=9)
)t1
where t1.rn=1
group by t1.APARTMENT_NAME
order by sum(t1.lenth)/count(t1.SERVICE_SID)





--好评数
--剔除巡检、剔除自动好评
select * from (
select t2.apartment_name,t2.OWNER_NAME ,t2.FAMILY_NAME,t2.rn 单数,ROW_NUMBER() over (partition by t2.apartment_name,t2.OWNER_NAME,t2.FAMILY_NAME,t2.OWNER_PHONE order by t2.apartment_name,t2.OWNER_NAME,t2.rn desc ) as rnn from(
select t1.apartment_name,t1.OWNER_NAME,t1.FAMILY_NAME,t1.OWNER_PHONE,
ROW_NUMBER() over (partition by t1.apartment_name,t1.OWNER_NAME,t1.FAMILY_NAME,t1.OWNER_PHONE order by t1.apartment_name,t1.OWNER_NAME,t1.FAMILY_NAME,t1.OWNER_PHONE ) as rn from(
SELECT hs.apartment_sid,ha.apartment_name,hs.PROCESS_TIME,hs.REMARK,hs.PROCESS_USER,a.OWNER_SID,a.OWNER_NAME,a.FAMILY_NAME,a.OWNER_PHONE,evaluation_item1,evaluation_item2,evaluation_item3  , evaluation_item1+evaluation_item2+evaluation_item3 总分
from home_service_main hs
left join home_apartment ha on hs.apartment_sid=ha.apartment_sid
left join home_owner a on hs.PROCESS_USER=a.owner_sid
where  (service_status  =  6  or  service_status  =  9)
and SERVICE_CATEGORY in(select CATEGORY_SID
from HOME_SERVICE_CATEGORY
where CATEGORY_NAME not in('巡检'))and hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)and hs.PROCESS_TIME is not null
and evaluation_item1+evaluation_item2+evaluation_item3 !=15--包括自动好评则剔除这个限制条件
and evaluation_item1+evaluation_item2+evaluation_item3 >=12--好评剔除自动好评
--and evaluation_item1+evaluation_item2+evaluation_item3 <12--中差评
and hs.PROCESS_TIME<='20170119'
)t1)t2)t3
where rnn=1
order by t3.单数 desc




--悦嘉家车辆违停记录
select a.CREATED_ON 创建时间,b.APARTMENT_NAME 小区名称,c.ROOM_NO 房号,f.CAR_OWNER 车主姓名,a.CAR_NO 车牌号,
f.CAR_PHONE 联系电话,f.CAR_BRAND 品牌,f.CAR_MODEL 车型,f.CAR_COLOR 颜色,a.VIOLATIONS_REMARK 违停备注
,e.OWNER_NAME 创建人,a.PACKING_LOCATION 停车位置,a.VIOLATIONS_SID 违停ID,f.CAR_YEAR 购车年份
 from HOME_CAR_VIOLATIONS a
 left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
left join HOME_CAR_APARTMENT c on a.CAR_NO=c.CAR_NO
left join HOME_APARTMENT d on c.APARTMENT_SID = d.APARTMENT_SID
left join HOME_CAR f on a.CAR_NO=f.CAR_NO
where a.created_on>='20150701'
and e.OWNER_STATUS=1
--and b.APARTMENT_NAME like('%东方润园%')
order by a.CREATED_ON desc


--悦园区小区车辆管理
select f.CREATED_ON 维护时间,b.APARTMENT_NAME 小区名称,c.ROOM_NO 房号,f.CAR_OWNER 车主姓名,c.CAR_PLACE_NO 车位号,c.CAR_NO 车牌号,
f.CAR_PHONE 联系电话,f.CAR_BRAND 品牌,f.CAR_MODEL 车型,f.CAR_COLOR 颜色
,e.OWNER_NAME 创建人,f.CAR_YEAR 购车年份
 from HOME_CAR f
 left join HOME_OWNER e on f.CREATEDBY=e.OWNER_SID
left join HOME_CAR_APARTMENT c on f.CAR_NO=c.CAR_NO
left join HOME_APARTMENT b on c.APARTMENT_SID = b.APARTMENT_SID
where f.created_on<'20150903'
and e.OWNER_STATUS=1
--and b.APARTMENT_NAME like('%东方郡%')
order by f.CREATED_ON desc


--快递代收记录（悦嘉家）
select a.CREATED_ON 到站时间,a.MODIFIED_ON 领取时间,a.EXPRESS_REMARK 领取密码,
a.EXPRESS_PHONE 联系电话,a.EXPRESS_NO 快递单号,a.EXPRESS_COMPANY 快递公司名称,
b.OWNER_NO 房号,c.APARTMENT_NAME 项目名称,
(CASE a.EXPRESS_STATUS
                 WHEN '2' THEN
                  '已到站'
                 WHEN '3' THEN
                  '已领取'
                 ELSE
                  ''
               END) AS 快递状态
 from HOME_EXPRESS a
 left join HOME_OWNER b on a.OWNER_SID =b.OWNER_SID
 left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
order by a.CREATED_ON desc


--快递代收按项目汇总
select t1.项目名称,count(t1.到站时间) 快递代收数 from(
select a.CREATED_ON 到站时间,a.MODIFIED_ON 领取时间,a.EXPRESS_REMARK 领取密码,
a.EXPRESS_PHONE 联系电话,a.EXPRESS_NO 快递单号,a.EXPRESS_COMPANY 快递公司名称,
b.OWNER_NO 房号,c.APARTMENT_NAME 项目名称,
(CASE a.EXPRESS_STATUS
                 WHEN '2' THEN
                  '已到站'
                 WHEN '3' THEN
                  '已领取'
                 ELSE
                  ''
               END) AS 快递状态
 from HOME_EXPRESS a
 left join HOME_OWNER b on a.OWNER_SID =b.OWNER_SID
 left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where a.CREATED_ON<'20170226'
and a.CREATED_ON>='20170101'
--and b.OWNER_NO like('%一期%')--东方福邸一期、二期分别统计fu'di
)t1
group by t1.项目名称
order by count(t1.到站时间) desc



--主帖图片数、点赞数
--剔除物业工作人员
select t2.* ,t2.点赞数+t2.回复数 互动,f.POST_CONTENT 发帖内容  from (
select t1.项目名称,t1.房号,t1.姓名,t1.昵称,t1.手机,t1.业主类型,t1.发帖类型,t1.发帖时间,t1.主帖ID,t1.图片数,
count(t1.点赞) 点赞数,count(t1.回复) 回复数 from
(--t1.帖子内容,
select e.OWNER_NO 房号,e.OWNER_NAME 姓名,e.OWNER_PHONE 手机,e.FAMILY_NAME 昵称,b.POST_SID 主帖ID,c.APARTMENT_NAME 项目名称,
(CASE e.OWNER_CATEGORY
                 WHEN '0' THEN
                  '业主'
                 WHEN '1' THEN
                  '租户'
                 WHEN '2' THEN
                  '家属'
                 ELSE
                  ''
               END) AS 业主类型,
d.TYPE_NAME 发帖类型,b.CREATED_ON 发帖时间,b.POST_CONTENT 帖子内容,a.LIKE_SID 点赞,f.COMMENT_SID 回复,
b.POST_IMAGES,LEN(b.POST_IMAGES)-LEN(replace(b.POST_IMAGES,';',''))+1 as 图片数
from HOME_NEIGHBOR_POST b
left join HOME_NEIGHBOR_LIKE a on  a.POST_SID = b.POST_SID
left join HOME_OWNER e on b.CREATEDBY = e.OWNER_SID
left join  HOME_APARTMENT c on e.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on b.POST_TYPE = d.TYPE_SID
left join HOME_NEIGHBOR_COMMENT f on b.POST_SID = f.POST_SID
where e.OWNER_NO not like ('%物业%')
and e.OWNER_NO not like ('%总部%')
and e.OWNER_NAME not like ('%马甲%') and  e.OWNER_NAME not like ('%王孟%')
and e.OWNER_NAME not like ('%东方郡%')and e.OWNER_NAME not like ('%王一艳%')
and e.FAMILY_NAME not like ('%客服%')
and e.FAMILY_NAME not like ('%悦悦%')
and e.FAMILY_NAME not like ('%管家%')
and d.TYPE_NAME in('随便说说')
and a.CREATED_ON >='20170123'
and a.CREATED_ON <'20170211'
)t1
group by t1.项目名称,t1.房号,t1.姓名,t1.昵称,t1.手机,t1.业主类型,t1.发帖类型,t1.发帖时间,t1.图片数,t1.主帖ID
--order by t1.项目名称,t1.图片数 desc
)t2
left join HOME_NEIGHBOR_POST f on t2.主帖ID=f.POST_SID
order by t2.点赞数+t2.回复数 desc




--各类型发帖数
select t1.发帖类型,count(t1.主帖ID) 各类型发帖数 from(
select e.OWNER_NO 房号,e.OWNER_NAME 姓名,e.OWNER_PHONE 手机,e.FAMILY_NAME 昵称,b.POST_SID 主帖ID,c.APARTMENT_NAME 项目名称,
(CASE e.OWNER_CATEGORY
                 WHEN '0' THEN
                  '业主'
                 WHEN '1' THEN
                  '租户'
                 WHEN '2' THEN
                  '家属'
                 ELSE
                  ''
               END) AS 业主类型,
d.TYPE_NAME 发帖类型,b.CREATED_ON 发帖时间,b.POST_CONTENT 帖子内容,
b.POST_IMAGES,LEN(b.POST_IMAGES)-LEN(replace(b.POST_IMAGES,';',''))+1 as 图片数
from HOME_NEIGHBOR_POST b
left join HOME_OWNER e on b.CREATEDBY = e.OWNER_SID
left join  HOME_APARTMENT c on e.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on b.POST_TYPE = d.TYPE_SID
where e.OWNER_NO not like ('%物业%')
and e.OWNER_NO not like ('%总部%')
and e.OWNER_NAME not like ('%马甲%') and  e.OWNER_NAME not like ('%王孟%')
and e.OWNER_NAME not like ('%东方郡%')and e.OWNER_NAME not like ('%王一艳%')
and e.FAMILY_NAME not like ('%客服%')
and e.FAMILY_NAME not like ('%悦悦%')
and e.FAMILY_NAME not like ('%管家%')
and c.APARTMENT_NAME not in('幸福家园','体验小区')
--and d.TYPE_NAME in('随便说说')
and b.CREATED_ON >='20170101'
and b.CREATED_ON <'20170326')t1
group by t1.发帖类型
order by count(t1.主帖ID) desc




--访客证数
select t1.APARTMENT_NAME,count(distinct(t1.CARD_SID)) 访客证数 from(
select b.APARTMENT_NAME,a.CARD_SID,a.CARD_NO 访客证编号,a.OWNER_SID, a.VISITOR_NAME 访客姓名, a.VISITOR_CAR_NO 访客车牌号,a.VISIT_TIME 到访时间, a.LEAVE_TIME 离开时间,a.END_DATE 离开日期,a.PASS_COUNT 来访次数,a.CREATED_ON 访客证生成时间,a.MODIFIED_ON 来访验证时间
,c.OWNER_NO 业主房号,c.OWNER_NAME 业主真实名字,c.OWNER_PHONE 业主手机号,c.FAMILY_NAME 业主昵称,c.OWNER_STATUS 用户状态,c.OWNER_CATEGORY 业主类型
 from HOME_VISITOR_CARD a
left join HOME_APARTMENT b on a.APARTMENT_SID=b.APARTMENT_SID
left join HOME_OWNER c on c.OWNER_SID=a.OWNER_SID
where c.OWNER_STATUS=1
--and c.owner_no like('%一期%')
and c.OWNER_TYPE like ('%1%')
and a.CREATED_ON>='20160101'
and a.CREATED_ON<'20161231'
--and b.APARTMENT_NAME in('富越香郡')
--order by a.CREATED_ON desc
)t1
group by t1.APARTMENT_NAME
order by count(distinct(t1.CARD_SID)) desc


--账单类型：
--代收水费
--物业服务费
--车位服务费
--代收公共能耗费


--物业账单细分,账单数,房号数、金额等
--不细分则去掉 t1.账单名称 即可
--只计算有支付时间的部分
select t1.APARTMENT_NAME,t1.账单名称,count(distinct(t1.房号)) 使用房号数,count(distinct(t1.账单编号)) 账单数,sum(t1.账单金额) 缴费金额 from(
select a.BILL_ITEM_SID 账单编号,a.BILL_ITEM_NAME 账单名称,a.ROOM_NO 房号,a.BILL_ITEM_DESC 账单描述,a.BILL_ITEM_MONEY 账单金额,
a.PAY_TIME 支付时间,a.CREATED_ON 创建时间,a.PAY_BILL 支付订单号,a.BILL_DATE_SPAN 账单周期范围,a.REMARK 备注,a.BILL_ITEM_MONTH 销帐状态,
(CASE a.BILL_ITEM_STATUS
                 WHEN '0' THEN
                  '已上传'
                 WHEN '1' THEN
                  '待支付'
                 WHEN '2' THEN
                  '已支付'
                 WHEN '9' THEN
                  '取消'
                 WHEN '5' THEN
                  '订单提交'
                 ELSE
                  ''
               END) AS 账单状态,
(CASE b.Type
                 WHEN '0' THEN
                  '生活缴费'
                 WHEN '1' THEN
                  '服务窗'
                 WHEN '2' THEN
                  '悦嘉家App'
                 ELSE
                  ''
               END) AS 缴费入口,c.APARTMENT_NAME
from HOME_PROPERTY_BILL_ITEM a
left join BillPayZFB b on b.SheetID=a.PAY_BILL
left join HOME_APARTMENT c on c.APARTMENT_SID=a.APARTMENT_SID
where  a.CREATED_ON<'20170326'
and   a.CREATED_ON>='20170226'
and a.BILL_ITEM_STATUS =2
--and a.ROOM_NO like ('%二期%')--东方福邸一期二期分开统计
and a.PAY_TIME is not null
--order by b.Type desc
)t1
group by t1.APARTMENT_NAME,t1.账单名称
order by t1.账单名称,sum(t1.账单金额) desc



--物业账单数据按月汇总,按小区加上t1.APARTMENT_NAME即可
--细分加上 t1.账单名称 即可
--只计算有支付时间的部分
select t1.APARTMENT_NAME,t1.月,t1.账单名称,count(distinct(t1.房号)) 使用房号数,count(distinct(t1.账单编号)) 账单数,sum(t1.账单金额) 缴费金额 from(
select cast(year(a.CREATED_ON) as varchar)+'年'+cast(month(a.CREATED_ON) as varchar)+'月' 月,a.BILL_ITEM_SID 账单编号,a.BILL_ITEM_NAME 账单名称,a.ROOM_NO 房号,a.BILL_ITEM_DESC 账单描述,a.BILL_ITEM_MONEY 账单金额,
a.PAY_TIME 支付时间,a.CREATED_ON 创建时间,a.PAY_BILL 支付订单号,a.BILL_DATE_SPAN 账单周期范围,a.REMARK 备注,a.BILL_ITEM_MONTH 销帐状态,
(CASE a.BILL_ITEM_STATUS
                 WHEN '0' THEN
                  '已上传'
                 WHEN '1' THEN
                  '待支付'
                 WHEN '2' THEN
                  '已支付'
                 WHEN '9' THEN
                  '取消'
                 WHEN '5' THEN
                  '订单提交'
                 ELSE
                  ''
               END) AS 账单状态,
(CASE b.Type
                 WHEN '0' THEN
                  '生活缴费'
                 WHEN '1' THEN
                  '服务窗'
                 WHEN '2' THEN
                  '悦嘉家App'
                 ELSE
                  ''
               END) AS 缴费入口,c.APARTMENT_NAME
from HOME_PROPERTY_BILL_ITEM a
left join BillPayZFB b on b.SheetID=a.PAY_BILL
left join HOME_APARTMENT c on c.APARTMENT_SID=a.APARTMENT_SID
where  a.CREATED_ON<'20170221'
--and a.CREATED_ON>='20160101'
and a.BILL_ITEM_STATUS =2
--and a.ROOM_NO like ('%二期%')--东方福邸一期二期分开统计
and a.PAY_TIME is not null
--order by b.Type desc
)t1
group by t1.月,t1.账单名称,t1.APARTMENT_NAME
--order by t1.账单名称,t1.APARTMENT_NAME desc
order by t1.APARTMENT_NAME,t1.账单名称,t1.月




--房号对应下的物业账单单数及金额
--只计算有支付时间的部分
select t1.APARTMENT_NAME,t1.房号,count(distinct(t1.账单编号)) 账单数,sum(t1.账单金额) 缴费金额 from(
select cast(year(a.CREATED_ON) as varchar)+'年'+cast(month(a.CREATED_ON) as varchar)+'月' 月,a.BILL_ITEM_SID 账单编号,a.BILL_ITEM_NAME 账单名称,a.ROOM_NO 房号,a.BILL_ITEM_DESC 账单描述,a.BILL_ITEM_MONEY 账单金额,
a.PAY_TIME 支付时间,a.CREATED_ON 创建时间,a.PAY_BILL 支付订单号,a.BILL_DATE_SPAN 账单周期范围,a.REMARK 备注,a.BILL_ITEM_MONTH 销帐状态,
(CASE a.BILL_ITEM_STATUS
                 WHEN '0' THEN
                  '已上传'
                 WHEN '1' THEN
                  '待支付'
                 WHEN '2' THEN
                  '已支付'
                 WHEN '9' THEN
                  '取消'
                 WHEN '5' THEN
                  '订单提交'
                 ELSE
                  ''
               END) AS 账单状态,
(CASE b.Type
                 WHEN '0' THEN
                  '生活缴费'
                 WHEN '1' THEN
                  '服务窗'
                 WHEN '2' THEN
                  '悦嘉家App'
                 ELSE
                  ''
               END) AS 缴费入口,c.APARTMENT_NAME
from HOME_PROPERTY_BILL_ITEM a
left join BillPayZFB b on b.SheetID=a.PAY_BILL
left join HOME_APARTMENT c on c.APARTMENT_SID=a.APARTMENT_SID
where  a.CREATED_ON<'20170221'
--and a.CREATED_ON>='20160101'
and a.BILL_ITEM_STATUS =2
--and a.ROOM_NO like ('%二期%')--东方福邸一期二期分开统计
and a.PAY_TIME is not null--只计算有支付时间的部分
--order by b.Type desc
)t1
group by t1.APARTMENT_NAME,t1.房号
order by count(distinct(t1.账单编号)) desc



--房号去重按月按项目
select * from(
select t2.月,t2.APARTMENT_NAME 小区名称,count(t2.rn) 安装户数 FROM(
select t1.* from (
SELECT  cast(year(a.CREATED_ON) as varchar)+'年'+cast(month(a.CREATED_ON) as varchar)+'月' 月,a.OWNER_type,a.CREATED_ON,a.OWNER_NO,a.OWNER_SID ,b.APARTMENT_NAME, ROW_NUMBER() over (partition by b.APARTMENT_NAME,a.OWNER_NO order by a.CREATED_ON,b.APARTMENT_NAME ) as rn
          FROM HOME_OWNER AS a
   left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
     WHERE a.OWNER_type like('%1%')
        -- and a.OWNER_NO  like ('%一期%')--门禁数据不剔除
    -- and b.APARTMENT_NAME  in ('东方润园')
 -- and b.APARTMENT_NAME not in ('幸福家园','体验小区','金橡臻园')
)t1
where t1.rn =1
--and t1.CREATED_ON >= '2017-01-01 12:00:00'
and t1.CREATED_ON < '2017-01-21'
)t2
group by t2.APARTMENT_NAME,t2.月
)t3
order by t3.月,t3.安装户数 asc




--安装户数月新增
select * from(
select t2.月,count(t2.rn) 安装户数 FROM(
select t1.* from (
SELECT  cast(year(a.CREATED_ON) as varchar)+'年'+cast(month(a.CREATED_ON) as varchar)+'月' 月,a.OWNER_type,a.CREATED_ON,a.OWNER_NO,a.OWNER_SID ,b.APARTMENT_NAME, ROW_NUMBER() over (partition by b.APARTMENT_NAME,a.OWNER_NO order by a.CREATED_ON,b.APARTMENT_NAME ) as rn
          FROM HOME_OWNER AS a
   left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
     WHERE a.OWNER_type like('%1%')
        -- and a.OWNER_NO  like ('%一期%')--门禁数据不剔除
    -- and b.APARTMENT_NAME  in ('东方润园')
 -- and b.APARTMENT_NAME not in ('幸福家园','体验小区','金橡臻园')
)t1
where t1.rn =1--注释这一句即为求注册ID数
--and t1.CREATED_ON >= '2017-01-01 12:00:00'
and t1.CREATED_ON < '2017-01-21'
)t2
group by t2.月
)t3
order by t3.月




--月新增/累计注册安装户数/ID数
--日新增/累计数,将Y_month的计算规则替换成 convert(varchar(10),a.CREATED_ON,120) 即可
select * from (
select t3.月,t3.Y_month,t3.rn_M 月新增数,
sum(t3.rn_M) over(partition by t3.Row order by t3.Y_month) 累计注册数,t3.Row
 from(
select t2.月,
ROW_NUMBER() over (partition by t2.Y_month order by t2.Y_month,t2.rn_M desc) as Row--按月分组月新增注册
,t2.Y_month,t2.rn_A,t2.rn_M
from(
select t1.*,ROW_NUMBER() over (partition by t1.APARTMENT_NAME,t1.Y_month order by t1.APARTMENT_NAME,t1.Y_month  ) as rn_A,
ROW_NUMBER() over (partition by t1.Y_month order by t1.Y_month  ) as rn_M
from (
SELECT   cast(year(a.CREATED_ON) as varchar)+'年'+cast(month(a.CREATED_ON) as varchar)+'月' 月,
a.OWNER_type,a.CREATED_ON,a.OWNER_NO,a.OWNER_SID ,b.APARTMENT_NAME, convert(varchar(7),a.CREATED_ON,120) Y_month,
ROW_NUMBER() over (partition by b.APARTMENT_NAME,a.OWNER_NO order by a.CREATED_ON,b.APARTMENT_NAME ) as rn
FROM HOME_OWNER AS a
left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
WHERE a.OWNER_type like('%1%')
        -- and a.OWNER_NO  like ('%一期%')--门禁数据不剔除
    -- and b.APARTMENT_NAME  in ('东方润园')
 --and b.APARTMENT_NAME not in ('幸福家园','体验小区','金橡臻园')
)t1
where t1.CREATED_ON >= '2015-01-01'--这一句不要改动
--and t1.Y_month < '2017-12-01'
and t1.rn =1--求ID数则注释这一句
--order by t1.Y_month
)t2
)t3
where t3.Row =1)t4
where t4.Y_month>='2016-12-01'--取日累计可限制时间



--按项目按月汇总的累计安装户数/ID数
select t3.月,t3.Y_month,t3.rn_M 月新增数,t3.APARTMENT_NAME 小区名称,
sum(t3.rn_A) over(partition by t3.Row,t3.APARTMENT_NAME order by t3.Y_month) 累计注册数,t3.Row
 from(
select t2.月,t2.APARTMENT_NAME,
ROW_NUMBER() over (partition by t2.APARTMENT_NAME,t2.Y_month order by t2.APARTMENT_NAME,t2.Y_month,t2.rn_A desc) as Row--按月分组月新增注册
,t2.Y_month,t2.rn_A,t2.rn_M
from(
select t1.*,ROW_NUMBER() over (partition by t1.APARTMENT_NAME,t1.Y_month order by t1.APARTMENT_NAME,t1.Y_month  ) as rn_A,
ROW_NUMBER() over (partition by t1.Y_month order by t1.Y_month  ) as rn_M
from (
SELECT   cast(year(a.CREATED_ON) as varchar)+'年'+cast(month(a.CREATED_ON) as varchar)+'月' 月,
a.OWNER_type,a.CREATED_ON,a.OWNER_NO,a.OWNER_SID ,b.APARTMENT_NAME, convert(varchar(7),a.CREATED_ON,120) Y_month,
ROW_NUMBER() over (partition by b.APARTMENT_NAME,a.OWNER_NO order by a.CREATED_ON,b.APARTMENT_NAME ) as rn
FROM HOME_OWNER AS a
left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
WHERE a.OWNER_type like('%1%')
        -- and a.OWNER_NO  like ('%一期%')--门禁数据不剔除
    -- and b.APARTMENT_NAME  in ('东方润园')
 and b.APARTMENT_NAME not in ('幸福家园','体验小区','金橡臻园')
)t1
where t1.CREATED_ON >= '2015-12-01 '
--and t1.Y_month < '2017-12-01'
and t1.rn =1--求ID数则注释这一句
--order by t1.Y_month
)t2
)t3
where t3.Row =1



--邻居圈互动ID数
--发帖、点赞、回复
select t1.APARTMENT_NAME,count(t1.ID) 邻居圈互动 from(
select b.APARTMENT_NAME,c.CREATEDBY ID
from HOME_NEIGHBOR_POST a
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_APARTMENT b on a.APARTMENT_SID=b.APARTMENT_SID
left join HOME_NEIGHBOR_LIKE c on a.POST_SID =c.POST_SID
left join HOME_NEIGHBOR_COMMENT d on a.POST_SID =d.POST_SID 
where a.CREATED_ON >='20150101'
and a.CREATED_ON <'20170317'
and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
 --and e.OWNER_NO  like ('%二期%')--东方福邸一期、二期分别统计
and c.CREATEDBY is not null--d.CREATEDBY,c.CREATEDBY
union --union剔除重复值,union all保留重复值
select b.APARTMENT_NAME,a.CREATEDBY ID
from HOME_NEIGHBOR_POST a
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_APARTMENT b on a.APARTMENT_SID=b.APARTMENT_SID
left join HOME_NEIGHBOR_LIKE c on a.POST_SID =c.POST_SID
left join HOME_NEIGHBOR_COMMENT d on a.POST_SID =d.POST_SID 
where a.CREATED_ON >='20150101'
and a.CREATED_ON <'20170317'
and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
 --and e.OWNER_NO  like ('%二期%')--东方福邸一期、二期分别统计
and d.CREATEDBY is not null--d.CREATEDBY,c.CREATEDBY
union 
select b.APARTMENT_NAME,d.CREATEDBY ID
from HOME_NEIGHBOR_POST a
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_APARTMENT b on a.APARTMENT_SID=b.APARTMENT_SID
left join HOME_NEIGHBOR_LIKE c on a.POST_SID =c.POST_SID
left join HOME_NEIGHBOR_COMMENT d on a.POST_SID =d.POST_SID 
where a.CREATED_ON >='20150101'
and a.CREATED_ON <'20170317'
and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
 --and e.OWNER_NO  like ('%二期%')--东方福邸一期、二期分别统计
and d.CREATEDBY is not null--d.CREATEDBY,c.CREATEDBY
)t1
group by t1.APARTMENT_NAME 
order by count(distinct(t1.ID)) desc



--邻居圈互动数项目月新增
select t1.APARTMENT_NAME,t1.Y_month,count(t1.ID) 邻居圈互动 from(
select b.APARTMENT_NAME,a.CREATEDBY ID,convert(varchar(7),a.CREATED_ON,120) Y_month
from HOME_NEIGHBOR_POST a
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_APARTMENT b on a.APARTMENT_SID=b.APARTMENT_SID
left join HOME_NEIGHBOR_LIKE c on a.POST_SID =c.POST_SID
left join HOME_NEIGHBOR_COMMENT d on a.POST_SID =d.POST_SID
where a.CREATED_ON <'20170313'
and c.CREATEDBY is not null--d.CREATEDBY,c.CREATEDBY
and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
union --union剔除重复值,union all保留重复值
select b.APARTMENT_NAME,c.CREATEDBY ID,convert(varchar(7),a.CREATED_ON,120) Y_month
from HOME_NEIGHBOR_POST a
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_APARTMENT b on a.APARTMENT_SID=b.APARTMENT_SID
left join HOME_NEIGHBOR_LIKE c on a.POST_SID =c.POST_SID
left join HOME_NEIGHBOR_COMMENT d on a.POST_SID =d.POST_SID
where a.CREATED_ON <'20170313'
and c.CREATEDBY is not null--d.CREATEDBY,c.CREATEDBY
and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
union --union剔除重复值,union all保留重复值
select b.APARTMENT_NAME,d.CREATEDBY ID,convert(varchar(7),a.CREATED_ON,120) Y_month
from HOME_NEIGHBOR_POST a
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_APARTMENT b on a.APARTMENT_SID=b.APARTMENT_SID
left join HOME_NEIGHBOR_LIKE c on a.POST_SID =c.POST_SID
left join HOME_NEIGHBOR_COMMENT d on a.POST_SID =d.POST_SID
where a.CREATED_ON >='20160101'
and a.CREATED_ON <'20170101'
 and e.OWNER_NO  like ('%二期%')--东方福邸一期、二期分别统计
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and d.CREATEDBY is not null--d.CREATEDBY,c.CREATEDBY
)t1
group by t1.APARTMENT_NAME,t1.Y_month
order by t1.APARTMENT_NAME,t1.Y_month,count(distinct(t1.ID)) desc



--送水提报ID数
select t1.小区名称,count(t1.OWNER_SID) 提报ID数 from
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
left join HOME_OWNER d on d.OWNER_SID =a.CREATEDBY
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where APARTMENT_NAME NOT IN('幸福家园')
and a.CREATED_ON >='20170226'
and b.CATEGORY_NAME  in('送水')
and a.CREATED_ON <'20170326'
)t1
group by t1.小区名称
order by count(t1.OWNER_SID) desc





---------------------时间格式转换
--Sql Server 中一个非常强大的日期格式化函数
Select CONVERT(varchar(100), GETDATE(), 0): 05 16 2006 10:57AM
Select CONVERT(varchar(100), GETDATE(), 1): 05/16/06
Select CONVERT(varchar(100), GETDATE(), 2): 06.05.16
Select CONVERT(varchar(100), GETDATE(), 3): 16/05/06
Select CONVERT(varchar(100), GETDATE(), 4): 16.05.06
Select CONVERT(varchar(100), GETDATE(), 5): 16-05-06
Select CONVERT(varchar(100), GETDATE(), 6): 16 05 06
Select CONVERT(varchar(100), GETDATE(), 7): 05 16, 06
Select CONVERT(varchar(100), GETDATE(), 8): 10:57:46
Select CONVERT(varchar(100), GETDATE(), 9): 05 16 2006 10:57:46:827AM
Select CONVERT(varchar(100), GETDATE(), 10): 05-16-06
Select CONVERT(varchar(100), GETDATE(), 11): 06/05/16
Select CONVERT(varchar(100), GETDATE(), 12): 060516
Select CONVERT(varchar(100), GETDATE(), 13): 16 05 2006 10:57:46:937
Select CONVERT(varchar(100), GETDATE(), 14): 10:57:46:967
Select CONVERT(varchar(100), GETDATE(), 20): 2006-05-16 10:57:47
Select CONVERT(varchar(100), GETDATE(), 21): 2006-05-16 10:57:47.157
Select CONVERT(varchar(100), GETDATE(), 22): 05/16/06 10:57:47 AM
Select CONVERT(varchar(100), GETDATE(), 23): 2006-05-16
Select CONVERT(varchar(100), GETDATE(), 24): 10:57:47
Select CONVERT(varchar(100), GETDATE(), 25): 2006-05-16 10:57:47.250
Select CONVERT(varchar(100), GETDATE(), 100): 05 16 2006 10:57AM
Select CONVERT(varchar(100), GETDATE(), 101): 05/16/2006
Select CONVERT(varchar(100), GETDATE(), 102): 2006.05.16
Select CONVERT(varchar(100), GETDATE(), 103): 16/05/2006
Select CONVERT(varchar(100), GETDATE(), 104): 16.05.2006
Select CONVERT(varchar(100), GETDATE(), 105): 16-05-2006
Select CONVERT(varchar(100), GETDATE(), 106): 16 05 2006
Select CONVERT(varchar(100), GETDATE(), 107): 05 16, 2006
Select CONVERT(varchar(100), GETDATE(), 108): 10:57:49
Select CONVERT(varchar(100), GETDATE(), 109): 05 16 2006 10:57:49:437AM
Select CONVERT(varchar(100), GETDATE(), 110): 05-16-2006
Select CONVERT(varchar(100), GETDATE(), 111): 2006/05/16
Select CONVERT(varchar(100), GETDATE(), 112): 20060516
Select CONVERT(varchar(100), GETDATE(), 113): 16 05 2006 10:57:49:513
Select CONVERT(varchar(100), GETDATE(), 114): 10:57:49:547
Select CONVERT(varchar(100), GETDATE(), 120): 2006-05-16 10:57:49
Select CONVERT(varchar(100), GETDATE(), 121): 2006-05-16 10:57:49.700
Select CONVERT(varchar(100), GETDATE(), 126): 2006-05-16T10:57:49.827
Select CONVERT(varchar(100), GETDATE(), 130): 18 ???? ?????? 1427 10:57:49:907AM
Select CONVERT(varchar(100), GETDATE(), 131): 18/04/1427 10:57:49:920AM
select cast(year(GETDATE()) as varchar)+'年'+cast(month(GETDATE()) as varchar)+'月'：2017年2月--非日期格式

常用：
Select CONVERT(varchar(100), GETDATE(), 8): 10:57:46
Select CONVERT(varchar(100), GETDATE(), 24): 10:57:47
Select CONVERT(varchar(100), GETDATE(), 108): 10:57:49
Select CONVERT(varchar(100), GETDATE(), 12): 060516
Select CONVERT(varchar(100), GETDATE(), 23): 2006-05-16
Select convert(varchar(7),GETDATE(,120): 2017-02



----报错42000磁盘已满
查看tempdb当前大小
exec sp_helpdb tempdb
对tempdb进行收缩
use tempdb
go
dbcc shrinkfile(tempdev, 1024)
use tempdb
go
dbcc shrinkfile(templog, 512)
