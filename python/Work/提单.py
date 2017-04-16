#SQL取值
--select t2.小区名称,t2.服务类型,count(distinct(t2.SERVICE_SID)) 提报单数 from(
--select t1.*,ROW_NUMBER() over (partition by t1.服务类型,t1.TYPE_NAME  order by t1.rn desc ) as rnn
--from(
select a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,
ROW_NUMBER() over (partition by c.APARTMENT_NAME,b.CATEGORY_NAME,a.TYPE_NAME order by c.APARTMENT_NAME,b.CATEGORY_NAME,a.TYPE_NAME ) as rn,
 a.SERVICE_STATUS,a.CREATED_ON 呼叫时间,b.CATEGORY_NAME 服务类型,
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
and a.CREATED_ON >='20160101'
and a.CREATED_ON <'20160201'
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
--)t1
--)t2
--group by t2.小区名称,t2.服务类型
--order by t2.服务类型, count(distinct(t2.SERVICE_SID)) desc

#导入数据
