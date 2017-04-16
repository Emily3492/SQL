#SQL取值

#####工作人员取值
select REPLACE(t3.dept,' ','-') dept_1,t3.OWNER_TAG,t3.created_on,t3.OWNER_SID,t3.OWNER_NO ,t3.OWNER_NAME ,--t3.USER_SID,t3.ROLE_NAME,t3.ROLE_SID,
t3.OWNER_PHONE ,t3.OWNER_STATUS ,t3.OWNER_TYPE,t3.GROUP_NAME
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
where  a.OWNER_TYPE in('2','3','4','5')--工作人员--工作人员
--and a.created_on >='20170226'
--and a.CREATED_ON  <='20170326'
and a.OWNER_STATUS=1
)t1)t3



#一行
select REPLACE(t3.dept,' ','-') dept_1,t3.OWNER_TAG,t3.created_on,t3.OWNER_SID,t3.OWNER_NO,t3.OWNER_NAME,t3.OWNER_PHONE ,t3.OWNER_STATUS ,t3.OWNER_TYPE,t3.GROUP_NAME from(select t1.*,ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' '+isnull(t1.d,'') +' '+isnull(t1.f,'')) dept from(select a.OWNER_TAG,a.created_on,a.OWNER_SID,a.OWNER_NO ,a.OWNER_NAME ,a.OWNER_PHONE ,a.OWNER_STATUS ,a.GROUP_SID,a.DEPT_SID,a.OWNER_TYPE,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX ,d.GROUP_NAME ,n.DEPT_NAME g,g.DEPT_NAME a,f.DEPT_NAME b,e.DEPT_NAME c,c.DEPT_NAME d,b.DEPT_NAME f from HOME_OWNER a left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID where a.OWNER_TYPE in('2','3','4','5')and a.OWNER_STATUS=1)t1)t3


#日志取值
select ROW_NUMBER() over (partition by h.OWNER_SID order by h.OWNER_SID desc) as rn,
h.SYSTEM_TYPE,h.CREATED_ON log_created,
a.OWNER_PHONE,a.OWNER_STATUS,a.OWNER_TYPE,
h.CONTENT,h.OWNER_SID,a.OWNER_NO,a.OWNER_NAME
from Home_OwnerLog h
left join HOME_OWNER a on h.OWNER_SID=a.OWNER_SID
where a.OWNER_TYPE in('2','3','4','5')--工作人员
and h.CREATED_ON>='20170320'
--and h.CREATED_ON<'20170226'
--and a.OWNER_STATUS=1
--and h.SYSTEM_TYPE = 1--0悦嘉家\悦园区,1悦服务



#一行
select ROW_NUMBER() over (partition by h.OWNER_SID order by h.OWNER_SID desc) as rn,h.SYSTEM_TYPE,h.CREATED_ON log_created,a.OWNER_PHONE,a.OWNER_STATUS,a.GROUP_SID,a.DEPT_SID,a.OWNER_TYPE,h.CONTENT,h.OWNER_SID,a.OWNER_NO,a.OWNER_NAME from Home_OwnerLog h left join HOME_OWNER a on h.OWNER_SID=a.OWNER_SID where a.OWNER_TYPE in('2','3','4','5')and a.OWNER_STATUS=1 and h.SYSTEM_TYPE=1



####################################################导入数据
hc = sqlContext
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils
from pyspark.sql import Row
import pandas as pd
import time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import os  # 设置存储路径
print(os.getcwd())
os.chdir("/home/wyy/code/")  # 包安装路径 D:\Program Files\Anaconda2\Scripts,要定位到有权限的文件夹下
print(os.getcwd())
# 直接连接数据库
import pyodbc
conn=pyodbc.connect('DRIVER={FreeTDS};SERVER=111.111.111.111;port=1111;DATABASE=JOY_HOME;UID=11;PWD=1111;TDS_Version=8.0')
cur = conn.cursor()
#取出悦服务用户数
aa = cur.execute("select REPLACE(t3.dept,' ','-') dept_1,t3.OWNER_TAG,t3.created_on,t3.OWNER_SID,t3.OWNER_NO,t3.OWNER_NAME,t3.OWNER_PHONE ,t3.OWNER_STATUS ,t3.OWNER_TYPE,t3.GROUP_NAME from(select t1.*,ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' '+isnull(t1.d,'') +' '+isnull(t1.f,'')) dept from(select a.OWNER_TAG,a.created_on,a.OWNER_SID,a.OWNER_NO ,a.OWNER_NAME ,a.OWNER_PHONE ,a.OWNER_STATUS ,a.GROUP_SID,a.DEPT_SID,a.OWNER_TYPE,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX ,d.GROUP_NAME ,n.DEPT_NAME g,g.DEPT_NAME a,f.DEPT_NAME b,e.DEPT_NAME c,c.DEPT_NAME d,b.DEPT_NAME f from HOME_OWNER a left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID where a.OWNER_TYPE in('2','3','4','5')and a.OWNER_STATUS=1)t1)t3")
info = cur.fetchall()
cur.close()

#取出悦服务日志数
cur1=conn.cursor()
aa = cur1.execute("select ROW_NUMBER() over (partition by h.OWNER_SID order by h.OWNER_SID desc) as rn,h.SYSTEM_TYPE,h.CREATED_ON,a.OWNER_PHONE,a.OWNER_STATUS,a.OWNER_TYPE,h.CONTENT,h.OWNER_SID,a.OWNER_NO,a.OWNER_NAME from Home_OwnerLog h left join HOME_OWNER a on h.OWNER_SID=a.OWNER_SID where a.OWNER_TYPE in('2','3','4','5')and a.OWNER_STATUS=1 and h.SYSTEM_TYPE=1and h.SYSTEM_TYPE=1")
info1= cur1.fetchall()
cur1.close()
conn.close()
#info1[0]#查看第一行

#用户表转换成dataframe
import pandas as pd
info2 = [list(x) for x in info]
data = pd.DataFrame(info2,columns=['dept_1','OWNER_TAG','created_on','OWNER_SID', 'OWNER_NO', 'OWNER_NAME','OWNER_PHONE','OWNER_STATUS','OWNER_TYPE','GROUP_NAME'])
data.head()#用户表
hc = sqlContext
b = hc.createDataFrame(data)  #转成pyspark格式的dataframe,Row
b.taka(2) #随机查看两条数据

#日志表转换成dataframe
import pandas as pd
info3 = [list(x) for x in info1]
data1 = pd.DataFrame(info3,columns=['rn','SYSTEM_TYPE','CREATED_ON','OWNER_PHONE', 'OWNER_STATUS', 'OWNER_TYPE','CONTENT','OWNER_SID','OWNER_NO','OWNER_NAME'])
data1.head()#日志表
hc = sqlContext
b1 = hc.createDataFrame(data1)  #转成pyspark格式的dataframe,Row
b1.taka(2) #随机查看两条数据
