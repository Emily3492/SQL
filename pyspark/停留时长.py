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
conn=pyodbc.connect('DRIVER={FreeTDS};SERVER=...;port=1433;DATABASE=...;UID=...;PWD=...;TDS_Version=8.0')
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


################################################################################计算时长
#hc = sqlContext

#在cmd下将板块分类表格传输到linux的code文件夹下
#D:\datasforspark>PSCP.EXE livegroupbyfuwu.xlsx wyy@192.168.21.129:/home/wyy/code
#pwd查看文件所目录
#/home/wyy/code/livegroupbyfuwu.xlsx
dff = pd.read_excel('/home/wyy/code/livegroupbyfuwu.xlsx')
module_dict = dict(zip(dff[u'板块名'],dff[u'关键字']))
dff1 = data1
dff1 = dff1[(dff1.CREATED_ON<'2017-04-06') & (dff1.CREATED_ON>'2017-03-20')]
dff1['CREATED_UNIX'] = dff1['CREATED_ON'].apply(lambda x:time.mktime(time.strptime(str(x)[:19],'%Y-%m-%d %H:%M:%S')))
for i in dff1.columns:
    dff1[i] = dff1[i].astype(unicode)
df = hc.createDataFrame(dff1)
def map_fun(x):
    value = {}
    value['CREATED_UNIX'] = x.CREATED_UNIX
    value['module'] = []
    for i in set(module_dict.keys()):
        name = module_dict[i]
        if re.search(name,x.CONTENT):
            value['module'].append(i)
    return (x.OWNER_SID,[value])
df1 = df.rdd.map(map_fun).reduceByKey(lambda x,y:x+y).map(lambda x:Row(OWNER_SID=x[0],value=x[1]))

def count_APP(x):
    '''
    函数功能：计算APP停留时长、停留次数、次均时长
    '''
    stats = x.value
    APP_count = 0
    APP_time = 0
    APP_ave = 0
    if len(stats)>1:
        stats.sort(key=lambda p:float(p['CREATED_UNIX']))
        a = []
        b = {}
        for l in stats:
            if 'logout' in l['module']:
                b['logout'] = l['CREATED_UNIX']
                a.append(b)
                b = {}
            else:
                for m in l['module']:
                    b[m] = l['CREATED_UNIX']
        a = [i for i in a if i.has_key('logout') and i.has_key('login')]
        if a:
            APP_count = len(a)
            APP_time = sum([float(j['logout'])-float(j['login']) for j in a])
            APP_ave  = round(APP_time*1.0/APP_count)
    return Row(OWNER_SID=x.OWNER_SID,APP_count=APP_count,APP_time=APP_time,APP_ave=APP_ave,value=x.value)

def count_module(x):
    '''
    函数功能：计算每个模块的停留时长、次数、次均时长
    '''
    module_time = dict(zip(module_dict.keys(),[0]*len(module_dict)))
    module_count = dict(zip(module_dict.keys(),[0]*len(module_dict)))
    module_ave = dict(zip(module_dict.keys(),[0]*len(module_dict)))
    stats = x.value
    if len(stats)>1:
        stats.sort(key=lambda p:float(p['CREATED_UNIX']))
        for l in range(1,len(stats)):
            for m in stats[l-1]['module']:
                t = float(stats[l]['CREATED_UNIX']) - float(stats[l-1]['CREATED_UNIX'])
                module_time[m] += t
                module_count[m] += 1
    for m in set(module_dict.keys()):
        if module_count[m] >0:
            module_ave[m] = round(module_time[m]*1.0/module_count[m],4)
    module_time['all'] = x.APP_time
    module_ave['all'] = x.APP_ave
    module_count['all'] = x.APP_count
    return Row(OWNER_SID=x.OWNER_SID,module_time=module_time,module_count=module_count,module_ave=module_ave)

df2 = df1.map(count_APP).map(count_module).toDF()


#导出结果
dff2 = pd.read_excel('...owner_0403.xlsx')
dff2 = dff2[['dept_1','OWNER_PHONE','OWNER_NAME','OWNER_SID']]
dff2 = dff2.drop_duplicates(['OWNER_SID'])
for i in dff2.columns:
    dff2[i] = dff2[i].astype(unicode)
df3 = hc.createDataFrame(dff2)
df4 = df2.join(df3,df2.OWNER_SID==df3.OWNER_SID,'leftouter')

def f1(x):
    val = x.module_time
    val['OWNER_SID'] = x.OWNER_SID
    val['dept_1'] = x.dept_1
    val['OWNER_PHONE'] = x.OWNER_PHONE
    val['OWNER_NAME'] = x.OWNER_NAME
    return Row(**val)
def f2(x):
    val = x.module_count
    val['OWNER_SID'] = x.OWNER_SID
    val['dept_1'] = x.dept_1
    val['OWNER_PHONE'] = x.OWNER_PHONE
    val['OWNER_NAME'] = x.OWNER_NAME
    return Row(**val)
def f3(x):
    val = x.module_ave
    val['OWNER_SID'] = x.OWNER_SID
    val['dept_1'] = x.dept_1
    val['OWNER_PHONE'] = x.OWNER_PHONE
    val['OWNER_NAME'] = x.OWNER_NAME
    return Row(**val)
def f4(x):
    val = x.module_time
    val = val.update(x.module_count)
    val = val.update(x.module_ave)s
    val['OWNER_SID'] = x.OWNER_SID
    val['dept_1'] = x.dept_1
    val['OWNER_PHONE'] = x.OWNER_PHONE
    val['OWNER_NAME'] = x.OWNER_NAME
    return Row(**val)
df2.rdd.map(f1).toDF().toPandas().to_excel('.../module_time.xlsx')
df2.rdd.map(f2).toDF().toPandas().to_excel('.../module_count.xlsx')
df2.rdd.map(f3).toDF().toPandas().to_excel('.../module_ave.xlsx')
df2.rdd.map(f4).toDF().toPandas().to_excel('.../module_all.xlsx')
