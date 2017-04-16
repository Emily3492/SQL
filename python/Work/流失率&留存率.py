#########################################################注意：只看第三部分留存率计算，月流失率=1-月留存率

2.1#概念解析
用户流失率=流失客户数量/总客户数量  #（以自然日为基准日）
新用户流失率=新用户流失数/新用户数  #（以用户注册日为基准日）

用户流失天数=当前时间-距离当前时间最近的登陆时间  #得到用户最近一次登陆距离当前时间的天数
新用户流失天数=距离注册日最近的登录时间-注册日期  #得到用户最早一次登陆距离注册时间的天数
#注意：注册当天的登录记录不算

9月新用户：注册日期为9月
流失天数>30天即为流失用户 #0-截止该日尚未注册；1-流失；2-未流失

本次代码以9月份为例

#日志数据原始表提取
select a.OWNER_SID,b.OWNER_NAME,b.FAMILY_NAME,a.CREATED_ON LogCREATED,b.CREATED_ON ownerCREATED,b.OWNER_NO,b.APARTMENT_SID,c.APARTMENT_NAME
from Home_OwnerLog a
left join HOME_OWNER b on a.OWNER_SID=b.OWNER_SID
left join HOME_APARTMENT c on b.APARTMENT_SID =c.APARTMENT_SID
where a.SYSTEM_TYPE = 0--内部计算活跃度不剔除，计算项目绩效时剔除
and b.OWNER_TYPE =1--内部计算活跃度不剔除，计算项目绩效时剔除
and b.FAMILY_NAME not in('悦悦')
--and a.CREATED_ON >='20160901'
and b.OWNER_NO not like '%物业%'

#9月份新增用户数

select str(9) month,count(distinct(a.OWNER_SID)) sum_owner
from HOME_OWNER a
where a.CREATED_ON >='20160826'
and a.OWNER_type = 1
and a.CREATED_ON <'20160926'

##新增用户ID按天汇总，按月汇总及历史ID数（在SQL中进行取数）

SQL代码见 notepad
格式如下：
    日期       截止该日OWNER_SID用户数(历史加新增)
 10月1 日           5600
 10月2 日           5723
 ...                ...
 10月31日           5891


######################################################### python连接数据库取数
#python连接数据库
import os  # 设置存储路径

os.chdir("D:\\work\\python")  # 包安装路径 D:\Program Files\Anaconda2\Scripts
print(os.getcwd())
# 直接连接数据库
import pyodbc
conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 10.0};SERVER=111.111.111.111;DATABASE=JOY_HOME;UID=11;PWD=1111')
cur = conn.cursor()
aa = cur.execute("select a.OWNER_SID,b.OWNER_NAME,b.FAMILY_NAME,a.CREATED_ON LogCREATED,b.CREATED_ON ownerCREATED,b.OWNER_NO,b.APARTMENT_SID,c.APARTMENT_NAME from  HOME_OWNER b left join Home_OwnerLog a on a.OWNER_SID=b.OWNER_SID left join HOME_APARTMENT c on b.APARTMENT_SID =c.APARTMENT_SID where a.SYSTEM_TYPE = 0 and b.OWNER_TYPE =1 and b.FAMILY_NAME not in('悦悦') and b.OWNER_NO not like '%物业%'")
info = cur.fetchall()

cur.close()
conn.close()
#数据赋值后关闭SQL连接，最后连接的先关闭
#cur.close()
#conn.close()

#转换成dataframe格式（直接转换活跃度写法）
import pandas as pd
info1 = [list(x) for x in info]
df = pd.DataFrame(info1,columns=['OWNER_SID', 'OWNER_NAME', 'FAMILY_NAME', 'LogCREATED', 'ownerCREATED', 'OWNER_NO','APARTMENT_SID','APARTMENT_NAME'])
df.head()
data=df

# 例子：转换成dataframe格式（参考直接转换“活跃度”写法）
import pandas as pd

columns = ['OWNER_SID', 'OWNER_NAME', 'FAMILY_NAME', 'LogCREATED', 'ownerCREATED', 'OWNER_NO', 'APARTMENT_SID','APARTMENT_NAME']
a = pd.DataFrame(0, columns=range(8), index=range(len(info)))

# 创建时若只指定了表格的内容（通过一个嵌套的list），没有指定列名和索引。这时列名就自动为 0,1,2 ；索引自动为数值0,1.。，这时列名就自动为 0,1,2 ；索引自动为数值0,1.
# index为索引，columns为列名
# import numpy as np
a.columns = ['OWNER_SID', 'OWNER_NAME', 'FAMILY_NAME', 'LogCREATED', 'ownerCREATED', 'OWNER_NO', 'APARTMENT_SID', 'APARTMENT_NAME']
for i in range(len(info)):
    for j in range(8):
        x = i
        y = columns[j]
        a.loc[x, y] = info[i][j]

        a.head()  # 查看前五行数据
        a.tail()  # 查看后五行数据
len(a)  # 查看行数
a.index  # 查看索引
a.columns  # 查看列名
a.values  # 查看内容
#a.to_excel('flow.xlsx',encoding='utf-8')#导出原始数据
#a.to_csv('flow.csv',encoding='gbk'
data=a
1######################################################################用户流失:#流失率,流失=1,未流失=0
import time
#import sys#解决中文乱码问题
#reload(sys)
#sys.setdefaultencoding('utf8')
#data = pd.read_excel('.../.../flow.xlsx',encoding='utf-8')#导入excel数据，下面为转换时间戳
data['log_timestamp'] = data['LogCREATED'].apply(lambda x:(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))
data['owner_timestamp'] = data['ownerCREATED'].apply(lambda x:int(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))

#流失率,流失=1,未流失=0
df=data.drop_duplicates(['OWNER_SID'])[['OWNER_SID']]
#30天为流失，如果是7天，3天，次日2，更改数字30即可
def f(x):
    if 0<x<31:
        return 0
    else:
        return 1
flow_sum={}
owner_count={}
#now_timestamp = 1475337599
now_timestamp = int(time.mktime(time.strptime('2016-09-26 23:59:59',"%Y-%m-%d %H:%M:%S")))
# t1 = time.mktime(time.strptime('2016-08-26 00:00:00', "%Y-%m-%d %H:%M:%S"))#9月新用户
# t2 = time.mktime(time.strptime('2016-09-25 23:59:59', "%Y-%m-%d %H:%M:%S"))
# data_new = data[(data.owner_timestamp >= t1) & (data.owner_timestamp <= t2)]#新用户
for i in range(1,32):#10月有31天，#"2016-10-01 23:59:59"时间戳为1475337599，格林尼治时间，1970年

   # data1=data_new[data.log_timestamp<=now_timestamp]#新用户
    data1 = data[data.log_timestamp<=now_timestamp]#老用户
    data2 = data1[['OWNER_SID','log_timestamp']].groupby(data1['OWNER_SID']).max().reset_index(drop=True)#reset_index(drop=True)去除索引
    columns_name = '10-'+str(i)+'D'
    columns_name_1 = '10-'+str(i)+'flow'
    columns_name_2 = '10-'+str(i)+'SID'
    data2[columns_name] = data2['log_timestamp'].apply(lambda x:(now_timestamp-int(x))/86400)
    data2[columns_name_1] = data2[columns_name].apply(f)
    data3 = data2[['OWNER_SID',columns_name,columns_name_1]]
    result = pd.merge(df,data3, how='left', on='OWNER_SID')
    flow_sum[columns_name_1]=result[columns_name_1].sum()
    df = result
    data4 = data[data.owner_timestamp<=now_timestamp]
    data5 = data4.drop_duplicates(['OWNER_SID'])[['OWNER_SID']]
    owner_count[columns_name_2]=data5['OWNER_SID'].count()#日新增用户ID数，9月1日之前的历史数据从SQL取值，加在新增数上得到截止该日注册用户ID数
    now_timestamp+=86400

flow_sum.keys()
flow_sum.values()
type(flow_sum)
print df

a = pd.DataFrame(flow_sum,index=range(1))#流失人数求和
aa = pd.DataFrame(owner_count,index=range(1))#新增注册ID数求和
b = a.T #转置
bb = aa.T #转置

#df.to_csv('flowDetial.csv',encoding='utf-8')#导出流失天数数据，如有中文用encoding='utf-8'
#b.to_excel('flowsum.xlsx',encoding='utf-8')
b.to_csv('10_flowsum.csv',encoding='utf-8')#导出流失ID数汇总
#b.to_csv('10new_flowsum.csv',encoding='utf-8')#导出流失ID数汇总,新用户
bb.to_csv('10_owner_count.csv',encoding='utf-8')#导出10月累计新增用户数,换SQL语句提取
2################################################################新用户流失,#1：未流失，空值：流失
import pandas as pd
import time
#import sys
#reload(sys)
#sys.setdefaultencoding('utf8')
data['log_timestamp'] = data['LogCREATED'].apply(lambda x:(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))
data['owner_timestamp'] = data['ownerCREATED'].apply(lambda x:int(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))

t1 = time.mktime(time.strptime('2016-08-26 00:00:00',"%Y-%m-%d %H:%M:%S"))
t2 = time.mktime(time.strptime('2016-09-25 23:59:59',"%Y-%m-%d %H:%M:%S"))
data_new = data[(data.owner_timestamp>t1) & (data.owner_timestamp<t2)]
df_new = data_new.drop_duplicates(['OWNER_SID'])[['OWNER_SID']]#去重
data_new_1 = data_new[['OWNER_SID','log_timestamp','owner_timestamp']]
data_new_1['maxmum_time'] = data_new_1['owner_timestamp'].apply(lambda x:x+2592000)#7天为86400*7，3天为86400*3
data_new_1['minimum_time']= data_new_1['owner_timestamp'].apply(lambda x:x+86400)
data_new_2 = data_new_1[(data_new_1.maxmum_time>data_new_1.log_timestamp) &(data_new_1.minimum_time<data_new_1.log_timestamp)].drop_duplicates(['OWNER_SID'])[['OWNER_SID']]
data_new_2['flow']=1 #1：未流失，空值：流失
result = pd.merge(df_new,data_new_2, how='left', on='OWNER_SID')
a=result['flow'].sum()
print a #30天新用户留存数（9月）
print result #用户留存情况详情表

3#########################################################################################################只看这部分留存
######################################################################用户留存#留存率,留存=1,未留存=0
月留存:0<x<31
周留存:0<x<8
3日留存:0<x<4
次日留存:0<x<2

#import sys#解决中文乱码问题
#reload(sys)
#sys.setdefaultencoding('utf8')
#data = pd.read_excel('.../.../flow.xlsx',encoding='utf-8')#导入excel数据，下面为转换时间戳
import time
data['log_timestamp'] = data['LogCREATED'].apply(lambda x:(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))
data['owner_timestamp'] = data['ownerCREATED'].apply(lambda x:int(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))

df=data.drop_duplicates(['OWNER_SID'])[['OWNER_SID']]
#30天为留存，如果是7天，3天，次日2，更改数字31即可
def f(x):
    if 0<x<31:
        return 1
    else:
        return 0
flow_sum={}
owner_count={}
#now_timestamp = 1475337599
now_timestamp = int(time.mktime(time.strptime('2016-09-26 23:59:59',"%Y-%m-%d %H:%M:%S")))
for i in range(1,32):#10月有31天，#"2016-10-01 23:59:59"时间戳为1475337599，格林尼治时间，1970年
    data1 = data[data.log_timestamp<=now_timestamp]#老用户
    data2 = data1[['OWNER_SID','log_timestamp']].groupby(data1['OWNER_SID']).max().reset_index(drop=True)#reset_index(drop=True)去除索引
    columns_name = '10-'+str(i)+'D'
    columns_name_1 = '10-'+str(i)+'stay'
    columns_name_2 = '10-'+str(i)+'SID'
    data2[columns_name] = data2['log_timestamp'].apply(lambda x:(now_timestamp-int(x))/86400)
    data2[columns_name_1] = data2[columns_name].apply(f)
    data3 = data2[['OWNER_SID',columns_name,columns_name_1]]
    result = pd.merge(df,data3, how='left', on='OWNER_SID')
    flow_sum[columns_name_1]=result[columns_name_1].sum()
    df = result
    data4 = data[data.owner_timestamp<=now_timestamp]
    data5 = data4.drop_duplicates(['OWNER_SID'])[['OWNER_SID']]
    owner_count[columns_name_2]=data5['OWNER_SID'].count()#日新增用户ID数，9月1日之前的历史数据从SQL取值，加在新增数上得到截止该日注册用户ID数
    now_timestamp+=86400

flow_sum.keys()
flow_sum.values()
type(flow_sum)
print df

a = pd.DataFrame(flow_sum,index=range(1))#留存人数求和
aa = pd.DataFrame(owner_count,index=range(1))#新增注册ID数求和
b = a.T #转置
bb = aa.T #转置

#df.to_csv('flowDetial.csv',encoding='utf-8')#导出留存天数数据，如有中文用encoding='utf-8'
#b.to_excel('flowsum.xlsx',encoding='utf-8')
b.to_csv('10_30staysum.csv',encoding='utf-8')#导出留存ID数汇总
#b.to_csv('10new_flowsum.csv',encoding='utf-8')#导出留存ID数汇总,新用户
bb.to_csv('10_owner_count.csv',encoding='utf-8')#导出10月累计新增用户数,换SQL语句提取
