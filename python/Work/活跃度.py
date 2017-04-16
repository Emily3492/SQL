

--用户表关联10月日志
select a.CREATED_ON id_created,b.CREATED_ON log_created,a.OWNER_SID,a.APARTMENT_SID,a.OWNER_NAME, a.OWNER_TYPE ,b.CONTENT
from HOME_OWNER a
left join Home_OwnerLog b on a.OWNER_SID = b.OWNER_SID
where a.OWNER_TYPE=1
 and a.FAMILY_NAME not in ('悦悦')
 and a.OWNER_NO not like '%物业%'
--and b.CREATED_ON >='2016-09-26'#python中取值不需要这个限制
--and b.CREATED_ON <'2016-10-26'
and b.SYSTEM_TYPE=0
order by a.OWNER_SID
一行：select a.CREATED_ON id_created,b.CREATED_ON log_created,a.OWNER_SID,a.APARTMENT_SID,a.OWNER_NAME, a.OWNER_TYPE ,b.CONTENT from HOME_OWNER a left join Home_OwnerLog b on a.OWNER_SID = b.OWNER_SID where a.OWNER_TYPE=1 and a.FAMILY_NAME not in ('悦悦') and a.OWNER_NO not like '%物业%'  and b.SYSTEM_TYPE=0

--取用户表数据
select b.CREATED_ON,b.OWNER_SID,b.OWNER_TYPE,APARTMENT_SID from HOME_OWNER b
where b.OWNER_TYPE  = 1
--and b.CREATED_ON>='20161101'
一行：select b.CREATED_ON,b.OWNER_SID,b.OWNER_TYPE,APARTMENT_SID from HOME_OWNER b where b.OWNER_TYPE  = 1


--取出每日日期（日活跃度索引），python中也有代码直接处理，仅做参考
select distinct(CAST(created_on AS DATE)) data from Home_OwnerLog
where created_on >='20160926'
and created_on<'20161026'
order by data asc

################################这里提取的日志数据为10月份的，累计用户数要另外提取,sql限制条件取值不适用累计ID数。
#用户表关联10月份日志，导入数据
import os
os.chdir("D:\\work_all\\python")
print(os.getcwd())
import pandas as pd
import time
import pyodbc
conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 10.0};SERVER=111.111.111.111;DATABASE=JOY_HOME;UID=11;PWD=1111')
cur = conn.cursor()
aa = cur.execute("select a.CREATED_ON id_created,b.CREATED_ON log_created,a.OWNER_SID,a.APARTMENT_SID,a.OWNER_NAME, a.OWNER_TYPE ,b.CONTENT from HOME_OWNER a left join Home_OwnerLog b on a.OWNER_SID = b.OWNER_SID where a.OWNER_TYPE=1 and a.FAMILY_NAME not in ('悦悦') and a.OWNER_NO not like '%物业%'  and b.SYSTEM_TYPE=0")
info = cur.fetchall()
cur.close()

info2 = [list(x) for x in info]
log = pd.DataFrame(info2,columns=['id_created', 'log_created', 'OWNER_SID', 'APARTMENT_SID', 'OWNER_NAME', 'OWNER_TYPE','content'])
log['content1'] = log['content'].apply(lambda x: x.decode('gbk'))#将content转换成gbk格式显示并加在log的dataframe中去
log.head()
log1 = log.drop(['content'], axis=1)#删除乱码显示的内容后存在dataframe的log1中
log1.head()

#以上为日志数据，剔除工作人员，接下来把取值SQL替换成用户的SQL数据取值取出用户数据owner
#select b.CREATED_ON,b.OWNER_SID,b.OWNER_TYPE from HOME_OWNER b where b.OWNER_TYPE  = 1

cur = conn.cursor()
aa = cur.execute("select b.CREATED_ON,b.OWNER_SID,b.OWNER_TYPE,b.OWNER_NAME,b.OWNER_NO,b.APARTMENT_SID from HOME_OWNER b where b.OWNER_TYPE  = 1")
info1 = cur.fetchall()
cur.close()
conn.close()
info3 = [list(x) for x in info1]
owner = pd.DataFrame(info3,columns=['CREATED_ON','OWNER_SID','OWNER_TYPE','OWNER_NAME','OWNER_NO','APARTMENT_SID'])
owner.head()

#将两个数据结果直接转换成dataframe（用这个转换成dataframe）
info1 = [list(x) for x in info]
df = pd.DataFrame(info1,columns=['id_created', 'log_created', 'OWNER_SID', 'APARTMENT_SID', 'OWNER_NAME', 'OWNER_TYPE','content'])
df.head()
data=df
#df1 = df.apply(lambda x:x['content'].encode('utf-8'))

#loc方法将数据转化成dataframe格式（参考）
columns = ['id_created', 'log_created', 'OWNER_SID', 'APARTMENT_SID', 'OWNER_NAME', 'OWNER_TYPE' ]
a = pd.DataFrame(0, columns=range(6), index=range(len(info)))
# 创建时若只指定了表格的内容（通过一个嵌套的list），没有指定列名和索引。这时列名就自动为 0,1,2 ；索引自动为数值0,1.。
# index为索引，columns为列名
import numpy as np
a.columns = ['id_created', 'log_created', 'OWNER_SID', 'APARTMENT_SID', 'OWNER_NAME', 'OWNER_TYPE']
for i in range(len(info)):
    for j in range(6):
        x = i
        y = columns[j]
        a.loc[x, y] = info[i][j]

a.columns = ['id_created', 'log_created', 'OWNER_SID', 'APARTMENT_SID', 'OWNER_NAME', 'OWNER_TYPE']
for i in range(len(info)):
    for j in range(6):
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
#a.to_csv('flow.csv',encoding='gbk')
data=a
########################################################################计算活跃度,10月时间区间为0926-1025
#log1 ,owner

log1['login_timestamp'] = log1['log_created'].apply(lambda x:(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))
owner['apply_timestamp']=owner['CREATED_ON'].apply(lambda x:(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))
log1_login =log1[['OWNER_SID','login_timestamp']]
owner_apply = owner[['OWNER_SID','apply_timestamp']]

#周活跃度
df_new = pd.DataFrame(0,index=range(4),columns=['apply_count','login_count','liveness'])
t = int(time.mktime(time.strptime('2016-09-26 00:00:00',"%Y-%m-%d %H:%M:%S")))
t1 = 60*60*24*7

for i in range(4):
    log1_login_1 = log1_login[(log1_login.login_timestamp>=t) & (log1_login.login_timestamp<t+t1)]
    owner_apply_1 = owner_apply[owner_apply.apply_timestamp<t+t1]
    df_new.iloc[i, 0] = owner_apply_1.drop_duplicates(['OWNER_SID']).count()[0]
    df_new.iloc[i, 1] =log1_login_1.drop_duplicates(['OWNER_SID']).count()[0]
    if df_new.iloc[i, 0]==0:
        df_new.iloc[i, 2]=0
    else:
        df_new.iloc[i, 2] = df_new.iloc[i, 1]*1.0/df_new.iloc[i, 0]
    t += t1

 #周访问次数login_count如下,算访问路径时候需要，活跃度不需要#################################
for i in range(4):
    log1_login_1 = log1_login[(log1_login.login_timestamp>=t) & (log1_login.login_timestamp<t+t1)]
    owner_apply_1 = owner_apply[owner_apply.apply_timestamp<t+t1]
    df_new.iloc[i, 0] =owner_apply_1.drop_duplicates(['OWNER_SID']).count()[0]
    df_new.iloc[i, 1] =log1_login_1['OWNER_SID'].count()
    if df_new.iloc[i, 0]==0:
        df_new.iloc[i, 2]=0
    else:
        df_new.iloc[i, 2] = df_new.iloc[i, 1]*1.0/df_new.iloc[i, 0]
    t += t1
    df_new.to_excel('login_count0926-1025.xlsx', encoding='utf-8')
 #############################################################################################
df_new.head() #查看数据
df_new.to_excel('week-liveness0926-1025.xlsx',encoding='utf-8')#导出结果数据

#日活跃度，if 时间区间中含31号，then index=range(1,32),else index=range(1,31)
df_new = pd.DataFrame(0,index=range(30),columns=['apply_count','login_count','liveness'])
#df_new = pd.DataFrame(0,index=range(1,31),columns=['apply_count','login_count','liveness'])
t = int(time.mktime(time.strptime('2016-09-26 00:00:00',"%Y-%m-%d %H:%M:%S")))
t1 = 60*60*24
for i in range(30):
    log1_login_1 = log1_login[(log1_login.login_timestamp>=t) & (log1_login.login_timestamp<t+t1)]
    owner_apply_1 = owner_apply[owner_apply.apply_timestamp<t+t1]
    df_new.iloc[i, 0] = owner_apply_1.drop_duplicates(['OWNER_SID']).count()[0]
    df_new.iloc[i, 1] =log1_login_1.drop_duplicates(['OWNER_SID']).count()[0]
    if df_new.iloc[i, 0]==0:
        df_new.iloc[i, 2]=0
    else:
        df_new.iloc[i, 2] = df_new.iloc[i, 1]*1.0/df_new.iloc[i, 0]
    t += t1
dates = pd.date_range('20160926', periods=30)
df_new.index=dates

df_new.head() #查看数据
df_new.to_excel('day-liveness0926-1025.xlsx',encoding='utf-8')#结果导出


#月活跃度，对各节点求活跃度
