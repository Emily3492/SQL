#####################################################################
计算示例：次日留存率=次日登录的新增用户数/新增用户数
#新用户次日留存率，注册后24h-48h内有登陆记录
#新用户3日留存率，注册后24h-72h内有登陆记录
#新用户7日留存率，注册后24h-7*24h内有登陆记录
#新用户月留存率，注册后24h-30*24h内有登陆记录
#新用户月流失数=新增用户数-月流失用户数

#导入数据
#python连接数据库
import os  # 设置存储路径
print(os.getcwd())
os.chdir("D:\\work_all\\python")  # 包安装路径 D:\Program Files\Anaconda2\Scripts
print(os.getcwd())
# 直接连接数据库
import pyodbc
conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 10.0};SERVER=111.111.111.111;DATABASE=JOY_HOME;UID=11;PWD=1111')
cur = conn.cursor()
aa = cur.execute("select a.OWNER_SID,b.OWNER_NAME,b.FAMILY_NAME,a.CREATED_ON LogCREATED,b.CREATED_ON ownerCREATED,b.OWNER_NO,b.APARTMENT_SID,c.APARTMENT_NAME from HOME_OWNER b left join Home_OwnerLog a on a.OWNER_SID=b.OWNER_SID left join HOME_APARTMENT c on b.APARTMENT_SID =c.APARTMENT_SID where a.SYSTEM_TYPE = 0 and b.OWNER_TYPE like('%1%')")
info = cur.fetchall()
cur.close()
import pandas as pd
info1 = [list(x) for x in info]
data = pd.DataFrame(info1,columns=['OWNER_SID', 'OWNER_NAME', 'FAMILY_NAME', 'LogCREATED', 'ownerCREATED', 'OWNER_NO','APARTMENT_SID','APARTMENT_NAME'])
data.head()#日志表


cur1 = conn.cursor()
aa = cur1.execute("select OWNER_NO,OWNER_SID,CREATED_ON,APARTMENT_SID,OWNER_NAME, OWNER_TYPE from HOME_OWNER where OWNER_TYPE like('%1%')")
info2 = cur1.fetchall()
cur1.close()
conn.close()
info3 = [list(x) for x in info2]
d = pd.DataFrame(info3,columns=['OWNER_NO','OWNER_SID', 'CREATED_ON', 'APARTMENT_SID', 'OWNER_NAME', ' OWNER_TYPE'])
d.head()#用户表


#留存率和流失率
#data = pd.read_excel('.../.../log.xlsx',encoding='utf-8')#导入excel数据，下面为转换时间戳
#d = pd.read_excel('.../.../create.xlsx',encoding='gbk')
import time
data['log_timestamp'] = data['LogCREATED'].apply(lambda x:(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))
data['owner_timestamp'] = data['ownerCREATED'].apply(lambda x:int(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))
d['create_timestamp'] = d['CREATED_ON'].apply(lambda x:int(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))
dates = pd.date_range(start='20170101',end='20170407')
columns_name=['apply_count','day1_remain_count','day1_remain_ratio','day3_remain_count','day3_remain_ratio','week_remain_count','week_remain_ratio','month_remain_count','month_remain_ratio','month_off_count','month_off_ratio']
df = pd.DataFrame('',index=dates,columns=columns_name)
t_start = int(time.mktime(time.strptime('2017-01-01 00:00:00',"%Y-%m-%d %H:%M:%S")))
for i in dates:
    value={}
    data_new = data[(data.owner_timestamp>=t_start) & (data.owner_timestamp<t_start+86400)]
    d_new = d[(d.create_timestamp>=t_start) & (d.create_timestamp<t_start+86400)]
    value['apply_count'] = d_new.drop_duplicates(['OWNER_SID'])['OWNER_SID'].count()
    if value['apply_count']!=0:
        data1 = data_new[data_new.log_timestamp>=t_start+86400]
        data2 = data1[['OWNER_SID', 'log_timestamp','owner_timestamp']].groupby(data1['OWNER_SID']).min().reset_index(drop=True)
        data2['day_len'] = data2['log_timestamp']- data2['owner_timestamp']
        value['day1_remain_count'] = data2[(data2.day_len<172800) & (data2.day_len>86399)]['OWNER_SID'].count()
        value['day1_remain_ratio'] = round(value['day1_remain_count']*1.0/value['apply_count'],4)
        value['day3_remain_count'] = data2[(data2.day_len<259200) & (data2.day_len>86399)]['OWNER_SID'].count()
        value['day3_remain_ratio'] = round(value['day3_remain_count']*1.0/value['apply_count'],4)
        value['week_remain_count'] = data2[(data2.day_len<604800) & (data2.day_len>86399)]['OWNER_SID'].count()
        value['week_remain_ratio'] = round(value['week_remain_count']*1.0/value['apply_count'],4)
        value['month_remain_count'] = data2[(data2.day_len<2592000) & (data2.day_len>86399)]['OWNER_SID'].count()
        value['month_remain_ratio'] = round(value['month_remain_count']*1.0/value['apply_count'],4)
        value['month_off_count'] = value['apply_count']-value['month_remain_count']
        value['month_off_ratio'] = 1-value['month_remain_ratio']
        for j in columns_name:
            df.loc[i,j]=value[j]
    else:
        for j in columns_name:
            df.loc[i,j]=0
    t_start+=86400

df.head()

df.to_csv('20170407stayflow.csv',encoding='utf-8')

###########################################################留存率和流失率计算方法2
计算示例：次日留存率=次日登录的新增用户数/新增用户数
#新用户次日留存率，注册后24h-2*24h内有登陆记录
#新用户3日留存率，注册后2*24h-3*24h内有登陆记录
#新用户7日留存率，注册后6*24h-7*24h内有登陆记录
#新用户月留存率，注册后29*24h-30*24h内有登陆记录
#新用户月流失数=新增用户数-月流失用户数

#data = pd.read_excel('.../.../log.xlsx',encoding='utf-8')#导入excel数据，下面为转换时间戳
#d = pd.read_excel('.../.../create.xlsx',encoding='gbk')
import time
data['log_timestamp'] = data['LogCREATED'].apply(lambda x:(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))
data['owner_timestamp'] = data['ownerCREATED'].apply(lambda x:int(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))
d['create_timestamp'] = d['CREATED_ON'].apply(lambda x:int(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))
dates = pd.date_range(start='20161218',end='20170405')
columns_name=['apply_count','day1_remain_count','day1_remain_ratio','day3_remain_count','day3_remain_ratio','week_remain_count','week_remain_ratio','month_remain_count','month_remain_ratio','month_off_count','month_off_ratio']
df = pd.DataFrame('',index=dates,columns=columns_name)
t_start = int(time.mktime(time.strptime('2016-12-18 00:00:00',"%Y-%m-%d %H:%M:%S")))
for i in dates:
    value={}
    data_new = data[(data.owner_timestamp>=t_start) & (data.owner_timestamp<t_start+86400)]
    d_new = d[(d.create_timestamp>=t_start) & (d.create_timestamp<t_start+86400)]
    value['apply_count'] = d_new.drop_duplicates(['OWNER_SID'])['OWNER_SID'].count()
    if value['apply_count']!=0:
        data1 = data_new[data_new.log_timestamp>=t_start+86400]
        data2 = data1[['OWNER_SID', 'log_timestamp','owner_timestamp']].groupby(data1['OWNER_SID']).min().reset_index(drop=True)
        data2['day_len'] = data2['log_timestamp']- data2['owner_timestamp']
        value['day1_remain_count'] = data2[(data2.day_len<172800) & (data2.day_len>86399)]['OWNER_SID'].count()
        value['day1_remain_ratio'] = round(value['day1_remain_count']*1.0/value['apply_count'],4)
        value['day3_remain_count'] = data2[(data2.day_len<259200) & (data2.day_len>172799)]['OWNER_SID'].count()
        value['day3_remain_ratio'] = round(value['day3_remain_count']*1.0/value['apply_count'],4)
        value['week_remain_count'] = data2[(data2.day_len<604800) & (data2.day_len>518399)]['OWNER_SID'].count()
        value['week_remain_ratio'] = round(value['week_remain_count']*1.0/value['apply_count'],4)
        value['month_remain_count'] = data2[(data2.day_len<2592000) & (data2.day_len>2505599)]['OWNER_SID'].count()
        value['month_remain_ratio'] = round(value['month_remain_count']*1.0/value['apply_count'],4)
        value['month_off_count'] = value['apply_count']-value['month_remain_count']
        value['month_off_ratio'] = 1-value['month_remain_ratio']
        for j in columns_name:
            df.loc[i,j]=value[j]
    else:
        for j in columns_name:
            df.loc[i,j]=0
    t_start+=86400

df.head()

df.to_csv('2017-4stayflow.csv',encoding='utf-8')
