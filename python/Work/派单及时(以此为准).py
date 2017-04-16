###############################SQL取值所有提单
select e.OWNER_NO,a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,a.SERVICE_STATUS
,a.CREATED_ON ,d.CREATED_ON RESPONSE_TIME,a.PROCESS_TIME ,a.SERVICE_CATEGORY,b.CATEGORY_NAME ,c.APARTMENT_NAME
from HOME_SERVICE_MAIN  a
left join HOME_SERVICE_HIST d on d.SERVICE_SID=a.SERVICE_SID
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where APARTMENT_NAME NOT IN('幸福家园')
and d.HIST_TYPE = 2
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'

派单及时规则：
--工作时间段（8:30-18:00）提报的单子，15min内响应为及时响应；
--其他时间段提报的单子，9点之前响应为及时响应。

一行：
select e.OWNER_NO,a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,a.SERVICE_STATUS,a.CREATED_ON ,d.CREATED_ON RESPONSE_TIME,a.PROCESS_TIME ,a.SERVICE_CATEGORY,b.CATEGORY_NAME ,c.APARTMENT_NAME from HOME_SERVICE_MAIN a left join HOME_SERVICE_HIST d on d.SERVICE_SID=a.SERVICE_SID left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID where APARTMENT_NAME NOT IN('幸福家园') and d.HIST_TYPE = 2 AND a.SERVICE_STATUS NOT IN ('3') AND a.SERVICE_DESC NOT LIKE'%测%'

#########################################python 取值

import os  # 设置存储路径
print(os.getcwd())
os.chdir("D:\\work_all\\python")  # 包安装路径 D:\Program Files\Anaconda2\Scripts
print(os.getcwd())
# 直接连接数据库
import pyodbc
conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 10.0};SERVER=111.111.111.111;DATABASE=JOY_HOME;UID=11;PWD=1111')
cur = conn.cursor()
aa = cur.execute("select a.room_no,e.OWNER_NO,a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,a.SERVICE_STATUS,a.CREATED_ON ,d.CREATED_ON RESPONSE_TIME,a.PROCESS_TIME ,a.SERVICE_CATEGORY,b.CATEGORY_NAME ,c.APARTMENT_NAME from HOME_SERVICE_MAIN a left join HOME_SERVICE_HIST d on d.SERVICE_SID=a.SERVICE_SID left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID where APARTMENT_NAME NOT IN('幸福家园') and d.HIST_TYPE = 2 AND a.SERVICE_STATUS NOT IN ('3') AND a.SERVICE_DESC NOT LIKE'%测%'")
info = cur.fetchall()
cur.close()


#响应数据
import pandas as pd
info1 = [list(x) for x in info]
df = pd.DataFrame(info1,columns=['room_no','OWNER_NO','SERVICE_SID', 'APARTMENT_SID', 'TYPE_SID', 'TYPE_NAME', 'SERVICE_NO', 'SERVICE_DESC','SERVICE_STATUS','CREATED_ON','RESPONSE_TIME','PROCESS_TIME','SERVICE_CATEGORY','CATEGORY_NAME','APARTMENT_NAME'])

#先是把东方福邸的全部提出来，单独形成个dataframe，然后标记为东方福邸1和2，然后把这个dataframe拼接到原来的dataframe上去
#东方福邸一期二期分开
#待调整
import re
d = df[df.APARTMENT_NAME==u'东方福邸']
def f(x):
    one = u'一期'
    two = u'二期'
    if re.search(one,unicode(x)):
        return u'东方福邸一期'
    elif re.search(two,unicode(x)):
        return u'东方福邸二期'
d['APARTMENT_NAME'] = d['OWNER_NO'].apply(f)
df = pd.concat([df,d])
df.tail()

cur1 = conn.cursor()
a1 = cur1.execute("select a.room_no,e.OWNER_NO,a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,a.SERVICE_STATUS,a.CREATED_ON ,a.PROCESS_TIME ,a.SERVICE_CATEGORY,b.CATEGORY_NAME ,c.APARTMENT_NAME from HOME_SERVICE_MAIN a left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID where APARTMENT_NAME NOT IN('幸福家园') AND a.SERVICE_STATUS NOT IN ('3') AND a.SERVICE_DESC NOT LIKE'%测%'")
info2 = cur1.fetchall()
cur1.close()
conn.close()

#总单数
import pandas as pd
info3 = [list(x) for x in info2]
df1= pd.DataFrame(info3,columns=['room_no','OWNER_NO','SERVICE_SID', 'APARTMENT_SID', 'TYPE_SID', 'TYPE_NAME', 'SERVICE_NO', 'SERVICE_DESC','SERVICE_STATUS','CREATED_ON','PROCESS_TIME','SERVICE_CATEGORY','CATEGORY_NAME','APARTMENT_NAME'])

import re
d1 = df1[df1.APARTMENT_NAME==u'东方福邸']
def f(x):
    one = u'一期'
    two = u'二期'
    if re.search(one,unicode(x)):
        return u'东方福邸一期'
    elif re.search(two,unicode(x)):
        return u'东方福邸二期'
d1['APARTMENT_NAME'] = d1['OWNER_NO'].apply(f)
df1 = pd.concat([df1,d1])

df1.tail()

####################python
import numpy as np
import time
df = df[df.SERVICE_STATUS!=3]
def f1(x):
    test = u'测'
    if re.search(test,unicode(x)):
        return False
    else:
        return True
df['X']= df['SERVICE_DESC'].apply(f1)
df = df[df.X==True]#SERVICE_DESC不含'测'
df['CREATED_ON_UNIX'] = df['CREATED_ON'].apply(lambda x:time.mktime(time.strptime(str(x)[:19],'%Y-%m-%d %H:%M:%S')))


df1 = df1[df1.SERVICE_STATUS!=3]
df1['X']= df1['SERVICE_DESC'].apply(f1)
df1 = df1[df1.X==True]#SERVICE_DESC不含'测'
df1['CREATED_ON_UNIX'] = df1['CREATED_ON'].apply(lambda x:time.mktime(time.strptime(str(x)[:19],'%Y-%m-%d %H:%M:%S')))


columns_name = []
for i in ['history','year','month','week','day']:
    for j in ['all','intime','ratio','noreact']:
        name = i+'_'+j
        columns_name.append(name)
index_name=df.drop_duplicates(['APARTMENT_NAME'])['APARTMENT_NAME']
dff = pd.DataFrame(0,index=index_name,columns=columns_name)
#######################################################################################不同数据选取
#响应及时(所有):
data = df
data11=df1

#响应及时（不包含家政、巡检）:
data = df[(df.CATEGORY_NAME!=u'家政服务') & (df.CATEGORY_NAME!=u'巡检')]
data11 = df1[(df1.CATEGORY_NAME!=u'家政服务') & (df1.CATEGORY_NAME!=u'巡检')]

#响应及时（只包含家政）:
data = df[df.CATEGORY_NAME==u'家政服务']
data11 = df1[df1.CATEGORY_NAME==u'家政服务']

#响应及时（巡检）:
dota = df[df.CATEGORY_NAME==u'巡检']
dota11 = df1[df1.CATEGORY_NAME==u'巡检']
data=dato[dato["room_no"].notnull()]#筛选出room_no不为空的,巡检-业主提报及时用这个
data11 = dato11[dato11["room_no"].notnull()]

#响应及时（不包含家政，包含巡检的业主提报）:
dato = df[(df.CATEGORY_NAME!=u'家政服务') & (df.CATEGORY_NAME!=u'巡检')]
dato11 = df1[(df1.CATEGORY_NAME!=u'家政服务') & (df1.CATEGORY_NAME!=u'巡检')]

dota = df[df.CATEGORY_NAME==u'巡检']
dota11 = df1[df1.CATEGORY_NAME==u'巡检']
doto=dota[dota["room_no"].notnull()]#筛选出room_no不为空的,巡检-业主提报及时用这个
doto11 = dota11[dota11["room_no"].notnull()]

data = pd.concat([dato,doto])
data11 = pd.concat([dato11,doto11])

#历史、当年、当月、前一周、前一天
t = '2017-03-01'
t_now = time.mktime(time.strptime(t,'%Y-%m-%d'))#设置当前时间
t_dict = {}
t_dict['history'] = 0
t_dict['year'] = time.mktime(time.strptime(t[:4],'%Y'))#当年
#t_dict['month']= time.mktime(time.strptime(t[:7],'%Y-%m'))#当月
t_dict['month'] = t_now-2419200#前一个月
t_dict['week'] = t_now-604800
t_dict['day'] = t_now-86400

def react_count(x):
    a = 0
    t = time.mktime(time.strptime(str(x['RESPONSE_TIME'])[:19],'%Y-%m-%d %H:%M:%S'))
    if '08:30:00'<=str(x['CREATED_ON'])[11:19]<'18:00:00':
        if t-x['CREATED_ON_UNIX']<900:
            a = 1
    elif str(x['CREATED_ON'])[11:19]<'08:30:00':
        if t<time.mktime(time.strptime(str(x['CREATED_ON'])[:11]+'09:00:00','%Y-%m-%d %H:%M:%S')):
            a = 1
    else:
        if t<time.mktime(time.strptime(str(x['CREATED_ON'])[:10],'%Y-%m-%d'))+118800:
            a = 1
    return a


for i in ['history','year','month','week','day']:
    data1 = data[(data.CREATED_ON_UNIX<t_now) & (data.CREATED_ON_UNIX>=t_dict[i])]
    data12 = data11[(data11.CREATED_ON_UNIX<t_now) & (data11.CREATED_ON_UNIX>=t_dict[i])]
    dff[i+'_all'] = data12.drop_duplicates(['SERVICE_SID','APARTMENT_NAME'])[['SERVICE_SID','APARTMENT_NAME']].groupby(data12.drop_duplicates(['SERVICE_SID','APARTMENT_NAME'])['APARTMENT_NAME']).size()
    data2 = data1[data1['RESPONSE_TIME'].isnull()]
    dff[i+'_noreact'] = data2.drop_duplicates(['SERVICE_SID','APARTMENT_NAME'])[['SERVICE_SID','APARTMENT_NAME']].groupby(data2.drop_duplicates(['SERVICE_SID','APARTMENT_NAME'])['APARTMENT_NAME']).size()
    data3 = data1[data1['RESPONSE_TIME'].notnull()]
    if len(data3)==0:
        data3['react_status'] = 0
    else:
        data3['react_status'] = data3.apply(lambda x:react_count(x),1)
    data4 = data3[data3.react_status==1]
    dff[i+'_intime'] = data4.drop_duplicates(['SERVICE_SID','APARTMENT_NAME'])[['SERVICE_SID','APARTMENT_NAME']].groupby(data4.drop_duplicates(['SERVICE_SID','APARTMENT_NAME'])['APARTMENT_NAME']).size()
    dff[i+'_ratio'] = dff[i+'_intime']*1.0/dff[i+'_all']

dff.tail()
    dff.to_csv('201701-02-xunjianintime1.csv', encoding='gbk')


######################################################################时长可用
#响应总时长
#总响应单数
#平均响应时长
#ave_RESPONSE_1工作时间段(8:30-18:00)；
#ave_RESPONSE_1非工作时间段。

#历史、当年、当月、前一周、前一天
t = '2017-04-01'
t_now = time.mktime(time.strptime(t,'%Y-%m-%d'))#设置当前时间
t_dict = {}
t_dict['history'] = 0
t_dict['year'] = time.mktime(time.strptime(t[:4],'%Y'))
#t_dict['month']= time.mktime(time.strptime(t[:7],'%Y-%m'))#当月
t_dict['month'] = t_now-2419200#前一个月
t_dict['week'] = t_now-604800
t_dict['day'] = t_now-86400

data = data.dropna(subset=['RESPONSE_TIME'])
len(data)#响应提单数

index_name = data['APARTMENT_NAME'].drop_duplicates()
columns_name = ['length_time_RESPONSE_all','cnt_RESPONSE_all','ave_RESPONSE_all','length_time_RESPONSE_1','cnt_RESPONSE_1','ave_RESPONSE_1','length_time_RESPONSE_2','cnt_RESPONSE_2','ave_RESPONSE_2']
df = pd.DataFrame(0,index=index_name,columns=columns_name)

def fun_marktime(x):
    if '08:30:00'<=x['CREATED_ON'][11:19]<'18:00:00':
        return 1
    else:
        return 2
def fun_meantime(x):
    RESPONSE_TIME_unix = time.mktime(time.strptime(x['RESPONSE_TIME'][:19],'%Y-%m-%d %H:%M:%S'))
    CREATED_ON_unix = time.mktime(time.strptime(x['CREATED_ON'][:19],'%Y-%m-%d %H:%M:%S'))
    time_length = RESPONSE_TIME_unix - CREATED_ON_unix
    return time_length
for i in ['RESPONSE_TIME','CREATED_ON']:
    data[i] = data[i].astype(str)
data['marktime'] = data.apply(lambda x:fun_marktime(x),1)#1是指对行操作，可以对多行操作
data['time_length'] = data.apply(lambda x:fun_meantime(x),1)

df['length_time_RESPONSE_all'] = data.groupby(data['APARTMENT_NAME']).sum()['time_length']/3600#所有回帖时长
df['cnt_RESPONSE_all'] = data.groupby(data['APARTMENT_NAME']).size()#所有回帖数
df['ave_RESPONSE_all'] = df['length_time_RESPONSE_all']*1.0/df['cnt_RESPONSE_all']
df['length_time_RESPONSE_1'] = data[data.marktime==1].groupby(data[data.marktime==1]['APARTMENT_NAME']).sum()['time_length']/3600
df['cnt_RESPONSE_1'] = data[data.marktime==1].groupby(data[data.marktime==1]['APARTMENT_NAME']).size()
df['ave_RESPONSE_1'] = df['length_time_RESPONSE_1']*1.0/df['cnt_RESPONSE_1']
df['length_time_RESPONSE_2'] = data[data.marktime==2].groupby(data[data.marktime==2]['APARTMENT_NAME']).sum()['time_length']/3600
df['cnt_RESPONSE_2'] = data[data.marktime==2].groupby(data[data.marktime==2]['APARTMENT_NAME']).size()
df['ave_RESPONSE_2'] = df['length_time_RESPONSE_2']*1.0/df['cnt_RESPONSE_2']

dff.tail()
    dff.to_csv('201701-03-xunjianintime1.csv', encoding='gbk')
