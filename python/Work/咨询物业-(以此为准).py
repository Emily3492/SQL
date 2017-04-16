SQL取值：(多条客服回复取最近的客服回复)
#咨询物业主帖，客服及时回复的帖子数


select a.POST_OKFLAG,b.OWNER_NO,a.POST_SID,ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,
a.CREATEDBY,b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,
a.CREATED_ON time,e.CREATED_ON react_time,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME
from HOME_NEIGHBOR_POST a
LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY
left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID
left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID
left join HOME_NEIGHBOR_COMMENT e on a.POST_SID = e.POST_SID
left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID
where d.TYPE_NAME in('咨询物业')
and a.CREATED_ON >='20161026'
--and b.OWNER_NO like ('%一期%')
--and a.CREATED_ON <'20161222'
 and a.POST_OKFLAG like('%1%')--剔除屏蔽帖
and b.FAMILY_NAME not in('悦悦')--剔除，外部人员不能起昵称为悦悦
and b.OWNER_TYPE=1--类型为业主
and a.POST_OKFLAG like('%1%')--剔除已屏蔽的帖子(未保存至草稿箱,未屏蔽,屏蔽为0)
--and c.APARTMENT_NAME not in ('幸福家园','体验小区','恒基小区','金橡臻园')


一行：
select *from(select a.POST_OKFLAG,b.OWNER_NO,a.POST_SID,ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,a.CREATEDBY,b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,a.CREATED_ON time,e.CREATED_ON react_time,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME from HOME_NEIGHBOR_POST a LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID left join HOME_NEIGHBOR_COMMENT e on a.POST_SID = e.POST_SID left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID where d.TYPE_NAME in('咨询物业') and b.FAMILY_NAME not in('悦悦') and b.OWNER_TYPE=1)t1 where t1.rn=1
#python取值
import os
os.chdir("D:\\work_all\\python")
print(os.getcwd())
import pandas as pd
import time
import pyodbc
conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 10.0};SERVER=111.111.111.111;DATABASE=JOY_HOME;UID=11;PWD=1111')
cur = conn.cursor()
aa = cur.execute("select a.POST_OKFLAG,b.OWNER_NO,a.POST_SID,ROW_NUMBER() over (partition by c.APARTMENT_NAME,a.POST_SID order by c.APARTMENT_NAME,a.POST_SID,a.CREATED_ON,e.CREATED_ON ) as rn,a.CREATEDBY,b.FAMILY_NAME,f.FAMILY_NAME react_name,a.POST_TYPE ,a.POST_CONTENT,a.CREATED_ON time,e.CREATED_ON react_time,c.APARTMENT_NAME,d.TYPE_SID,d.TYPE_NAME from HOME_NEIGHBOR_POST a LEFT join HOME_OWNER b on b.OWNER_SID = a.CREATEDBY left join HOME_APARTMENT c on a.APARTMENT_SID = c.APARTMENT_SID left join HOME_NEIGHBOR_POST_TYPE d on a.POST_TYPE = d.TYPE_SID left join HOME_NEIGHBOR_COMMENT e on a.POST_SID = e.POST_SID left join HOME_OWNER f on e.AT_OWNER = f.OWNER_SID where b.OWNER_TYPE like('%1%')")
info = cur.fetchall()

cur.close()
conn.close()

info1 = [list(x) for x in info]
post = pd.DataFrame(info1,columns=['POST_OKFLAG','OWNER_NO','POST_SID','rn','CREATEDBY','FAMILY_NAME','react_name','POST_TYPE','POST_CONTENT','time','react_time','APARTMENT_NAME','TYPE_SID','TYPE_NAME'])
post.head()
post = post[post.POST_OKFLAG!=0]#剔除已屏蔽的帖子(未保存至草稿箱,未屏蔽,屏蔽为0)
post.head()
data=post

#先是把东方福邸的全部提出来，单独形成个dataframe，然后标记为东方福邸1和2，然后把这个dataframe拼接到原来的dataframe上去
#东方福邸一期二期分开
import re
d = data[data.APARTMENT_NAME==u'东方福邸']
def f(x):
    one = u'一期'
    two = u'二期'
    if re.search(one,unicode(x)):
        return u'东方福邸一期'
    elif re.search(two,unicode(x)):
        return u'东方福邸二期'
d['APARTMENT_NAME'] = d['OWNER_NO'].apply(f)
data = pd.concat([data,d])
data.tail()
#data = data[(data.rn==1)]#多条回复只取最早的一条(如果第一条回复不是客服回复，那就把客服回复给剔除了....)

#2017-03-08
import pandas as pd
import time
#data = pd.read_excel('.../post0307.xlsx')
index_name = data['APARTMENT_NAME'].drop_duplicates()
columns_name = ['cnt_post','cnt_react','cnt_react_intime','length_time_react_all','cnt_post_react_all','avg_time_react_all','length_time_react_1','cnt_post_react_1','avg_time_react_1','length_time_react_2','cnt_post_react_2','avg_time_react_2']
df = pd.DataFrame(0,index=index_name,columns=columns_name)

#帖子总数（对POST_SID去重求总数）
t_start = '2017-04-01'
t_end = '2017-04-10'

data1 = data[(data.TYPE_NAME==u'咨询物业') & (data.FAMILY_NAME!=u'悦悦') & (data.time>=t_start) & (data.time<t_end)]
df['cnt_post'] = data1.drop_duplicates(['POST_SID','APARTMENT_NAME'])[['POST_SID','APARTMENT_NAME']].groupby(data1.drop_duplicates(['POST_SID','APARTMENT_NAME'])['APARTMENT_NAME']).size()

 #data22= data1.sort(columns='react_time',ascending=True)#排序，默认升序
# data_all = data22.drop_duplicates(['APARTMENT_NAME', 'POST_SID'], keep='first')#对帖子去重

#回复数(react_time求总数)

def fun_service(x):
    a = u'客服|管家'
    if re.search(a,unicode(x)):
        return 1
    else:
        return 0
data1['check_service'] = data1['react_name'].apply(fun_service)
data2 = data1[data1.check_service==1]

data22 = data2.sort(columns='react_time',ascending=True)#排序，默认升序
#data1 = data.sort(columns='CREATED_ON',ascending=False)#排序，降序
data2 = data22.drop_duplicates(['APARTMENT_NAME', 'POST_SID'], keep='first')#只保留客服回复的第一条数据
df['cnt_react'] = data2.groupby(data2['APARTMENT_NAME']).size()

#及时回复数(对符合规则的react_time求总数)
def fun_marktime(x):
    if '08:30:00'<=x['time'][11:19]<'18:00:00':
        return 1
    else:
        return 2
def fun_intime(x):#及时规则:12小时内及时回复
     t_time = time.mktime(time.strptime(str(x['time'])[:19],'%Y-%m-%d %H:%M:%S'))
     t_react_time = time.mktime(time.strptime(str(x['react_time'])[:19],'%Y-%m-%d %H:%M:%S'))
     if x.marktime==1:
         if str(x['time'])[:10]==str(x['react_time'])[:10]:
             return True
         else:
             return False
     else:
         if t_react_time>t_time and t_react_time-t_time<43200:
             return True
         else:
             return False

# def fun_intime(x):#及时规则:9点前及时回复
#     today_0 = time.mktime(time.strptime(x['time'][:10],'%Y-%m-%d'))
#     react_time_unix = time.mktime(time.strptime(x['react_time'][:19],'%Y-%m-%d %H:%M:%S'))
#     if x['time'][11:19]<'08:30:00':
#         if react_time_unix<today_0+32400:
#             return True
#         else:
#             return False
#     elif x['time'][11:19]>='18:00:00':
#         if react_time_unix<today_0+118800:
#             return True
#         else:
#             return False
#     else:
#         if react_time_unix<today_0+86400:
#            return True
#         else:
#            return False

for i in ['react_time','time']:
    data2[i] = data2[i].astype(str)
data2['marktime'] = data2.apply(lambda x:fun_marktime(x),1)#1是指对行操作，可以对多行操作
data2['intime'] = data2.apply(lambda x:fun_intime(x),1)#1是指对行操作，可以对多行操作
data3 = data2[data2.intime==True]
df['cnt_react_intime'] = data3.groupby(data3['APARTMENT_NAME']).size()


df.tail()
df.to_excel('20170410_reactintime.xlsx', encoding='gbk')

#平均回帖时长
for i in ['time','react_time']:
    data2[i] = data2[i].astype(str)

data2=data2[data2.react_time!='NaT']

# data1['time_unix'] = data1['time'].apply(lambda x:time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S")))
# data1['react_time_unix'] = data1['react_time'].apply(lambda x:time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S")))

def fun_meantime(x):
    react_time_unix = time.mktime(time.strptime(x['react_time'][:19],'%Y-%m-%d %H:%M:%S'))
    time_unix = time.mktime(time.strptime(x['time'][:19],'%Y-%m-%d %H:%M:%S'))
    time_length = react_time_unix - time_unix
    return time_length
data2 = data2.sort(columns = 'react_time')
data2['time_length'] = data2.apply(lambda x:fun_meantime(x),1)

data4 = data2.drop_duplicates(['APARTMENT_NAME','POST_SID'])
df['length_time_react_all'] = data4.groupby(data4['APARTMENT_NAME']).sum()['time_length']/3600#所有回帖时长
df['cnt_post_react_all'] = data4.groupby(data4['APARTMENT_NAME']).size()#所有回帖数
df['avg_time_react_all'] = df['length_time_react_all']*1.0/df['cnt_post_react_all']
df['length_time_react_1'] = data4[data4.marktime==1].groupby(data4[data4.marktime==1]['APARTMENT_NAME']).sum()['time_length']/3600
df['cnt_post_react_1'] = data4[data4.marktime==1].groupby(data4[data4.marktime==1]['APARTMENT_NAME']).size()
df['avg_time_react_1'] = df['length_time_react_1']*1.0/df['cnt_post_react_1']
df['length_time_react_2'] = data4[data4.marktime==2].groupby(data4[data4.marktime==2]['APARTMENT_NAME']).sum()['time_length']/3600
df['cnt_post_react_2'] = data4[data4.marktime==2].groupby(data4[data4.marktime==2]['APARTMENT_NAME']).size()
df['avg_time_react_2'] = df['length_time_react_2']*1.0/df['cnt_post_react_2']

df.head()
df.to_csv('20170325_postintime.csv',encoding='gbk')


########################################################################################无用

##这是没有空值的情况，对于react_time里有空值时，将空值赋很大的时间


def f(x):
    if str(x)!='NaT':
        t = time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))
    else:
        t = 1514735999.0
    return t

def f1(x):
    a = u'客服|管家'
    if re.search(a, unicode(x)):
        return 'Yes'
    else:
        return 'No'
def f2(x):
    if '08:30:00' <= x[-8:] <'18:00:00':
        a = 1
    elif '18:00:00'<=x[-8:]<'22:00:00':
        a = 2
    elif x[-8:]>='22:00:00':
        a = 3
    else:
        a = 3
    return a
def f3(x):
    s = time.mktime(time.strptime(x[:19],"%Y-%m-%d %H:%M:%S"))-time.mktime(time.strptime(x[:10],"%Y-%m-%d"))
    if s<30600:
        t = time.mktime(time.strptime(x[:11]+'09:00:00', "%Y-%m-%d %H:%M:%S"))
    elif s>79200:
        t = time.mktime(time.strptime(x[:10],"%Y-%m-%d"))+118800
    else:
        t = time.mktime(time.strptime(x[:19], "%Y-%m-%d %H:%M:%S"))
    return t
def f4(x):
    if str(x) != 'NaT':
        t = time.mktime(time.strptime(str(x)[:10], "%Y-%m-%d"))
    else:
        t = 1514735999.0
    return t

#data = pd.read_excel()
for i in ['time','react_time']:
    data[i] = data[i].astype(str)
data['react_timestamp'] = data['react_time'].apply(f)
data['time1'] = data['time'].apply(f2)
data['time2'] = data['time'].apply(f3)
data['time3'] = data.react_timestamp-data.time2
data['is_service'] = data['react_name'].apply(f1)
data['time_0'] = data['time'].apply(lambda x:time.mktime(time.strptime(x[:10],"%Y-%m-%d")))
data['react_time_0'] = data['react_time'].apply(f4)
df = pd.DataFrame(0,index=data['APARTMENT_NAME'].drop_duplicates(),columns=['count_all','reply_count','reply_intime','reply_timelength_1','reply_count_1','reply_timelength_2','reply_count_2','reply_timelength_sameday_1','reply_count_sameday_1','reply_timelength_sameday_2','reply_count_sameday_2'])
data1 = data[(data.TYPE_NAME==u'咨询物业') & (data.FAMILY_NAME!=u'悦悦') & (data.OWNER_TYPE==1)]
data3 = data1[data1.is_service == 'Yes']
data4 = data3[((data3.time1==1) & (data3.time3<3600) & (data3.time3>0))|((data3.time1==2) & (data3.time3<7200) & (data3.time3>0))|((data3.time1==3) & (data3.time3<0))]
data2 = data1.drop_duplicates(['POST_SID'])
df['count_all'] = data2.groupby(data2['APARTMENT_NAME']).size()
df['reply_intime'] = data4.groupby(data4['APARTMENT_NAME']).size()
df['reply_count'] = data3.groupby(data3['APARTMENT_NAME']).size()
df['reply_timelength_1'] = data3[data3.time1==1].groupby(data3[data3.time1==1]['APARTMENT_NAME']).sum()['time3']/3600
df['reply_count_1'] = data3[data3.time1==1].groupby(data3[data3.time1==1]['APARTMENT_NAME']).size()
df['reply_timelength_2'] = data3[data3.time1 == 2].groupby(data3[data3.time1 == 2]['APARTMENT_NAME']).sum()['time3']/3600
df['reply_count_2'] = data3[data3.time1 == 2].groupby(data3[data3.time1 == 2]['APARTMENT_NAME']).size()
df['reply_timelength_sameday_1'] = data3[(data3.time1==1) & (data3.time_0==data3.react_time_0)].groupby(data3[(data3.time1==1) & (data3.time_0==data3.react_time_0)]['APARTMENT_NAME']).sum()['time3']/3600
df['reply_count_sameday_1'] = data3[(data3.time1==1) & (data3.time_0==data3.react_time_0)].groupby(data3[(data3.time1==1) & (data3.time_0==data3.react_time_0)]['APARTMENT_NAME']).size()
df['reply_timelength_sameday_2'] = data3[(data3.time1 == 2) & (data3.time_0 == data3.react_time_0)].groupby(data3[(data3.time1 == 2) & (data3.time_0 == data3.react_time_0)]['APARTMENT_NAME']).sum()['time3'] / 3600
df['reply_count_sameday_2'] = data3[(data3.time1 == 2) & (data3.time_0 == data3.react_time_0)].groupby(data3[(data3.time1 == 2) & (data3.time_0 == data3.react_time_0)]['APARTMENT_NAME']).size()
df
