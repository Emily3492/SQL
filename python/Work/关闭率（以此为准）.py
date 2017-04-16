SQL详情表查询：
#关闭详情表
select top 2000 * from(
select e.CREATED_ON close_time,e.HIST_TYPE,a.SERVICE_DESC,d.OWNER_NO,a.SERVICE_SID,a.APARTMENT_SID
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
and b.CATEGORY_NAME not in ('家政服务','巡检')
and ( a.SERVICE_STATUS=4 or a.SERVICE_STATUS=6 or  a.SERVICE_STATUS=9)
and ( e.HIST_TYPE=4 or e.HIST_TYPE=6 or  e.HIST_TYPE=9)
)t1
where t1.rn=1

一行:

select * from(select e.CREATED_ON close_time,e.HIST_TYPE,a.SERVICE_DESC,d.OWNER_NO,a.SERVICE_SID,a.APARTMENT_SID,ROW_NUMBER() over (partition by a.SERVICE_SID  order by a.SERVICE_SID ,e.CREATED_ON ) as rn,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.CREATED_ON ,a.RESPONSE_TIME ,a.PROCESS_TIME ,a.SERVICE_CATEGORY,b.CATEGORY_NAME ,c.APARTMENT_NAME,a.SERVICE_STATUS from HOME_SERVICE_MAIN a left join HOME_SERVICE_HIST e on a.SERVICE_SID  = e.SERVICE_SID left join HOME_OWNER d on a.CREATEDBY= d.OWNER_SID left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID where APARTMENT_NAME NOT IN('幸福家园') AND a.SERVICE_STATUS NOT IN ('3') AND a.SERVICE_DESC NOT LIKE'%测%' and b.CATEGORY_NAME not in ('家政服务','巡检') and ( a.SERVICE_STATUS=4 or a.SERVICE_STATUS=6 or  a.SERVICE_STATUS=9) and ( e.HIST_TYPE=4 or e.HIST_TYPE=6 or  e.HIST_TYPE=9))t1 where t1.rn=1


#提单详情表（求提单总数用）


select e.OWNER_NO,a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,a.SERVICE_STATUS
,a.CREATED_ON ,a.PROCESS_TIME ,a.SERVICE_CATEGORY,b.CATEGORY_NAME ,c.APARTMENT_NAME
from HOME_SERVICE_MAIN  a
left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where APARTMENT_NAME NOT IN('幸福家园')
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'

一行：
select e.OWNER_NO,a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,a.SERVICE_STATUS,a.CREATED_ON ,a.PROCESS_TIME ,a.SERVICE_CATEGORY,b.CATEGORY_NAME ,c.APARTMENT_NAME from HOME_SERVICE_MAIN a left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID where APARTMENT_NAME NOT IN('幸福家园') AND a.SERVICE_STATUS NOT IN ('3') AND a.SERVICE_DESC NOT LIKE'%测%'


及时关闭率规则：
#提单公共维修、投诉总数用SQL取值
大条件：
and ( a.SERVICE_STATUS=4 or a.SERVICE_STATUS=6 or  a.SERVICE_STATUS=9)
AND a.SERVICE_STATUS NOT IN ('3')
AND a.SERVICE_DESC NOT LIKE'%测%'
CATEGORY_NAME in('公共维修','投诉')

-1天内关闭为及时
a.TYPE_NAME like '%安保投诉%' 、 '%服务态度投诉%'、 '%绿化投诉%'、 '%清洁卫生投诉%'

-7天内关闭为及时
and (a.TYPE_NAME like '%停车投诉%' 、'%报警设备%'、 '%道闸故障%'、'%电梯故障%'、
'%健身设施故障%'、 '%其他设施设备故障%'、 '%弱电系统%'、 '%消防大类%'、'%照明故障%')—7

-25天内关闭为及时
and a.TYPE_NAME like '%装修投诉%'—25

#导入数据
import os  # 设置存储路径
print(os.getcwd())
os.chdir("D:\\work_all\\python")  # 包安装路径 D:\Program Files\Anaconda2\Scripts
print(os.getcwd())
# 直接连接数据库
import pyodbc
conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 10.0};SERVER=111.111.111.111;DATABASE=JOY_HOME;UID=11;PWD=1111')
cur = conn.cursor()
aa = cur.execute("select * from(select e.CREATED_ON close_time,e.HIST_TYPE,a.SERVICE_DESC,d.OWNER_NO,a.SERVICE_SID,a.APARTMENT_SID,ROW_NUMBER() over (partition by a.SERVICE_SID  order by a.SERVICE_SID ,e.CREATED_ON ) as rn,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.CREATED_ON ,a.RESPONSE_TIME ,a.PROCESS_TIME ,a.SERVICE_CATEGORY,b.CATEGORY_NAME ,c.APARTMENT_NAME,a.SERVICE_STATUS from HOME_SERVICE_MAIN a left join HOME_SERVICE_HIST e on a.SERVICE_SID  = e.SERVICE_SID left join HOME_OWNER d on a.CREATEDBY= d.OWNER_SID left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID where a.SERVICE_STATUS NOT IN ('3') AND a.SERVICE_DESC NOT LIKE'%测%' and ( a.SERVICE_STATUS=4 or a.SERVICE_STATUS=6 or  a.SERVICE_STATUS=9) and ( e.HIST_TYPE=4 or e.HIST_TYPE=6 or  e.HIST_TYPE=9))t1 where t1.rn=1")
info = cur.fetchall()
cur.close()

#关闭数据
import pandas as pd
info1 = [list(x) for x in info]
df = pd.DataFrame(info1,columns=['close_time','HIST_TYPE', 'SERVICE_DESC', 'OWNER_NO', 'SERVICE_SID', 'APARTMENT_SID', 'rn','TYPE_SID','TYPE_NAME','SERVICE_NO','CREATED_ON','RESPONSE_TIME','PROCESS_TIME','SERVICE_CATEGORY','CATEGORY_NAME','APARTMENT_NAME','SERVICE_STATUS'])
df.head()

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
data=df

cur1 = conn.cursor()
a1 = cur1.execute("select e.OWNER_NO,a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.SERVICE_DESC,a.SERVICE_STATUS,a.CREATED_ON ,a.PROCESS_TIME ,a.SERVICE_CATEGORY,b.CATEGORY_NAME ,c.APARTMENT_NAME from HOME_SERVICE_MAIN a left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID where APARTMENT_NAME NOT IN('幸福家园') AND a.SERVICE_STATUS NOT IN ('3') AND a.SERVICE_DESC NOT LIKE'%测%'")
info2 = cur1.fetchall()
cur1.close()
conn.close()

#总单数
import pandas as pd
info3 = [list(x) for x in info2]
df1= pd.DataFrame(info3,columns=['OWNER_NO','SERVICE_SID', 'APARTMENT_SID', 'TYPE_SID', 'TYPE_NAME', 'SERVICE_NO', 'SERVICE_DESC','SERVICE_STATUS','CREATED_ON','PROCESS_TIME','SERVICE_CATEGORY','CATEGORY_NAME','APARTMENT_NAME'])

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
data1=df1

############################################计算关闭率及提单总数
import pandas as pd
import time
import re
#data = pd.read_excel('.../关闭率.xlsx')
data = data[data.SERVICE_STATUS!=3]
def f1(x):
    test = u'测'
    if re.search(test,unicode(x)):
        return False
    else:
        return True

data['X']= data['SERVICE_DESC'].apply(f1)
data = data[data.X==True]#SERVICE_DESC不含'测'
data = data[data['close_time'].notnull()]
data = data[(data.CATEGORY_NAME==u'公共维修')|(data.CATEGORY_NAME==u'投诉')]
#data.columns#查看字段名，注意有空格的字段也要加上空格
data['CREATED_ON_UNIX'] = data['CREATED_ON'].apply(lambda x:time.mktime(time.strptime(str(x)[:19],'%Y-%m-%d %H:%M:%S')))


data1 = data1[data1.SERVICE_STATUS!=3]
def f2(x):
    test1 = u'测'
    if re.search(test1,unicode(x)):
        return False
    else:
        return True

data1['X']= data1['SERVICE_DESC'].apply(f1)
data1 = data1[data1.X==True]#SERVICE_DESC不含'测'
data1 = data1[(data1.CATEGORY_NAME==u'公共维修')|(data1.CATEGORY_NAME==u'投诉')]
data1['CREATED_ON_UNIX'] = data1['CREATED_ON'].apply(lambda x:time.mktime(time.strptime(str(x)[:19],'%Y-%m-%d %H:%M:%S')))


columns_name = []
for i in ['history','year','month','week','day']:
    for j in ['all','intime','ratio']:
        name = i+'_'+j
        columns_name.append(name)
index_name=df.drop_duplicates(['APARTMENT_NAME'])['APARTMENT_NAME']
dff = pd.DataFrame(0,index=index_name,columns=columns_name)

t = '2017-04-10'
t_now = time.mktime(time.strptime(t,'%Y-%m-%d'))#设置当前时间
t_dict = {}
t_dict['history'] = 0
t_dict['year'] = time.mktime(time.strptime(t[:4],'%Y'))
t_dict['month'] = t_now-2419200#前一个月
#t_dict['month']= time.mktime(time.strptime(t[:7],'%Y-%m'))#当月
t_dict['week'] = t_now-604800
t_dict['day'] = t_now-86400

#0为及时，1为不及时
def check_intime(x):
    a = 0
    one = u'安保投诉|服务态度投诉|绿化投诉|清洁卫生投诉|'
    seven = u'停车投诉|报警设备|道闸故障|电梯故障|健身设施故障|其他设施设备故障|弱电系统|消防大类|照明故障'
    twentyfive = u'装修投诉'
    if re.search(one,unicode(x['TYPE_NAME'])):
        if time.mktime(time.strptime(str(x['close_time'])[:19],'%Y-%m-%d %H:%M:%S'))-time.mktime(time.strptime(str(x['CREATED_ON'])[:19],'%Y-%m-%d %H:%M:%S'))<86400:
            a = 1
    if re.search(seven,unicode(x['TYPE_NAME'])):
        if time.mktime(time.strptime(str(x['close_time'])[:19],'%Y-%m-%d %H:%M:%S'))-time.mktime(time.strptime(str(x['CREATED_ON'])[:19],'%Y-%m-%d %H:%M:%S'))<604800:
            a = 1
    if re.search(twentyfive,unicode(x['TYPE_NAME'])):
        if time.mktime(time.strptime(str(x['close_time'])[:19],'%Y-%m-%d %H:%M:%S'))-time.mktime(time.strptime(str(x['CREATED_ON'])[:19],'%Y-%m-%d %H:%M:%S'))<2160000:
            a = 1
    return a

for i in ['history','year','month','week','day']:
    data2 = data[(data.CREATED_ON_UNIX<t_now) & (data.CREATED_ON_UNIX>=t_dict[i])]
    data4 = data1[(data1.CREATED_ON_UNIX<t_now) & (data1.CREATED_ON_UNIX>=t_dict[i])]
    dff[i+'_all'] = data4.drop_duplicates(['SERVICE_SID','APARTMENT_NAME'])[['SERVICE_SID','APARTMENT_NAME']].groupby(data4.drop_duplicates(['SERVICE_SID','APARTMENT_NAME'])['APARTMENT_NAME']).size()
    if len(data2)==0:
        data2['react_status'] = 0
    else:
        data2['react_status'] = data2.apply(lambda x:check_intime(x),1)
    data3 = data2[data2.react_status==1]
    dff[i+'_intime'] = data3.drop_duplicates(['SERVICE_SID','APARTMENT_NAME'])[['SERVICE_SID','APARTMENT_NAME']].groupby(data3.drop_duplicates(['SERVICE_SID','APARTMENT_NAME'])['APARTMENT_NAME']).size()
    dff[i+'_ratio'] = dff[i+'_intime']*1.0/dff[i+'_all']


     dff.head()
dff.to_excel('20170410_closeintime.xlsx', encoding='gbk')


################################################################################关闭时长
#关闭总时长
#关闭单数
#平均关闭时长
#ave_RESPONSE_1工作时间段(8:30-18:00)；
#ave_RESPONSE_1非工作时间段。

data = data.dropna(subset=['close_time'])
len(data)#响应提单数

index_name = data['APARTMENT_NAME'].drop_duplicates()
columns_name = ['length_time_close_all','cnt_close_all','ave_close_all','length_time_close_1','cnt_close_1','ave_close_1','length_time_close_2','cnt_close_2','ave_close_2']
df = pd.DataFrame(0,index=index_name,columns=columns_name)

def fun_marktime(x):
    if '08:30:00'<=x['CREATED_ON'][11:19]<'18:00:00':
        return 1
    else:
        return 2
def fun_meantime(x):
    RESPONSE_TIME_unix = time.mktime(time.strptime(x['close_time'][:19],'%Y-%m-%d %H:%M:%S'))
    CREATED_ON_unix = time.mktime(time.strptime(x['CREATED_ON'][:19],'%Y-%m-%d %H:%M:%S'))
    time_length = RESPONSE_TIME_unix - CREATED_ON_unix
    return time_length
for i in ['close_time','CREATED_ON']:
    data[i] = data[i].astype(str)
data['marktime'] = data.apply(lambda x:fun_marktime(x),1)#1是指对行操作，可以对多行操作
data['time_length'] = data.apply(lambda x:fun_meantime(x),1)

df['length_time_close_all'] = data.groupby(data['APARTMENT_NAME']).sum()['time_length']/3600#所有回帖时长
df['cnt_close_all'] = data.groupby(data['APARTMENT_NAME']).size()#所有回帖数
df['ave_close_all'] = df['length_time_close_all']*1.0/df['cnt_close_all']
df['length_time_close_1'] = data[data.marktime==1].groupby(data[data.marktime==1]['APARTMENT_NAME']).sum()['time_length']/3600
df['cnt_close_1'] = data[data.marktime==1].groupby(data[data.marktime==1]['APARTMENT_NAME']).size()
df['ave_close_1'] = df['length_time_close_1']*1.0/df['cnt_close_1']
df['length_time_close_2'] = data[data.marktime==2].groupby(data[data.marktime==2]['APARTMENT_NAME']).sum()['time_length']/3600
df['cnt_close_2'] = data[data.marktime==2].groupby(data[data.marktime==2]['APARTMENT_NAME']).size()
df['ave_close_2'] = df['length_time_close_2']*1.0/df['cnt_close_2']
