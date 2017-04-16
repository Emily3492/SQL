###############################SQL取值所有提单
select a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,
a.CREATED_ON ,a.RESPONSE_TIME ,a.PROCESS_TIME ,a.SERVICE_CATEGORY,b.CATEGORY_NAME ,c.APARTMENT_NAME,a.SERVICE_STATUS
from HOME_SERVICE_MAIN  a
left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID
left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID
where APARTMENT_NAME NOT IN('幸福家园')

一行：select a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.CREATED_ON ,a.RESPONSE_TIME ,a.PROCESS_TIME ,a.SERVICE_CATEGORY,b.CATEGORY_NAME ,c.APARTMENT_NAME,a.SERVICE_STATUS from HOME_SERVICE_MAIN a left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID where APARTMENT_NAME NOT IN('幸福家园')
#########################################python 取值

import os  # 设置存储路径
print(os.getcwd())
os.chdir("D:\\work_all\\python")  # 包安装路径 D:\Program Files\Anaconda2\Scripts
print(os.getcwd())
# 直接连接数据库
import pyodbc
conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 10.0};SERVER=111.111.111.111;DATABASE=JOY_HOME;UID=11;PWD=1111')
cur = conn.cursor()
aa = cur.execute("select d.OWNER_NO,a.SERVICE_SID,a.APARTMENT_SID,a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.CREATED_ON ,a.RESPONSE_TIME ,a.PROCESS_TIME ,a.SERVICE_CATEGORY,b.CATEGORY_NAME ,c.APARTMENT_NAME,a.SERVICE_STATUS from HOME_SERVICE_MAIN a left join HOME_OWNER d on a.CREATEDBY= d.OWNER_SID left join HOME_SERVICE_CATEGORY b on a.SERVICE_CATEGORY = b.CATEGORY_SID left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID where APARTMENT_NAME NOT IN('幸福家园')")
info = cur.fetchall()
cur.close()
conn.close()

import pandas as pd
info1 = [list(x) for x in info]
df = pd.DataFrame(info1,columns=['OWNER_NO','SERVICE_SID', 'APARTMENT_SID', 'TYPE_SID', 'TYPE_NAME', 'SERVICE_NO', 'CREATED_ON','RESPONSE_TIME','PROCESS_TIME','SERVICE_CATEGORY','CATEGORY_NAME','APARTMENT_NAME','SERVICE_STATUS'])

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

df.head()
####################python
import time
t_11 = int(time.mktime(time.strptime('2016-11-26 00:00:00',"%Y-%m-%d %H:%M:%S")))
t_10 = int(time.mktime(time.strptime('2016-10-26 00:00:00',"%Y-%m-%d %H:%M:%S")))
t_9 = int(time.mktime(time.strptime('2016-9-26 00:00:00',"%Y-%m-%d %H:%M:%S")))
#df = pd.read_excel('.../SERVICE.xlsx',encoding='gbk')
df['created_timestamp'] = df['CREATED_ON'].apply(lambda x:(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))
columns_name=['history_all','history_overtime','history_ratio','now_all','now_overtime','now_ratio','preced_all','preced_overtime','preced_ratio','change']
index_name=df.drop_duplicates(['APARTMENT_NAME'])['APARTMENT_NAME']

#######################################################################################1.响应超时
#响应超时(所有):
data = df
#响应超时（不包含家政、巡检）:
data = df[(df.CATEGORY_NAME!=u'家政服务') & (df.CATEGORY_NAME!=u'巡检')]
#响应超时（只包含家政）:
data = df[df.CATEGORY_NAME==u'家政服务']

time_length = 60*15
df_response = pd.DataFrame(0,index=index_name,columns=columns_name)
data1 = data[data['RESPONSE_TIME'].notnull()]
data1['response_timestamp'] = data1['RESPONSE_TIME'].apply(lambda x:(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))
data_history = data[(data.created_timestamp<t_11)]
data_history_overtime = data1[(data1.response_timestamp-data1.created_timestamp>=time_length)]
data_now = data[(data.created_timestamp>=t_10) & (data.created_timestamp<t_11)]
data_now1 = data1[(data1.created_timestamp>=t_10) & (data1.created_timestamp<t_11)]
data_now_overtime = data_now1[(data_now1.response_timestamp-data_now1.created_timestamp>=time_length)]
data_preced = data[(data.created_timestamp>=t_9) & (data.created_timestamp<t_10)]
data_preced1 = data1[(data1.created_timestamp>=t_9) & (data1.created_timestamp<t_10)]
data_preced_overtime = data_preced1[(data_preced1.response_timestamp-data_preced1.created_timestamp>=time_length)]
df_response.ix[:,'history_all']= data_history[['SERVICE_SID']].groupby(data_history['APARTMENT_NAME']).size()
df_response.ix[:,'history_overtime'] = data_history_overtime[['SERVICE_SID']].groupby(data_history_overtime['APARTMENT_NAME']).size()
df_response.ix[:,'now_all'] = data_now[['SERVICE_SID']].groupby(data_now['APARTMENT_NAME']).size()
df_response.ix[:,'now_overtime'] = data_now_overtime[['SERVICE_SID']].groupby(data_now_overtime['APARTMENT_NAME']).size()
df_response.ix[:,'preced_all'] = data_preced[['SERVICE_SID']].groupby(data_preced['APARTMENT_NAME']).size()
df_response.ix[:,'preced_overtime'] = data_preced_overtime[['SERVICE_SID']].groupby(data_preced_overtime['APARTMENT_NAME']).size()
df_response['history_ratio'] = df_response['history_overtime']*1.0/df_response['history_all']
df_response['now_ratio'] = df_response['now_overtime']*1.0/df_response['now_all']
df_response['preced_ratio'] = df_response['preced_overtime']*1.0/df_response['preced_all']
df_response['change'] = df_response['now_ratio']-df_response['preced_ratio']

df_response.columns=['历史总单数','历史超时单数','历史比率','当月总单数','当月超时单数','当月比率','上月总单数','上月超时单数','上月比率','较上月变化']

df_response.head(15)
df_response.tail(15)

df_response.to_csv('11-25-sheet_response.csv',encoding='utf-8')

########################################################################################################2.处理超时
#处理超时(所有):
data = df
#处理超时（不包含家政、巡检）:
data = df[(df.CATEGORY_NAME!=u'家政服务') & (df.CATEGORY_NAME!=u'巡检')]
#处理超时（只包含家政）:
data = df[df.CATEGORY_NAME==u'家政服务']

time_length = 60*60*24
df_process = pd.DataFrame(0,index=index_name,columns=columns_name)
data1 = data[data['PROCESS_TIME'].notnull()]
data1['process_timestamp'] = data1['PROCESS_TIME'].apply(lambda x:(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))
data_history = data[(data.created_timestamp<t_11)]
data_history_overtime = data1[(data1.process_timestamp-data1.created_timestamp>=time_length)]
data_now = data[(data.created_timestamp>=t_10) & (data.created_timestamp<t_11)]
data_now1 = data1[(data1.created_timestamp>=t_10) & (data1.created_timestamp<t_11)]
data_now_overtime = data_now1[(data_now1.process_timestamp-data_now1.created_timestamp>=time_length)]
data_preced = data[(data.created_timestamp>=t_9) & (data.created_timestamp<t_10)]
data_preced1 = data1[(data1.created_timestamp>=t_9) & (data1.created_timestamp<t_10)]
data_preced_overtime = data_preced1[(data_preced1.process_timestamp-data_preced1.created_timestamp>=time_length)]
df_process.ix[:,'history_all']= data_history[['SERVICE_SID']].groupby(data_history['APARTMENT_NAME']).size()
df_process.ix[:,'history_overtime'] = data_history_overtime[['SERVICE_SID']].groupby(data_history_overtime['APARTMENT_NAME']).size()
df_process.ix[:,'now_all'] = data_now[['SERVICE_SID']].groupby(data_now['APARTMENT_NAME']).size()
df_process.ix[:,'now_overtime'] = data_now_overtime[['SERVICE_SID']].groupby(data_now_overtime['APARTMENT_NAME']).size()
df_process.ix[:,'preced_all'] = data_preced[['SERVICE_SID']].groupby(data_preced['APARTMENT_NAME']).size()
df_process.ix[:,'preced_overtime'] = data_preced_overtime[['SERVICE_SID']].groupby(data_preced_overtime['APARTMENT_NAME']).size()
df_process['history_ratio'] = df_process['history_overtime']*1.0/df_process['history_all']
df_process['now_ratio'] = df_process['now_overtime']*1.0/df_process['now_all']
df_process['preced_ratio'] = df_process['preced_overtime']*1.0/df_process['preced_all']
df_process['change'] = df_process['now_ratio']-df_process['preced_ratio']

df_process.columns=['历史总单数','历史超时单数','历史比率','当月总单数','当月超时单数','当月比率','上月总单数','上月超时单数','上月比率','较上月变化']

df_process.head(15)
df_process.tail(15)

df_process.to_csv('11-25-sheet_process.csv',encoding='utf-8')

#########################################################################################################################3.提单关闭率
#提单（所有）:
data = df
#提单（不包含家政、巡检）:
data = df[(df.CATEGORY_NAME!=u'家政服务') & (df.CATEGORY_NAME!=u'巡检')]
#提单（只包含家政）:
data = df[df.CATEGORY_NAME==u'家政服务']

df['created_timestamp'] = df['CREATED_ON'].apply(lambda x:(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))
columns_name=['history_all','history_close','history_ratio','now_all','now_close','now_ratio','preced_all','preced_close','preced_ratio','change']
index_name=df.drop_duplicates(['APARTMENT_NAME'])['APARTMENT_NAME']
df_sheet = pd.DataFrame(0,index=index_name,columns=columns_name)

data['SERVICE_STATUS'] = data['SERVICE_STATUS'].astype(str)#将这一字段改成str格式
data1 = data[data['SERVICE_STATUS'].isin(['4','6','9'])]
data_history = data[(data.created_timestamp<t_11)]
data_history_close =data1[(data1.created_timestamp<t_11)]
data_now = data[(data.created_timestamp>=t_10) & (data.created_timestamp<t_11)]
data_now_close = data1[(data1.created_timestamp>=t_10) & (data1.created_timestamp<t_11)]
data_preced = data[(data.created_timestamp>=t_9) & (data.created_timestamp<t_10)]
data_preced_close = data1[(data1.created_timestamp>=t_9) & (data1.created_timestamp<t_10)]
df_sheet.ix[:,'history_all']= data[['SERVICE_SID']].groupby(data['APARTMENT_NAME']).size()
df_sheet.ix[:,'history_close'] = data1[['SERVICE_SID']].groupby(data1['APARTMENT_NAME']).size()
df_sheet.ix[:,'now_all'] = data_now[['SERVICE_SID']].groupby(data_now['APARTMENT_NAME']).size()
df_sheet.ix[:,'now_close'] = data_now_close[['SERVICE_SID']].groupby(data_now_close['APARTMENT_NAME']).size()
df_sheet.ix[:,'preced_all'] = data_preced[['SERVICE_SID']].groupby(data_preced['APARTMENT_NAME']).size()
df_sheet.ix[:,'preced_close'] = data_preced_close[['SERVICE_SID']].groupby(data_preced_close['APARTMENT_NAME']).size()
df_sheet['history_ratio'] = df_sheet['history_close']*1.0/df_sheet['history_all']
df_sheet['now_ratio'] = df_sheet['now_close']*1.0/df_sheet['now_all']
df_sheet['preced_ratio'] = df_sheet['preced_close']*1.0/df_sheet['preced_all']
df_sheet['change'] = df_sheet['now_ratio']-df_sheet['preced_ratio']

df_sheet.columns=['历史总单数','历史关闭单数','历史比率','当月总单数','当月关闭单数','当月比率','上月总单数','上月关闭单数','上月比率','较上月变化']

df_sheet.head(15)
df_sheet.tail(15)

df_sheet.to_csv('11-25-sheet_close.csv',encoding='utf-8')





select top 2000 * from(
select e.CREATED_ON close_time,e.HIST_TYPE,a.SERVICE_DESC,d.OWNER_NO,a.SERVICE_SID,a.APARTMENT_SID
,ROW_NUMBER() over (partition by a.SERVICE_SID  order by a.SERVICE_SID ,e.CREATED_ON ) as rn,
a.TYPE_SID,a.TYPE_NAME,a.SERVICE_NO,a.CREATED_ON ,a.RESPONSE_TIME ,a.PROCESS_TIME ,a.SERVICE_CATEGORY,b.CATEGORY_NAME ,c.APARTMENT_NAME,a.SERVICE_STATUS
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
