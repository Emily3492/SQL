--满意度剔除巡检、家政服务（python），个人满意度剔除巡检（SQL或者python）
SELECT CATEGORY_NAME, ha.apartment_name,  hs.PROCESS_TIME,hs.REMARK,hs.PROCESS_USER ,a.OWNER_SID ,a.OWNER_NAME,a.FAMILY_NAME,a.OWNER_PHONE,
evaluation_item1,
evaluation_item2 ,
evaluation_item3  , evaluation_item1+evaluation_item2+evaluation_item3
from  home_service_main  hs  left join home_owner a on hs.PROCESS_USER =a.owner_sid
left join HOME_SERVICE_CATEGORY b on hs.SERVICE_CATEGORY = b.CATEGORY_SID
left  join  home_apartment  ha  on hs.apartment_sid  =  ha.apartment_sid
where  (service_status  =  6  or  service_status  =  9)
and  SERVICE_CATEGORY  in(select CATEGORY_SID from HOME_SERVICE_CATEGORY where CATEGORY_NAME not in('巡检','家政服务'))--计算项目满意度剔除巡检、家政，个人满意度只剔除巡检，更改SQL取值。
and    hs.apartment_sid  in  (select APARTMENT_SID from HOME_APARTMENT)
and hs.PROCESS_TIME is not null

#################提单类型：###############
#公共维修
#家政服务
#入室维修
#送水
#投诉
#巡检
#########################################
#一行：
SELECT  CATEGORY_NAME,hs.apartment_sid,ha.apartment_name,hs.PROCESS_TIME,hs.REMARK,hs.PROCESS_USER,a.OWNER_SID,a.OWNER_NAME,a.FAMILY_NAME,a.OWNER_PHONE,evaluation_item1,evaluation_item2,evaluation_item3  , evaluation_item1+evaluation_item2+evaluation_item3 from home_service_main hs left join home_owner a on hs.PROCESS_USER=a.owner_sid left join HOME_SERVICE_CATEGORY b on hs.SERVICE_CATEGORY = b.CATEGORY_SID left join home_apartment ha on hs.apartment_sid=ha.apartment_sid where  (service_status  =  6  or  service_status  =  9)and SERVICE_CATEGORY in(select CATEGORY_SID from HOME_SERVICE_CATEGORY )and hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)and hs.PROCESS_TIME is not null
#############################导入数据， fetchall有问题，用SQL代码跑出原始数据后再导入
# data = pd.read_excel('D:\\work_all\\python\\0207.xlsx')，这份excel数据只剔除了巡检，计算项目时需要剔除家政、巡检。
#python 执行不了where CATEGORY_NAME not in('巡检','家政服务')语句，所以导入全部数据，用python进行筛选。
###############注意：计算个人满意度时，只剔除巡检，

import os  # 设置存储路径
os.chdir("D:\\work_all\\python")  # 包安装路径 D:\Program Files\Anaconda2\Scripts
print(os.getcwd())
# 直接连接数据库
import pyodbc
conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 10.0};SERVER=111.111.111.111;DATABASE=JOY_HOME;UID=11;PWD=1111')
cur = conn.cursor()
aa = cur.execute("SELECT c.OWNER_NO,CATEGORY_NAME,hs.apartment_sid,ha.apartment_name,hs.PROCESS_TIME,hs.REMARK,hs.PROCESS_USER,a.OWNER_SID,a.OWNER_NAME,a.FAMILY_NAME,a.OWNER_PHONE,evaluation_item1,evaluation_item2,evaluation_item3  , evaluation_item1+evaluation_item2+evaluation_item3 from home_service_main hs left join  home_owner c on hs.CREATEDBY=c.owner_sid left join home_owner a on hs.PROCESS_USER=a.owner_sid left join HOME_SERVICE_CATEGORY b on hs.SERVICE_CATEGORY = b.CATEGORY_SID left join home_apartment ha on hs.apartment_sid=ha.apartment_sid where  (service_status  =  6  or  service_status  =  9)and SERVICE_CATEGORY in(select CATEGORY_SID from HOME_SERVICE_CATEGORY )and hs.apartment_sid in (select APARTMENT_SID from HOME_APARTMENT)and hs.PROCESS_TIME is not null")
info = cur.fetchall()
cur.close()
conn.close()
import pandas as pd
info1 = [list(x) for x in info]
data = pd.DataFrame(info1,columns=['OWNER_NO','CATEGORY_NAME','apartment_sid', 'apartment_name', 'PROCESS_TIME', 'REMARK', 'PROCESS_USER','OWNER_SID', 'OWNER_NAME','FAMILY_NAME','OWNER_PHONE','evaluation_item1','evaluation_item2','evaluation_item3','Unnamed: 9'])
data.head()#日志表

#先是把东方福邸的全部提出来，单独形成个dataframe，然后标记为东方福邸1和2，然后把这个dataframe拼接到原来的dataframe上去
#东方福邸一期二期分开
#待调整
import re
d = data[data.apartment_name==u'东方福邸']
def f(x):
    one = u'一期'
    two = u'二期'
    if re.search(one,unicode(x)):
        return u'东方福邸一期'
    elif re.search(two,unicode(x)):
        return u'东方福邸二期'
d['apartment_name'] = d['OWNER_NO'].apply(f)
data = pd.concat([data,d])

##########################满意度
index_name=data['apartment_name'].drop_duplicates()
columns_name = ['cnt_negative','cnt_moderate','cnt_positive','cnt_positive_r','cnt_all','cnt_all_r']
df = pd.DataFrame(0,index=index_name,columns=columns_name)

import re
def class_remark(x):
    a = u'7天自动好评'
    if re.search(a,unicode(x)):
        return 'auto'
    else:
        return 'manual'
def f_rate(x):
    if float(x['Unnamed: 9'])<1:
        a = 'remove'
    elif float(x['Unnamed: 9'])<9:
        a = 'negative'
    elif float(x['Unnamed: 9'])<12:
        a = 'moderate'
    elif float(x['Unnamed: 9'])<16:
        a = 'positive_r'
    if float(x['Unnamed: 9'])==float(15) and x['auto_manual']=='auto':
        a = 'positive_nr'
    return a

import time
t_start = int(time.mktime(time.strptime('20170401',"%Y%m%d")))
t_end = int(time.mktime(time.strptime('20170410',"%Y%m%d")))
data = data.dropna(subset=['PROCESS_TIME'])
data = data.fillna(0)
data['auto_manual'] = data['REMARK'].apply(class_remark)
data['comment'] = data.apply(lambda x:f_rate(x),1)#apply默认是对列操作，1是对行操作
data['timestamp'] = data['PROCESS_TIME'].apply(lambda x:time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S")))
data2 = data[(data.comment!='remove')& (data.timestamp>=t_start) & (data.timestamp<t_end)]
data1=data2[(data2['CATEGORY_NAME']!=u'巡检')&(data2['CATEGORY_NAME']!=u'家政服务')]#剔除类型为家政服务和小区巡检的提单
data_negative = data1[data1.comment=='negative']
df['cnt_negative'] = data_negative['comment'].groupby(data_negative['apartment_name']).size()
data_moderate = data1[data1.comment=='moderate']
df['cnt_moderate'] = data_moderate['comment'].groupby(data_moderate['apartment_name']).size()
data_positive = data1[(data1.comment=='positive_r')|(data1.comment=='positive_nr')]#|代表或者的意思
df['cnt_positive'] = data_positive['comment'].groupby(data_positive['apartment_name']).size()
data_positive_r = data1[data1.comment=='positive_r']
df['cnt_positive_r'] = data_positive_r['comment'].groupby(data_positive_r['apartment_name']).size()
df['cnt_all'] = data1['comment'].groupby(data1['apartment_name']).size()
data_all_r = data1[(data1.comment!='positive_nr')]
df['cnt_all_r'] = data_all_r['comment'].groupby(data_all_r['apartment_name']).size()

df.head()
len(df)

df.to_csv('20170410_satisfy_month.csv',encoding='gbk')
####################################################################################客服好评数

import re
def class_remark(x):
    a = u'7天自动好评'
    if re.search(a,unicode(x)):
        return 'auto'
    else:
        return 'manual'
def f_rate(x):
    a = ''
    if float(x['Unnamed: 9'])>=12:
        a = 'positive_r'
    elif float(x['Unnamed: 9'])>=9:
        a = 'moderate'
    elif float(x['Unnamed: 9'])>=1:
        a = 'negative'
    else:
        a = 'remove'
    if float(x['Unnamed: 9']) == float(15) and x['auto_manual'] == 'auto':
        a = 'positive_nr'
    return a

import time
t_start = int(time.mktime(time.strptime('20151201',"%Y%m%d")))
t_end = int(time.mktime(time.strptime('20170207',"%Y%m%d")))
data['auto_manual'] = data['REMARK'].apply(class_remark)
data['comment'] = data.apply(lambda x:f_rate(x),1)
data['timestamp'] = data['PROCESS_TIME'].apply(lambda x:time.mktime(time.strptime(str(x)[:10],"%Y-%m-%d")))
data2 = data[(data.comment!='remove')& (data.timestamp>t_start) & (data.timestamp<t_end)]
data1=data2[(data2['CATEGORY_NAME']!=u'巡检')&(data2['CATEGORY_NAME']!=u'家政服务')]#剔除类型为家政服务和小区巡检的提单

########################索引按ID
df0 = data1[['apartment_name','PROCESS_USER','OWNER_NAME','FAMILY_NAME','OWNER_PHONE']]
df = df0.set_index('PROCESS_USER').drop_duplicates()

data_negative = data1[data1.comment=='negative']
df['cnt_negative'] = data_negative['comment'].groupby(data_negative['PROCESS_USER']).size()
data_moderate = data1[data1.comment=='moderate']
df['cnt_moderate'] = data_moderate['comment'].groupby(data_moderate['PROCESS_USER']).size()
data_positive = data1[(data1.comment=='positive_r')|(data1.comment=='positive_nr')]
df['cnt_positive'] = data_positive['comment'].groupby(data_positive['PROCESS_USER']).size()
data_positive_r = data1[data1.comment=='positive_r']
df['cnt_positive_r'] = data_positive_r['comment'].groupby(data_positive_r['PROCESS_USER']).size()
df['cnt_all'] = data1['comment'].groupby(data1['PROCESS_USER']).size()
data_all_r = data1[(data1.comment!='positive_nr') & (data1.comment!='remove')]
df['cnt_all_r'] = data_all_r['comment'].groupby(data_all_r['PROCESS_USER']).size()
df

df.count()

df.to_excel('0118ID_satisfy.xlsx', encoding='gbk')

############################################若只提取客服对应的好评，只需将f_rate(x)替换以下代码
def f_rate(x):
    b = u'客服'
    if re.search(b,unicode(x['FAMILY_NAME'])):
        if float(x['Unnamed: 9'])<1:
            a = 'remove'
        elif float(x['Unnamed: 9'])<9:
            a = 'negative'
        elif float(x['Unnamed: 9'])<12:
            a = 'moderate'
        elif float(x['Unnamed: 9'])<16:
            a = 'positive_r'
        if float(x['Unnamed: 9'])==float(15) and x['auto_manual']=='auto':
            a = 'positive_nr'
    else:
        a = 'remove'
    return a



def f_rate(x):
    a = ''
    if float(x['Unnamed: 9'])>=12:
        a = 'positive_r'
    elif float(x['Unnamed: 9'])>=9:
        a = 'moderate'
    elif float(x['Unnamed: 9'])>=1:
        a = 'negative'
    else:
        a = 'remove'
    if float(x['Unnamed: 9'])==float(15) and x['auto_manual']=='auto':
        a = 'positive_nr'
    return a
