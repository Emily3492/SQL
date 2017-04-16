#SQL取值
#注册户数
select t1.* from (
SELECT  a.CREATED_ON,a.OWNER_NO,a.OWNER_SID ,b.APARTMENT_NAME, ROW_NUMBER() over (partition by b.APARTMENT_NAME,a.OWNER_NO order by a.CREATED_ON,b.APARTMENT_NAME ) as rn
          FROM HOME_OWNER AS a
          left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
          where a.OWNER_type like('%1%')
)t1
where t1.rn =1


#一行：
select t1.* from (SELECT  a.CREATED_ON,a.OWNER_NO,a.OWNER_SID ,b.APARTMENT_NAME, ROW_NUMBER() over (partition by b.APARTMENT_NAME,a.OWNER_NO order by a.CREATED_ON,b.APARTMENT_NAME ) as rn FROM HOME_OWNER AS a left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID where a.OWNER_type like('%1%'))t1 where t1.rn =1

#新增注册户数中申请门禁户数
select t1.* from (
SELECT  a.OWNER_NO,a.CREATED_ON owner_created,d.CREATED_ON door_created  ,a.OWNER_SID ,c.SHEET_SID 申请单,b.APARTMENT_NAME, ROW_NUMBER() over (partition by b.APARTMENT_NAME,a.OWNER_NO order by a.CREATED_ON,b.APARTMENT_NAME ) as rn
          FROM HOME_OWNER AS a
          left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
          left join HOME_UDITEM_SHEET c on a.OWNER_SID = c.USER_SID
          left join HOME_UD_SHEET d
         on c.SHEET_SID = d.SHEET_SID
        -- WHERE a.CREATED_ON >= '20161114'
         --and  a.CREATED_ON < '20161122'
        -- and d.CREATED_ON >= '20161114'
        -- and  d.CREATED_ON < '20161122'
        --and b.APARTMENT_NAME in ('东方郡','东方福邸','江滨花园','绿野春天','依山郡','银爵世纪')
          where a.OWNER_type like('%1%')
         and d.remark  is null
         and c.SHEET_SID  is not null
)t1
where t1.rn =1

#一行：
select t1.* from (SELECT  a.OWNER_NO,a.CREATED_ON owner_created,d.CREATED_ON door_created,a.OWNER_SID ,c.SHEET_SID SQD,b.APARTMENT_NAME, ROW_NUMBER() over (partition by b.APARTMENT_NAME,a.OWNER_NO order by a.CREATED_ON,b.APARTMENT_NAME ) as rn FROM HOME_OWNER AS a left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID left join HOME_UDITEM_SHEET c on a.OWNER_SID = c.USER_SID left join HOME_UD_SHEET d on c.SHEET_SID = d.SHEET_SID where a.OWNER_type like('%1%') and d.remark is null)t1 where t1.rn =1

#申请ID数
select t2.* from (
select t1.*,ROW_NUMBER() over (partition by t1.小区名称,t1.用户ID order by t1.申请时间,t1.小区名称 desc ) as rn
from (
select a.SHEET_SID,a.CREATED_ON 申请时间,b.USER_SID 用户ID,b.DOOR_SID 门ID,c.DOOR_SID,
c.DOOR_NAME 门名称,c.APARTMENT_SID 小区ID,d.APARTMENT_SID,d.APARTMENT_NAME 小区名称,
(CASE a.FLAG
                 WHEN '0' THEN
                  '待审'
                 WHEN '1' THEN
                  '审核通过'
                 WHEN '2' THEN
                  '驳回'
                 ELSE
                  ''
               END) AS 单据状态
from HOME_UD_SHEET a
         left join HOME_UDITEM_SHEET b
                on a.SHEET_SID = b.SHEET_SID
         left join HOME_APARTMENT_DOOR c
                on b.DOOR_SID = c.DOOR_SID
         left join HOME_APARTMENT d
                on c.APARTMENT_SID = d.APARTMENT_SID
where d.APARTMENT_NAME not in('幸福家园')
--and a.CREATED_ON >= '20161009'
--and a.CREATED_ON < '20170213'
and a.remark  is null
)t1
--order by t1.申请时间,t1.小区名称  desc
)t2
where t2.rn = 1


#一行
select t2.* from (select t1.*,ROW_NUMBER() over (partition by t1.APARTMENT_NAME,t1.USER_SID order by t1.CREATED_ON,t1.APARTMENT_NAME desc ) as rn from (select a.SHEET_SID,a.CREATED_ON,b.USER_SID,b.DOOR_SID,c.DOOR_NAME,c.APARTMENT_SID,d.APARTMENT_NAME from HOME_UD_SHEET a left join HOME_UDITEM_SHEET b on a.SHEET_SID = b.SHEET_SID left join HOME_APARTMENT_DOOR c on b.DOOR_SID = c.DOOR_SID left join HOME_APARTMENT d on c.APARTMENT_SID = d.APARTMENT_SID where d.APARTMENT_NAME not in('幸福家园')and a.remark  is null)t1)t2 where t2.rn = 1

#门禁使用数据，求使用次数及ID，重复ID
select t1.*,ROW_NUMBER() over (partition by t1.APARTMENT_NAME,t1.USER_SID order by t1.APARTMENT_NAME,t1.USER_SID desc)rn
from (
select c.APARTMENT_NAME,b.APARTMENT_SID ,b.OWNER_SID,b.OWNER_NO,b.OWNER_NAME,
 a.DoorLog_SID,a.USER_SID,a.DOOR_SID,a.Content,a.OPENTIME
from HOME_Blue_User_DoorLog a
left join HOME_OWNER b
      on a.USER_SID = b.OWNER_SID
      left join HOME_APARTMENT c
      on b.APARTMENT_SID = c.APARTMENT_SID
where c.APARTMENT_NAME not in('幸福家园')
and b.OWNER_type like('%1%')
--and a.OPENTIME >= '20161010'
--and a.OPENTIME < '20161017'
)t1

#一行：
select t1.*,ROW_NUMBER() over (partition by t1.APARTMENT_NAME,t1.USER_SID order by t1.APARTMENT_NAME,t1.USER_SID desc)rn from (select c.APARTMENT_NAME,b.APARTMENT_SID ,b.OWNER_SID,b.OWNER_NO,b.OWNER_NAME,a.DoorLog_SID,a.USER_SID,a.DOOR_SID,a.Content,a.OPENTIME from HOME_Blue_User_DoorLog a left join HOME_OWNER b on a.USER_SID = b.OWNER_SID left join HOME_APARTMENT c on b.APARTMENT_SID = c.APARTMENT_SID where c.APARTMENT_NAME not in('幸福家园')and b.OWNER_type like('%1%'))t1


#注册ID数
SELECT  a.created_on,a.OWNER_NO,a.OWNER_SID ,b.APARTMENT_NAME
          FROM HOME_OWNER AS a
          left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
         WHERE   b.APARTMENT_NAME not in ('幸福家园','体验小区','金橡臻园')
          and a.OWNER_type like('%1%')--业主
#一行：
SELECT  a.created_on,a.OWNER_NO,a.OWNER_SID ,b.APARTMENT_NAME FROM HOME_OWNER AS a left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID WHERE b.APARTMENT_NAME not in ('幸福家园','体验小区','金橡臻园')and a.OWNER_type like('%1%')

##########################################################################导入数据
import os  # 设置存储路径
os.chdir("D:\\work_all\\python")  # 包安装路径 D:\Program Files\Anaconda2\Scripts
print(os.getcwd())
import pyodbc
conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 10.0};SERVER=111.111.111.111;DATABASE=JOY_HOME;UID=11;PWD=1111')
cur = conn.cursor()
#注册户数
aa = cur.execute("select t1.* from (SELECT  a.CREATED_ON,a.OWNER_NO,a.OWNER_SID ,b.APARTMENT_NAME, ROW_NUMBER() over (partition by b.APARTMENT_NAME,a.OWNER_NO order by a.CREATED_ON,b.APARTMENT_NAME ) as rn FROM HOME_OWNER AS a left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID where a.OWNER_type like('%1%'))t1 where t1.rn =1")
info = cur.fetchall()
cur.close()

import pandas as pd#转换成dataframe格式
info4 = [list(x) for x in info]
data1 = pd.DataFrame(info4,columns=['CREATED_ON', 'OWNER_NO', 'OWNER_SID', 'APARTMENT_NAME', 'rn'])


#先是把东方福邸的全部提出来，单独形成个dataframe，然后标记为东方福邸1和2，然后把这个dataframe拼接到原来的dataframe上去
#东方福邸一期二期分开
#待调整

data1['APARTMENT_NAME'] = data1['APARTMENT_NAME'].astype(unicode)
import re
d = data1[data1.APARTMENT_NAME==u'东方福邸']
def f(x):
    one = u'一期'
    two = u'二期'
    if re.search(one,unicode(x)):
        return u'东方福邸一期'
    elif re.search(two,unicode(x)):
        return u'东方福邸二期'
d['APARTMENT_NAME'] = d['OWNER_NO'].apply(f)
data1 = data1.append(d)

data1.tail()

#新增户数中申请门禁
cur1 = conn.cursor()
aa = cur1.execute("select t1.* from (SELECT a.OWNER_NO,a.CREATED_ON owner_created,d.CREATED_ON door_created,a.OWNER_SID,c.SHEET_SID SQD,b.APARTMENT_NAME,ROW_NUMBER() over (partition by b.APARTMENT_NAME,a.OWNER_NO order by a.CREATED_ON,b.APARTMENT_NAME ) as rn FROM HOME_OWNER AS a left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID left join HOME_UDITEM_SHEET c on a.OWNER_SID = c.USER_SID left join HOME_UD_SHEET d on c.SHEET_SID = d.SHEET_SID where a.OWNER_type like('%1%') and d.remark is null)t1 where t1.rn =1")
info1 = cur1.fetchall()
cur1.close()

#转换成dataframe格式
info5 = [list(x) for x in info1]
data2 = pd.DataFrame(info5,columns=['OWNER_NO', 'owner_created', 'door_created', 'OWNER_SID', 'SHEET_SID','APARTMENT_NAME','rn'])


#先是把东方福邸的全部提出来，单独形成个dataframe，然后标记为东方福邸1和2，然后把这个dataframe拼接到原来的dataframe上去
#东方福邸一期二期分开
#待调整
import re
d = data2[data2.APARTMENT_NAME==u'东方福邸']
def f(x):
    one = u'一期'
    two = u'二期'
    if re.search(one,unicode(x)):
        return u'东方福邸一期'
    elif re.search(two,unicode(x)):
        return u'东方福邸二期'
d['APARTMENT_NAME'] = d['OWNER_NO'].apply(f)
data2 = data2.append(d)

data2.tail()

#申请数据
cur2 = conn.cursor()
aa = cur2.execute("select t2.* from (select t1.*,ROW_NUMBER() over (partition by t1.APARTMENT_NAME,t1.USER_SID order by t1.CREATED_ON,t1.APARTMENT_NAME desc ) as rn from (select e.OWNER_NO,a.SHEET_SID,a.CREATED_ON,b.USER_SID,b.DOOR_SID,c.DOOR_NAME,c.APARTMENT_SID,d.APARTMENT_NAME from HOME_UD_SHEET a left join HOME_OWNER e on a.CREATEDBY=e.OWNER_SID left join HOME_UDITEM_SHEET b on a.SHEET_SID = b.SHEET_SID left join HOME_APARTMENT_DOOR c on b.DOOR_SID = c.DOOR_SID left join HOME_APARTMENT d on c.APARTMENT_SID = d.APARTMENT_SID where d.APARTMENT_NAME not in('幸福家园')and a.remark  is null)t1)t2 where t2.rn = 1")
info2 = cur2.fetchall()
cur2.close()

info6 = [list(x) for x in info2]#转换成dataframe格式
data3 = pd.DataFrame(info6,columns=['OWNER_NO','SHEET_SID', 'CREATED_ON', 'USER_SID', 'DOOR_SID','DOOR_NAME','APARTMENT_SID','APARTMENT_NAME','rn'])

#先是把东方福邸的全部提出来，单独形成个dataframe，然后标记为东方福邸1和2，然后把这个dataframe拼接到原来的dataframe上去
#东方福邸一期二期分开
#待调整
import re
d = data3[data3.APARTMENT_NAME==u'东方福邸']
def f(x):
    one = u'一期'
    two = u'二期'
    if re.search(one,unicode(x)):
        return u'东方福邸一期'
    elif re.search(two,unicode(x)):
        return u'东方福邸二期'
d['APARTMENT_NAME'] = d['OWNER_NO'].apply(f)
data3 = data3.append(d)

data3.tail()

#使用数据
cur3 = conn.cursor()
aa = cur3.execute("select t1.*,ROW_NUMBER() over (partition by t1.APARTMENT_NAME,t1.USER_SID order by t1.APARTMENT_NAME,t1.USER_SID desc)rn from (select c.APARTMENT_NAME,b.APARTMENT_SID ,b.OWNER_SID,b.OWNER_NO,b.OWNER_NAME,a.DoorLog_SID,a.USER_SID,a.DOOR_SID,a.Content,a.OPENTIME from HOME_Blue_User_DoorLog a left join HOME_OWNER b on a.USER_SID = b.OWNER_SID left join HOME_APARTMENT c on b.APARTMENT_SID = c.APARTMENT_SID where c.APARTMENT_NAME not in('幸福家园')and b.OWNER_type like('%1%'))t1")
info3 = cur3.fetchall()
cur3.close()

#转换成dataframe格式
info7 = [list(x) for x in info3]
data4 = pd.DataFrame(info7,columns=['APARTMENT_NAME', 'APARTMENT_SID', 'OWNER_SID', 'OWNER_NO','OWNER_NAME', 'DoorLog_SID','USER_SID','DOOR_SID','Content','OPENTIME','rn'])


#先是把东方福邸的全部提出来，单独形成个dataframe，然后标记为东方福邸1和2，然后把这个dataframe拼接到原来的dataframe上去
#东方福邸一期二期分开
#待调整
import re
d = data4[data4.APARTMENT_NAME==u'东方福邸']
def f(x):
    one = u'一期'
    two = u'二期'
    if re.search(one,unicode(x)):
        return u'东方福邸一期'
    elif re.search(two,unicode(x)):
        return u'东方福邸二期'
d['APARTMENT_NAME'] = d['OWNER_NO'].apply(f)
data4 = data4.append(d)

data4.tail()

###注册ID数，新增
cur4 = conn.cursor()
aa = cur4.execute("SELECT a.created_on,a.OWNER_NO,a.OWNER_SID ,b.APARTMENT_NAME FROM HOME_OWNER AS a left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID WHERE  a.OWNER_type like('%1%')")
info8 = cur4.fetchall()

#转换成dataframe格式
info9 = [list(x) for x in info8]
data5 = pd.DataFrame(info9,columns=['created_on', 'OWNER_NO', 'OWNER_SID', 'APARTMENT_NAME'])

#先是把东方福邸的全部提出来，单独形成个dataframe，然后标记为东方福邸1和2，然后把这个dataframe拼接到原来的dataframe上去
#东方福邸一期二期分开
#待调整
import re
d = data5[data5.APARTMENT_NAME==u'东方福邸']
def f(x):
    one = u'一期'
    two = u'二期'
    if re.search(one,unicode(x)):
        return u'东方福邸一期'
    elif re.search(two,unicode(x)):
        return u'东方福邸二期'
d['APARTMENT_NAME'] = d['OWNER_NO'].apply(f)
data5 = data5.append(d)

data5.tail()

# 使用结束后使用
cur4.close()
conn.close()


####################################################上线第一周、第一月
#data1 = pd.read_excel('.../注册户数.xlsx',encoding='gbk')
#data2 = pd.read_excel('.../新增注册户申请门禁户数.xlsx',encoding='gbk')
#data3 = pd.read_excel('.../门禁申请ID数.xlsx',encoding='gbk')
#data4 = pd.read_excel('.../门禁使用数.xlsx',encoding='gbk')
#i=u'东方郡'
#id_count_week
#size出来的结果是，以groupby后面的APART..变量为key，对owner_sid聚合
import pandas as pd
import time
index_name=[u'东方郡',u'东方福邸',u'江滨花园',u'绿野春天',u'依山郡',u'银爵世纪',u'克拉公寓',u'银座公寓',u'逸天广场',u'天鸿香榭里',u'东方福邸一期',u'东方福邸二期',u'东方郡',u'盛元慧谷']
columns_name=[u'上线时间',u'APP新增户',u'门禁申请户',u'门禁申请ID数',u'门禁使用ID数',u'开门次数']
df_week = pd.DataFrame(0,index=index_name,columns=columns_name)
df_month = pd.DataFrame(0,index=index_name,columns=columns_name)
#单个给上线时间赋值可以df_week.loc['东方郡','上线时间']='20160712',此处不单独添加了。
index_time = ['20160712','20161019','20161021','20161114','20161115','20161117','20161129','20161206','20161211','20170214','20161019','20161019','20170324','20170405']
index_timestamp = {u'东方郡':1468252800,u'东方福邸':1476806400,u'江滨花园':1476979200,u'绿野春天':1479052800,u'依山郡':1479139200,u'银爵世纪':1479312000,u'克拉公寓':1480348800,u'银座公寓':1480953600,u'逸天广场':1481385600,u'天鸿香榭里':1487001600,u'东方福邸一期':1476806400,u'东方福邸二期':1476806400,u'东方郡':1490313600,u'东方郡':1491321600}
for i in range(12):
    df_week.iloc[i,0]=index_time[i]
    df_month.iloc[i, 0] = index_time[i]
week_timestamp = 60*60*24*7
month_timestamp = 60*60*24*30
def to_timestamp(x):
    if str(x)!='nan'and x is not None:
        a = int(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S")))
    else:
        a = 0
    return a
data1['created_timestamp'] = data1['CREATED_ON'].apply(to_timestamp)
data2['owner_timestamp'] = data2['owner_created'].apply(to_timestamp)
data2['door_timestamp'] = data2['door_created'].apply(to_timestamp)
data3['apply_timestamp'] = data3['CREATED_ON'].apply(to_timestamp)
data4['use_timestamp'] = data4['OPENTIME'].apply(to_timestamp)
# data5 = data4.drop_duplicates(['OWNER_SID'])[['OWNER_SID']]#去重
# value['apply_count'] = d_new.drop_duplicates(['OWNER_SID'])['OWNER_SID'].count()
for i in index_name:
    data1_week = data1[(data1.created_timestamp>=index_timestamp[i]) & (data1.created_timestamp<index_timestamp[i]+week_timestamp)]
    new_user_week = data1_week[['OWNER_SID']].groupby(data1_week['APARTMENT_NAME']).size()
    df_week.loc[i, u'APP新增户']=new_user_week[i]
    data1_month = data1[(data1.created_timestamp >= index_timestamp[i]) & (data1.created_timestamp <index_timestamp[i] + month_timestamp)]
    new_user_month = data1_month[['OWNER_SID']].groupby(data1_month['APARTMENT_NAME']).size()
    df_month.loc[i, u'APP新增户'] = new_user_month[i]
for i in index_name:#擎天半岛、东方福邸二期 第一周无，要把week注释
    data2_1 = data2[(data2.owner_timestamp>=index_timestamp[i]) & (data2.owner_timestamp<index_timestamp[i]+week_timestamp) & (data2.door_timestamp!=0)]
    data2_week = data2_1[(data2_1.owner_timestamp>=index_timestamp[i]) & (data2_1.owner_timestamp<index_timestamp[i]+week_timestamp)]
    new_user_week = data2_week[['OWNER_SID']].groupby(data2_week['APARTMENT_NAME']).size()
    df_week.loc[i, u'门禁申请户'] = new_user_week[i]#擎天半岛、东方福邸二期 第一周无，要把week注释
    data2_month = data2[(data2.owner_timestamp >= index_timestamp[i]) & (data2.owner_timestamp < index_timestamp[i] + month_timestamp)]
    new_user_month = data2_month[['OWNER_SID']].groupby(data2_month['APARTMENT_NAME']).size()
    df_month.loc[i, u'门禁申请户'] = new_user_month[i]
for i in index_name:
    data3_week = data3[(data3.apply_timestamp>=index_timestamp[i]) & (data3.apply_timestamp<index_timestamp[i]+week_timestamp)]
    apply_ID_week = data3_week[['USER_SID']].groupby(data3_week['APARTMENT_NAME']).size()
    df_week.loc[i, u'门禁申请ID数'] = apply_ID_week[i]
    data3_month = data3[(data3.apply_timestamp >= index_timestamp[i]) & (data3.apply_timestamp <index_timestamp[i] + month_timestamp)]
    apply_ID_month = data3_month[['USER_SID']].groupby(data3_month['APARTMENT_NAME']).size()
    df_month.loc[i, u'门禁申请ID数'] = apply_ID_month[i]
for i in index_name:
    data4_week = data4[(data4.use_timestamp >= index_timestamp[i]) & (data4.use_timestamp <index_timestamp[i] + week_timestamp)]
    id_count_week = data4_week.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])[['OWNER_SID','APARTMENT_NAME']].groupby(data4_week.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])['APARTMENT_NAME']).size()
    use_count_week = data4_week.groupby(data4_week['APARTMENT_NAME']).size()
    df_week.loc[i, u'门禁使用ID数'] = id_count_week[i]
    df_week.loc[i, u'开门次数'] = use_count_week[i]
    data4_month = data4[(data4.use_timestamp >= index_timestamp[i]) & (data4.use_timestamp <index_timestamp[i] + month_timestamp)]
    id_count_month = data4_month.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])[['OWNER_SID','APARTMENT_NAME']].groupby(data4_month.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])['APARTMENT_NAME']).size()
    use_count_month = data4_month.groupby(data4_month['APARTMENT_NAME']).size()
    df_month.loc[i, u'门禁使用ID数'] = id_count_month[i]
    df_month.loc[i, u'开门次数'] = use_count_month[i]


#d_new.drop_duplicates(['OWNER_SID'])['OWNER_SID'].count()

df_week
df_month

df_week.head()
df_week.tail()
df_week.to_csv('week_door20161219.csv', encoding='gbk')

df_month.head()
df_month.tail()
df_month.to_csv('month_door20161219.csv', encoding='gbk')


####################################################历史数据及截至当前的前一周、前一月数据
#data_1 = pd.read_excel('.../注册户数.xlsx',encoding='gbk')    data1
#data_2 = pd.read_excel('.../注册ID数.xlsx',encoding='gbk')    data5
#data_3 = pd.read_excel('.../门禁申请ID数.xlsx',encoding='gbk')data3
#data_4 = pd.read_excel('.../门禁使用数.xlsx',encoding='gbk')  data4
#i=u'东方郡'
#id_count_week
#size出来的结果是，以groupby后面的APART..变量为key，对owner_sid聚合
import pandas as pd
import time
import datetime
from dateutil.parser import parse #把数据转换成datatime格式

index_name=[u'东方郡',u'东方福邸',u'江滨花园',u'绿野春天',u'依山郡',u'银爵世纪',u'克拉公寓',u'银座公寓',u'逸天广场',u'擎天半岛',u'天鸿香榭里',u'东方福邸一期',u'东方福邸二期',u'盛元慧谷']
columns_name=[u'项目入住户数',u'注册ID数',u'门禁申请ID数',u'门禁使用ID数',u'开门次数']

df = pd.DataFrame(0,index=index_name,columns=columns_name)


t = '2017-04-10 00:00:00'
s_week = (parse(t)+datetime.timedelta(days=-7)).strftime('%Y-%m-%d %H:%M:%S')#前一周
#s_month = (parse(t)+datetime.timedelta(days=-84)).strftime('%Y-%m-%d %H:%M:%S')#2月是小月,前一月
s_month= time.strftime('%Y-%m-%d %H:%M:%S',(time.strptime(t[:7],'%Y-%m')))#当月
s_year = time.strftime('%Y-%m-%d %H:%M:%S',(time.strptime(t[:4],'%Y')))#当年

#历史
data_1 = data1[data1['CREATED_ON']<t]
data_2 = data5[data5['created_on']<t]
data_3 = data3[data3['CREATED_ON']<t]
data_4 = data4[data4['OPENTIME']<t]

df[u'项目入住户数'] = data_1['OWNER_SID'].groupby(data_1['APARTMENT_NAME']).size()
df[u'注册ID数'] = data_2['OWNER_SID'].groupby(data_2['APARTMENT_NAME']).size()#列名需要修改
df[u'门禁申请ID数'] = data_3.drop_duplicates(['USER_SID','APARTMENT_NAME'])['USER_SID'].groupby(data_3.drop_duplicates(['USER_SID','APARTMENT_NAME'])['APARTMENT_NAME']).size()###
df[u'门禁使用ID数'] = data_4.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])['OWNER_SID'].groupby(data_4.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])['APARTMENT_NAME']).size()###
df[u'开门次数'] = data_4['OWNER_SID'].groupby(data_4['APARTMENT_NAME']).size()

df.tail()
df.to_csv('20170325door_history.csv', encoding='gbk')

#近一周
data_1 = data1[(data1['CREATED_ON']<t) & (data1['CREATED_ON']>=s_week)]
data_2 = data5[(data5['created_on']<t) & (data5['created_on']>=s_week)]
data_3 = data3[(data3['CREATED_ON']<t) & (data3['CREATED_ON']>=s_week)]
data_4 = data4[(data4['OPENTIME']<t) & (data4['OPENTIME']>=s_week)]

df[u'项目入住户数'] = data_1['OWNER_SID'].groupby(data_1['APARTMENT_NAME']).size()
df[u'注册ID数'] = data_2['OWNER_SID'].groupby(data_2['APARTMENT_NAME']).size()#列名需要修改
df[u'门禁申请ID数'] = data_3.drop_duplicates(['USER_SID','APARTMENT_NAME'])['USER_SID'].groupby(data_3.drop_duplicates(['USER_SID','APARTMENT_NAME'])['APARTMENT_NAME']).size()###
df[u'门禁使用ID数'] = data_4.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])['OWNER_SID'].groupby(data_4.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])['APARTMENT_NAME']).size()###
df[u'开门次数'] = data_4['OWNER_SID'].groupby(data_4['APARTMENT_NAME']).size()
df.tail()
df.to_csv('20170410door_week.csv', encoding='gbk')

#近一月/当月
data_1 = data1[(data1['CREATED_ON']<t) & (data1['CREATED_ON']>=s_month)]
data_2 = data5[(data5['created_on']<t) & (data5['created_on']>=s_month)]
data_3 = data3[(data3['CREATED_ON']<t) & (data3['CREATED_ON']>=s_month)]
data_4 = data4[(data4['OPENTIME']<t) & (data4['OPENTIME']>=s_month)]

df[u'项目入住户数'] = data_1['OWNER_SID'].groupby(data_1['APARTMENT_NAME']).size()
df[u'注册ID数'] = data_2['OWNER_SID'].groupby(data_2['APARTMENT_NAME']).size()#列名需要修改
df[u'门禁申请ID数'] = data_3.drop_duplicates(['USER_SID','APARTMENT_NAME'])['USER_SID'].groupby(data_3.drop_duplicates(['USER_SID','APARTMENT_NAME'])['APARTMENT_NAME']).size()###
df[u'门禁使用ID数'] = data_4.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])['OWNER_SID'].groupby(data_4.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])['APARTMENT_NAME']).size()###
df[u'开门次数'] = data_4['OWNER_SID'].groupby(data_4['APARTMENT_NAME']).size()
df.tail()
df.to_csv('20170410-month-door.csv', encoding='gbk')


#当年
data_1 = data1[(data1['CREATED_ON']<t) & (data1['CREATED_ON']>=s_year)]
data_2 = data5[(data5['created_on']<t) & (data5['created_on']>=s_year)]
data_3 = data3[(data3['CREATED_ON']<t) & (data3['CREATED_ON']>=s_year)]
data_4 = data4[(data4['OPENTIME']<t) & (data4['OPENTIME']>=s_year)]

df[u'项目入住户数'] = data_1['OWNER_SID'].groupby(data_1['APARTMENT_NAME']).size()
df[u'注册ID数'] = data_2['OWNER_SID'].groupby(data_2['APARTMENT_NAME']).size()#列名需要修改
df[u'门禁申请ID数'] = data_3.drop_duplicates(['USER_SID','APARTMENT_NAME'])['USER_SID'].groupby(data_3.drop_duplicates(['USER_SID','APARTMENT_NAME'])['APARTMENT_NAME']).size()###
df[u'门禁使用ID数'] = data_4.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])['OWNER_SID'].groupby(data_4.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])['APARTMENT_NAME']).size()###
df[u'开门次数'] = data_4['OWNER_SID'].groupby(data_4['APARTMENT_NAME']).size()
df.tail()
df.to_csv('20170325-year.csv', encoding='gbk')
