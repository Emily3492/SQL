##SQL取值：
SELECT  a.OWNER_SID,a.CREATED_ON 注册时间,b.APARTMENT_NAME 项目名称,a.OWNER_PHONE 帐号,a.FAMILY_NAME 昵称,a.OWNER_NO 房号,
(CASE a.OWNER_CATEGORY
                 WHEN '0' THEN
                  '业主'
                 WHEN '1' THEN
                  '租户'
                 WHEN '2' THEN
                  '家属'
                 ELSE
                  ''
               END) AS 租户类型,
(CASE a.OWNER_STATUS
                 WHEN '0' THEN
                  '否'
                 WHEN '1' THEN
                  '是'
                 ELSE
                  ''
               END) AS 是否启用,
(CASE c.VERIFICATION_TAG
                 WHEN '0' THEN
                  '未申请'
                 WHEN '1' THEN
                  '待验证'
                 WHEN '2' THEN
                  '已验证'
                 WHEN '3' THEN
                  '验证未通过'
                 ELSE
                  ''
               END) AS 验证状态,
c.CREATED_ON 验证时间
          FROM HOME_OWNER AS a
          left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
          left join HOME_OWNER_VERIFICATION c on a.OWNER_SID = c.OWNER_SID
         WHERE -- a.CREATED_ON < '20161221'
        -- and a.OWNER_NO not like '%物业服务中心%'--门禁数据不剔除
           --and a.CREATED_ON  >= '20161201'
         -- and  a.CREATED_ON < '2016-10-23'
        -- and b.APARTMENT_NAME not in ('普升福邸','蓝爵国际','体验小区','幸福家园','房屋租售中心')
          b.APARTMENT_NAME not in ('幸福家园','体验小区','荀庄','林语别墅','金橡臻园')
          and a.OWNER_type = 1--业主
order by a.CREATED_ON

#一行：
# SELECT  a.OWNER_SID,a.CREATED_ON ,b.APARTMENT_NAME ,a.OWNER_PHONE ,a.FAMILY_NAME ,a.OWNER_NO FROM HOME_OWNER AS a left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID left join HOME_OWNER_VERIFICATION c on a.OWNER_SID = c.OWNER_SID WHERE  b.APARTMENT_NAME not in ('幸福家园','体验小区','金橡臻园') and a.OWNER_type = 1 order by a.CREATED_ON
####################################################导入数据
import os  # 设置存储路径
print(os.getcwd())
os.chdir("D:\\work_all\\python")  # 包安装路径 D:\Program Files\Anaconda2\Scripts
print(os.getcwd())
# 直接连接数据库
import pyodbc
conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 10.0};SERVER=111.111.111.111;DATABASE=JOY_HOME;UID=11;PWD=1111')
cur = conn.cursor()
aa = cur.execute("SELECT a.OWNER_SID,a.CREATED_ON ,b.APARTMENT_NAME,a.OWNER_PHONE ,a.FAMILY_NAME ,a.OWNER_NO FROM HOME_OWNER AS a left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID left join HOME_OWNER_VERIFICATION c on a.OWNER_SID = c.OWNER_SID WHERE  b.APARTMENT_NAME not in ('幸福家园','体验小区','金橡臻园') and a.OWNER_type like('%1%') order by a.CREATED_ON")
info = cur.fetchall()
cur.close()
conn.close()
import pandas as pd
info1 = [list(x) for x in info]
data = pd.DataFrame(info1,columns=['OWNER_SID','CREATED_ON','APARTMENT_NAME','OWNER_PHONE', 'FAMILY_NAME', 'OWNER_NO'])
data.head()#用户表



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

#################################################python 求安装户数及ID
#当年、当月、前一周、历史
#data = pd.read_excel('/.../安装户数/ID.xlsx')
import datetime
import time
import pandas as pd
def fun_t(x):#两种选择:1(代表前一天),0(代表当前)即else的情况
    if x==1:
        a = datetime.datetime.now()+datetime.timedelta(days=-1)#当前时间减1天，datetime.timedelta(days=-1)为一个函数，表示减一天
        b = a.strftime('%Y%m%d')#将datatime格式的时间转为年月日
        t = time.mktime(time.strptime(b,'%Y%m%d'))+86400
    else:
        b = time.strftime('%Y%m%d',time.localtime(time.time()))
        t = time.time()
    t_month = time.mktime(time.strptime(b[:6],"%Y%m"))
    t_week = t-86400*7
    t_year = time.mktime(time.strptime(b[:4],'%Y'))
    return b,t,t_month,t_week,t_year
b,t,t_month,t_week,t_year = fun_t(1)#如果是以当前时间为准，则括号内输入0即可


index_name=data['APARTMENT_NAME'].drop_duplicates()
dates = pd.date_range(start=b[:6]+'01',end=b)
columns_name = ['month_SID','month_NO','week_SID','week_NO','year_SID','year_NO','history_SID','history_NO']

for i in dates:
    day_SID = str(i)[8:10]+'_SID'
    day_NO = str(i)[8:10]+'_NO'
    columns_name.append(day_SID)
    columns_name.append(day_NO)
df = pd.DataFrame(0,index=index_name,columns=columns_name)

data['time_stamp'] = data['CREATED_ON'].apply(lambda x:time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S")))
data11 = data.sort(columns='CREATED_ON',ascending=True)#排序，默认升序
#data1 = data.sort(columns='CREATED_ON',ascending=False)#排序，降序
data1 = data11.drop_duplicates(['APARTMENT_NAME', 'OWNER_NO'], keep='first')#户数
#data1 = data[['time_stamp','APARTMENT_NAME','OWNER_NO']].groupby(data['OWNER_NO']).min()#取最早注册的房号，东方福邸分一期二期，可能会被踢掉
data_month = data[(data.time_stamp<t) & (data.time_stamp>=t_month)]
data_week = data[(data.time_stamp<t) & (data.time_stamp>=t_week)]
data_year = data[(data.time_stamp<t) & (data.time_stamp>=t_year)]
data_history  = data[(data.time_stamp<t)]
data1_month = data1[(data1.time_stamp<t) & (data1.time_stamp>=t_month)]
data1_week = data1[(data1.time_stamp<t) & (data1.time_stamp>=t_week)]
data1_year = data1[(data1.time_stamp<t) & (data1.time_stamp>=t_year)]
data1_history = data1[(data1.time_stamp<t)]

df['history_SID'] = data_history['OWNER_SID'].groupby(data_history['APARTMENT_NAME']).size()
df['history_NO'] = data1_history['OWNER_NO'].groupby(data1_history['APARTMENT_NAME']).size()
df['month_SID'] = data_month['OWNER_SID'].groupby(data_month['APARTMENT_NAME']).size()
df['month_NO'] = data1_month['OWNER_NO'].groupby(data1_month['APARTMENT_NAME']).size()
df['week_SID'] = data_week['OWNER_SID'].groupby(data_week['APARTMENT_NAME']).size()
df['week_NO'] = data1_week['OWNER_NO'].groupby(data1_week['APARTMENT_NAME']).size()
df['year_SID'] = data_year['OWNER_SID'].groupby(data_year['APARTMENT_NAME']).size()
df['year_NO'] = data1_year['OWNER_NO'].groupby(data1_year['APARTMENT_NAME']).size()

for i in dates:
    t_day = time.mktime(time.strptime(str(i),"%Y-%m-%d %H:%M:%S"))
    data_day = data[(data.time_stamp>=t_day) & (data.time_stamp<t_day+86400)]
    data1_day = data1[(data1.time_stamp>=t_day) & (data1.time_stamp<t_day+86400)]
    day_SID = str(i)[8:10] + '_SID'
    day_NO = str(i)[8:10] + '_NO'
    df[day_SID] = data_day['OWNER_SID'].groupby(data_day['APARTMENT_NAME']).size()
    df[day_NO] = data1_day['OWNER_NO'].groupby(data1_day['APARTMENT_NAME']).size()

df.tail()
df.to_csv('20170409WNER_NO-week.csv', encoding='gbk')

#################################################################自定义结束时间求周、月、年、历史数据
import datetime
import time
import pandas as pd

index_name=data['APARTMENT_NAME'].drop_duplicates()
columns_name = ['month_SID','month_NO','week_SID','week_NO','year_SID','year_NO','history_SID','history_NO']


#历史、当年、当月、前一周、前一天
t = '2017-03-26'
t_now = time.mktime(time.strptime(t,'%Y-%m-%d'))#设置当前时间
t_month = t_now-2419200
#t_month = time.mktime(time.strptime(t[:7],'%Y-%m'))#如果求当月1号开始的数据，则t_month换成这一句
t_week = t_now-86400*7
t_year = time.mktime(time.strptime(t[:4],'%Y'))
t_history=0


data['time_stamp'] = data['CREATED_ON'].apply(lambda x:time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S")))
data11 = data.sort(columns='CREATED_ON',ascending=True)#排序，默认升序
#data1 = data.sort(columns='CREATED_ON',ascending=False)#排序，降序
data1 = data11.drop_duplicates(['APARTMENT_NAME', 'OWNER_NO'], keep='first')#户数
#data1 = data[['time_stamp','APARTMENT_NAME','OWNER_NO']].groupby(data['APARTMENT_NAME']).min()#取最早注册的房号,这样会把东方福邸一期二期踢掉
data_month = data[(data.time_stamp<t_now) & (data.time_stamp>=t_month)]
data_week = data[(data.time_stamp<t_now) & (data.time_stamp>=t_week)]
data_year = data[(data.time_stamp<t_now) & (data.time_stamp>=t_year)]
data_history = data[(data.time_stamp<t_now)]
data1_month = data1[(data1.time_stamp<t_now) & (data1.time_stamp>=t_month)]
data1_week = data1[(data1.time_stamp<t_now) & (data1.time_stamp>=t_week)]
data1_year = data1[(data1.time_stamp<t_now) & (data1.time_stamp>=t_year)]
data1_history = data1[(data1.time_stamp<t_now)]


df = pd.DataFrame(0,index=index_name,columns=columns_name)
df['month_SID'] = data_month['OWNER_SID'].groupby(data_month['APARTMENT_NAME']).size()
df['month_NO'] = data1_month['OWNER_NO'].groupby(data1_month['APARTMENT_NAME']).size()
df['week_SID'] = data_week['OWNER_SID'].groupby(data_week['APARTMENT_NAME']).size()
df['week_NO'] = data1_week['OWNER_NO'].groupby(data1_week['APARTMENT_NAME']).size()
df['year_SID'] = data_year['OWNER_SID'].groupby(data_year['APARTMENT_NAME']).size()
df['year_NO'] = data1_year['OWNER_NO'].groupby(data1_year['APARTMENT_NAME']).size()
df['history_SID'] = data_history['OWNER_SID'].groupby(data_history['APARTMENT_NAME']).size()
df['history_NO'] = data1_history['OWNER_NO'].groupby(data1_history['APARTMENT_NAME']).size()

df.tail
df.to_csv('20170325OWNER_.csv', encoding='gbk')
#######################################################################################月累计户数/ID数,一次跑出所有月份
#data = pd.read_excel('安装户数/注册ID数.xlsx')
import time
dates = pd.date_range('20170201', '20170501', freq='M')
apartment_name = data['APARTMENT_NAME'].drop_duplicates()
df = pd.DataFrame(0, index=apartment_name, columns=dates)

data1 = data.sort(columns='CREATED_ON',ascending=True)#排序，默认升序
#data1 = data.sort(columns='CREATED_ON',ascending=False)#排序，降序
data2 = data1.drop_duplicates(['APARTMENT_NAME', 'OWNER_NO'], keep='first')#户数
#data2=data1#ID数

for i in dates:
    data3 = data2[data2['CREATED_ON'] <str(i)[:11] + '23:59:59']
    df[i] = data3['OWNER_NO'].groupby(data3['APARTMENT_NAME']).size()

    df.tail()
    df.to_csv('20170328WONER_NOmonth.csv', encoding='gbk')

    #pd.DataFrame.sort?


#######################################################################################月新增/累计，ID/户数
#python
import pandas as pd
import time
import calendar
#data = pd.read_excel('.../owner.xlsx',encoding='gbk')
#data = pd.read_excel('.../户数和ID数/owner1.xlsx')
data = data.sort(columns=['CREATED_ON'])
data1 = data.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])#ID数
data3 = data.drop_duplicates(['OWNER_NO','APARTMENT_NAME'])#户数

apartment_name = data.drop_duplicates(['APARTMENT_NAME'])['APARTMENT_NAME']
month_time = pd.period_range('201612',periods=5,freq='M')#http://blog.csdn.net/pipisorry/article/details/52209377
df1 = pd.DataFrame(0,index=apartment_name,columns=month_time)#月新增ID数
df2 = pd.DataFrame(0,index=apartment_name,columns=month_time)#月累计ID数
df3 = pd.DataFrame(0,index=apartment_name,columns=month_time)#月新增户数
df4 = pd.DataFrame(0,index=apartment_name,columns=month_time)#月累计户数

data['created_timestamp'] = data['CREATED_ON'].apply(lambda x:int(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))
time_list = ['2016-12-31','2017-01-31','2017-02-28','2017-03-31']
#dates = pd.date_range('20170201','20170401',freq='M')
t_start = int(time.mktime(time.strptime('2016-11-30 23:59:59',"%Y-%m-%d %H:%M:%S")))
for i in range(len(time_list)):
    t_end = int(time.mktime(time.strptime(time_list[i]+' 23:59:59',"%Y-%m-%d %H:%M:%S")))
    dat1 = data1[(data1.created_timestamp >= t_start) & (data1.created_timestamp < t_end)]#新增
    da1 = data3[(data3.created_timestamp >= t_start) & (data3.created_timestamp < t_end)]#新增
    dat2 = dat1[['OWNER_SID']].groupby(dat1['APARTMENT_NAME']).size()
    df1.ix[:, i] = dat2 #','前面是行后面是列，1:2表示第1行至第2行，省略表示行首到行尾。
    da2 = da1[['OWNER_SID']].groupby(da1['APARTMENT_NAME']).size()
    df3.ix[:, i] = da2 #','前面是行后面是列，1:2表示第1行至第2行，省略表示行首到行尾。
    t_start = t_end
    dat3 = data1[data1.created_timestamp<t_end]
    dat4 = dat3[['OWNER_SID']].groupby(dat3['APARTMENT_NAME']).size()
    df2.ix[:, i] = dat4
    da3 = data3[data3.created_timestamp<t_end]
    da4 = da3[['OWNER_SID']].groupby(da3['APARTMENT_NAME']).size()
    df4.ix[:, i] = da4

    df1.head()#月新增用户ID
    df2.head()#月累计用户ID
    df3.head()#月新增户数
    df4.head()#月累计户数

    df1.to_csv('201611monthsum_ownerID_apart.csv', encoding='gbk')
    df2.to_csv('2017sum_ownerID_apart.csv', encoding='gbk')
    df3.to_csv('201611monthsum_ownerNO_apart.csv', encoding='gbk')
    df4.to_csv('20170sum_ownerNO_apart.csv', encoding='gbk')

#####################################################################################待调整
import pandas as pd
import time
import calendar

#data = pd.read_excel('.../户数和ID数/owner1.xlsx')
data = data.sort(columns=['CREATED_ON'])
data1 = data.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])#ID数
data3 = data.drop_duplicates(['OWNER_NO','APARTMENT_NAME'])#户数
dates = pd.date_range('20170201','20170401',freq='M')

index_name = data['APARTMENT_NAME'].drop_duplicates()
df = pd.DataFrame(0,index = index_name,columns=[])
for i in dates:
    t_end = str(i)[:10]+' 23:59:59'
    t_start = str(i)[:8]+'01 00:00:00'
    data2 = data1[(data1.CREATED_ON<t_end) & (data1.CREATED_ON>=t_start)]#ID数
    data4 = data3[data3.CREATED_ON<t_end) & (data3.CREATED_ON>=t_start)]#户数
    df['new_ID'] = data2['OWNER_SID'].groupby(data2['APARTMENT_NAME']).size()
    df['all_ID'] = data1['OWNER_SID'].groupby(data1['APARTMENT_NAME']).size()
    df['new_user'] = data4['OWNER_SID'].groupby(data4['APARTMENT_NAME']).size()
    df['all_user'] = data3['OWNER_SID'].groupby(data3['APARTMENT_NAME']).size()


    df.tail
