#SQL取值
SELECT  a.created_on,a.OWNER_NO,a.OWNER_SID ,b.APARTMENT_NAME
          FROM HOME_OWNER AS a
          left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID
         WHERE a.CREATED_ON >= '20151101'
          and a.OWNER_type = 1--业主

#一行
SELECT a.created_on,a.OWNER_NO,a.OWNER_SID ,b.APARTMENT_NAME FROM HOME_OWNER AS a left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID WHERE a.CREATED_ON >= '20151101' and a.OWNER_type = 1
#连接数据库
#python连接数据库
import os  # 设置存储路径
os.chdir("D:\\work_all\\python")  # 包安装路径 D:\Program Files\Anaconda2\Scripts
print(os.getcwd())
# 直接连接数据库
import pyodbc
conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 10.0};SERVER=111.111.111.111;DATABASE=JOY_HOME;UID=11;PWD=1111')
cur = conn.cursor()
aa = cur.execute("SELECT a.created_on,a.OWNER_NO,a.OWNER_SID ,b.APARTMENT_NAME FROM HOME_OWNER AS a left join HOME_APARTMENT b on a.APARTMENT_SID = b.APARTMENT_SID WHERE  a.OWNER_type = 1")
info = cur.fetchall()

cur.close()
conn.close()
#数据赋值后关闭SQL连接，最后连接的先关闭
#cur.close()
#conn.close()

#转换成dataframe格式（直接转换活跃度写法）
import pandas as pd
info1 = [list(x) for x in info]
df = pd.DataFrame(info1,columns=['created_on', 'OWNER_NO', 'OWNER_SID', 'APARTMENT_NAME'])
df.head()
data=df

#python
import time
#data = pd.read_excel('.../owner.xlsx',encoding='gbk')
apartment_name = data.drop_duplicates(['APARTMENT_NAME'])['APARTMENT_NAME']
month_time = pd.period_range('201512',periods=15,freq='M')#http://blog.csdn.net/pipisorry/article/details/52209377
df1 = pd.DataFrame(0,index=apartment_name,columns=month_time)#新增用户数
df2 = pd.DataFrame(0,index=apartment_name,columns=month_time)#累计用户数
data['created_timestamp'] = data['created_on'].apply(lambda x:int(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))
time_list = ['2015-12-31','2016-01-31','2016-02-29','2016-03-31','2016-04-30','2016-05-31','2016-06-30','2016-07-31','2016-08-31','2016-09-30','2016-10-31','2016-11-30','2016-12-31','2017-01-31','2017-02-28']
t_start = int(time.mktime(time.strptime('2015-11-30 23:59:59',"%Y-%m-%d %H:%M:%S")))
for i in range(len(time_list)):
    t_end = int(time.mktime(time.strptime(time_list[i]+' 23:59:59',"%Y-%m-%d %H:%M:%S")))
    data1 = data[(data.created_timestamp >= t_start) & (data.created_timestamp < t_end)]
    data2 = data1[['OWNER_SID']].groupby(data1['APARTMENT_NAME']).size()
    df1.ix[:, i] = data2 #','前面是行后面是列，1:2表示第1行至第2行，省略表示行首到行尾。
    t_start = t_end
    data3 = data[data.created_timestamp<t_end]
    data4 = data3[['OWNER_SID']].groupby(data3['APARTMENT_NAME']).size()
    df2.ix[:, i] = data4

    df1.head()#月新增用户ID
    df2.head()#月累计用户ID

    df2.to_csv('201611sum_ownerID_apart.csv', encoding='gbk')
    df1.to_csv('201611monthsum_ownerID_apart.csv', encoding='gbk')
