import os
os.chdir("D:\\work\\python")
print(os.getcwd())
import pandas as pd
import time
import pyodbc
conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 10.0};SERVER=111.111.111.111;DATABASE=JOY_HOME;UID=11;PWD=1111')
cur = conn.cursor()
aa = cur.execute("select a.CREATED_ON id_created,b.CREATED_ON log_created,a.OWNER_SID,a.APARTMENT_SID,a.OWNER_NAME, a.OWNER_TYPE ,b.CONTENT from HOME_OWNER a left join Home_OwnerLog b on a.OWNER_SID = b.OWNER_SID where a.OWNER_TYPE=1 and a.FAMILY_NAME not in ('悦悦') and a.OWNER_NO not like '%物业%'  and b.SYSTEM_TYPE=0")
info = cur.fetchall()
cur.close()
conn.close()

#直接转换成dataframe（用这个转换成dataframe）
info2 = [list(x) for x in info]
df = pd.DataFrame(info2,columns=['id_created', 'log_created', 'OWNER_SID', 'APARTMENT_SID', 'OWNER_NAME', 'OWNER_TYPE','content'])
df['content1'] = df['content'].apply(lambda x: x.decode('gbk'))

def class_content(x):
    neighbor = u'邻居圈|浏览帖子'
    domestic = u'家政'
    house = u'房产|房屋'
    notice = u'公告'
    purchase = u'团购|悦购'
    estate = u'物业服务'
    passing = u'访客通行|通行证'
    opendoor = u'一键开门'
    intoscreen = u'进入主界面'
    express = u'快递'
    if re.search(notice,unicode(x)):
        return '公告'
    elif re.search(neighbor,unicode(x)):
        return '邻居圈'
    elif re.search(domestic,unicode(x)):
        return '家政服务'
    elif re.search(house,unicode(x)):
        return '房屋租售'
    elif re.search(purchase,unicode(x)):
        return '悦购'
    elif re.search(estate,unicode(x)):
        return '物业服务'
    elif re.search(passing,unicode(x)):
        return '访客通行'
    elif re.search(opendoor,unicode(x)):
        return '一键开门'
    elif re.search(intoscreen,unicode(x)):
        return '进入主界面'
    elif re.search(express,unicode(x)):
        return '快递'
    else:
        return '其他'
#data = pd.read_excel('.../log.xlsx')
data=df
data['log_timestamp'] = data['log_created'].apply(lambda x:(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))
data['content'] = data['CONTENT'].apply(class_content)
data1 = data[['OWNER_SID','log_timestamp','content']]
day_count ={'2016-07':31, '2016-08':31, '2016-09':30, '2016-10':31, '2016-11':30}
dates = pd.period_range('201607',freq='M',periods=5)
columns_name=['邻居圈','家政服务','房屋租售','公告','悦购','物业服务','访客通行','一键开门','进入主界面']
df = pd.DataFrame(0,index=dates,columns=columns_name)
t_start = int(time.mktime(time.strptime('2016-07-26 00:00:00',"%Y-%m-%d %H:%M:%S")))
for i in day_count:
    t = 60*60*24*day_count[i]
    data2 = data1[(data1.log_timestamp>=t_start) & (data1.log_timestamp<t_start+t)]
    data3 = data2[['OWNER_SID']].groupby(data2['content']).size()
    df.ix[i,:] = data3
    t_start+=t
