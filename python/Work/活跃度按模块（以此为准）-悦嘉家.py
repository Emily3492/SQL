#python连接数据库取值
#用户表关联10月份日志，导入数据
import os
os.chdir("D:\\work_all\\python")
print(os.getcwd())
import pandas as pd
import time
import pyodbc
conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 10.0};SERVER=111.111.111.111;DATABASE=JOY_HOME;UID=11;PWD=1111')
cur = conn.cursor()
aa = cur.execute("select a.OWNER_NO,a.CREATED_ON id_created,b.CREATED_ON log_created,a.OWNER_SID,a.APARTMENT_SID,a.OWNER_NAME, a.OWNER_TYPE ,b.CONTENT,c.APARTMENT_NAME from HOME_OWNER a left join Home_OwnerLog b on a.OWNER_SID = b.OWNER_SID left join HOME_APARTMENT c on a.APARTMENT_SID=c.APARTMENT_SID where a.OWNER_TYPE=1 and a.FAMILY_NAME not in ('悦悦') and a.OWNER_NO not like '%物业%'  and b.SYSTEM_TYPE=0")
info = cur.fetchall()
cur.close()

info2 = [list(x) for x in info]
log3 = pd.DataFrame(info2,columns=['OWNER_NO','id_created', 'log_created', 'OWNER_SID', 'APARTMENT_SID', 'OWNER_NAME', 'OWNER_TYPE','content','APARTMENT_NAME'])
#log3['content1'] = log3['content'].apply(lambda x: x.decode('gbk'))#将content转换成gbk格式显示并加在log的dataframe中去
log3.head()
#log = log3.drop(['content'], axis=1)#删除乱码显示的内容后存在dataframe的log1中
log=log3

import re
d = log[log.APARTMENT_NAME==u'东方福邸']
def f(x):
    one = u'一期'
    two = u'二期'
    if re.search(one,unicode(x)):
        return u'东方福邸一期'
    elif re.search(two,unicode(x)):
        return u'东方福邸二期'
d['APARTMENT_NAME'] = d['OWNER_NO'].apply(f)
log = pd.concat([log,d])
log.tail()

#以上为日志数据，剔除工作人员，接下来把取值SQL替换成用户的SQL数据取值取出用户数据owner
#select b.CREATED_ON,b.OWNER_SID,b.OWNER_TYPE from HOME_OWNER b where b.OWNER_TYPE  = 1

cur = conn.cursor()
aa = cur.execute("select b.CREATED_ON,b.OWNER_SID,b.OWNER_TYPE,b.OWNER_NAME,b.OWNER_NO,b.APARTMENT_SID,a.APARTMENT_NAME from HOME_OWNER b left join HOME_APARTMENT a on a.APARTMENT_SID=b.APARTMENT_SID where b.OWNER_TYPE like('%1%')")
info1 = cur.fetchall()
cur.close()
conn.close()
info3 = [list(x) for x in info1]
owner = pd.DataFrame(info3,columns=['CREATED_ON','OWNER_SID','OWNER_TYPE','OWNER_NAME','OWNER_NO','APARTMENT_SID','APARTMENT_NAME'])
owner.head()

#先是把东方福邸的全部提出来，单独形成个dataframe，然后标记为东方福邸1和2，然后把这个dataframe拼接到原来的dataframe上去
d = owner[owner.APARTMENT_NAME==u'东方福邸']
def f(x):
    one = u'一期'
    two = u'二期'
    if re.search(one,unicode(x)):
        return u'东方福邸一期'
    elif re.search(two,unicode(x)):
        return u'东方福邸二期'
d['APARTMENT_NAME'] = d['OWNER_NO'].apply(f)
owner = pd.concat([owner,d])
owner.tail()
#分类
import re
#对房屋租售下的拨打看房热线进行分类
def class_content2(x):
    callhouse = u'拨打看房'
    if re.search(callhouse,unicode(x)):
        return 'callhouse'
    else:
        return 'other'
d = log
d['content2'] = d['content'].apply(class_content2)
d1 = d[d.content2=='callhouse']

def class_content(x):
    neighbor = u'邻居圈|浏览帖子'
    domestic = u'家政'
    house = u'房产|房屋'
    notice = u'公告'
    purchase = u'团购|悦购'
    estate = u'物业服务'
    passing = u'访客证'
    opendoor = u'一键开门'
    intoscreen = u'进入主界面'
    express = u'快递'
    water = u'送水'
    education = u'欢乐学'
    if re.search(notice,unicode(x)):
        return 'notice'
    elif re.search(neighbor,unicode(x)):
        return 'neighbor'
    elif re.search(domestic,unicode(x)):
        return 'domestic'
    elif re.search(house,unicode(x)):
        return 'house'
    elif re.search(purchase,unicode(x)):
        return 'purchase'
    elif re.search(estate,unicode(x)):
        return 'estate'
    elif re.search(passing,unicode(x)):
        return 'passing'
    elif re.search(opendoor,unicode(x)):
        return 'opendoor'
    elif re.search(intoscreen,unicode(x)):
        return 'intoscreen'
    elif re.search(express,unicode(x)):
        return 'express'
    elif re.search(water,unicode(x)):
        return 'water'
    elif re.search(education,unicode(x)):
        return 'education'
    else:
        return 'other'

log['content2'] = log['content'].apply(class_content)
log = pd.concat([log,d1])


log['login_timestamp'] = log['log_created'].apply(lambda x:(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))
owner['apply_timestamp']=owner['CREATED_ON'].apply(lambda x:(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))

content2_list = ['neighbor','domestic','house','notice','purchase','estate','passing','opendoor','intoscreen','express', 'water','education','callhouse']
columns_name=['all_apply_id','all_login_id','all_login_count','all_liveness']
for i in content2_list:
    a = i+'_'+'login_id'
    b = i+'_'+'login_count'
    c = i+'_'+'liveness'
    columns_name.append(a)
    columns_name.append(b)
    columns_name.append(c)


#day
index_name = pd.date_range(start='20161226',end='20170406')
df = pd.DataFrame(0,index=index_name,columns=columns_name)
t = int(time.mktime(time.strptime('2016-12-26 00:00:00',"%Y-%m-%d %H:%M:%S")))
t1 = 60*60*24
for i in index_name:
    #log['content2'] = log['content1'].apply(class_content1)
    owner1 = owner[owner.apply_timestamp < t + t1]
    log2 = log[(log.login_timestamp>=t) & (log.login_timestamp<t+t1)]
    df.loc[i, 'all_apply_id'] = owner1.drop_duplicates(['OWNER_SID']).count()['OWNER_SID']
    df.loc[i, 'all_login_id'] = log2.drop_duplicates(['OWNER_SID']).count()['OWNER_SID']
    df.loc[i, 'all_login_count'] = log2.count()['OWNER_SID']
    df.loc[i, 'all_liveness'] = round(df.loc[i, 'all_login_id']*1.0/df.loc[i, 'all_apply_id'],4)
    for j in content2_list:
        log3 = log2[log2.content2==j]
        a = j + '_' + 'login_id'
        b = j + '_' + 'login_count'
        c = j + '_' + 'liveness'
        df.loc[i, a] = log3.drop_duplicates(['OWNER_SID']).count()['OWNER_SID']
        df.loc[i, b] = log3.count()['OWNER_SID']
        df.loc[i, c] = round(df.loc[i, a] * 1.0 / df.loc[i, 'all_apply_id'], 4)
    t += t1
df.head()
df.tail()
df.to_csv('day_liveness171226-0405new.csv', encoding='utf-8')

#week
#只需要改
index_name = pd.date_range('20170107',periods=12,freq='7D')#t+t1天的前一天的日期
df = pd.DataFrame(0,index=index_name,columns=columns_name)
t = int(time.mktime(time.strptime('2017-01-01 00:00:00',"%Y-%m-%d %H:%M:%S")))
t1 = 60*60*24*7


df.head()
df.tail()
df.to_csv('week_liveness0331.csv', encoding='utf-8')


#month-16
index_name = ['201602','201603','201604','201605','201606','201607','201608','201609','201610','201611','201612']
df = pd.DataFrame(0,index=index_name,columns=columns_name)
t = int(time.mktime(time.strptime('2016-02-26 00:00:00',"%Y-%m-%d %H:%M:%S")))
t1 = 60*60*24*30
#int(time.mktime(time.strptime('2016-09-26 00:00:00',"%Y-%m-%d %H:%M:%S")))
for i in index_name:
    t_start = int(time.mktime(time.strptime(str(int(i)-1)+'26',"%Y%m%d")))
    t_end = int(time.mktime(time.strptime(i+'26',"%Y%m%d")))
    owner1 = owner[owner.apply_timestamp < t_end]
    log1 = log[(log.login_timestamp>=t_start) & (log.login_timestamp<t_end)]
    df.loc[i, 'all_apply_id'] = owner1.drop_duplicates(['OWNER_SID']).count()['OWNER_SID']
    df.loc[i, 'all_login_id'] = log1.drop_duplicates(['OWNER_SID']).count()['OWNER_SID']
    df.loc[i, 'all_login_count'] = log1.count()['OWNER_SID']
    df.loc[i, 'all_liveness'] = round(df.loc[i, 'all_login_id'] * 1.0 / df.loc[i, 'all_apply_id'], 4)
    for j in content2_list:
        log2 = log1[log1.content2 == j]
        a = j + '_' + 'login_id'
        b = j + '_' + 'login_count'
        c = j + '_' + 'liveness'
        df.loc[i, a] = log2.drop_duplicates(['OWNER_SID']).count()['OWNER_SID']
        df.loc[i, b] = log2.count()['OWNER_SID']
        df.loc[i, c] = round(df.loc[i, a] * 1.0 / df.loc[i, 'all_apply_id'], 4)
    t += t1

df.head()
df.tail()
df.to_csv('month_liveness.csv', encoding='utf-8')


#month -17年
index_name = ['201701','201702','201703']
df = pd.DataFrame(0,index=index_name,columns=columns_name)
t = int(time.mktime(time.strptime('2017-01-26 00:00:00',"%Y-%m-%d %H:%M:%S")))
t1 = 60*60*24*30
#int(time.mktime(time.strptime('2016-09-26 00:00:00',"%Y-%m-%d %H:%M:%S")))
for i in index_name[1:]:
    t_start = int(time.mktime(time.strptime(index_name[index_name.index(i)-1]+'26',"%Y%m%d")))
    t_end = int(time.mktime(time.strptime(i+'26',"%Y%m%d")))
    owner1 = owner[owner.apply_timestamp < t_end]
    log1 = log[(log.login_timestamp>=t_start) & (log.login_timestamp<t_end)]
    df.loc[i, 'all_apply_id'] = owner1.drop_duplicates(['OWNER_SID']).count()['OWNER_SID']
    df.loc[i, 'all_login_id'] = log1.drop_duplicates(['OWNER_SID']).count()['OWNER_SID']
    df.loc[i, 'all_login_count'] = log1.count()['OWNER_SID']
    df.loc[i, 'all_liveness'] = round(df.loc[i, 'all_login_id'] * 1.0 / df.loc[i, 'all_apply_id'], 4)
    for j in content2_list:
        log2 = log1[log1.content2 == j]
        a = j + '_' + 'login_id'
        b = j + '_' + 'login_count'
        c = j + '_' + 'liveness'
        df.loc[i, a] = log2.drop_duplicates(['OWNER_SID']).count()['OWNER_SID']
        df.loc[i, b] = log2.count()['OWNER_SID']
        df.loc[i, c] = round(df.loc[i, a] * 1.0 / df.loc[i, 'all_apply_id'], 4)
    t += t1

df.to_csv('month_liveness20170305.csv', encoding='utf-8')


########################################
# month
# 按小区，模块分布#比率待调整

index_name=owner['APARTMENT_NAME'].drop_duplicates()
df = pd.DataFrame(0,index=index_name,columns=columns_name)

t_start = int(time.mktime(time.strptime('20170315',"%Y%m%d")))
t_end = int(time.mktime(time.strptime('20170410',"%Y%m%d")))
owner1 = owner[owner.apply_timestamp < t_end]
log1 = log[(log.login_timestamp>=t_start) & (log.login_timestamp<t_end)]
owner2 = owner1.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])[['OWNER_SID','APARTMENT_NAME']]
log2 = log1.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])[['OWNER_SID','APARTMENT_NAME']]

df['all_apply_id'] = owner2['OWNER_SID'].groupby(owner2['APARTMENT_NAME']).size()
df['all_login_id'] = log2['OWNER_SID'].groupby(log2['APARTMENT_NAME']).size()
df['all_login_count'] = log1['OWNER_SID'].groupby(log1['APARTMENT_NAME']).size()
#df['all_liveness'] = round(df['all_login_id'] * 1.0 / df['all_apply_id'], 4)
for j in content2_list:
    log3 = log1[log1.content2 == j]
    log4 = log3.drop_duplicates(['OWNER_SID'])
    a = j + '_' + 'login_id'
    b = j + '_' + 'login_count'
    #c = j + '_' + 'liveness'
    df[a] = log4['OWNER_SID'].groupby(log4['APARTMENT_NAME']).size()
    df[b] = log3['OWNER_SID'].groupby(log3['APARTMENT_NAME']).size()
    #df[c] = round(df[a] * 1.0 / df['all_apply_id'], 4)

    df.tail()
    df.to_csv('dapartment_liveness2017315-409_week.csv', encoding='gbk')




#########################################################前一日、周、月的活跃度数据，按小区分组
#前一日、周、月的活跃度数据，按小区分组
#当前时间可以设定
import pandas as pd
import time
import re

#固定时间
t = time.mktime(time.strptime('2017-04-05','%Y-%m-%d'))
t_list = {}
t_list['day'] = t-86400
t_list['week'] = t-604800
t_list['month'] = t-2592000

#改用查找，查找到相应字符则mark。根据mark结果进行汇总。content_mark_check
def class_content(x):
    a = []
    neighbor = u'邻居圈|浏览帖子'
    domestic = u'家政'
    house = u'房产|房屋'
    notice = u'公告'
    purchase = u'团购|悦购'
    estate = u'物业服务'
    passing = u'访客证'
    opendoor = u'一键开门'
    intoscreen = u'进入主界面'
    express = u'快递'
    water = u'送水'
    education = u'欢乐学'
    callhouse = u'拨打看房'
    if re.search(notice,unicode(x)):
        a.append('notice')
    if re.search(neighbor,unicode(x)):
         a.append('neighbor')
    if re.search(domestic,unicode(x)):
        a.append('domestic')
    if re.search(house,unicode(x)):
        a.append('house')
    if re.search(purchase,unicode(x)):
        a.append('purchase')
    if re.search(estate,unicode(x)):
        a.append('estate')
    if re.search(passing,unicode(x)):
        a.append('passing')
    if re.search(opendoor,unicode(x)):
        a.append('opendoor')
    if re.search(intoscreen,unicode(x)):
        a.append('intoscreen')
    if re.search(express,unicode(x)):
        a.append('express')
    if re.search(water,unicode(x)):
        a.append('water')
    if re.search(education,unicode(x)):
        a.append('education')
    if re.search(callhouse,unicode(x)):
        a.append('callhouse')
    return str(a)

#log = pd.read_excel('.../活跃度/log1.xlsx')
#owner = pd.read_excel('.../活跃度/owner1.xlsx')
log['login_time_unix'] = log['log_created'].apply(lambda x:time.mktime(time.strptime(str(x)[:19],'%Y-%m-%d %H:%M:%S')))
log['content_mark'] = log['content1'].apply(class_content)
owner['register_time_unix'] = owner['CREATED_ON'].apply(lambda x:time.mktime(time.strptime(str(x)[:19],'%Y-%m-%d %H:%M:%S')))
owner1 = owner.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])
owner2 = owner1[owner1.register_time_unix<t]

index_name = owner['APARTMENT_NAME'].drop_duplicates()
module_list = ['neighbor','domestic','house','notice','purchase','estate','passing','opendoor','intoscreen','express', 'water','education','callhouse']

def fun(x,j):
    if j in eval(x):#eval('[1,2,3]')=[1,2,3],eval('(12,3,4)')=(12,3,4),eval("{'a':12,'b':3,'c':4}")={'a': 12, 'b': 3, 'c': 4}
        return True
    else:
        return False

for i in ['day','week','month']:
    df = pd.DataFrame(0,index=index_name,columns=['all_login','all_register','all_liveness'])
    log1 = log[(log.login_time_unix<t) & (log.login_time_unix>t_list[i])]
    log2 = log1.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])
    df['all_login'] = log2['OWNER_SID'].groupby(log2['APARTMENT_NAME']).size()
    df['all_register'] = owner2['OWNER_SID'].groupby(owner2['APARTMENT_NAME']).size()
    df['all_liveness'] = df['all_login']*1.0/df['all_register']
    df['all_count'] = log1['OWNER_SID'].groupby(log1['APARTMENT_NAME']).size()
    for j in module_list:
        log2['content_mark_check'] = log2.apply(lambda x:fun(x.content_mark,j),1)
        log3 = log2[log2.content_mark_check==True]
        log1['content_mark_check'] = log1.apply(lambda x:fun(x.content_mark,j),1)
        log33 = log1[log1.content_mark_check==True]
        df[j+'_count'] = log33['OWNER_SID'].groupby(log33['APARTMENT_NAME']).size()
        df[j+'_login'] = log3['OWNER_SID'].groupby(log3['APARTMENT_NAME']).size()
        df[j+'_liveness'] = df[j+'_login']*1.0/df['all_register']
    name = 'liveness_now_'+i+'.xlsx'
    df.to_excel(name)

####################################################################一次性跑出所有日、周、月数据
#自定时间：start,end
#自定模块:neighbor,all...

#log = pd.read_excel('.../活跃度/log1.xlsx')
#owner = pd.read_excel('...活跃度/owner1.xlsx')
def class_content(x):
    a = []
    neighbor = u'邻居圈|浏览帖子'
    domestic = u'家政'
    house = u'房产|房屋'
    notice = u'公告'
    purchase = u'团购|悦购'
    estate = u'物业服务'
    passing = u'访客证'
    opendoor = u'一键开门'
    intoscreen = u'进入主界面'
    express = u'快递'
    water = u'送水'
    education = u'欢乐学'
    callhouse = u'拨打看房'
    if re.search(notice,unicode(x)):
        a.append('notice')
    if re.search(neighbor,unicode(x)):
         a.append('neighbor')
    if re.search(domestic,unicode(x)):
        a.append('domestic')
    if re.search(house,unicode(x)):
        a.append('house')
    if re.search(purchase,unicode(x)):
        a.append('purchase')
    if re.search(estate,unicode(x)):
        a.append('estate')
    if re.search(passing,unicode(x)):
        a.append('passing')
    if re.search(opendoor,unicode(x)):
        a.append('opendoor')
    if re.search(intoscreen,unicode(x)):
        a.append('intoscreen')
    if re.search(express,unicode(x)):
        a.append('express')
    if re.search(water,unicode(x)):
        a.append('water')
    if re.search(education,unicode(x)):
        a.append('education')
    if re.search(callhouse,unicode(x)):
        a.append('callhouse')
    return str(a)
def fun(x,j):
    if j=='all':
        return True
    else:
        if j in eval(x):#eval('[1,2,3]')=[1,2,3],eval('(12,3,4)')=(12,3,4),eval("{'a':12,'b':3,'c':4}")={'a': 12, 'b': 3, 'c': 4}
            return True
        else:
            return False


module = 'all'#所有模块,module = 'all'
t_start = '2016-12-01 00:00:00'
t_end = '2017-03-20 00:00:00'

import time
import re

t_end1 = (datetime.datetime.strptime(t_end, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(days = 1)).strftime('%Y-%m-%d %H:%M:%S')
t_list = {'day':[t_end1],'week':[t_end],'month':[t_start,t_end]}
t_list['day'].extend([str(x) for x in pd.date_range(t_start,t_end,freq='D')])
t_list['week'].extend([str(x) for x in pd.date_range(t_start,t_end,freq='7D')])
t_list['month'].extend([str(x) for x in pd.date_range(t_start,t_end,freq='M')])

log['content_mark'] = log['content1'].apply(class_content)
log['content_mark_check'] = log.apply(lambda x:fun(x.content_mark,module),1)
owner1 = owner.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])

index_name = owner['APARTMENT_NAME'].drop_duplicates()
for i in ['day','week','month']:#月份的12月31日代表的是1月
    t_list[i] = list(set(t_list[i]))
    t_list[i].sort()
    t0 = t_list[i][0]
    df = pd.DataFrame(0,index=index_name,columns=[])
    for t1 in t_list[i][1:]:
        log1 = log[(log.log_created<t1) & (log.log_created>=t0)]
        log2 = log1.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])
        log3 = log2[log2.content_mark_check==True]
        owner2 = owner1[owner1.CREATED_ON<t1]
        df[t0+'_login'+'_'+module] = log3['OWNER_SID'].groupby(log3['APARTMENT_NAME']).size()
        df[t0+'_register'+'_'+module] = owner2['OWNER_SID'].groupby(owner2['APARTMENT_NAME']).size()
        df[t0+'_liveness'+'_'+module] = df[t0+'_login'+'_'+module]*1.0/df[t0+'_register'+'_'+module]
        df[t0+'_count'+'_'+module] = log1['OWNER_SID'].groupby(log1['APARTMENT_NAME']).size()
        t0=t1
    name = 'Liveness_history_'+i+'_'+module+'.xlsx'
    df.to_excel(name)


#############################################################################日分类活跃度
content2_list = ['neighbor','domestic','house','notice','purchase','estate','passing','opendoor','intoscreen','express', 'water','education','callhouse']
columns_name=['all_apply_id','all_login_id','all_login_count','all_liveness']
for i in content2_list:
a = i+'_'+'login_id'
b = i+'_'+'login_count'
c = i+'_'+'liveness'
columns_name.append(a)
columns_name.append(b)
columns_name.append(c)
index_name = pd.date_range(start='20170126',end='20170226')
df = pd.DataFrame(0,index=index_name,columns=columns_name)

import re
#对房屋租售下的拨打看房热线进行分类
def class_content2(x):
callhouse = u'拨打看房'
if re.search(callhouse,unicode(x)):
    return 'callhouse'
else:
    return 'other'
d = log
d['content2'] = d['content1'].apply(class_content2)
d1 = d[d.content2=='callhouse']


def class_content1(x):
neighbor = u'邻居圈|浏览帖子'
domestic = u'家政'
house = u'房产|房屋'
notice = u'公告'
purchase = u'团购|悦购'
estate = u'物业服务'
passing = u'访客证'
opendoor = u'一键开门'
intoscreen = u'进入主界面'
express = u'快递'
water = u'送水'
education = u'欢乐学'
if re.search(notice,unicode(x)):
    return 'notice'
elif re.search(neighbor,unicode(x)):
    return 'neighbor'
elif re.search(domestic,unicode(x)):
    return 'domestic'
elif re.search(house,unicode(x)):
    return 'house'
elif re.search(purchase,unicode(x)):
    return 'purchase'
elif re.search(estate,unicode(x)):
    return 'estate'
elif re.search(passing,unicode(x)):
    return 'passing'
elif re.search(opendoor,unicode(x)):
    return 'opendoor'
elif re.search(intoscreen,unicode(x)):
    return 'intoscreen'
elif re.search(express,unicode(x)):
    return 'express'
elif re.search(water,unicode(x)):
    return 'water'
elif re.search(education,unicode(x)):
    return 'education'
else:
    return 'other'

log['content2'] = log['content1'].apply(class_content1)
log = pd.concat([log,d1])

#id_count_month = data4_month.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])[['OWNER_SID','APARTMENT_NAME']].groupby(data4_month.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])['APARTMENT_NAME']).size()

log['login_timestamp'] = log['log_created'].apply(lambda x:(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))
owner['apply_timestamp']=owner['CREATED_ON'].apply(lambda x:(time.mktime(time.strptime(str(x)[:19],"%Y-%m-%d %H:%M:%S"))))
for i in index_name:
t = time.mktime(time.strptime(str(i),'%Y-%m-%d %H:%M:%S'))
owner1 = owner[owner.apply_timestamp < t + 86400]
log2 = log[(log.login_timestamp>=t) & (log.login_timestamp<t+86400)]
df.loc[i, 'all_apply_id'] = owner1.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])[['OWNER_SID','APARTMENT_NAME']].count()['OWNER_SID']
df.loc[i, 'all_login_id'] = log2.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])[['OWNER_SID','APARTMENT_NAME']].count()['OWNER_SID']
df.loc[i, 'all_login_count'] = log2.count()['OWNER_SID']
df.loc[i, 'all_liveness'] = round(df.loc[i, 'all_login_id']*1.0/df.loc[i, 'all_apply_id'],4)
for j in content2_list:
    log3 = log2[log2.content2==j]
    a = j + '_' + 'login_id'
    b = j + '_' + 'login_count'
    c = j + '_' + 'liveness'
    df.loc[i, a] = log3.drop_duplicates(['OWNER_SID','APARTMENT_NAME'])[['OWNER_SID','APARTMENT_NAME']].count()['OWNER_SID']
    df.loc[i, b] = log3.count()['OWNER_SID']
    df.loc[i, c] = round(df.loc[i, a] * 1.0 / df.loc[i, 'all_apply_id'], 4)
