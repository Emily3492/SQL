hc = sqlContext
from pyspark.sql import Row
import pandas as pd
import time
import re
dff = pd.read_excel('.../前端日志埋点0324改1.xlsx')
module_dict = dict(zip(dff[u'板块名'],dff[u'关键字']))
dff1 = pd.read_excel('.../LOG_inner.xlsx')
dff1 = dff1[(dff1.CREATED_ON<'2017-04-01') & (dff1.CREATED_ON>'2017-03-01')]
dff1['CREATED_UNIX'] = dff1['CREATED_ON'].apply(lambda x:time.mktime(time.strptime(str(x)[:19],'%Y-%m-%d %H:%M:%S')))
for i in dff1.columns:
    dff1[i] = dff1[i].astype(unicode)
df = hc.createDataFrame(dff1)
def map_fun(x):
    module = ['all']
    for i in set(module_dict.keys()):
        name = module_dict[i]
        if re.search(name,x.CONTENT):
            module.append(i)
    return (x.OWNER_SID,module)
df1 = df.rdd.map(map_fun).reduceByKey(lambda x,y:x+y).map(lambda x:Row(OWNER_SID=x[0],module=x[1]))
#个人点击次数
def per_login(x):
    value = {}
    module_list = module_dict.keys()
    module_list.append('all')
    for i in module_list:
        value[i] = x['module'].count(i)
    return Row(OWNER_SID=x.OWNER_SID,**value)
df2 = df1.map(per_login).toDF().toPandas().to_excel('.../每个人在每个模块的点击次数.xlsx')

#分部门和岗位
dff2 = pd.read_excel('.../owner_0403.xlsx')
dff2 = dff2[dff2.created_on<'2017-04-01']
dff2 = dff2[['dept_1','OWNER_SID','OWNER_TAG']]
for i in dff2.columns:
    dff2[i] = dff2[i].astype(unicode)
df3 = hc.createDataFrame(dff2)
df1 = df1.toDF()
df4 = df3.join(df1,df3.OWNER_SID==df1.OWNER_SID,'leftouter')

def map_fun1(x):
    if x.module:
        a = x.module
        a.append('Apply')
        a.append('Login')
    else:
        a = ['Apply']
    return (x.key,a)

def live_count(x):
    module_dict1 = dict(zip(module_dict.keys(),[0]*len(module_dict)))
    module_dict1['all'] = 0
    for m in module_dict1:
        module_dict1[m] = x['module'].count(m)
    return Row(key=x.key,**module_dict1)
def live_person(x):
    module_dict1 = dict(zip(module_dict.keys(),[0]*len(module_dict)))
    module_dict1['all'] = 0
    for m in module_dict1:
        module_dict1[m] = x['module'].count('Login')
    module_dict1['Apply'] = x['module'].count('Apply')
    return Row(key=x.key,**module_dict1)
def livenes(x):
    module_dict1 = dict(zip(module_dict.keys(),[0]*len(module_dict)))
    module_dict1['all'] = 0
    cntApply = x['module'].count('Apply')
    for m in module_dict1:
        module_dict1[m] = round(x['module'].count('Login')*1.0/cntApply,4)
    return Row(key=x.key,**module_dict1)

for i in set(range(6)):
    df5 = df4.rdd.filter(lambda x:x['dept_1'].count('-')>=i).map(lambda x:Row(key='-'.join(x['dept_1'].split('-')[:i+1])+'-'+x['OWNER_TAG'],module=x.module))
    df6 = df5.map(map_fun1).reduceByKey(lambda x,y:x+y).map(lambda x:Row(key=x[0],module=x[1]))
    name1 = '.../'+str(i+1)+'级部门中各岗位各模块的点击次数.xlsx'
    df6.map(live_count).toDF().toPandas().to_excel(name1)
    df7 = df5.map(map_fun1).reduceByKey(lambda x,y:x+y).map(lambda x:Row(key=x[0],module=x[1]))
    name2 = '.../'+str(i+1)+'级部门中各岗位的注册人数和各模块的活跃人数.xlsx'
    df7.map(live_person).toDF().toPandas().to_excel(name2)
    df8 = df5.map(map_fun1).reduceByKey(lambda x,y:x+y).map(lambda x:Row(key=x[0],module=x[1]))
    name3 = '.../'+str(i+1)+'级部门中各岗位各模块的活跃度.xlsx'
    df8.map(livenes).toDF().toPandas().to_excel(name3)
