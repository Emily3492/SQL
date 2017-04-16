#部门为空的值去掉
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
    value = {}
    value['CREATED_UNIX'] = x.CREATED_UNIX
    value['module'] = []
    for i in set(module_dict.keys()):
        name = module_dict[i]
        if re.search(name,x.CONTENT):
            value['module'].append(i)
    return (x.OWNER_SID,[value])
df1 = df.rdd.map(map_fun).reduceByKey(lambda x,y:x+y).map(lambda x:Row(OWNER_SID=x[0],value=x[1]))
def count_APP(x):
    '''
    函数功能：计算APP停留时长、停留次数、次均时长
    '''
    stats = x.value
    APP_count = 0
    APP_time = 0
    APP_ave = 0
    if len(stats)>1:
        stats.sort(key=lambda p:float(p['CREATED_UNIX']))
        a = []
        b = {}
        for l in stats:
            if 'logout' in l['module']:
                b['logout'] = l['CREATED_UNIX']
                a.append(b)
                b = {}
            else:
                for m in l['module']:
                    b[m] = l['CREATED_UNIX']
        a = [i for i in a if i.has_key('logout') and i.has_key('login')]
        if a:
            APP_count = unicode(len(a))
            APP_time = unicode(([float(j['logout'])-float(j['login']) for j in a]))
            APP_ave  = unicode(round(APP_time*1.0/APP_count))
    return Row(OWNER_SID=x.OWNER_SID,APP_count=APP_count,APP_time=APP_time,APP_ave=APP_ave,value=x.value)
def count_module(x):
    '''
    函数功能：计算每个模块的停留时长、次数、次均时长
    '''
    module_time = dict(zip(module_dict.keys(),[0]*len(module_dict)))
    module_count = dict(zip(module_dict.keys(),[0]*len(module_dict)))
    module_ave = dict(zip(module_dict.keys(),[0]*len(module_dict)))
    stats = x.value
    if len(stats)>1:
        stats.sort(key=lambda p:float(p['CREATED_UNIX']))
        for l in range(1,len(stats)):
            for m in stats[l-1]['module']:
                t = float(stats[l]['CREATED_UNIX']) - float(stats[l-1]['CREATED_UNIX'])
                module_time[m] += t
                module_count[m] += 1
    for m in set(module_dict.keys()):
        if module_count[m] >0:
            module_ave[m] = round(module_time[m]*1.0/module_count[m],4)
    time_dict = dict(zip(module_time.keys(),map(lambda x:unicode(x),module_time.values())))
    count_dict = dict(zip(module_count.keys(),map(lambda x:unicode(x),module_count.values())))
    ave_dict = dict(zip(module_ave.keys(),map(lambda x:unicode(x),module_ave.values())))
    time_dict['all'] = x.APP_time
    count_dict['all'] = x.APP_ave
    ave_dict['all'] = x.APP_count
    return Row(OWNER_SID=x.OWNER_SID,module_time=time_dict,module_count=count_dict,module_ave=ave_dict)
df2 = df1.map(count_APP).map(count_module).toDF()


dff2 = pd.read_excel('.../owner_0403.xlsx')
dff2 = dff2[['dept_1','OWNER_SID','OWNER_TAG']]
for i in dff2.columns:
    dff2[i] = dff2[i].astype(unicode)
df3 = hc.createDataFrame(dff2)
df4 = df3.join(df2,df3.OWNER_SID==df2.OWNER_SID,'leftouter')



#OWNER_TAG
def timeCount_fun(x):
    module_dict1 = dict(zip(module_dict.keys(),[0]*len(module_dict)))
    module_dict1['all'] = 0
    stats = [s for s in x.value if s]
    if stats!=[]:
        for m in module_dict1:
            for l in stats:
                module_dict1[m]+=float(l[m])
    return Row(key=x.key,**module_dict1)
def ave_fun(x):
    module_dict1 = dict(zip(module_dict.keys(),[0]*len(module_dict)))
    module_dict1['all'] = 0
    stats = [s for s in x.value if s]
    for m in module_dict1:
        for l in stats:
            module_dict1[m]+=float(l[m])
    module_dict2 = dict(zip(module_dict1.keys(),[0]*len(module_dict1)))
    for n in module_dict1:
        module_dict2[n] = round(module_dict1[n]*1.0/len(x.value),4)
    return Row(key=x.key,**module_dict2)


###第六级
for i in set(range(6)):
    df5 = df4.rdd.filter(lambda x:x['dept_1'].count('-')>=i).map(lambda x:Row(key='-'.join(x['dept_1'].split('-')[:i+1])+'-'+x['OWNER_TAG'],module_time=x.module_time,module_count=x.module_count,module_ave=x.module_ave))
    df6 = df5.map(lambda x:(x.key,[x.module_time])).reduceByKey(lambda x,y:x+y).map(lambda x:Row(key=x[0],value=x[1]))
    name1 = '/Users/Tyrone/Desktop/test/dept_1_time_'+str(i)+'.xlsx'
    df6.map(timeCount_fun).toDF().toPandas().to_excel(name1)
    df7 = df5.map(lambda x:(x.key,[x.module_count])).reduceByKey(lambda x,y:x+y).map(lambda x:Row(key=x[0],value=x[1]))
    name2 = '/Users/Tyrone/Desktop/test/dept_1_count_'+str(i)+'.xlsx'
    df7.map(timeCount_fun).toDF().toPandas().to_excel(name2)
    df8 = df5.map(lambda x:(x.key,[x.module_ave])).reduceByKey(lambda x,y:x+y).map(lambda x:Row(key=x[0],value=x[1]))
    name3 = '/Users/Tyrone/Desktop/test/dept_1_ave_'+str(i)+'.xlsx'
    df8.map(ave_fun).toDF().toPandas().to_excel(name3)
