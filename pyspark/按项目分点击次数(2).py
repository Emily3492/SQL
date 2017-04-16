from pyspark.sql import Row
import pandas as pd
import time
import re
hc = sqlContext
dff = pd.read_excel('.../前端日志埋点0324改1.xlsx')
for i in dff.columns:
    dff[i] = dff[i].astype(unicode)
apart_dict = dict(zip(dff.num1,dff.apartment))
module_dict = dict(zip(dff.num2,dff.module))
del module_dict['nan']
dff1 = pd.read_excel('.../LOG_inner.xlsx')
dff1 = dff1[(dff1.CREATED_ON<'2017-04-01') & (dff1.CREATED_ON>'2017-03-01')]
for i in dff1.columns:
    dff1[i] = dff1[i].astype(unicode)
def class_module(x):
    for i in module_dict:
        name = module_dict[i]
        if re.search(name,unicode(x)):
            return i
            break
dff1['module_class'] = dff1['CONTENT'].apply(class_module)
dff2 = dff1.dropna(subset=['module_class'])

def class_apart(x):
    for i in set(apart_dict.keys()):
        name = apart_dict[i]
        if re.search(name,unicode(x)):
            return name
            break
dff2['apartment_class'] = dff2['CONTENT'].apply(class_apart)
dff3 = dff2.dropna(subset=['apartment_class'])
dff4 = dff3[['OWNER_SID','module_class','apartment_class']]
df = hc.createDataFrame(dff4)
df1 = df.rdd.map(lambda x:Row(apartment_class=x.apartment_class,value={'module_class':x['module_class'],'OWNER_SID':x['OWNER_SID']})).map(lambda x:(x.apartment_class,[x.value])).reduceByKey(lambda x,y:x+y).map(lambda x:Row(apartment_class=x[0],value=x[1]))
def count_fun(x):
    module_sid = {'a':[],'b':[],'c':[],'d':[],'e':[],'f':[],'g':[],'h':[],'i':[],'j':[],'k':[],'l':[],'m':[],'n':[],
    'o':[],'p':[],'q':[],'r':[]}
    module_cnt = {}
    module_freq = {}
    for l in x.value:
        module_sid[l['module_class']].append(l['OWNER_SID'])
    for c in module_sid:
        module_cnt[module_dict[c]] = len(set(module_sid[c]))
    for f in module_sid:
        module_freq[module_dict[f]] = len(module_sid[f])
    return Row(apartment_class=x.apartment_class,module_cnt=module_cnt,module_freq=module_freq)
df2 = df1.map(count_fun)
df2.map(lambda x:Row(apartment_class=x.apartment_class,**x['module_cnt'])).toDF().toPandas().to_excel('/Users/Tyrone/Desktop/个数.xlsx')
df2.map(lambda x:Row(apartment_class=x.apartment_class,**x['module_freq'])).toDF().toPandas().to_excel('/Users/Tyrone/Desktop/次数.xlsx')


df3
def flatmap_fun(x):
    ret = []
    for k in x.value:
        ret.append(Row(apartment_class=x.apartment_class,module_name=module_dict[k['module_class']],OWNER_SID=k['OWNER_SID']))
    return ret
df1.flatMap(flatmap_fun).toDF().toPandas().to_excel('/Users/Tyrone/Desktop/原始记录.xlsx')
