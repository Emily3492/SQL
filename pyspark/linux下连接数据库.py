##############################################连接mysql数据库
import MySQLdb
conn = MySQLdb.connect(
    host='111.11.11.11',
    port = 11111,
    user='root',
    passwd='111111',
    db='JH_Server_Commerce',
    charset='utf8'
)

cur = conn.cursor()
aa = cur.execute("select goods_name,category_id from goods")
info = cur.fetchmany(aa)

# 使用结束后使用
cur.close()
conn.close()

##########################################################################连接sql server 数据库
import pyodbc
#conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 10.0};SERVER=139.129.166.169;DATABASE=JOY_HOME;UID=sa;PWD=Yuedu2016qwerASDF')
#linux下pyspark连接数据库用以下语句
conn=pyodbc.connect('DRIVER={FreeTDS};SERVER=111.111.111.111;port=1433;DATABASE=JOY_HOME;UID=11;PWD=111111;TDS_Version=8.0;')
cur = conn.cursor()
aa = cur.execute("select* from HOME_OWNER ")
info = cur.fetchall()

# 使用结束后使用
cur.close()
conn.close()

###################################查看数据
df.head(10)
df.tail(10)
df.head()
df.tail()

#中文字符解析报错,原因是数据类型是varchar(不是unicode编码),需要用nvarchar（unicode编码）存储。
#####################
from pyspark import SparkContext
from pyspark import HiveContext
import sys
import datetime
import numpy as np
from numpy import array
import pandas as pd
import os
import pickle
import re
import math
from pyspark.sql import Row
import time
import os
import pickle
reload(sys)
sys.setdefaultencoding('utf8')
hc=sqlContext
