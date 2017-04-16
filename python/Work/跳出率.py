3.1  # 概念解析
跳出率 = 日志中只访问首页的次数 / 总访问次数  # 跳出率越低越好

# 日志数据原始表提取
select count(CONTENT) drump'
from Home_OwnerLog a
where a.CONTENT like '%进入主界面%'
and a.CREATED_ON >= '2016-10-26'
and a.CREATED_ON <= '2016-11-25'
union all
select count(CONTENT)
from Home_OwnerLog a
where a.CREATED_ON >= '2016-10-26' and a.CREATED_ON <= '2016-11-25'

#用SQL跑的结果，python有点问题
# python连接数据库处理数据
import os  # 设置存储路径
print(os.getcwd())
os.chdir("D:\\work\\python")  # 包安装路径 D:\Program Files\Anaconda2\Scripts
print(os.getcwd())
# 直接连接数据库
import pyodbc
conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 10.0};SERVER=111.111.111.111;DATABASE=JOY_HOME;UID=11;PWD=1111')
cur = conn.cursor()
data = cur.execute( "select count(CONTENT) drump from Home_OwnerLog a where a.CONTENT like '%进入主界面%' and a.CREATED_ON >= '2016-10-26' and a.CREATED_ON <= '2016-11-25' union all select count(CONTENT) al from Home_OwnerLog a where a.CREATED_ON >= '2016-10-26' and a.CREATED_ON <= '2016-11-25'")
data = cur.fetchall()
data
# 查看list[(...,...,...)]
data1 = data[0][0]
# 取list中[]中第一个数，()中第一个数




jump
rate = str(Z1 * 1.0 / M1 * 100) + '%'
