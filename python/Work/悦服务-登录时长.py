#SQL取值

#####工作人员取值
select REPLACE(t3.dept,' ','-') dept_1,t3.rn,t3.created_on,t3.OWNER_SID,t3.OWNER_NO ,t3.OWNER_NAME ,t3.USER_SID,t3.ROLE_NAME,t3.ROLE_SID,
t3.OWNER_PHONE ,t3.OWNER_STATUS ,t3.GROUP_SID,t3.DEPT_SID,t3.OWNER_TYPE
,t3.PARENT_DEPT_SID,t3.REMARK ,t3.SORT_INDEX ,t3.GROUP_NAME
from(
select t1.*,
ltrim(isnull(t1.g,'')+' '+isnull(t1.a,'')+' '+isnull(t1.b,'')+' '+isnull(t1.c,'')+' '+isnull(t1.d,'') +' '+isnull(t1.f,'')) dept
from(select ROW_NUMBER() over (partition by a.OWNER_SID order by a.OWNER_SID desc ) as rn,
a.created_on,a.OWNER_SID,a.OWNER_NO ,a.OWNER_NAME ,k.USER_SID,m.ROLE_NAME,m.ROLE_SID,
a.OWNER_PHONE ,a.OWNER_STATUS ,a.GROUP_SID,a.DEPT_SID,a.OWNER_TYPE
,b.PARENT_DEPT_SID,b.REMARK ,b.SORT_INDEX ,d.GROUP_NAME ,n.DEPT_NAME g,g.DEPT_NAME a,f.DEPT_NAME b,e.DEPT_NAME c,c.DEPT_NAME d,b.DEPT_NAME f
  from HOME_OWNER a
left join HOME_USER_ROLE k on k.USER_SID=a.OWNER_SID
left join HOME_GROUP_ROLE m on k.ROLE_SID=m.ROLE_SID
  left join HOME_GROUP_DEPT b on a.DEPT_SID=b.DEPT_SID
left join HOME_GROUP_DEPT c on c.DEPT_SID=b.PARENT_DEPT_SID
left join HOME_GROUP_DEPT e on e.DEPT_SID=c.PARENT_DEPT_SID
left join HOME_GROUP_DEPT f on f.DEPT_SID=e.PARENT_DEPT_SID
left join HOME_GROUP_DEPT g on g.DEPT_SID=f.PARENT_DEPT_SID
left join HOME_GROUP_DEPT n on n.DEPT_SID=g.PARENT_DEPT_SID
left join HOME_GROUP d on a.GROUP_SID=d.GROUP_SID
--where  a.OWNER_SID in('0000c52a-8688-46ef-a4b2-4f90a449cedd')
where a.OWNER_TYPE not LIKE ('%1%')--工作人员
--and a.created_on >='20170226'
--and a.CREATED_ON  <='20170326'
--and m.ROLE_NAME like('%工程专员%')--or b.DEPT_NAME like('%高级经理%')
and a.OWNER_STATUS=1)t1)t3
--剔除重复,按部门汇总时需要,按小区汇总不需要


#一行



#日志取值





####################################################导入数据
import os  # 设置存储路径
print(os.getcwd())
os.chdir("D:\\work_all\\python")  # 包安装路径 D:\Program Files\Anaconda2\Scripts
print(os.getcwd())
# 直接连接数据库
import pyodbc
conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 10.0};SERVER=111.111.111.111;DATABASE=JOY_HOME;UID=11;PWD=1111')
cur = conn.cursor()
aa = cur.execute("                       ")
info = cur.fetchall()
cur.close()
conn.close()
#转换成dataframe
import pandas as pd
info1 = [list(x) for x in info]
data = pd.DataFrame(info1,columns=['SYSTEM_TYPE','CREATED_ON','content','OWNER_SIDE', 'apartment_name', 'OWNER_STATUS','OWNER_TYPE','a','b','c','d','f','dept','SORT_INDEX','OWNER_NO','OWNER_NAME','OWNER_PHONE'])
data.head()#日志表
data['CONTENT'] = data['content'].apply(lambda x: x.decode('gbk'))#将content转换成gbk格式显示并加在log的dataframe中去
data.head()
log = data.drop(['content'], axis=1)#删除乱码显示的内容后存在dataframe的log1中
log.head()
