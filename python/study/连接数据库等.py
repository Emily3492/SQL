##############################################连接mysql数据库
import MySQLdb
conn = MySQLdb.connect(
    host='111.11.11.11',
    port = 11111,
    user='root',
    passwd='11111',
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
conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 10.0};SERVER=111.111.111.111;DATABASE=JOY_HOME;UID=11;PWD=11111')
#linux下pyspark连接数据库用以下语句
#conn=pyodbc.connect('DRIVER={FreeTDS};SERVER=111.111.111.111;port=1433;DATABASE=JOY_HOME;UID=11;PWD=111;TDS_Version=8.0;')
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

########################################正则表达式，import re

#正则表达式(regular expression)主要功能是从字符串(string)中通过特定的模式(pattern)，搜索想要找到的内容。
#字符串相关的处理函数。我们可以通过这些函数实现简单的搜索功能，比如说从字符串“I love you”中搜索是否有“you”这一子字符串。
#但有些时候，我们只是模糊地知道我们想要找什么，而不能具体说出我是在找“you”，比如说，我想找出字符串中包含的数字，
#这些数字可以是0到9中的任何一个。这些模糊的目标可以作为信息写入正则表达式，传递给Python，从而让Python知道我们想要找的是什么。

import re
m = re.search('[0-9]','abcd4ef')
print(m.group(0))#m = re.search('c','abcd4ef')
m = re.match('e','ebcd4ef')
print(m.group(0))
m = re.match('e','abcd4ef')#若改为c等非第一个字符，则返回none
print(m.group(0))
print m
ma=re.sub('4','3','abcd4ef')
ma

#对于返回的m, 我们使用m.group()来调用结果。
m = re.search(pattern, string)  # 搜索整个字符串，直到发现符合的子字符串。
m = re.match(pattern, string)   # 从头开始检查字符串是否符合正则表达式。必须从字符串的第一个字符开始就相符。
str = re.sub(pattern, replacement, string) # 在string中利用正则变换pattern进行搜索，对于搜索到的字符串，用另一字符串replacement替换。返回替换后的字符串。
re.split()    # 根据正则表达式分割字符串， 将分割后的所有子字符串放在一个表(list)中返回
re.findall()  # 根据正则表达式搜索字符串，将所有符合的子字符串放在一给表(list)中返回

#正则表达式的常用语法：
1）单个字符:
.          任意的一个字符
a|b        字符a或字符b
[afg]      a或者f或者g的一个字符
[0-4]      0-4范围内的一个字符
[a-f]      a-f范围内的一个字符
[^m]       不是m的一个字符
\s         一个空格
\S         一个非空格
\d         [0-9]
\D         [^0-9]
\w         [0-9a-zA-Z]
\W         [^0-9a-zA-Z]

2）重复
紧跟在单个字符之后，表示多个这样类似的字符
*         重复 >=0 次
+         重复 >=1 次
?         重复 0或者1 次
{m}       重复m次。比如说 a{4}相当于aaaa，再比如说[1-3]{2}相当于[1-3][1-3]
{m, n}    重复m到n次。比如说a{2, 5}表示a重复2到5次。小于m次的重复，或者大于n次的重复都不符合条件。

正则表达          相符的字符串举例
[0-9]{3,5}       9678
a?b              b
a+b              aaaaab

3) 位置
^         字符串的起始位置
$         字符串的结尾位置

正则表达          相符的字符串举例        不相符字符串
^ab.*c$          abeec               cabeec (如果用re.search(), 将无法找到。)

4）返回控制
我们有可能对搜索的结果进行进一步精简信息。比如下面一个正则表达式：output_(\d{4})
该正则表达式用括号()包围了一个小的正则表达式，\d{4}。 这个小的正则表达式被用于从结果中筛选想要的信息（在这里是四位数字）。这样被括号圈起来的正则表达式的一部分，称为群(group)。
我们可以m.group(number)的方法来查询群。group(0)是整个正则表达的搜索结果，group(1)是第一个群……

#例子
import re
m = re.search("output_(\d{4})", "output_1986.txt")
print(m.group(1))
print(m.group(0))
#我们还可以将群命名，以便更好地使用m.group查询:
import re
m = re.search("output_(?P<year>\d{4})", "output_1986.txt")   #(?P<name>...) 为group命名
print(m.group("year"))

#python计算器
>>> 17 / 3  # int / int -> int
5
>>> 17 / 3.0  # int / float -> float
5.666666666666667
>>> 17 // 3.0  # explicit floor division discards the fractional part
5.0
>>> 17 % 3  # the % operator returns the remainder of the division
2
>>> 5 * 3 + 2  # result * divisor + remainder
17
>>> 5 ** 2  # 5 squared#幂乘方
25
>>> 2 ** 7  # 2 to the power of 7
128

#交互模式中，最近一个表达式的值赋给变量 _。这样我们就可以把它当作一个桌面计算器，很方便的用于连续计算，例如:
>>> tax = 12.5 / 100
>>> price = 100.50
>>> price * tax
12.5625
>>> price + _
113.0625
>>> round(_, 2)
113.06

#如果你前面带有 \ 的字符被当作特殊字符，你可以使用 原始字符串，方法是在第一个引号前面加上一个 r:
>>> print 'C:\some\name'  # here \n means newline!
C:\some
ame
>>> print r'C:\some\name'  # note the r before the quote
C:\some\name

#\ 可以用来转义引号:
>>> 'spam eggs'  # single quotes
'spam eggs'
>>> 'doesn\'t'  # use \' to escape the single quote...
"doesn't"
>>> "doesn't"  # ...or use double quotes instead
"doesn't"
>>> '"Yes," he said.'
'"Yes," he said.'
>>> "\"Yes,\" he said."
'"Yes," he said.'
>>> '"Isn\'t," she said.'
'"Isn\'t," she said.'

#查看行数
len(df)
#查看某一列不同值
 a = []
 a = list(zip(df.rn))
 set(a)
