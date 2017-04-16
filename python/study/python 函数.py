# 当有人问你昨天是几号，是很容易就得到答案的
# 但当要计算出100天前是几号，就不那么容易得出了
# 而Python中datetime的timedelta则可以轻松完成计算
#
# 例如：
import datetime
(datetime.datetime.now() - datetime.timedelta(days = 100)).strftime("%Y-%m-%d")

附：
datetime模块定义了下面这几个类：

datetime.date：表示日期的类,
常用的属性有year, month, day；

datetime.time：表示时间的类,
常用的属性有hour, minute, second, microsecond；

datetime.datetime：表示日期时间,

datetime.timedelta：表示时间间隔，即两个时间点之间的长度

datetime.tzinfo：与时区有关的相关信息。
