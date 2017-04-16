#isinstance检查一个对象是否是某个特定类型的实例
a=4.5
b=2
print'a is %s,b is %s'%(type(a),type(b))
#a is <type 'float'>,b is <type 'int'>
a/b
isinstance(a,int)
False
#instance可以接受由类型组成的元组，如果想检查某个对象是否属于元组中所指定的那些:
isinstance(a,(int,float))
True
#属性和方法
a='foo'
getattr(a,'split')
# <function split>
#对于字符串以及大部分Python集合类型，该函数会返回True
#iter函数,确定是否可迭代
def isiterable(obj):
    try:
        iter(obj)
        return True
    except TypeError:#不可迭代
        return False

isiterable('a string')
#True
isiterable([1,2,3])
#True
isiterable(5)
#False
#在编写需要处理多类型输入的函数时用到这个功能，编写可以接受任何序列（列表，元组，ndarray）或迭代器的函数。
#先检查是否是列表（或者NumPy数组），如果不是，将其转换成是：
if not isinstance(x,list) and isiterable(x):
    x = list(x)

x=12
In [23]: type(x)
Out[23]: int

In [24]: x=[1,2,3]
In [25]: type(a)
Out[25]: str

In [26]: x={1,2,3}
In [27]: type(x)
Out[27]: set

In [28]: x=(1,2,3)
In [29]: type(x)
Out[29]: tuple

In [30]: a=['1,2,3']
In [31]: type(a)
Out[31]: list

#模块(module)含有函数和变量定义以及从其他.py文件引入的此类东西的.py文件。假设我们有下面这样一个模块：
#some_module.py
PI=3.14159

def f(x):
    return x + 2

def g(a,b):
    return a + b

#如果想要引入some_module.py中定义的变量和函数,我们可以在同一个目录下创建另一个文件：
import some_module
result = some_module.f(5)
pi=some_module.PI

#还可以写成这样：
from some_module import f,g,PI
result = g(5,PI)

#通过as关键字，你可以引入不同的变量名（定义别名）：
import some_module as sm
from some_module import PI as pi,g as gf

r1.sm.f(pi)
r2.gf(6,pi)
