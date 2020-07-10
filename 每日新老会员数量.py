# !/usr/bin/python3
# -*- coding: UTF-8 -*-

import pymysql
import datetime
import time
import sys
# 初始化数据库连接，使用pymysql模块

start = datetime.datetime.now() #记录开始运行的时间
print('程序开始运行，时间：',start)
"""用于记录执行时间的函数"""
def now_time():#输出当前的时刻
    now_time1 = time.strftime('%Y.%m.%d %H:%M:%S ', time.localtime(time.time()))
    return(now_time1)
# print(now_time())
# print("当前时间： ",time.strftime('%Y.%m.%d %H:%M:%S ',time.localtime(time.time())))

"""打开一个文本，标记查询的渠道"""
f = open(r"B渠道新老会员.txt", "a+")
qudao = 'B渠道'
f.write(str(qudao))
f.write('|查询日期|当天下单会员数量|当天老客会员数|当天新客会员数|当天以前会员总数|以前下过单今天没下的会员数')
f.write('\n')

# 打开数据库连接
db = pymysql.connect("localhost", "root", "myroot", "marketing") #本地数据库连接
db_gy = pymysql.connect("47.106.34.159", "gy", "s3sldjxmdlsydsd@20200619", "gy")

# 使用cursor()方法获取操作游标
cursor = db.cursor()
# cursor_gy = db_gy.cursor()

#创建查询的日期
begin = datetime.date(2019,9,30) #开始日期的前一天
end = datetime.date(2020,5,1)  #结束日期的前一天
# star_today = begin
date_today = begin
delta = datetime.timedelta(days=1)

"""创建两条查询语句：查以前下单的字段和当天的字段"""
# MySQL语句1：查渠道以前下单的指标字段
old_number = ''' SELECT vip_name
	FROM
		orders 
	WHERE
	dealer IS NULL 
	AND channel = 'B'
	AND dealtime < '{} 00:00:00' '''

# MySQL语句2：查渠道当天下单的指标字段
today_number = '''SELECT
	vip_name 
FROM
	orders 
WHERE
	dealer IS NULL 
	AND channel = 'B'
	AND dealtime >= '{} 00:00:00' 
	AND dealtime < '{} 00:00:00'  '''

"""执行SQL1生成以前的下单指标的字段，生成一个初始列表"""
try:
    print('开始执行SQL1，当前时间：', now_time())  # 记录的当前的时刻
    # 查库以前下单的字段
    old_number_r = old_number.format((date_today + delta))
    print('今天以前下单的老客列表查库语句：', old_number_r)
    cursor.execute(old_number_r)
    old_number_array = cursor.fetchall()
    # print('今天以前下单的老客列表查库结果',old_number_array)
    print( 'SQL1下过单的列表结果执行完成：', now_time())
except:
    print(date_today, "以前字段列表，SQL1执行失败。")
    sys.exit(0)
try:
    print('开始转换SQL1结果为列表，当前时间：', now_time())  # 记录的当前的时刻
    old_user_number = []
    for u in old_number_array:
        old_user_number.append(u[0])
    # print('以前下单用户列表：', old_user_number)
    print('历史下单字段列表完成。当前时间：', now_time())
except:
    print(date_today, "以前字段列表，SQL1结果转换列表失败。")
    sys.exit(0)

"""开始循环"""
while date_today <= end:
    date_today += delta
    """执行SQL2语句：当天下单会员，并生成列表（未去重）"""
    try:
        print('查询日期：', date_today)
        print('开始执行SQL2，当前时间：',now_time())# 记录当前的时刻
        # 查库当天下单会员（未去重）
        today_number_r = today_number.format(date_today,(date_today + delta))
        print('今天下单的会员查库语句：',today_number_r)
        cursor.execute(today_number_r)
        today_number_array = cursor.fetchall()
        # print('今天下单的会员查库结果：',today_number_array)
        print(now_time(),'SQL1执行完成。')
    except:
        print( "SQL1执行失败。",date_today)
        break
    try:
        print('开始转换SQL1结果为列表，当前时间：',now_time())  # 记录的当前的时刻
        today_user_number = []
        for i in today_number_array:
            today_user_number.append(i[0])
        # print('今日下单用户列表（未去重）：', today_user_number)
    except:
        print("SQL2转化列表出错。查询日期：",date_today)
        break

    try:
        print('开始执行set方法去重，计算元素数量，求出结果。当前时间：', now_time())  # 记录的当前的时刻
        list1 = today_user_number  # 今天下单列表
        # print(list1)
        set1 = set(list1)
        # print(set1)
        ren1 = len(set1)
        print('今天下单数量:',ren1)

        print('=================================')
        list2 = old_user_number # 老客列表
        # print(list2)
        set2 = set(list2)
        # print(set2)
        ren2 = len(set2)
        print('以前下过单的数量',ren2)

        print('=================================')
        list3 = list1 + list2  # 总数列表
        # print(list3)
        set3 = set(list3)
        # print(set3)
        ren3 = len(set3)
        print('含今天所有下单数量',ren3)

        print('=================================')
        zong = ren1
        print('今天下单总客数', zong)

        lao = ren1 + ren2 - ren3
        print('今天下单中的老客数:', lao)

        xin = ren1 - lao
        print('今天下单中的新客数：', xin)

        yiqian = ren2
        print('今天以前客户数', yiqian)

        chenmo = yiqian - lao
        print('以前下今天没下的数量', chenmo)

        print('完成set方法去重，计算元素数量，求出结果。当前时间：', now_time())  # 记录的当前的时刻
    except:
        print("set方法出错。查询日期：",date_today)
        break

    """将查询结果写入TXT文本中"""
    try:
        print('执行TXT文本写入，当前时间：',now_time())  # 记录的当前的时刻
        # 将结果写入文本
        # f.write(str(qudao),'|',str(date_today),'|',str(len(today_user_number)),'|',str(len(old_user_number)),'|',str(len(intersection)),'|',str(len(only_list1)),'|',str(len(only_list2)))
        f.write(str(qudao)) #查询渠道
        f.write('|')
        f.write(str(date_today)) #查询日期
        f.write('|')
        f.write(str(zong)) #当天下单总数
        f.write('|')
        f.write(str(lao)) #历史下单老客数
        f.write('|')
        f.write(str(xin)) #当天下单新客数
        f.write('|')
        f.write(str(yiqian)) #当天以前总数
        f.write('|')
        f.write(str(chenmo)) #历史老客今天未光顾数
        f.write('\n')
        print(date_today,'结果已写入。完成时间：',now_time())
    except:
        print("写入TXT执行有误。查询日期",date_today)
        break
    """将含今天的总客户列表作为明天的以前客户列表"""
    old_user_number += today_user_number
# 关闭文本
f.close()
# 关闭数据库连接
db.close()

end1 = datetime.datetime.now()
print(end1)
print('Running time: %s Seconds' % (end1 - start))  # 输出运行时间
