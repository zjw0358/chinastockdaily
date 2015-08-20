#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2015-08-13 08:31:27
# @Author  : Wang Zhicheng (wzc_jd@126.com)
# @Version : $Id$

import pandas as pd
import tushare as ts
import mysql.connector
import datetime
import tushare_mysql

model_table = 'sz000001'
Engine = tushare_mysql.togglemysql()
symbols = tushare_mysql.symbolList(Engine)

cursor = Engine.cursor(buffered=True)
cursor.execute("use {};".format(tushare_mysql.DB_NAME))

sql = "SELECT DATE FROM `{}`.`{}` ORDER BY DATE DESC LIMIT 1;".format(tushare_mysql.DB_NAME, model_table)
cursor.execute(sql)

date = cursor.fetchall()[0][0]
date_str = str(date)
date_tuple = date_str.split('-')
date_tuple = [int(part) for part in date_tuple]
date_tuple[2] += 1

next_date = datetime.date(*date_tuple)
next_date = str(next_date)
today = datetime.date.today()
today = str(today)

for each in symbols:
	symbol_df = tushare_mysql.getHistKlineDf(symbol=each,
									  beginDate=next_date,
									  endDate=today,
									  retry_count=5)

	symbol_df[1]['volume']=symbol_df[1]['volume']*100
	df = symbol_df[1]

	tushare_mysql.insertDatabase(each, df, Engine)

tushare_mysql.togglemysql()