#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2015-08-13 08:31:27
# @Author  : Your Name (you@example.org)
# @Link    : http://example.org
# @Version : $Id$

import pandas as pd
import tushare as ts
import mysql.connector
import datetime
import tushare_mysql

mysqlEngine = tushare_mysql.toggleMysql()
cursor = mysqlEngine.cursor(buffered = True)
cursor.execute("use {};".format(tushare_mysql.DB_NAME))

stmts = '''
	SELECT CODE, TIMETOMARKET FROM `CHINASTOCK`.`STOCK_BASICS`;
	'''
cursor.execute(stmts)
for pair in cursor:
	# print "symbol: {}\tIPO: {}".format(*pair)
	symbol = pair[0]
	date_int = pair[1]
	date_str = str(date_int)
	date_tuple = (int(date_str[0:4]), int(date_str[4:6]), int(date_str[6:]))
	ipo = datetime.date(*date_tuple)
	ipo = str(ipo)
	
	symbol_df = tushare_mysql.getHistKlineDf(symbol = symbol
									   		beginDate = ipo,
									   		endDate = '2012-8-15',
									   		ktype = 'D'
									   		retry_count = 5,
									  	 	pause = .01)
	df = symbol_df[1]
	df['volume'] = df['volume'] * 100
	tushare_mysql.insertDatabase(symbol, df, mysqlEngine)

	