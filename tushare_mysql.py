#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2015-08-13 08:31:27
# @Author  : Wang Zhicheng (wzc_jd@126.com)
# @Version : $Id$

import pandas as pd
import tushare as ts
from tushare.stock import cons as ct
import mysql.connector

DB_NAME = 'chinastock'
wzc_at_local_mysql = {
    'user': 'wzc',
    'password': 'ericwang',
    'host': '127.0.0.1',
}


class NumpyMySQLConverter(mysql.connector.conversion.MySQLConverter):
    """ A mysql.connector Converter that handles Numpy types """

    def _float32_to_mysql(self, value):
        return float(value)

    def _float64_to_mysql(self, value):
        return float(value)

    def _int32_to_mysql(self, value):
        return int(value)

    def _int64_to_mysql(self, value):
        return int(value)


def togglemysql(sqlEngine = None):
	'''Login MySQL server 
	return: MySQL Engine
	'''
	
	if sqlEngine is not None:
		sqlEngine.close
		return None
	else:
		mysqlEngine = mysql.connector.connect(**wzc_at_local_mysql)
		mysqlEngine.set_converter_class(NumpyMySQLConverter)
		return mysqlEngine

	# cursor = mysqlEngine.cursor()
	# cursor.execute("use " + DB_NAME)


def formatStockDf(dataframe):

	columns = ct.DAY_PRICE_COLUMNS
	dropped_columns = columns[6:len(columns)-1]
	dataframe.drop(dropped_columns, axis = 1, inplace = True)
	return dataframe


def getHistKlineDf(symbol, 
					beginDate = None, 
					endDate = None, 
					ktype = 'D',
					retry_count= 5, 
					pause = .01):
	'''code：股票代码，即6位数字代码，或者指数代码（sh=上证指数 sz=深圳成指 hs300=沪深300指数 sz50=上证50 zxb=中小板 cyb=创业板）
	start：开始日期，格式YYYY-MM-DD
	end：结束日期，格式YYYY-MM-DD
	klinetype：数据类型，D=日k线 W=周 M=月 5=5分钟 15=15分钟 30=30分钟 60=60分钟，默认为D
	retry_count : int, 默认 3
	           如遇网络等问题重复执行的次数 
	pause : int, 默认 0
	          重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
	'''
	try:
		df = ts.get_hist_data(code = symbol, 
							  start = beginDate,
							  end = endDate,
							  ktype = ktype,
							  retry_count = retry_count,
							  pause = pause)
	except Exception, e:
		print e

	df = formatStockDf(df)

	return (symbol, df)


def tblNmae(symbol):
	if symbol[0] == '6':
		name = 'sh' + symbol
	else:
		name = 'sz' + symbol
	return name


def addtoMysql(symbol, dataframe, sqlEngine, database = DB_NAME):
	dataframe_to_sql_config = {
	    'con': sqlEngine,
	    'flavor': 'mysql',
	    'if_exists': 'append',
	    'schema': database
	}

	cursor = sqlEngine.cursor(buffered = True)
	cursor.execute("use " + database)


	dataframe.to_sql(name = tblNmae(symbol), **dataframe_to_sql_config)

	# sql_stmts = """
	# 	SET @TBL_NAME := `{0}`.`{1}`;
	# 	SET @STR := CONCAT("ALTER TABLE ", @TBL_NAME);

	# 	SET @STR := CONCAT(@STR, "\NADD COLUMN IDX INT AUTO_INCREMENT PRIMARY KEY`,");
	# 	SET @STR := CONCAT(@STR, "\NCHANGE COLUMN `DATE` `DATE` DATE,");
	# 	SET @STR := CONCAT(@STR, "\NCHANGE COLUMN `OPEN` `OPEN` DECIMAL(6,3) NOT NULL,");
	# 	SET @STR := CONCAT(@STR, "\NCHANGE COLUMN `HIGH` `HIGH` DECIMAL(6,3) NOT NULL,");
	# 	SET @STR := CONCAT(@STR, "\NCHANGE COLUMN `LOW` `LOW` DECIMAL(6,3) NOT NULL,");
	# 	SET @STR := CONCAT(@STR, "\NCHANGE COLUMN `CLOSE` `CLOSE` DECIMAL(6,3) NOT NULL,");
	# 	SET @STR := CONCAT(@STR, "\NCHANGE `VOLUME` `VOLUME` BIGINT NOT NULL COMMENT 'VOLUME IN SHARES (NOT IN LOT)';");

	# 	PREPARE ALTER_STOCK_TBL FROM @STR;
	# 	EXECUTE ALTER_STOCK_TBL;
	# 	DEALLOCATE PREPARE ALTER_STOCK_TBL;
	# 	""".format(DB_NAME, name)
	sql_stmts = """
		ALTER TABLE `{DB}`.`{TBL}`
		ADD COLUMN IDX INT AUTO_INCREMENT PRIMARY KEY,
		CHANGE COLUMN `DATE` `DATE` DATE NOT NULL UNIQUE,
		CHANGE COLUMN `OPEN` `OPEN` DECIMAL(6,3) NOT NULL,
		CHANGE COLUMN `HIGH` `HIGH` DECIMAL(6,3) NOT NULL,
		CHANGE COLUMN `LOW` `LOW` DECIMAL(6,3) NOT NULL,
		CHANGE COLUMN `CLOSE` `CLOSE` DECIMAL(6,3) NOT NULL,
		CHANGE COLUMN `VOLUME` `VOLUME` BIGINT NOT NULL COMMENT "VOLUME IN SHARES (NOT IN LOT)";
		""".format(DB=database, TBL=name)

	#todo:  
	cursor.execute(sql_stmts)

def _initTable(symbol, sqlEngine):
	'''update stock database for the given stock SYMBOL'''
	symbol_df = gethistKlineDf(symbol = symbol)
	addtoMysql(*symbol_df, sqlEngine = sqlEngine)

def insertTableFromIPODate(symbol, dataframe, sqlEngine):
	'''将各个股票表市场数据从IPO日期起的数据补全
	'''
	sql_stmts ='''
		INSERT INTO `%s` 
		(`DATE`, `OPEN`, `HIGH`, `CLOSE`, `LOW`, `VOLUME`, `TURNOVER`) 
		VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
		'''.format(tblNmae(symbol))

	cursor = sqlEngine.cursor(buffered=True)
	cursor.execute('use {};'.format(DB_NAME))
	cursor.executemany(sql_stmts,
					   dataframe)


def symbolList(sqlEngine, database = DB_NAME, table = 'stock_basics'):

	symbols = []
	cursor = sqlEngine.cursor(buffered = True)
	cursor.execute("use {};".format(database))

	stmts = '''
		SELECT CODE FROM `{}`.`{}`;
		'''.format(database, table)

	cursor.execute(stmts)
	for each in cursor:
		symbols.append(each[0]) 
	
	return symbols


def lastTickDate(symbol, sqlEngine, database = DB_NAME):
	pass



def _initChinaStock():
	mysqlEngine = togglemysql()

	sqlAllSymbol = '''
		USE `CHINASTOCK`;
		SELECT CODE FROM `CHINASTOCK`.`STOCK_BASICS`;
		'''
	cursor = mysqlEngine.cursor(buffered = True)
	cursor.execute("USE `CHINASTOCK`;")
	cursor.execute("SELECT CODE FROM `CHINASTOCK`.`STOCK_BASICS`;")
	for eachSymbol in cursor:
		_initTable(eachSymbol[0], mysqlEngine)

	togglemysql(mysqlEngine)


if __name__ == '__main__':
	_initChinaStock()