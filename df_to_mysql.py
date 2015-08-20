#__author__ = 'Wang Zhicheng'

from __future__ import print_function

import mysql.connector
from mysql.connector import errorcode
import pandas as pd
from pandas.io import sql
import tushare as ts

DB_NAME = 'chinastock'

config = {
    'user': 'wzc',
    'password': 'ericwang',
    'host': '127.0.0.1',
}
cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()
cursor.execute("use chinastock")

df = ts.get_industry_classified()

sqlstatement='''
    create table if not exists industry_classified(
        idx int primary key,
        code char(8) unique not null,
        name char(8) unique not null,
        c_name varchar(20));'''

cursor.execute(sqlstatement)


def _write_mysql(frame, table, names, cur):
    bracketed_names = ['`' + column + '`' for column in names]
    col_names = ','.join(bracketed_names)
    wildcards = ','.join([r'%s'] * len(names))
    insert_query = "INSERT INTO %s (%s) VALUES (%s)" % (
        table, col_names, wildcards)

    data = [[None if type(y) == float and np.isnan(y) else y for y in x] for x in frame.values]

    cur.executemany(insert_query, data)


sql._write_mysql = _write_mysql


df.to_sql(name="industry_classified",
    con=cnx,
    flavor="mysql",
    if_exists="append")
