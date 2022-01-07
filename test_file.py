"""
test for
"""

import pandas as pd
import pymysql
from sqlalchemy.orm import sessionmaker

from sqlalchemy import create_engine
import datetime

pymysql.install_as_MySQLdb()
coin_type_list = ['swap', 'spot']
time_interval = '1m'
coin_type ='swap'
database_connect = create_engine(f'mysql://root:1996@localhost:3306/{coin_type}_{time_interval}?charset=utf8')


def check_data(_database_connect):
    """
    检查数据库每个表的最新数据的情况
    :param _database_connect:
    """
    _symbol_list = _database_connect.table_names()
    print('币种个数', len(_symbol_list))
    the_time_list = []
    for symbol in _symbol_list:

        sql = f"select * from {symbol} order by candle_begin_time desc limit 1"

        df = pd.read_sql(sql, con=database_connect, index_col='candle_begin_time',parse_dates=['candle_begin_time'])

        the_time_list.append(df.index._data[0])

    if (len(set(the_time_list))) == 1:

        print('全部币种已更新', the_time_list[0])
    else:
        raise ValueError
    return the_time_list[0]



