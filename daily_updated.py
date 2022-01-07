"""
daily updated lasted data

"""
import pandas as pd
import pymysql
import time
from sqlalchemy import create_engine
from Function import get_symbol_list, connect_exchange, daily_kline_updated, check_data,getdate_list
from data_base_function import creat_new_table
pymysql.install_as_MySQLdb()



pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)
coin_type_list = ['swap']
time_interval = '1m'

client = connect_exchange()

for coin_type in coin_type_list:

    database_connect = create_engine(f'mysql://root:1996@localhost:3306/{coin_type}_{time_interval}?charset=utf8', pool_recycle=2)
    saved_kindle_date = check_data(database_connect)
    date_update_list = getdate_list(saved_kindle_date)
    symbol_update_list = get_symbol_list(client, coin_type)
    # date_update_list = ['2021-12-15','2021-12-16','2021-12-17','2021-12-18','2021-12-19','2021-12-20']
    # symbol_update_list = ['bzrxusdt']

    print('币种数量:', len(symbol_update_list))
    print('日期天数:', len(date_update_list))
    print('k线类型:', coin_type)
    print('k线的间隔:', time_interval)
    print('币种列表:', symbol_update_list)
    print('日期列表:', date_update_list)

    for symbol in symbol_update_list:
        creat_new_table(symbol.lower(), database_connect)
        for date_time in date_update_list:

            df = daily_kline_updated(client, date_time, symbol, time_interval, coin_type)
            if df.empty:
                print(symbol, date_time, '无数据跳过')
                continue
            try:

                df.to_sql(name=symbol.lower(), con=database_connect, index=False, if_exists='append')
            except:

                print(f'{symbol}_{date_time}数据已存在,跳过')
                time.sleep(0.1)
                continue

            time.sleep(0.1)

            print(symbol, date_time, '已保存')
