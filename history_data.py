"""
get history data write to mysql

"""
import pandas as pd
from Function import historical_kline, connect_exchange, get_symbol_list
from sqlalchemy import create_engine
from data_base_function import creat_new_table
from datetime import datetime

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)

client = connect_exchange()

#
"""
1m
3m
5m
15m
30m
1h
2h
4h
6h
8h
12h
1d
3d
1w
1M
"""



time_interval = '1m'
coin_type = 'swap'

start_time = '2017-01-09'
end_time = '2021-11-15'


symbol_list = get_symbol_list(client, coin_type)

print('币种数量:', len(symbol_list))
print('k线的间隔:', time_interval)
print('币种列表:', symbol_list)

database_connect = create_engine(f'mysql://root:1996@localhost:3306/{coin_type}_{time_interval}?charset=utf8')


for symbol in symbol_list:

    creat_new_table(symbol.lower(), database_connect)

    historical_kline(client, symbol, time_interval, database_connect,coin_type,start_time,end_time)
