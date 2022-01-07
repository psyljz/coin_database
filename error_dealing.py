
from sqlalchemy.orm import sessionmaker
import pymysql
from sqlalchemy import create_engine
pymysql.install_as_MySQLdb()
coin_type_list = ['swap']
time_interval = '1m'


# 删除指定数据库中的所有表 指定日期之后的数据
latest_date = "'2021-12-15'"
for coin_type in coin_type_list:
    database_connect = create_engine(f'mysql://root:1996@localhost:3306/{coin_type}_{time_interval}?charset=utf8')

    _symbol_list = database_connect.table_names()
    for symbol in _symbol_list:
        print(symbol)

        sql = f" delete from {symbol} where candle_begin_time >={latest_date}"

        DB_Session = sessionmaker(bind=database_connect)
        session = DB_Session()
        session.execute(sql)
        session.commit()

