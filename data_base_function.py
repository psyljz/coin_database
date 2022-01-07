from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer,DateTime,FLOAT,DECIMAL,REAL


def creat_new_table(symbol, data_base):

    Base = declarative_base()  # 生成orm基类

    class Proxy(Base):
        __tablename__ = symbol

        candle_begin_time = Column(DateTime, primary_key=True, nullable=False)  # 主键
        open = Column(REAL, nullable=True, default="")  # 协议类型
        high = Column(REAL, nullable=True, default="")  # 协议类型
        low = Column(REAL, nullable=True, default="")  # 协议类型
        close = Column(REAL, nullable=True, default="")  # 协议类型
        volume = Column(REAL, nullable=True, default="")  # 协议类型
        quote_volume = Column(REAL, nullable=True, default="")  # 协议类型
        trade_num = Column(REAL, nullable=True, default="")  # 协议类型
        taker_buy_base_asset_volume = Column(REAL, nullable=True, default="")  # 协议类型
        taker_buy_quote_asset_volume = Column(REAL, nullable=True, default="")  # 协议类型

    Base.metadata.create_all(data_base)

    return None


columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trade_num',
           'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
