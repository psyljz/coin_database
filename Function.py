"""

function

"""
from warpper import retry
from binance import Client
import time
import pandas as pd
from datetime import timedelta
import datetime
unlisted_symbol = ['bzrxusdt']  # 已经下架的币种

# @retry(retry_times=25, sleep_seconds=15, act_name='初始化交易所')
def connect_exchange():
    """

    :return:
    """
    return Client('', '')


@retry(retry_times=25, sleep_seconds=15, act_name='获取k线')
def get_swap_history_data(exchange, symbol, time_interval, start_timestamp):
    return exchange.futures_klines(symbol=symbol, interval=time_interval, startTime=start_timestamp, limit=1500)


@retry(retry_times=25, sleep_seconds=15, act_name='获取k线')
def get_spot_history_data(exchange, symbol, time_interval, start_timestamp):
    return exchange.get_klines(symbol=symbol, interval=time_interval, startTime=start_timestamp, limit=1000)


def historical_kline(exchange, symbol, time_interval, data_base, coin_type, start_time, end_time):
    """
    hh

    """
    start = f'{start_time} 8:00:00'

    start_timestamp = int(time.mktime(time.strptime(start, "%Y-%m-%d %H:%M:%S"))) * 1000

    while True:

        if coin_type == 'swap':

            df = get_swap_history_data(exchange, symbol, time_interval, start_timestamp)

        elif coin_type == 'spot':

            df = get_spot_history_data(exchange, symbol, time_interval, start_timestamp)
        else:
            raise ValueError('cointype输入有误')

        columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume',
                   'trade_num',
                   'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']

        df = pd.DataFrame(df, columns=columns, dtype='float')

        if df.empty:
            print('没有k线')
            break

        start = pd.to_datetime(int(int(df['candle_begin_time'].iloc[-1]) / 10000) * 10000, unit='ms')

        start = str(start + timedelta(minutes=1) + timedelta(hours=8))

        df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms')

        print(start, symbol)

        start_timestamp = int(time.mktime(time.strptime(start, "%Y-%m-%d %H:%M:%S"))) * 1000

        columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num',
                   'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']

        df = df[columns]

        df = df[df['candle_begin_time'] < pd.to_datetime(f'{end_time} 00:00:00')]
        print(len(df))
        if df.empty:
            break
        df.to_sql(name=symbol.lower(), con=data_base, index=False, if_exists='append')

        time.sleep(0.03)


def get_symbol_list(exchange, data_type):
    """

    :param data_type:
    :param exchange:
    :return:
    """

    if data_type == 'swap':

        _symbol_list = [x['symbol'] for x in exchange.futures_exchange_info()['symbols']]
        symbol_list_all = [symbol for symbol in _symbol_list if symbol.endswith('USDT')]

    elif data_type == 'spot':

        info = exchange.get_exchange_info()
        # 转化为dataframe
        df = pd.DataFrame(info['symbols'])

        df = df[df['quoteAsset'] == 'USDT']
        df.reset_index(inplace=True)

        df = df[df['status'] == 'TRADING']

        symbol_list_all = list(df['symbol'])

    else:
        raise ValueError

    return symbol_list_all


@retry(retry_times=25, sleep_seconds=15, act_name='获取k线')
def daily_kline_updated(exchange, start, symbol, time_interval, data_type):
    """
    hh

    """
    start += ' 08:00:00'

    start_timestamp = int(time.mktime(time.strptime(start, "%Y-%m-%d %H:%M:%S"))) * 1000

    if time_interval.endswith('m'):

        limit_number = int(1440 / int(time_interval[:-1]))
    else:
        raise ValueError

    if data_type == 'swap':

        df = exchange.futures_klines(symbol=symbol, interval=time_interval, startTime=start_timestamp,
                                     limit=limit_number)
    elif data_type == 'spot' and time_interval != '1m':

        df = exchange.get_klines(symbol=symbol, interval=time_interval, startTime=start_timestamp, limit=limit_number)
    elif data_type == 'spot' and time_interval == '1m':
        return spot_daily_kline_updated(exchange, start, symbol)
    else:
        raise ValueError

    columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trade_num',
               'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']

    df = pd.DataFrame(df, columns=columns, dtype='float')

    # 整理数据
    # 更改时区
    df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms')
    columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num',
               'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']

    df = df[columns]

    df = df[df['candle_begin_time'] < pd.to_datetime(start) + timedelta(hours=16)]
    return df


@retry(retry_times=25, sleep_seconds=15, act_name='获取k线')
def spot_daily_kline_updated(exchange, start, symbol):
    """
    hh

    """

    start_timestamp = int(time.mktime(time.strptime(start, "%Y-%m-%d %H:%M:%S"))) * 1000
    df_list = []

    while True:

        df = exchange.get_klines(symbol=symbol, interval='1m', startTime=start_timestamp, limit=1000)

        columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume',
                   'trade_num',
                   'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']

        df = pd.DataFrame(df, columns=columns, dtype='float')
        start_timestamp = int(df['candle_begin_time'].tail(1))
        df_list.append(df)

        if pd.to_datetime(start_timestamp, unit='ms') >= pd.to_datetime(start) + timedelta(hours=16) or df.shape[
            0] <= 1:
            break

    # 整理数据
    # 更改时区
    df = pd.concat(df_list, ignore_index=True)
    df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms')
    columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num',
               'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']

    df = df[columns]
    df.drop_duplicates(subset=['candle_begin_time'], keep='last', inplace=True)
    df.sort_values('candle_begin_time', inplace=True)
    df.reset_index(drop=True, inplace=True)

    df = df[df['candle_begin_time'] < pd.to_datetime(start) + timedelta(hours=16)]

    return df


def check_data(_database_connect):
    """
    检查数据库每个表的最新数据的情况
    :param _database_connect:
    """
    _symbol_list = _database_connect.table_names()
    print('数据库币种个数', len(_symbol_list))
    the_time_list = []
    _symbol_list = list(set(_symbol_list).difference(set(unlisted_symbol)))
    print('实际交易币种个数', len(_symbol_list))

    for symbol in _symbol_list:
        sql = f"select * from {symbol} order by candle_begin_time desc limit 1"


        df = pd.read_sql(sql, con=_database_connect, index_col='candle_begin_time', parse_dates=['candle_begin_time'])

        if len(df.index._data)==0:

            pass
        else:
            # print(symbol)

            # print(df.index._data[0])
            the_time_list.append(df.index._data[0])
            if (len(set(the_time_list))) == 1:
                pass
            else:
                print(symbol)
                print(df)
                raise ValueError

    if (len(set(the_time_list))) == 1:

        print('全部币种已更新已至', the_time_list[0])

    else:
        print(set(the_time_list))

        raise ValueError
    return the_time_list[0]


def getdate_list(_start_date):
    """
    :param _start_date:
    获得所需要更新k线的日期
    """
    _start_date = pd.to_datetime(_start_date) + timedelta(minutes=1)

    if datetime.datetime.now().hour >= 9:

        end_date = datetime.date.today()

    else:
        end_date = datetime.date.today() - timedelta(days=1)

    _start_date = str(_start_date)[:-9]

    end_date = str(end_date)

    date_list= getEveryDay(_start_date, end_date)

    if len(date_list) == 0:
        print('已全部更新到最新k线 [-.-]')
        exit()
    else:
        return date_list


def getEveryDay(begin_date, end_date):
    """

    :param begin_date: 开始日期
    :param end_date: 结束日期
    :return: 返回开始日期和结束日期之间的日期LIST
    """
    _date_list = []
    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    while begin_date < end_date:
        date_str = begin_date.strftime("%Y-%m-%d")
        _date_list.append(date_str)
        begin_date += datetime.timedelta(days=1)
    return _date_list

