'''
Created on Jul 30, 2018

@author: junma
'''
import urllib, requests, json, hmac, base64, hashlib
import logging
import pandas as pd

logging.basicConfig(level=logging.DEBUG)

def http_get_request(url, params, add_to_headers=None):
    headers = {
        "Content-type": "application/x-www-form-urlencoded",
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36',
    }
    if add_to_headers:
        headers.update(add_to_headers)
    postdata = urllib.parse.urlencode(params)
    print( postdata )
    response = requests.get(url, postdata, headers=headers, timeout=5) 
    try:
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            raise Exception( str(response))
        else:
            raise Exception('\nFailed. %s: %s'%( url, response ))
    except BaseException as e:
        raise("httpGet failed, detail is:%s,%s" %(response.text,e))


def http_post_request(url, params, add_to_headers=None):
    headers = {
        "Accept": "application/json",
        'Content-Type': 'application/json'
    }
    if add_to_headers:
        headers.update(add_to_headers)
    postdata = json.dumps(params)
    response = requests.post(url, postdata, headers=headers, timeout=10)
    
    try:
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            raise Exception( str(response))
        else:
            return
    except BaseException as e:
        print("httpPost failed, detail is:%s,%s" %(response.text,e))
        return

def createSign(pParams, method, host_url, request_path, secret_key):
    sorted_params = sorted(pParams.items(), key=lambda d: d[0], reverse=False)
    encode_params = urllib.parse.urlencode(sorted_params)
    payload = [method.upper(), host_url.lower(), request_path, encode_params]
    payload = '\n'.join(payload)
    
    logging.debug( payload )
    
    payload = payload.encode(encoding='UTF8')
    secret_key = secret_key.encode(encoding='UTF8')

    digest = hmac.new(secret_key, payload, digestmod=hashlib.sha256).digest()
    signature = base64.b64encode(digest)
    signature = signature.decode()
    return signature


#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, datetime, logging
logging.basicConfig()

MARKET_URL = "https://api.huobi.pro"

'''
Market data API
'''

# 获取KLine
def get_kline(symbol, period, size=150):
    """
    :param symbol
    :param period: 可选值：{1min, 5min, 15min, 30min, 60min, 1day, 1mon, 1week, 1year }
    :param size: 可选值： [1,2000]
    :return:
    """
    params = {'symbol': symbol,
              'period': period,
              'size': size}

    url = MARKET_URL + '/market/history/kline'
    return http_get_request(url, params)


# 获取marketdepth
def get_depth(symbol, type):
    """
    :param symbol
    :param type: 可选值：{ percent10, step0, step1, step2, step3, step4, step5 }
    :return:
    """
    params = {'symbol': symbol,
              'type': type}
    
    url = MARKET_URL + '/market/depth'
    return http_get_request(url, params)


# 获取tradedetail
def get_trade(symbol):
    """
    :param symbol
    :return:
    """
    params = {'symbol': symbol}

    url = MARKET_URL + '/market/trade'
    return http_get_request(url, params)


# 获取merge ticker
def get_ticker(symbol):
    """
    :param symbol: 
    :return:
    """
    params = {'symbol': symbol}

    url = MARKET_URL + '/market/detail/merged'
    return http_get_request(url, params)

# 获取 Market Detail 24小时成交量数据
def get_detail(symbol):
    """
    :param symbol
    :return:
    """
    params = {'symbol': symbol}

    url = MARKET_URL + '/market/detail'
    return http_get_request(url, params)

# 获取  支持的交易对
def get_symbols(long_polling=None, url=None):
    """
    """
    params = {}
    if long_polling:
        params['long-polling'] = long_polling
    path = '/v1/common/symbols'
    if url is not None:
        MARKET_URL = url
    return http_get_request( MARKET_URL+path, params)


class KType(object):
    K1MIN='1min'
    K5MIN='5min'
    K15MIN='15min'
    K30MIN='30min'
    K1HR='60min'
    K1DAY='1day'
    K1MTH='1mon'
    K1WK='1week'
    K1YR='1year'
    
class KlineWrapper(object):
    def __init__(self):
        pass
    
    @staticmethod
    def kline1day(sym, start_date, end_date):
        """
        @brief 
            T+1 kline of a crypto pair.
            Today's kline is removed, due to incompleteness.
        @param ktype (String): 9 types of kline supported by Huobi.
        @param start_date (String | datetime): "2016-07-01"
        @param end_date (String | datetime): "2017-07-01"
        
        @return df (dataframe): DF containing the kline, with an addition of index: date.
        """
        if type(start_date) == datetime.datetime and type(end_date) == datetime.datetime:
            st,ed = start_date, end_date
        else:
            st = datetime.datetime.strptime(start_date, '%Y-%m-%d')
            ed = datetime.datetime.strptime(end_date, '%Y-%m-%d')
            
        removal = False
        if (datetime.datetime.today()-ed).days == 0:
            print('*'*50)
            print("Discard today's k-line, because it's incomplete.")
            print('*'*50)
            removal = True
        
        dd = (ed-st).days+1
        
        df = pd.DataFrame( 
            get_kline(sym, KType.K1DAY, size=2000 )['data']) # Huobi limit: size=[0,2000]
        if removal: df = df.iloc[1:]
        
        dlist = [ed-datetime.timedelta(days=i) for i in range(0, dd)]
        if removal: dlist = dlist[1:]
        
        df['Date'] = df.id.apply( lambda s: datetime.datetime.fromtimestamp( float( s) ))
        df = df.set_index('Date')
        return df
    
    @staticmethod
    def kline1min(sym, start_date, end_date=None):
        if type(start_date) == datetime.datetime and type(end_date) == datetime.datetime:
            st,ed = start_date, end_date
        else:
            st = datetime.datetime.strptime(start_date, '%Y-%m-%d')
            if not end_date:
                ed = datetime.datetime.now()
            else:
                ed = datetime.datetime.strptime(end_date, '%Y-%m-%d')

        removal = False
        if (datetime.datetime.today()-ed).days == 0:
            print('*'*50)
            print("Discard today's k-line, because it's incomplete.")
            print('*'*50)
            removal = True

        dt = ed-st
        dd = (dt.days*24*3600 + dt.seconds)//60-1

        df = pd.DataFrame(
            get_kline(sym, KType.K1MIN )['data'])
        if removal: df = df.iloc[1:]

        """dlist = [ed-datetime.timedelta(days=i) for i in range(0, dd)]
        if removal: dlist = dlist[1:]
        try:
            assert( df.shape[0] == len(dlist) )
        except AssertionError as e:
            print( 'Resp: ', df.shape[0], '\nDate List: ', len(dlist))
            raise e
        """

        df['Timestamp'] = df.id.apply( lambda s: datetime.datetime.fromtimestamp( float( s) ))
        df = df.set_index('Timestamp')
        return df
 
    @staticmethod
    def kline5min(sym, start_date, end_date=None):
        if type(start_date) == datetime.datetime and type(end_date) == datetime.datetime:
            st,ed = start_date, end_date
        else:
            st = datetime.datetime.strptime(start_date, '%Y-%m-%d')
            if not end_date:
                ed = datetime.datetime.now()
            else:
                ed = datetime.datetime.strptime(end_date, '%Y-%m-%d')
            
        removal = False
        if (datetime.datetime.today()-ed).days == 0:
            print('*'*50)
            print("Discard today's k-line, because it's incomplete.")
            print('*'*50)
            removal = True
        
        dt = ed-st
        dd = (dt.days*24*3600 + dt.seconds)//300+1
        print( '*'*10, dd )
        
        df = pd.DataFrame( 
            get_kline(sym, KType.K5MIN, size=2000 )['data']) # Houbi limit to size=[0,2000]
        if removal: df = df.iloc[1:]
        
        df['Timestamp'] = df.id.apply( lambda s: datetime.datetime.fromtimestamp( float( s) ))
        df = df.set_index('Timestamp')
        return df
        
def __pretty( jsonobj ):
    print( json.dumps( jsonobj, indent='   ' ))
    
if __name__ == '__main__':
    df = KlineWrapper.kline5min('btcusdt', '2020-01-01') #, '2018-07-22')
    print( df.head() )
    print( df.tail())
    print( df.shape )

