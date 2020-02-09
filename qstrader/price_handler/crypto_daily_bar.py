'''
Created on Feb 04, 2020

@author: junma
'''
import os

import pandas as pd

from ..price_parser import PriceParser
from .base import AbstractBarPriceHandler
from ..event import BarEvent
from huobi.HuobiPublicServices import KlineWrapper

import logging
#FORMAT = "%(asctime)-15s %(clientip)s %(user)-8s %(message)s"
FORMAT = "%(asctime)-15s %(user)-8s %(message)s"
logging.basicConfig(format=FORMAT)
#d = {'clientip': 'localhost', 'user': 'junma'}

class CryptoBarPriceHandler(AbstractBarPriceHandler):
    """
    @brief:
    Open-High-Low-Close-Volume (OHLCV) data
    for each requested financial instrument and stream those to
    the provided events queue as BarEvents.
    
    @param events_queue:
    @param init_tickers: Symbols of the instruments involved in the trading session.
    @param start_date:
    @param end_date:
    @param calc_adj_return (Bool): Whether or not calculate extra column for "adjusted close".
    """
    def __init__(
        self, events_queue,
        init_tickers=None,
        start_date=None, end_date=None,
        calc_adj_returns=False, **args):
        
        args.update({'init_tickers': init_tickers,
                     'start_date': start_date, 'end_date': end_date,
                     'calc_adj_returns': calc_adj_returns })
        self.events_queue = events_queue
        self.continue_backtest = True
        self.tickers = {}
        self.tickers_data = {}
        
        self.start_date = start_date
        self.end_date = end_date
        
        if init_tickers is not None:
            for ticker in init_tickers:
                self.__subscribe_ticker(ticker)
                
        self.bar_stream = self._merge_sort_ticker_data()
        self.calc_adj_returns = calc_adj_returns
        if self.calc_adj_returns:
            self.adj_close_returns = []

    def __open_ticker_price_csv(self, ticker):
        """
        Opens the CSV files containing the equities ticks from
        the specified CSV data directory, converting them into
        them into a pandas DataFrame, stored in a dictionary.
        """
        ticker_path = os.path.join('..', 'data','crypto', "%s.csv" % ticker)
        if not os.path.exists( ticker_path):
            df = KlineWrapper.kline1day(ticker, self.start_date, self.end_date)
            #df = KlineWrapper.kline5min(ticker, self.start_date, self.end_date)
            df.columns = ['Amount', 'Close', 'Count', 'High', 'id', 'Low', 'Open', 'Volume']
            df['Adj Close'] = df.Close #FIXME: Assume nothing to be adjusted for crypto ( or currency?).
            self.tickers_data[ticker] = df
            df.to_csv( ticker_path )
            pass
        else:
            print("Using existing CSV: %s"%ticker_path)
            self.tickers_data[ticker] = pd.read_csv(
                ticker_path, header=0, parse_dates=True,
                index_col=0
            )
        self.tickers_data[ticker]["Ticker"] = ticker

    def __subscribe_ticker(self, ticker):
        """
        @brief 
        Normalize dataframe by selecting columns from source data,
        and maintain a copy of the normalized data in "self.tickers" for
        any concerned securities in the trading session.
        
        Normalization:
                    {'close':<val>, 
                    'adj_close':<val>, 
                    'timestamp':<ts> }
        
        @param ticker (String): equity name. For examples, APPL, SPX, AGG,...
        """
        if ticker not in self.tickers:
            try:
                self.__open_ticker_price_csv(ticker)
                dft = self.tickers_data[ticker]
                row0 = dft.iloc[0]

                close = PriceParser.parse(row0["Close"])
                adj_close = PriceParser.parse(row0["Adj Close"])

                ticker_prices = {
                    "close": close,
                    "adj_close": adj_close,
                    "timestamp": dft.index[0]
                }
                self.tickers[ticker] = ticker_prices
            except OSError:
                print(
                    "Could not subscribe ticker %s "
                    "as no data CSV found for pricing." % ticker
                )
        else:
            print(
                "Could not subscribe ticker %s "
                "as is already subscribed." % ticker
            )
            
    def _merge_sort_ticker_data(self):
        """
        Concatenates all of the separate equities DataFrames
        into a single DataFrame that is time ordered, allowing tick
        data events to be added to the queue in a chronological fashion.

        Note that this is an idealised situation, utilised solely for
        backtesting. In live trading ticks may arrive "out of order".
        """
        df = pd.concat(self.tickers_data.values()).sort_index()
        start = None
        end = None
        if self.start_date is not None:
            start = df.index.searchsorted(self.start_date)
        if self.end_date is not None:
            end = df.index.searchsorted(self.end_date)
        # This is added so that the ticker events are
        # always deterministic, otherwise unit test values
        # will differ
        df['colFromIndex'] = df.index
        df = df.sort_values(by=["colFromIndex", "Ticker"])
        if start is None and end is None:
            return df.iterrows()
        elif start is not None and end is None:
            return df.iloc[start:].iterrows()
        elif start is None and end is not None:
            return df.iloc[:end].iterrows()
        else:
            return df.iloc[start:end].iterrows()

    def _create_event(self, index, period, ticker, row):
        """
        @brief 
        Convert a row of dataframe to a BarEvent
        """
        open_price = PriceParser.parse(row["Open"])
        high_price = PriceParser.parse(row["High"])
        low_price = PriceParser.parse(row["Low"])
        close_price = PriceParser.parse(row["Close"])
        adj_close_price = PriceParser.parse(row["Adj Close"])
        volume = int(row["Volume"])
        bev = BarEvent(
            ticker, index, period, open_price,
            high_price, low_price, close_price,
            volume, adj_close_price
        )
        return bev

    def _store_event(self, event):
        """
        Store price event for closing price and adjusted closing price
        """
        ticker = event.ticker
        # If the calc_adj_returns flag is True, then calculate
        # and store the full list of adjusted closing price
        # percentage returns in a list
        # TODO: Make this faster
        if self.calc_adj_returns:
            prev_adj_close = self.tickers[ticker][
                "adj_close"
            ] / float(PriceParser.PRICE_MULTIPLIER)
            cur_adj_close = event.adj_close_price / float(
                PriceParser.PRICE_MULTIPLIER
            )
            self.tickers[ticker][
                "adj_close_ret"
            ] = cur_adj_close / prev_adj_close - 1.0
            self.adj_close_returns.append(self.tickers[ticker]["adj_close_ret"])
        self.tickers[ticker]["close"] = event.close_price
        self.tickers[ticker]["adj_close"] = event.adj_close_price
        self.tickers[ticker]["timestamp"] = event.time

    def stream_next(self):
        """
        @brief
            Push a new BarEvent to the queue.
        """
        try:
            index, row = next(self.bar_stream)
        except StopIteration:
            self.continue_backtest = False
            return
        # Obtain all elements of the bar from the dataframe
        ticker = row["Ticker"]
        period = 86400  # Seconds in a day
        # Create the tick event for the queue
        bev = self._create_event(index, period, ticker, row)
        # Store event
        self._store_event(bev)
        # Send event to queue
        self.events_queue.put(bev)
