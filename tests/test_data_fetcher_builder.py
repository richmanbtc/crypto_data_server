import os
import sys
from unittest import TestCase
from crypto_data_fetcher.bybit import BybitFetcher
from crypto_data_fetcher.ftx import FtxFetcher
from crypto_data_fetcher.binance_future import BinanceFutureFetcher
from crypto_data_fetcher.okex import OkexFetcher

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")
from data_fetcher_builder import DataFetcherBuilder

class TestDataFetcherBuilder(TestCase):
    def test_bybit(self):
        builder = DataFetcherBuilder()
        fetcher = builder.create_fetcher(exchange='bybit')
        self.assertIsInstance(fetcher, BybitFetcher)

    def test_ftx(self):
        builder = DataFetcherBuilder()
        fetcher = builder.create_fetcher(exchange='ftx')
        self.assertIsInstance(fetcher, FtxFetcher)

    def test_binance_future(self):
        builder = DataFetcherBuilder()
        fetcher = builder.create_fetcher(exchange='binance_future')
        self.assertIsInstance(fetcher, BinanceFutureFetcher)

    def test_okex(self):
        builder = DataFetcherBuilder()
        fetcher = builder.create_fetcher(exchange='okex')
        self.assertIsInstance(fetcher, OkexFetcher)
