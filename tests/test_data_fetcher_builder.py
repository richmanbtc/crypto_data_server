import os
import sys
from unittest import TestCase
from crypto_data_fetcher.bybit import BybitFetcher
from crypto_data_fetcher.ftx import FtxFetcher

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
