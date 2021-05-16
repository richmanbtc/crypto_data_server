import io
import logging
import os
import sys
import time
from unittest import TestCase
import pandas as pd

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")
from server import app, initialize

class TestServer(TestCase):
    def setUp(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(level=logging.DEBUG)

        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        self.app = app.test_client()
        start_time = time.time() - 2 * 24 * 60 * 60
        initialize(start_time=start_time, warmup=False, logger=logger)

    def test_ohlcv(self):
        end_time = time.time() - 24 * 60 * 60
        res = self.app.get('/ohlcv.parquet?exchange=bybit&markets=BTCUSD,ETHUSD&interval=3600&end_time={}'.format(end_time))

        f = io.BytesIO()
        f.write(res.data)
        f.seek(0)
        df = pd.read_parquet(f)

        self.assertEqual(sorted(df.reset_index()['market'].unique().tolist()), ['BTCUSD', 'ETHUSD'])

    def test_ohlcv_without_end_time(self):
        res = self.app.get('/ohlcv.parquet?exchange=bybit&markets=BTCUSD,ETHUSD&interval=3600')

        f = io.BytesIO()
        f.write(res.data)
        f.seek(0)
        df = pd.read_parquet(f)

        self.assertEqual(sorted(df.reset_index()['market'].unique().tolist()), ['BTCUSD', 'ETHUSD'])
