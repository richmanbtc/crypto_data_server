import io
import logging
import os
import sys
import time
from unittest import TestCase
import pandas as pd

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")
from server import app, initialize

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)

handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class TestServer(TestCase):
    def setUp(self):
        logger = logging.getLogger(__name__)
        self.logger = logger
        self.app = app.test_client()
        start_time = time.time() - 2 * 24 * 60 * 60
        initialize(start_time=start_time, warmup=False, logger=logger)

    def test_ohlcv(self):
        start_time = time.time() - 2 * 24 * 60 * 60
        end_time = time.time() - 24 * 60 * 60
        res = self.app.get('/ohlcv.parquet?exchange=bybit&markets=BTCUSD,ETHUSD&interval=3600&start_time={}&end_time={}'.format(start_time, end_time))

        f = io.BytesIO()
        f.write(res.data)
        f.seek(0)
        df = pd.read_parquet(f)

        self.assertTrue('op_mark' not in df.columns)
        self.assertTrue('hi_mark' not in df.columns)
        self.assertTrue('lo_mark' not in df.columns)
        self.assertTrue('cl_mark' not in df.columns)

        self.assertEqual(sorted(df.reset_index()['market'].unique().tolist()), ['BTCUSD', 'ETHUSD'])

    def test_ohlcv_without_start_time_and_end_time(self):
        res = self.app.get('/ohlcv.parquet?exchange=bybit&markets=BTCUSD,ETHUSD&interval=3600')

        f = io.BytesIO()
        f.write(res.data)
        f.seek(0)
        df = pd.read_parquet(f)

        self.assertEqual(sorted(df.reset_index()['market'].unique().tolist()), ['BTCUSD', 'ETHUSD'])

    def test_ohlcv_future(self):
        initialize(start_time=None, warmup=False, logger=self.logger)

        res = self.app.get('/ohlcv.parquet?exchange=ftx&markets=ATOM-20191227&interval=3600')

        f = io.BytesIO()
        f.write(res.data)
        f.seek(0)
        df = pd.read_parquet(f)

        self.assertEqual(sorted(df.reset_index()['market'].unique().tolist()), ['ATOM-20191227'])

    def test_ohlcv_future_df_is_none(self):
        initialize(start_time=None, warmup=False, logger=self.logger)

        # ALGO-20190329 df is None (curl https://ftx.com/api/markets/ALGO-20190329/candles?resolution=3600)
        res = self.app.get('/ohlcv.parquet?exchange=ftx&markets=ALGO-20190329,ATOM-20191227&interval=3600')

        f = io.BytesIO()
        f.write(res.data)
        f.seek(0)
        df = pd.read_parquet(f)

        self.assertEqual(sorted(df.reset_index()['market'].unique().tolist()), ['ATOM-20191227'])

    def test_ohlcv_with_mark(self):
        start_time = time.time() - 2 * 24 * 60 * 60
        end_time = time.time() - 24 * 60 * 60
        res = self.app.get('/ohlcv.parquet?exchange=bybit&markets=BTCUSD,ETHUSD&interval=3600&start_time={}&end_time={}&mark=1'.format(start_time, end_time))

        f = io.BytesIO()
        f.write(res.data)
        f.seek(0)
        df = pd.read_parquet(f)

        self.assertTrue('op_mark' in df.columns)
        self.assertTrue('hi_mark' in df.columns)
        self.assertTrue('lo_mark' in df.columns)
        self.assertTrue('cl_mark' in df.columns)

        self.assertEqual(sorted(df.reset_index()['market'].unique().tolist()), ['BTCUSD', 'ETHUSD'])

    def test_status_smoke(self):
        res = self.app.get('/status')
        self.assertEqual(res.status, '200 OK')

