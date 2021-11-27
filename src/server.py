from concurrent.futures import ThreadPoolExecutor
import io
import logging
import os
import click
from flask import Flask, request, send_file
from flask_caching import Cache
import numpy as np
import pandas as pd
import psutil
from store import Store
from store_warmup_bybit import StoreWarpupBybit
from store_warmup_ftx import StoreWarpupFtx
from store_warmup_binance_future import StoreWarpupBinanceFuture
from store_warmup_binance_spot import StoreWarpupBinanceSpot
from store_warmup_okex import StoreWarpupOkex

app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

logger = app.logger
logger.setLevel(level=logging.DEBUG)

cache = Cache(config={
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 60 * 60,
    'CACHE_THRESHOLD': 32
})
cache.init_app(app)

store = None

def make_cache_key(*args, **kwargs):
    path = request.path
    args = str(hash(frozenset(request.args.items())))
    return (path + args).encode('utf-8')

@app.route('/ohlcv.parquet')
@cache.cached(key_prefix=make_cache_key)
def ohlcv():
    exchange = request.args.get('exchange')
    markets = sorted(list(set(request.args.get('markets').split(','))))
    interval = request.args.get('interval', type=int)
    start_time = request.args.get('start_time', type=float)
    end_time = request.args.get('end_time', type=float)
    mark = request.args.get('mark', 0, type=int) != 0
    index = request.args.get('index', 0, type=int) != 0

    def get_df(market):
        df = store.get_df_ohlcv(
            exchange=exchange,
            market=market,
            interval=interval,
            price_type=None
        )
        if df is None:
            app.logger.warning('df is None {} {} {}'.format(exchange, market, interval))
            return None
        if df.shape[0] == 0:
            app.logger.warning('df.shape[0] == 0 {} {} {}'.format(exchange, market, interval))
            return None

        # join fr
        df_fr = store.get_df_fr(
            exchange=exchange,
            market=market,
        )
        if df_fr is None:
            df['fr'] = np.nan
        else:
            df = df.join(df_fr)

        # join mark price
        if mark:
            if exchange == 'bybit':
                df_mark = store.get_df_ohlcv(
                    exchange=exchange,
                    market=market,
                    interval=interval,
                    price_type='mark'
                )
            else:
                df_mark = None
            if df_mark is None:
                df['op_mark'] = np.nan
                df['hi_mark'] = np.nan
                df['lo_mark'] = np.nan
                df['cl_mark'] = np.nan
            else:
                df = df.join(df_mark, rsuffix='_mark')

        # join index price
        if index:
            if exchange == 'ftx':
                df_index = store.get_df_ohlcv(
                    exchange=exchange,
                    market=market,
                    interval=interval,
                    price_type='index'
                )
            else:
                df_index = None
            if df_index is None:
                df['op_index'] = np.nan
                df['hi_index'] = np.nan
                df['lo_index'] = np.nan
                df['cl_index'] = np.nan
            else:
                df = df.join(df_index, rsuffix='_index')

        if start_time is not None:
            df = df[pd.to_datetime(start_time, unit='s', utc=True) <= df.index]
        if end_time is not None:
            df = df[df.index < pd.to_datetime(end_time, unit='s', utc=True)]
        df = df.reset_index()
        df['market'] = market
        df = df.set_index(['market', 'timestamp'])
        return df

    with ThreadPoolExecutor(16) as executor:
        dfs = executor.map(get_df, markets)
        dfs = [df for df in dfs if df is not None]

    df = pd.concat(dfs)
    df = df.sort_values(['market', 'timestamp'])

    f = io.BytesIO()
    df.to_parquet(f)
    f.seek(0)

    return send_file(
        f,
        mimetype='application/octet-stream',
        as_attachment=True,
        attachment_filename='%ohlcv.parquet'
    )

@app.route('/status')
def status():
    process = psutil.Process(os.getpid())
    return {
        'store': store.status(),
        'memory': {
            'rss': process.memory_info().rss
        }
    }

def initialize(start_time=None, min_interval=None, warmup=False, logger=None):
    logger.info('initialize start_time={} min_interval={} warmup={}'.format(start_time, min_interval, warmup))

    global store
    store = Store(
        logger=logger,
        start_time=start_time,
    )

    if warmup:
        warpup_bybit = StoreWarpupBybit(
            store=store,
            logger=logger,
            min_interval=min_interval,
        )
        warpup_bybit.start()

        warpup_ftx = StoreWarpupFtx(
            store=store,
            logger=logger,
            min_interval=min_interval,
        )
        warpup_ftx.start()

        warpup_binance_future = StoreWarpupBinanceFuture(
            store=store,
            logger=logger,
            min_interval=min_interval,
        )
        warpup_binance_future.start()

        warpup_binance_spot = StoreWarpupBinanceSpot(
            store=store,
            logger=logger,
            min_interval=min_interval,
        )
        warpup_binance_spot.start()

        warpup_okex = StoreWarpupOkex(
            store=store,
            logger=logger,
            min_interval=min_interval,
        )
        warpup_okex.start()

@click.command()
@click.option('--start_time', type=int, default=None, help='data start time')
@click.option('--min_interval', type=int, default=None, help='min interval')
@click.option('--port', type=int, default=5000, help='port')
@click.option('--host', default='0.0.0.0', help='host')
def start(start_time, min_interval, port, host):
    initialize(start_time=start_time, min_interval=min_interval, warmup=True, logger=app.logger)
    app.run(debug=False, port=port, host=host, threaded=True)

if __name__ == '__main__':
    start()
