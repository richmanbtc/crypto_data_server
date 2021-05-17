import io
import logging
import click
from flask import Flask, request, send_file
import numpy as np
import pandas as pd
from store import Store
from store_warmup_bybit import StoreWarpupBybit
from store_warmup_ftx import StoreWarpupFtx

app = Flask(__name__)
store = None

logger = app.logger
logger.setLevel(level=logging.DEBUG)

app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

@app.route('/ohlcv.parquet')
def ohlcv():
    exchange = request.args.get('exchange')
    markets = sorted(list(set(request.args.get('markets').split(','))))
    interval = request.args.get('interval', type=int)
    end_time = request.args.get('end_time', type=float)

    dfs = []
    for market in markets:
        df = store.get_df_ohlcv(
            exchange=exchange,
            market=market,
            interval=interval,
            price_type=None
        )
        df_fr = store.get_df_fr(
            exchange=exchange,
            market=market,
        )
        if df_fr is None:
            df['fr'] = np.nan
        else:
            df = df.join(df_fr)
        if end_time is not None:
            df = df[df.index < pd.to_datetime(end_time, unit='s', utc=True)]
        df = df.reset_index()
        df['market'] = market
        df = df.set_index(['market', 'timestamp'])
        dfs.append(df)

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
    return {
        'store': store.status()
    }

def initialize(start_time=None, min_interval=None, warmup=False, logger=None):
    logger.info('initialize start_time={} min_interval={} warmup={}'.format(start_time, min_interval, warmup))

    global store
    store = Store(
        logger=logger,
        start_time=start_time,
    )

    warpup_bybit = StoreWarpupBybit(
        store=store,
        logger=logger,
        min_interval=min_interval,
    )
    if warmup:
        warpup_bybit.start()

    warpup_ftx = StoreWarpupFtx(
        store=store,
        logger=logger,
        min_interval=min_interval,
    )
    if warmup:
        warpup_ftx.start()

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
