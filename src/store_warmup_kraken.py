import threading
import time
import traceback
import ccxt

class StoreWarpupKraken:
    def __init__(self, store=None, logger=None, min_interval=None):
        self.store = store
        self.logger = logger
        self.min_interval = min_interval

    def start(self):
        self.thread = threading.Thread(target=self._run)
        self.thread.start()

    def _run(self):
        while True:
            time.sleep(60)
            try:
                self._loop()
            except KeyboardInterrupt:
                raise
            except Exception as e:
                self.logger.error('exception ' + traceback.format_exc())
                time.sleep(60)

    def _loop(self, raise_error=False):
        # https://docs.kraken.com/rest/#operation/getOHLCData
        intervals = [
            60,
            60 * 5,
            60 * 15,
            60 * 30,
            60 * 60,
            60 * 60 * 4,
            60 * 60 * 24,
            60 * 60 * 24 * 7,
        ]

        price_types = [
            None,
        ]

        kraken = ccxt.kraken()
        markets = list(kraken.publicGetAssetPairs()['result'].keys())

        # USD pair only
        markets = [x for x in markets if x.endswith('USD')]

        for symbol in markets:
            for interval in intervals:
                if self.min_interval is not None and interval < self.min_interval:
                    continue

                for price_type in price_types:
                    try:
                        self.store.get_df_ohlcv(
                            exchange='kraken',
                            market=symbol,
                            interval=interval,
                            price_type=price_type,
                            force_fetch=True
                        )
                    except KeyboardInterrupt:
                        raise
                    except Exception as e:
                        if raise_error:
                            raise
                        self.logger.error('exception ' + traceback.format_exc())
                        time.sleep(60)
