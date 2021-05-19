import re
import threading
import time
import traceback
import ccxt

class StoreWarpupBybit:
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
        intervals = [
            60,
            3 * 60,
            5 * 60,
            15 * 60,
            30 * 60,
            60 * 60,
            120 * 60,
            240 * 60,
            360 * 60,
            720 * 60,
            24 * 60 * 60,
        ]

        price_types = [
            None,
            'mark',
            'index',
            'premium_index',
        ]

        bybit = ccxt.bybit()
        symbols = bybit.v2PublicGetSymbols()['result']
        symbols = map(lambda x: x['name'], symbols)

        for symbol in symbols:
            # 期限付き先物は未対応
            if re.search(r'\d', symbol):
                continue
            for interval in intervals:
                for price_type in price_types:
                    is_premium_index_minute = price_type == 'premium_index' and interval == 60 # fr計算に必要
                    if self.min_interval is not None and interval < self.min_interval and not is_premium_index_minute:
                        continue
                    try:
                        self.store.get_df_ohlcv(
                            exchange='bybit',
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



