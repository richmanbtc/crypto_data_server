
import ccxt
from ccxt_rate_limiter import scale_limits, wrap_object
from ccxt_rate_limiter.bybit import bybit_limits, bybit_wrap_defs
from ccxt_rate_limiter.ftx import ftx_limits, ftx_wrap_defs
from ccxt_rate_limiter.rate_limiter_group import RateLimiterGroup
from crypto_data_fetcher.bybit import BybitFetcher
from crypto_data_fetcher.ftx import FtxFetcher

class DataFetcherBuilder:
    def __init__(self):
        self.bybit_rate_limiter = RateLimiterGroup(limits=scale_limits(bybit_limits(), 0.5))
        self.ftx_rate_limiter = RateLimiterGroup(limits=scale_limits(ftx_limits(), 0.5))

    def _create_bybit(self):
        bybit = ccxt.bybit({
            'enableRateLimit': False,
        })
        wrap_object(
            bybit,
            rate_limiter_group=self.bybit_rate_limiter,
            wrap_defs=bybit_wrap_defs()
        )
        return bybit

    def _create_ftx(self):
        ftx = ccxt.ftx({
            'enableRateLimit': False,
        })
        wrap_object(
            ftx,
            rate_limiter_group=self.ftx_rate_limiter,
            wrap_defs=ftx_wrap_defs()
        )
        return ftx

    def create_fetcher(self, exchange=None, logger=None):
        if exchange == 'bybit':
            return BybitFetcher(
                ccxt_client=self._create_bybit(),
                logger=logger,
            )
        elif exchange == 'ftx':
            return FtxFetcher(
                ccxt_client=self._create_ftx(),
                logger=logger,
            )
        else:
            raise Exception('unknown exchange {}'.format(exchange))

