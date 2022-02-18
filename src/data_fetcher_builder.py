import ccxt
from ccxt_rate_limiter import scale_limits, wrap_object
from ccxt_rate_limiter.bybit import bybit_limits, bybit_wrap_defs
from ccxt_rate_limiter.ftx import ftx_limits, ftx_wrap_defs
from ccxt_rate_limiter.binance import binance_limits, binance_wrap_defs
from ccxt_rate_limiter.okex import okex_limits, okex_wrap_defs
from ccxt_rate_limiter.kraken import kraken_limits, kraken_wrap_defs
from ccxt_rate_limiter.rate_limiter_group import RateLimiterGroup
from crypto_data_fetcher.bybit import BybitFetcher
from crypto_data_fetcher.ftx import FtxFetcher
from crypto_data_fetcher.binance_future import BinanceFutureFetcher
from crypto_data_fetcher.binance_spot import BinanceSpotFetcher
from crypto_data_fetcher.okex import OkexFetcher
from crypto_data_fetcher.kraken import KrakenFetcher


class DataFetcherBuilder:
    def __init__(self):
        self.bybit_rate_limiter = RateLimiterGroup(limits=scale_limits(bybit_limits(), 0.5))
        self.ftx_rate_limiter = RateLimiterGroup(limits=scale_limits(ftx_limits(), 0.5))
        self.binance_rate_limiter = RateLimiterGroup(limits=scale_limits(binance_limits(), 0.5))
        self.okex_rate_limiter = RateLimiterGroup(limits=scale_limits(okex_limits(), 0.5))
        self.kraken_rate_limiter = RateLimiterGroup(limits=scale_limits(kraken_limits(), 0.2))

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
        elif exchange == 'binance_future':
            return BinanceFutureFetcher(
                ccxt_client=self._create_binance(),
                logger=logger,
            )
        elif exchange == 'binance_spot':
            return BinanceSpotFetcher(
                ccxt_client=self._create_binance(),
                logger=logger,
            )
        elif exchange == 'okex':
            return OkexFetcher(
                ccxt_client=self._create_okex(),
                logger=logger,
            )
        elif exchange == 'kraken':
            return KrakenFetcher(
                ccxt_client=self._create_kraken(),
                logger=logger,
            )
        else:
            raise Exception('unknown exchange {}'.format(exchange))

    def _create_bybit(self):
        client = ccxt.bybit({
            'enableRateLimit': False,
        })
        wrap_object(
            client,
            rate_limiter_group=self.bybit_rate_limiter,
            wrap_defs=bybit_wrap_defs()
        )
        return client

    def _create_ftx(self):
        client = ccxt.ftx({
            'enableRateLimit': False,
        })
        wrap_object(
            client,
            rate_limiter_group=self.ftx_rate_limiter,
            wrap_defs=ftx_wrap_defs()
        )
        return client

    def _create_binance(self):
        client = ccxt.binance({
            'enableRateLimit': False,
        })
        wrap_object(
            client,
            rate_limiter_group=self.binance_rate_limiter,
            wrap_defs=binance_wrap_defs()
        )
        return client

    def _create_okex(self):
        client = ccxt.okex({
            'enableRateLimit': False,
        })
        wrap_object(
            client,
            rate_limiter_group=self.okex_rate_limiter,
            wrap_defs=okex_wrap_defs()
        )
        return client

    def _create_kraken(self):
        client = ccxt.kraken({
            'enableRateLimit': False,
        })
        wrap_object(
            client,
            rate_limiter_group=self.kraken_rate_limiter,
            wrap_defs=kraken_wrap_defs()
        )
        return client
