import logging
import os
import sys
import time
from unittest import TestCase

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")
from store import Store
from store_warmup_ftx import StoreWarpupFtx

class TestStoreWarpupFtx(TestCase):
    def test_loop_smoke(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(level=logging.DEBUG)

        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        store = Store(start_time=time.time() - 2 * 24 * 60 * 60, logger=logger)
        warmup = StoreWarpupFtx(store=store, logger=logger)
        warmup._loop(raise_error=True)
