#!/usr/bin/env python3
"""
unit test for paddleflow.pipeline.dsl.options.cache_options
"""

import pytest 

from paddleflow.pipeline import CacheOptions


class TestCacheOptions(object):
    """ unit test for CacheOptions
    """
    @pytest.mark.compile
    def test_compile(self):
        """ test compile
        """
        cache_options = CacheOptions()
        cache_dict = cache_options.compile()

        assert cache_dict == {
                "enable": False,
                "max_expired_time": -1
                }


        cache_dict = CacheOptions(True, "/home/xiaodu,/xiaodu", 1000).compile()
        assert cache_dict == {
                "enable": True,
                "fs_scope": "/home/xiaodu,/xiaodu",
                "max_expired_time": 1000
                }

