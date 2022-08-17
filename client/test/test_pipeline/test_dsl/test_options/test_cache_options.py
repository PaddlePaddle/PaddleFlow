#!/usr/bin/env python3
"""
unit test for paddleflow.pipeline.dsl.options.cache_options
"""

import pytest 

from paddleflow.pipeline import CacheOptions
from paddleflow.pipeline import FSScope


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

        fs_scope1 = FSScope("xiaoming", "/abce,/defeg")
        fs_scope2 = FSScope("xiaoming2", "/abce2,/defeg2")
        cache_dict = CacheOptions(True, [fs_scope1, fs_scope2], 1000).compile()
        assert cache_dict == {
                "enable": True,
                "fs_scope": [
                        {"name": "xiaoming", "path": "/abce,/defeg"}, 
                        {"name": "xiaoming2", "path": "/abce2,/defeg2"}
                        ],
                "max_expired_time": 1000
                }

