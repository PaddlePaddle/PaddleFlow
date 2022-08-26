#!/usr/bin/env python3
"""
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

"""
unit test for paddleflow.pipeline.dsl.options.cache_options
"""

import pytest 

from paddleflow.pipeline import CacheOptions
from paddleflow.pipeline import FSScope
from paddleflow.common.exception import PaddleFlowSDKException


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

        with pytest.raises(PaddleFlowSDKException):
            cache_dict = CacheOptions(True, "abd", 1000).compile()
