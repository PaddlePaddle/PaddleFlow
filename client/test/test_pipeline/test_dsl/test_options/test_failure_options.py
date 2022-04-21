#!/usr/bin/env python3
"""
unit test for paddleflow.pipeline.dsl.options.cache_options
"""

import pytest 

from paddleflow.pipeline import FailureOptions
from paddleflow.pipeline import FAIL_FAST
from paddleflow.pipeline import FAIL_CONTINUE

class TestCacheOptions(object):
    """ unit test for CacheOptions
    """
    @pytest.mark.compile
    def test_compile(self):
        """ test compile
        """
        fail = FailureOptions()
        assert fail.compile() == {"strategy": FAIL_FAST}

        fail = FailureOptions(strategy=FAIL_CONTINUE)
        assert fail.compile() == {"strategy": FAIL_CONTINUE}
