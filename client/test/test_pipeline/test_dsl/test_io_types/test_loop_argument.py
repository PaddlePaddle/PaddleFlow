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

""" unit test for paddelflow.pipeline.dsl.io_types.loop_argument
"""
import copy
import pytest

from .mock_step import Step

from paddleflow.pipeline.dsl.io_types import Artifact
from paddleflow.pipeline.dsl.io_types import Parameter
from paddleflow.pipeline.dsl.io_types.loop_argument import _LoopArgument
from paddleflow.pipeline.dsl.io_types.placeholder import ParameterPlaceholder
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

class TestParameter(object):
    """ unit test for Parameter
    """
    def test_loop_arugment(self):
        """ unit test
        """
        step = Step(name="test")
        loop = _LoopArgument([1,2,3], step)

        assert loop.argument == [1,2,3]
        assert str(loop.item) == "{{loop: test.PF_LOOP_ARGUMENT}}"

        with pytest.raises(PaddleFlowSDKException):
            _LoopArgument("defefe", step)
        
        import json
        data = json.dumps([1,2,3])
        loop = _LoopArgument(data, step)
        assert loop.argument == data

        data = "{{artifact: abc.def}}"
        loop = _LoopArgument(data, step)
        assert loop.argument == data

        data = Parameter(123)
        data.set_base_info("abc", step)
        loop = _LoopArgument(data, step)
        assert loop.argument == data

        data = Artifact()
        data.set_base_info("abc", step)
        loop = _LoopArgument(data, step)
        assert loop.argument == data

        with pytest.raises(PaddleFlowSDKException):
            _LoopArgument((1,2), step)
        
        with pytest.raises(PaddleFlowSDKException):
            loop = _LoopArgument(loop.item, step)

        with pytest.raises(PaddleFlowSDKException):
            loop = _LoopArgument("{{loop: test.abc}}", step)    
