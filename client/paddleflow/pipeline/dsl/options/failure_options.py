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

from .options import Options

from paddleflow.common.exception import PaddleFlowSDKException
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError


FAIL_FAST = "fail_fast"

FAIL_CONTINUE = "continue"

class FailureOptions(Options):
    """ The FailureOptions of Pipeline: Specify how to handle the rest steps of the pipeline  when there is any step failed
    """
    COMPILE_ATTR_MAP = {"strategy": "strategy"}

    def __init__(
            self,
            strategy: str=FAIL_FAST,
            ):
        """ create a new instance of CacheOptions

        Args:
            strategy (str): support two kinds of strategy, default is "fail_fast"::
            
                * "fail_fast": All job would be killed or cancelled
                * "continue": the steps of other branch would be scheduled normally 
            
        """
        if strategy not in [FAIL_CONTINUE, FAIL_FAST]:
            raise PaddleFlowSDKException(PipelineDSLError, f"only support [{FAIL_CONTINUE}, {FAIL_FAST}] strategy")
        self.strategy = strategy
