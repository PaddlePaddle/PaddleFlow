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

""" unit test for paddleflow.pipeline.dsl.inferer.ComponentInfer
"""

import pytest 
from pathlib import Path
from hashlib import md5

from .test_component_inferer import base_demo

from paddleflow.pipeline.dsl.inferer.step_inferer import ContainerStepInferer
from paddleflow.pipeline.dsl.component import ContainerStep
from paddleflow.pipeline.dsl.component import DAG
from paddleflow.pipeline.dsl.io_types import Artifact
from paddleflow.pipeline.dsl.io_types import Parameter
from paddleflow.pipeline.dsl.io_types.dicts import InputArtifactDict
from paddleflow.pipeline.dsl.io_types.dicts import OutputArtifactDict
from paddleflow.pipeline.dsl.io_types.dicts import ParameterDict
from paddleflow.pipeline.dsl.options import CacheOptions
from paddleflow.pipeline.dsl.utils.util import validate_string_by_regex
from paddleflow.pipeline.dsl.utils.consts import COMPONENT_NAME_REGEX
from paddleflow.pipeline.dsl.utils.consts import VARIBLE_NAME_REGEX
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException


class TestStepInferer(object):
    """ unit test for componentinfer
    """
    @pytest.mark.command
    def test_infer_command(self):
        """ test infer from loop
        """
        step1, dag1, step2, step3 = base_demo()
        inferer = ContainerStepInferer(step2)

        # 1. from parameter and artifact which not belong to any component
        with pytest.raises(PaddleFlowSDKException):
            step2.command = str(Parameter(23))
            inferer._infer_from_command()

        with pytest.raises(PaddleFlowSDKException):
            step2.command = str(Artifact())
            inferer._infer_from_command()

        # 2.  from it's self's artifact or parameter
        ## 2.1 parameter
        step2.command = str(step2.parameters["p2"])
        inferer._infer_from_command()
        assert step2.command == "{{p2}}"
        assert len(step2.parameters) == 2

        ## 2.2 artifact
        step2.command = str(step2.inputs["in1"])
        inferer._infer_from_command()
        assert step2.command == "{{in1}}"
        assert len(step2.inputs) == 1

        # 3. 直接引用其余节点的输出artifact或者parameter
        ## 3.1 artifact
        step2.command = str(step1.outputs["art2"])
        inferer._infer_from_command()
        assert len(step2.inputs) == 2

        new_art = ""
        for k, v in step2.inputs.items():
            if k == "in1":
                continue

            new_art = k
            assert new_art.startswith("command_art")
            
            m = md5()
            m.update("pf-entry-point.step1.art2".encode())
            assert int(new_art[len("command_art_"):], 36) == int(m.hexdigest(), 16)
            assert v.ref.name == "art2"
            assert v.ref.component_full_name == "pf-entry-point.step1"
            
            assert step2.command == "{{"  + new_art + "}}" and v.name == new_art
        
        step2.inputs.pop(new_art)
        
        ## 3.2 parameter
        step2.command = str(step1.parameters["num3"])
        inferer._infer_from_command()
        assert len(step2.parameters) == 3

        new_param = ""
        for k, v in step2.parameters.items():
            if k.startswith("p"):
                continue

            new_param = k
            assert new_param.startswith("command_param") 
            m = md5()
            m.update("pf-entry-point.step1.num3".encode())
            assert int(new_param[len("command_param_"):], 36) == int(m.hexdigest(), 16)
            assert v.ref.name == "num3"
            assert v.ref.component_full_name == "pf-entry-point.step1"
            assert step2.command == "{{"  + new_param + "}}" and v.name == new_param
        
        step2.parameters.pop(new_param)

        # 4. 引用父节点的 loop_argument
        dag1.loop_argument = [1,2,3]
        step2.command = str(dag1.loop_argument.item)
        inferer._infer_from_command()
        assert len(step2.parameters) == 3

        new_param = ""
        for k, v in step2.parameters.items():
            if k.startswith("p"):
                continue
            
            new_param = k
            assert new_param.startswith("command_param")
            m = md5()
            m.update((dag1.full_name + ".PF_LOOP_ARGUMENT").encode())
            assert int(new_param[len("command_param_"):], 36) == int(m.hexdigest(), 16)  
            assert step2.command == "{{"  + new_param + "}}" and v.name == new_param and \
                v.ref == "{{PF_PARENT.PF_LOOP_ARGUMENT}}"
        
        step2.parameters.pop(new_param)

        # 5. 引用本节点的 loop item
        step2.loop_argument = [1,2,3]
        step2.command = str(step2.loop_argument.item)
        inferer._infer_from_command()
        assert len(step2.parameters) == 2
        assert step2.command == "{{PF_LOOP_ARGUMENT}}"

        # 5. referenece parameter from sub component
        with pytest.raises(PaddleFlowSDKException):
            step3.command = str(step2.parameters["p1"])
            inferer3 = ContainerStepInferer(step3)
            inferer3._infer_from_command()

        step3.command = f'{step2.outputs["abc"]}'
        inferer3 = ContainerStepInferer(step3)
        inferer3._infer_from_command()
        assert len(step3.inputs) == 1

        
        step2.command = str(step2.loop_argument.item) + "abc"
        inferer._infer_from_command()
        assert len(step2.parameters) == 2
        assert step2.command == "{{PF_LOOP_ARGUMENT}}abc"

        
    @pytest.mark.env
    def test_env(self):
        """ test conditon
        """
        step1, dag1, step2, step3 = base_demo()
        inferer = ContainerStepInferer(step2)

        env1 = "env1"
        # 1. from parameter and artifact which not belong to any component
        with pytest.raises(PaddleFlowSDKException):
            step2.env[env1] = f"{Parameter(23)} > 10 "
            inferer._infer_from_env()

        with pytest.raises(PaddleFlowSDKException):
            step2.env[env1] = Artifact()
            inferer._infer_from_env()

        # 2.  from it's self's artifact or parameter
        ## 2.1 parameter
        step2.env[env1] = step2.parameters['p2']
        inferer._infer_from_env()
        assert step2.env[env1] == "{{p2}}"
        assert len(step2.parameters) == 2

        ## 2.2 artifact
        with pytest.raises(PaddleFlowSDKException):
            step2.env[env1] =  f"{step2.inputs['in1']} > 10"
            inferer._infer_from_env()
        

        # 3. 直接引用其余节点的parameter
        
        ## 3.1 parameter
        step2.env[env1] = f'{step1.parameters["num3"]} > 10'
        inferer._infer_from_env()
        assert len(step2.parameters) == 3

        new_param = ""
        for k, v in step2.parameters.items():
            if k.startswith("p"):
                continue

            new_param = k
            assert new_param.startswith("env_param")
            
            m = md5()
            m.update(b"pf-entry-point.step1.num3")
            assert int(new_param[len("env_param_"):], 36) == int(m.hexdigest(), 16) 
            assert v.ref.name == "num3"
            assert v.ref.component_full_name == "pf-entry-point.step1"
            assert step2.env[env1] == "{{" + f"{new_param}" + "}} > 10" and v.name == new_param
        
        step2.parameters.pop(new_param)

        # 4. 引用父节点的 loop_argument
        dag1.loop_argument = [1,2,3]
        step2.env[env1] = f'{dag1.loop_argument.item} > 10'
        inferer._infer_from_env()
        assert len(step2.parameters) == 3

        new_param = ""
        for k, v in step2.parameters.items():
            if k.startswith("p"):
                continue

            new_param = k
            m = md5()
            m.update((dag1.full_name + ".PF_LOOP_ARGUMENT").encode())
            assert int(new_param[len("env_param_"):], 36) == int(m.hexdigest(), 16) 
            assert new_param.startswith("env_param")
            assert step2.env[env1] == "{{" + f"{new_param}" + "}} > 10" and v.name == new_param and \
                v.ref == "{{PF_PARENT.PF_LOOP_ARGUMENT}}"
        
        step2.parameters.pop(new_param)

        # 5. 引用本节点的 loop item
        step2.loop_argument = [1,2,3]
        step2.env[env1] = f'{step2.loop_argument.item} > 10'
        inferer._infer_from_env()
        assert len(step2.parameters) == 2
        assert step2.env[env1] == "{{PF_LOOP_ARGUMENT}} > 10"

        # 5. referenece parameter from sub component
        with pytest.raises(PaddleFlowSDKException):
            step3.env[env1] = f'{step2.parameters["p1"]} > 10'
            inferer3 = ContainerStepInferer(step3)
            inferer3._infer_from_env()
