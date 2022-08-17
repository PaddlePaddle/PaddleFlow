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
import re
from typing import Dict

from paddleflow.pipeline.dsl.component import Component
from paddleflow.pipeline.dsl.component import DAG
from paddleflow.pipeline.dsl.component import Step
from paddleflow.pipeline.dsl.io_types import Artifact
from paddleflow.pipeline.dsl.io_types import Parameter
from paddleflow.pipeline.dsl.io_types import ArtifactPlaceholder
from paddleflow.pipeline.dsl.io_types import ParameterPlaceholder
from paddleflow.pipeline.dsl.utils.util import random_code
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError 
from paddleflow.pipeline.dsl.utils.consts import PARAM_NAME_CODE_LEN
from paddleflow.pipeline.dsl.utils.consts import DSL_TEMPLATE_REGEX
from paddleflow.pipeline.dsl.sys_params import PF_LOOP_ARGUMENT
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

class ComponentInferer(object):
    """ base inferer
    """
    FILED_TO_PREFIX = {
        "loop_argument": "loop",
        "condition": "condition",
    }

    def __init__(self, component: Component):
        """ create an instance of ComponentInfer

        Args:
            component: the component will be compile
        """
        self._component = component

        self._error_msg_prefix = f"infer artifact or paramter for {component.__class__.__name__}[{component.name}] failed: "

    def _generate_error_msg(self, msg: str):
        """ generate error msg

        Args:
            msg: base error msg

        """
        return self._error_msg_prefix + msg
    
    def infer(self, env: Dict):
        """ infer artifact, parameter and deps
        """
        self._infer_from_loop_argument()
        self._infer_from_condition()
        self._infer_deps()
        self._infer_env()

    def _generate_infer_pattern(self, io_type: str):
        """ generate the re.pattern
        """
        prefix_pattern = "|".join(self.FILED_TO_PREFIX.values())
        pattern = f"^({prefix_pattern})_{io_type}_[A-Za-z0-9]{PARAM_NAME_CODE_LEN}$"
        return re.compile(pattern)
    
    def _infer_from_loop_argument(self):
        """ infer parameter and input artifact from loop_argument
        """
        if self._component.loop_argument is None:
            return 

        prefix = self.FILED_TO_PREFIX["loop_argument"]
        loop = self._component.loop_argument
        if isinstance(loop.argument, Parameter) or isinstance(loop.argument, Artifact):
            if loop.obj is None:
                err_msg = self._generate_error_msg(f"cannot find which components the {type(loop)}[{loop}] belong to")
                raise PaddleFlowSDKException(PipelineDSLError, err_msg)
            if loop.obj == self:
                return 
            else:
                if isinstance(loop.argument, Parameter):
                    self._validate_inferred_parameter(loop.argument.component.full_name)

                    name = self._generate_art_or_param_name(prefix, "param")
                    self._component.parameters[name] = loop.argument
                    self._component.loop_argument = self._component.parameters[name]
                    return
                else:
                    name = self._generate_art_or_param_name(prefix, "art")
                    self._component.inputs[name] = loop
                    self._component.loop_argument = self._component.inputs[name]
                    return
        
        # todo: loop 最多只能有引用一个模板。
        if isinstance(loop, str):
            tpls = self._parse_template_from_string(loop)
            if len(tpls) == 0:
                return 
            elif len(tpls) == 1:
                if tpls[0] != loop:
                    err_msg = self._generate_error_msg(f"loop_arugment filed is not support template join with other string")
                    raise PaddleFlowSDKException(PipelineDSLError, err_msg)
                    
                inferred_result = self._infer_by_template(loop, prefix)
                if isinstance(inferred_result, Parameter):
                    self._component.loop_argument = self._component.parameters[inferred_result.name]
                elif isinstance(inferred_result, Artifact):
                    self._component.loop_argument = self._component.inputs[inferred_result.name]
                else:
                    self._component = inferred_result
                return 
            else: 
                err_msg = self._generate_error_msg(f"loop_arugment filed is not support template join with other template")
                raise PaddleFlowSDKException(PipelineDSLError, err_msg)

        return 

    def _infer_from_condition(self):
        """ set condition attribute
        """
        if self._component.condition is None:
            return 
        
        condition = self._component.condition
        if condition and not isinstance(condition, str):
            raise PaddleFlowSDKException(PipelineDSLError, 
                self._generate_error_msg("the condition attribute of component should be an instance of str"))

        tpls = self._parse_template_from_string(condition)
        prefix = self.FILED_TO_PREFIX["condition"]
        for tpl in tpls:
            arg = self._infer_by_template(tpl, prefix)
            if isinstance(arg, str):
                condition.replace(tpl, arg)
            else:
                condition.replace(tpl, "{{" + f"{arg.name}" + "}}")
            
        self._condition = condition

    def _parse_template_from_string(self, s: str):
        """ parse parameter or artifact template from string
        """
        pattern = re.compile(DSL_TEMPLATE_REGEX)
        matches = pattern.finditer(s)

        if matches is None:
            return []

        return [tpl for tpl in matches]
    
    def _infer_by_template(self, tpl: re.Match, filed_type: str):
        """ add parameter or input artifact according template
        """
        # template: {{parameter: $fullname.$parameter_name}} or {{artifact: $fullname.$artifact_name}}
        if tpl.group("type") == "artifact":
            return self._infer_art_by_template(tpl, filed_type)
        elif tpl.group("type") == "parameter":
            return self._infer_param_by_template(tpl, filed_type)
        elif tpl.group("type") == "loop":
            return self._infer_by_loop_template(tpl, filed_type)
    
    def _infer_art_by_template(self, tpl: re.Match, filed_type: str):
        """ infer artifact by template
        """
        full_name = tpl.group("component_full_name")
        ref_art_name = tpl.group("var_name")

        if full_name == self._component.full_name:
            return self._component.input[ref_art_name]
        
        art = ArtifactPlaceholder(ref_art_name, full_name)
        art_name = self._generate_art_or_param_name(filed_type, "art")
        self._component.inputs[art_name] = art

        return self._component.inputs[art_name]
        
    def _infer_param_by_template(self, tpl: re.Match, filed_type: str):
        """ infer parameter by template
        """
        full_name = tpl.group("component_full_name")
        self._validate_inferred_parameter(full_name)

        ref_param_name = tpl.group("var_name")

        if full_name == self._component.full_name:
            return self._component.parameters[ref_param_name]
        
        param = ParameterPlaceholder(ref_param_name, full_name)
        param_name = self._generate_art_or_param_name(filed_type, "param")
        self._component.parameters[param_name] = param

        return self._component.parameters[param_name]

    def _infer_by_loop_template(self, tpl: re.Match, filed_type: str):
        """ infer parameter or artifact by loop template
        """
        full_name = tpl.group("component_full_name")
        if full_name == self._component.full_name:
            return PF_LOOP_ARGUMENT
        else:
            if not self._is_parent(full_name):
                raise PaddleFlowSDKException(PipelineDSLError, self._generate_error_msg(
                    f"component can only reference itself or its parent component's loop item"))
            
            param_name = self._generate_art_or_param_name(filed_type, "param")
            self._component.parameters[param_name] = "{{PF_PARET.PF_LOOP_ARGUMENT}}"
            return self._component.parameters[param_name]
        
    def _validate_inferred_parameter(self, ref_cp_full_name):
        """ validate inferred parameter is illegal or not: parameter cannot passed from child to parent
        """
        ref_parent_name = ref_cp_full_name.rsplit(".", 1)[0]
        if not self._component.full_name.startswith(ref_parent_name + ".") and \
            not self._component.full_name.startswith(ref_cp_full_name + "."):
            raise PaddleFlowSDKException(PipelineDSLError, 
                self._generate_error_msg(f"only support  reference Parameter from sibling component, " + \
                    "ancestor component, and ancestor sibling component"))

    def _generate_art_or_param_name(self, prefix: str, io_type):
        """ generate art 
        """
        suffix = random_code(PARAM_NAME_CODE_LEN)
        return "_".join([prefix, io_type, suffix])

    def _infer_deps(self):
        """ infer deps from parameter and artifact
        """
        self._infer_deps_from_param()
        self._infer_deps_from_art()

    def _infer_deps_from_param(self):
        """ infer deps for component according it's parameter filed
        """
        for _, param in self._component.parameters:
            if not isinstance(param.ref, Parameter):
                continue

            self._infer_deps_by_cp_full_name(param.ref.full_name)

    def _infer_deps_from_art(self):
        """ infer deps for component according it's inputs filed
        """
        for _, art in self._component.inputs:
            self._infer_deps_by_cp_full_name(art.ref.full_name)
    
    def _infer_deps_by_cp_full_name(self, full_name: str):
        """ infer deps
        """
        if self._is_parent(full_name):
            return 
        
        parent_name = self._get_parent_full_name()
        up_name = full_name.replace(parent_name + ".", "").split(".")[0]
        self._component._dependences.add(up_name)

    def _is_parent(self, component_full_name:str):
        """ is parent
        """
        return self._component.full_name.startswith(component_full_name + ".")
    
    def _get_parent_full_name(self):
        """ get parent_component's full name
        """
        return self._component.full_name.rsplit(".", 1)[0]

    def _infer_env(self):
        """ infer env for component
        """
        raise NotImplementedError