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

VARIBLE_NAME_REGEX = "^[A-Za-z_][A-Za-z0-9_]{0,49}$"

PIPELINE_NAME_REGEX = "^[A-Za-z_][A-Za-z0-9_]{0,49}$"

COMPONENT_NAME_REGEX = "^[a-zA-Z][a-zA-Z0-9-]{0,29}$"

COMPONENT_FULL_NAME_REGEX = f"({COMPONENT_NAME_REGEX[1:-1]}\.)*({COMPONENT_NAME_REGEX[1:]})"

DSL_TEMPLATE_REGEX = "\{\{\s*(?P<type>parameter|artifact|loop):\s*(?P<component_full_name>" + \
    COMPONENT_FULL_NAME_REGEX.replace("$", "") + ")\." + \
    f"(?P<var_name>{VARIBLE_NAME_REGEX[1:-1]})" + "\s*\}\}"

## DSL ERROR CODE
PipelineDSLError = "PipelineDSLError"

PARAM_NAME_CODE_LEN = 6

ENTRY_POINT_NAME = "PF-ENTRY-POINT"