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

VARIBLE_NAME_REGEX = "^[a-zA-Z][a-zA-Z0-9_]*$"

PIPELINE_NAME_REGEX = "^[A-Za-z_][A-Za-z0-9-_]{1,49}[A-Za-z0-9_]$"

STEP_NAME_REGEX = "^[A-Za-z][A-Za-z0-9-]{1,250}[A-Za-z0-9-]$"

## DSL ERROR CODE
PipelineDSLError = "PipelineDSLError"
