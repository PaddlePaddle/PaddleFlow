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


class Placeholder(object):
    """ the placeholder for io types(parameter | artifact), in infer stage, would be use it

    .. note:: should not be created by user
    """
    def __init__(self, name:str, component_full_name: str):
        """ create an instaces of PlaceHolder
        """
        self.name = name
        self.component_full_name = component_full_name


class ArtifactPlaceholder(Placeholder):
    """ the placeholder for artifact
    """ 
    def __init__(self, name:str, component_full_name: str):
        """ create an instaces of ArtifactPlaceholder
        """
        super().__init__(name, component_full_name)
    

class ParameterPlaceholder(Placeholder):
    """ the placeholder for parameter
    """ 
    def __init__(self, name:str, component_full_name: str):
        """ create an instaces of ParameterPlaceholder
        """
        super().__init__(name, component_full_name)