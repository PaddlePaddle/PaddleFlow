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

from typing import Any

class Options(object):
    """ the base class for Opitons
    """
    COMPILE_ATTR_MAP = {}

    def compile(self):
        """ trans to dict
        """
        self._validate()
        
        result =  {}
        for attr, key in self.COMPILE_ATTR_MAP.items():
            value = getattr(self, attr, None)
            if value is not None:
                if isinstance(value, list):
                    value = self._compile_list(value)
                elif hasattr(value, "compile"):
                    value = value.compile()
                    
                result[key] = value
        
        return result

    def _compile_list(
            self, 
            attribute: Any
            ):
        """ compile list type attribute
        """
        result = []
        for item in attribute:
            if hasattr(item, "compile"):
                result.append(item.compile())
            else:
                result.append(item)
        
        return result

    def _validate(self):
        """ validate
        """
        pass