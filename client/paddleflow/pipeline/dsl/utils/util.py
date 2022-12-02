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
import random
import string
from typing import Union


def validate_string_by_regex(string: str, regex: str):
    """ Check if the string is legal or not

    Args:
        string {str}: the string which needed to validate
        regex {raw str}: the regex used for validation.

    Returns:
        flag {bool}: return a flag which indicate the name is legal or not
    """
    if not re.match(regex, string):
        return False
    else:
        return True


def random_code(length: int):
    """ generate a random string of the specified length, which consist of letters and digits,
    """
    return "".join(random.sample(string.ascii_letters + string.digits, length))

def trans_10_36(num):
    """ 将10进制数转换成36进制数 
    """
    chars = string.digits + string.ascii_lowercase
    result = ""
    symbol = ""
    
    if num < 0:
        symbol = "-"
        num = -num 
    
    while num >= 36:
        result = chars[num % 36] + result
        num = num // 36 
        
    result = chars[num % 36] + result
    return symbol + result