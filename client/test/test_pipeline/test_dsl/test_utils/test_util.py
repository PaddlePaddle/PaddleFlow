#!/usr/bin/env python3
""" unit test for paddleflow.pipeline.dsl.utils.util
"""
import os
import pytest
import string
from pathlib import Path

from paddleflow.pipeline.dsl.utils.util import validate_string_by_regex
from paddleflow.pipeline.dsl.utils.util import random_code


@pytest.mark.regex
def test_validate_string_by_regex():
    """ test validate_string_by_regex
    """
    assert validate_string_by_regex("ahz", ".*") == True
    assert validate_string_by_regex("123245d", "\d*$") == False

@pytest.mark.random_code
def test_random_code():
    """ test  random_code
    """
    assert len(random_code(8)) == 8
    code = random_code(6)

    for c in code:
        assert c in string.ascii_letters + string.digits

    assert len(code) == 6