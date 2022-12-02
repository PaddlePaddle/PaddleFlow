#!/usr/bin/env python3
""" unit test for paddleflow.pipeline.dsl.utils.util
"""
import os
import pytest
import string
from pathlib import Path

from paddleflow.pipeline.dsl.utils.util import validate_string_by_regex
from paddleflow.pipeline.dsl.utils.util import random_code
from paddleflow.pipeline.dsl.utils.util import trans_10_36


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
    

@pytest.mark.trans_10_36
def test_trans_10_36():
    """ test trans_10_36
    """
    assert 0 == int(trans_10_36(0), 36)
    assert -619636339069370109986290477894975592 == int(trans_10_36(-619636339069370109986290477894975592), 36)
    assert 619636339069370109986290477894975592 == int(trans_10_36(619636339069370109986290477894975592), 36)