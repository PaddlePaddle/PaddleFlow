#!/usr/bin/env python3
""" unit test for paddleflow.pipeline.dsl.utils.util
"""
import os
import pytest
from pathlib import Path

from paddleflow.pipeline.dsl.utils.util import validate_string_by_regex


@pytest.mark.regex
def test_validate_string_by_regex():
    """ test validate_string_by_regex
    """
    assert validate_string_by_regex("ahz", ".*") == True
    assert validate_string_by_regex("123245d", "\d*$") == False
