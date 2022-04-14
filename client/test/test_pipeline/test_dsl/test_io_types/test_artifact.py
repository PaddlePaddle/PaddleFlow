#!/usr/bin/env python3
""" unit test for artifact
"""
import copy
import pytest

from paddleflow.pipeline import Artifact
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

from .mock_step import Step

class TestArtifact(object):
    """ test Artifact
    """
    @pytest.mark.tpl
    def test_compile(self):
        """ test compile
        """
        art = Artifact()
        with pytest.raises(PaddleFlowSDKException):
            art.compile()
        
        step = Step(name="test")
        art.set_base_info(step=step, name="ahz")
        assert art.compile() == "{{test.ahz}}"


    @pytest.mark.baseinfo
    def test_set_base_info(self):
        """ test Artifat.set_base_info
        """
        art = Artifact()
        step = Step(name="test")
        art.set_base_info(step=step, name="ahz")

        with pytest.raises(PaddleFlowSDKException):
            art.set_base_info(step=step, name="Ahz 32")

        with pytest.raises(PaddleFlowSDKException):
            art.set_base_info(step=step, name="Ahz-32")
        
        with pytest.raises(PaddleFlowSDKException):
            art.set_base_info(step=step, name="32Ahz32")

    @pytest.mark.eq
    def test_eq(self):
        """ test __eq__
        """
        art1 = Artifact()
        art2 = Artifact()
        assert art1 == art2

        step = Step(name="test")
        art1.set_base_info(step=step, name="ahz")
        art2.set_base_info(step=step, name="ahz")
        assert art1 == art2

        art2.set_base_info(step=step, name="ahz123")
        assert art1 != art2

    @pytest.mark.deepcopy
    def test_deepcopy(self):
        """ test deepcopy
        """
        art1 = Artifact()
        art2 = copy.deepcopy(art1)

        assert  art1 == art2 and art1 is not art2
    
        step = Step(name="test")
        art1.set_base_info(step=step, name="ahz")
        art2 = copy.deepcopy(art1)
        assert  art1 == art2 and art1 is not art2
