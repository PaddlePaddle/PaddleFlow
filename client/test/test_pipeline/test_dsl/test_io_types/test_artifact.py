#!/usr/bin/env python3
""" unit test for artifact
"""
import copy
import pytest

from paddleflow.pipeline import Artifact
from paddleflow.pipeline.dsl.io_types.placeholder import ArtifactPlaceholder
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

from .mock_step import Step

class TestArtifact(object):
    """ test Artifact
    """
    @pytest.mark.baseinfo
    def test_set_base_info(self):
        """ test Artifat.set_base_info
        """
        art = Artifact()
        step = Step(name="test")
        art.set_base_info(component=step, name="ahz")

        al = ArtifactPlaceholder(name="def", component_full_name="abcd")
        art.set_base_info(component=step, name="ahz", ref=al)
        assert art.ref.name == "def"

        with pytest.raises(PaddleFlowSDKException):
            art.set_base_info(component=step, name="Ahz 32")

        with pytest.raises(PaddleFlowSDKException):
            art.set_base_info(component=step, name="Ahz-32")
        
        with pytest.raises(PaddleFlowSDKException):
            art.set_base_info(component=step, name="32Ahz32")

    @pytest.mark.eq
    def test_eq(self):
        """ test __eq__
        """
        art1 = Artifact()
        art2 = Artifact()
        assert art1 == art2

        step = Step(name="test")
        art1.set_base_info(component=step, name="ahz")
        art2.set_base_info(component=step, name="ahz")
        assert art1 == art2

        art2.set_base_info(component=step, name="ahz123")
        assert art1 != art2

    @pytest.mark.deepcopy
    def test_deepcopy(self):
        """ test deepcopy
        """
        art1 = Artifact()
        art2 = copy.deepcopy(art1)

        assert  art1 == art2 and art1 is not art2
    
        step = Step(name="test")
        art1.set_base_info(component=step, name="ahz")
        art2 = copy.deepcopy(art1)
        assert  art1 == art2 and art1 is not art2

        art1.set_base_info(component=step, name="ahz", ref=123)
        art2 = copy.deepcopy(art1)
        assert  art1 == art2 and art1 is not art2 and art2.ref == 123

    @pytest.mark.str
    def test_str(self):
        """ test str
        """
        step = Step(name="test")
        step.full_name = "pf-entry-points.test"
        
        art1 = Artifact()
        art1.set_base_info(component=step, name="xm")
        assert str(art1) == "{{artifact: pf-entry-points.test.xm}}"

        with pytest.raises(PaddleFlowSDKException):
            art1 = Artifact()
            str(art1)
