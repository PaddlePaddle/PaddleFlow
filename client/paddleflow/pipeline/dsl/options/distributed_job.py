from paddleflow.job import Member

from .options import Options
from typing import List

from paddleflow.common.exception import PaddleFlowSDKException
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError


class DistributedJob(object):
    """ distributed job which used by step
    """

    def __init__(
            self,
            framework: str,
            members: List[Member] = None,
    ):
        """ create a new instance for DistributedJob

        Args:
            framework (str): the framework of distributed job
            members List(Member): the members defined in distributed job
        """
        self.framework = framework

        if members:
            if not isinstance(members, list):
                members = [members]

            for member in members:
                if not isinstance(member, Member):
                    raise PaddleFlowSDKException(PipelineDSLError, "DistributedJob's members attribute should be a list of Member instance")

            self.members = members
        else:
            self.members = None

    def compile(self):
        """ trans to dict
        """
        result = {}

        if not self.framework:
            raise PaddleFlowSDKException(PipelineDSLError, "DistributedJob's framework attribute cannot empty")

        result["framework"] = self.framework

        if self.members:

            if not isinstance(self.members, list):
                self.members = [self.members]

            for member in self.members:
                if not isinstance(member, Member):
                    raise PaddleFlowSDKException(PipelineDSLError, "DistributedJob's members attribute should be a list of Member instance")
                result["members"].append(self.members)

        return result