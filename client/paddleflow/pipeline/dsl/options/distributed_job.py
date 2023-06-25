from paddleflow.job import Member

from .options import Options
from typing import List

from paddleflow.common.exception import PaddleFlowSDKException
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError


class DistributedJob(object):
    """ distributed job which used by step
    """

    def __int__(
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
        self.members = members

    def compile(self):
        """ trans to dict
        """
        result = {}

        if not self.framework:
            raise PaddleFlowSDKException(PipelineDSLError, "DistributedJob's framework attribute cannot empty")

        result["framework"] = self.framework

        if self.members:
            result["members"] = []

            if not isinstance(self.members, list):
                self.members = [self.members]

            for member in self.members:
                if not isinstance(member, Member):
                    raise PaddleFlowSDKException(PipelineDSLError, "DistributedJob's members attribute should be a list of Member instance")
                result["members"].append(self.members)

        return result
# class Member(object):
#
#     def __init__(
#             self,
#             role: str,
#             replicas: int,
#             image: str,
#             port: int,
#             command: str,
#             queue: str,
#             priority: str,
#     ):
#
#         self.role = role
#         self.replicas = replicas
#         self.image = image
#         self.port = port
#         self.command = command
#         self.queue = queue
#         self.priority = priority
#
#     def compile(self):#         """ trans to dict
#         """
#         result = {}
#
#         if not self.role:
#             raise PaddleFlowSDKException(PipelineDSLError, "Distributed Job's role attribute cannot empty")
#         result["role"] = self.role
#
#         if not self.replicas:
#             raise PaddleFlowSDKException(PipelineDSLError, "Distributed Job's replicas attribute cannot empty")
#         result["replicas"] = self.replicas
#
#         if self.image:
#             result["image"] = self.image
#
#         if self.port:
#             result["port"] = self.port
#
#         if self.command:
#             result["command"] = self.command
#
#         if self.queue:
#             result["queue"] = self.queue
#
#         if self.priority:
#             result["priority"] = self.priority
#         return result
