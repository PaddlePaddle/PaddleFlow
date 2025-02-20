import pytest
from paddleflow.pipeline import DistributedJob
from paddleflow.job import Member


class TestDistributedJob(object):
    """ unit test for DistributedJob
    """
    @pytest.mark.compile
    def test_compile(self):
        distributed_job = DistributedJob(
            framework="paddle"
        )
        distributed_job_dict = distributed_job.compile()

        assert distributed_job_dict == {
            "framework": "paddle",
            "members": [],
        }

        member_worker = Member(role="pworker", replicas=2, image="paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7",
                               command="sleep 30; echo worker")

        member_ps = Member(role="pserver", replicas=2, image="paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7",
                           command="sleep 30; echo ps")

        distributed_job_dict = DistributedJob(
            framework="paddle",
            members=[member_worker, member_ps]
        ).compile()

        assert distributed_job_dict == {
            "framework": "paddle",
            "members": [{'role': 'pworker', 'replicas': 2, 'image': 'paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7',
                         'command': 'sleep 30; echo worker'},
                        {'role': 'pserver', 'replicas': 2, 'image': 'paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7',
                         'command': 'sleep 30; echo ps'}],
        }


