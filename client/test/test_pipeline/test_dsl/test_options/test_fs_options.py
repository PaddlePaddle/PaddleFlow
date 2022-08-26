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

"""
unit test for paddleflow.pipeline.dsl.options.cache_options
"""

import pytest 

from paddleflow.pipeline import MainFS
from paddleflow.pipeline import ExtraFS
from paddleflow.pipeline import FSOptions
from paddleflow.common.exception import PaddleFlowSDKException


class TestFSOptions(object):
    """ unit test for CacheOptions
    """
    @pytest.mark.main
    def test_main_fs(self):
        """ unit test
        """
        main_fs = MainFS(name="ab", sub_path="defe", mount_path="/home/work")
        fs_options = FSOptions(main_fs=main_fs)
        result = fs_options.compile()
        assert result == {"main_fs": {"name": "ab", "sub_path": "defe", "mount_path": "/home/work"}}

        with pytest.raises(PaddleFlowSDKException):
            fs_options = FSOptions(main_fs="abdc")

        fs_options.main_fs = "abc"
        with pytest.raises(PaddleFlowSDKException):
            fs_options.compile()

        main_fs = MainFS(name="ab")
        fs_options = FSOptions(main_fs=main_fs)
        result = fs_options.compile()
        assert result == {"main_fs": {"name": "ab"}}

    @pytest.mark.extra
    def test_extra_fs(self):
        """ unit test for extra_fs
        """
        extra_fs1 = ExtraFS(name="ab", sub_path="defe", mount_path="/home/work", read_only=True)
        extra_fs2 = ExtraFS(name="ab2", sub_path="defe2", read_only=False)

        fs_options = FSOptions(extra_fs=extra_fs1)
        assert fs_options.extra_fs == [extra_fs1]

        fs_options = FSOptions(extra_fs=[extra_fs1, extra_fs2])
        result = fs_options.compile()

        assert result == {
            "extra_fs": [
                {"name": "ab", "sub_path": "defe", "mount_path": "/home/work", "read_only": True},
                {"name": "ab2", "sub_path": "defe2", "read_only": False},
            ]
        }

        fs_options.extra_fs = "abc"
        with pytest.raises(PaddleFlowSDKException):
            fs_options.compile()

        with pytest.raises(PaddleFlowSDKException):
            fs_options = FSOptions(extra_fs="abdc")

    @pytest.mark.all
    def test_all(self):
        """ test all
        """
        main_fs = MainFS(name="ab", sub_path="defe", mount_path="/home/work")
        extra_fs1 = ExtraFS(name="ab", sub_path="defe", mount_path="/home/work", read_only=True)
        extra_fs2 = ExtraFS(name="ab2", sub_path="defe2", read_only=False)

        fs_options = FSOptions(extra_fs=[extra_fs1, extra_fs2], main_fs=main_fs)
        result = fs_options.compile()
        assert result == {
            "main_fs": {"name": "ab", "sub_path": "defe", "mount_path": "/home/work"},
            "extra_fs": [
                {"name": "ab", "sub_path": "defe", "mount_path": "/home/work", "read_only": True},
                {"name": "ab2", "sub_path": "defe2", "read_only": False},
            ]
        }




        