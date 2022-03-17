#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserved

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

import paddle
import os

cont_min_ = [0, -3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
cont_max_ = [20, 600, 100, 50, 64000, 500, 100, 50, 500, 10, 10, 10, 50]
cont_diff_ = [20, 603, 100, 50, 64000, 500, 100, 50, 500, 10, 10, 10, 50]
hash_dim_ = 1000001
continuous_range_ = range(1, 14)
categorical_range_ = range(14, 40)


class WideDeepDataset:
    """WideDeepDataset"""
    def __init__(self, data_path):
        """__init__"""
        self.file_list = [os.path.join(data_path, x)
                          for x in os.listdir(data_path)]

    def line_process(self, line):
        """line_process"""
        features = line.rstrip('\n').split('\t')
        dense_feature = []
        sparse_feature = []
        for idx in continuous_range_:
            if features[idx] == "":
                dense_feature.append(0.0)
            else:
                dense_feature.append(
                    (float(features[idx]) - cont_min_[idx - 1]) / cont_diff_[idx - 1])
        for idx in categorical_range_:
            sparse_feature.append(
                [hash(str(idx) + features[idx]) % hash_dim_])
        label = [int(features[0])]
        return [dense_feature] + sparse_feature + [label]

    def __call__(self):
        """__call__"""
        for file in self.file_list:
            with open(file, 'r') as f:
                for line in f:
                    input_data = self.line_process(line)
                    yield input_data
