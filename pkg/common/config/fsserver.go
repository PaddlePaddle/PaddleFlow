/*
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
*/

package config

import (
	"os"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
)

// DefaultPV the global default pv instance
var DefaultPV *apiv1.PersistentVolume

// DefaultPVC the global default pvc instance
var DefaultPVC *apiv1.PersistentVolumeClaim

// InitDefaultPV initialize the default pv instance
func InitDefaultPV(path string) error {
	reader, err := os.Open(path)
	if err != nil {
		return err
	}
	defer reader.Close()
	return yaml.NewYAMLOrJSONDecoder(reader, 1024).Decode(&DefaultPV)
}

// InitDefaultPVC initialize the default pvc instance
func InitDefaultPVC(path string) error {
	reader, err := os.Open(path)
	if err != nil {
		return err
	}
	defer reader.Close()
	return yaml.NewYAMLOrJSONDecoder(reader, 1024).Decode(&DefaultPVC)
}
