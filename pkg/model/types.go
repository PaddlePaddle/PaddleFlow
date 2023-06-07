/*
Copyright (c) 2023 PaddlePaddle Authors. All Rights Reserve.

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

package model

import (
	"fmt"

	"database/sql/driver"
	"encoding/json"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
)

type Resource resources.Resource

func (r *Resource) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("failed to unmarshal Resource sturct: %v", value)
	}
	result := resources.Resource{}
	err := json.Unmarshal(bytes, &result)
	*r = Resource(result)
	return err
}

func (r Resource) Value() (driver.Value, error) {
	return json.Marshal(resources.Resource(r))
}

type Map map[string]string

func (m *Map) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("failed to unmarshal Map sturct: %v", value)
	}
	result := map[string]string{}
	err := json.Unmarshal(bytes, &result)
	*m = result
	return err
}

func (m Map) Value() (driver.Value, error) {
	return json.Marshal(map[string]string(m))
}
