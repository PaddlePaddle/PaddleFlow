/*
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

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

package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetFsNameAndUserNameByFsID(t *testing.T) {
	type args struct {
		fsID string
	}
	tests := []struct {
		name         string
		args         args
		wantUserName string
		wantFsName   string
	}{
		{
			name: "fs-root-abc",
			args: args{
				fsID: "fs-root-abc",
			},
			wantFsName:   "abc",
			wantUserName: "root",
		},
		{
			name: "fs-root-v-xxx",
			args: args{
				fsID: "fs-root-v-xxx",
			},
			wantFsName:   "v-xxx",
			wantUserName: "root",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotUserName, gotFsName, err := GetFsNameAndUserNameByFsID(tt.args.fsID)
			assert.Nil(t, err)
			if gotUserName != tt.wantUserName {
				t.Errorf("GetFsNameAndUserNameByFsID() gotUserName = %v, want %v", gotUserName, tt.wantUserName)
			}
			if gotFsName != tt.wantFsName {
				t.Errorf("GetFsNameAndUserNameByFsID() gotFsName = %v, want %v", gotFsName, tt.wantFsName)
			}
		})
	}
}
