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

package location_awareness

import (
	"crypto/md5"
	"encoding/hex"
	"testing"
)

func getMountID(clusterID, nodeName, mountPoint string) string {
	hash := md5.Sum([]byte(clusterID + nodeName + mountPoint))
	return hex.EncodeToString(hash[:])
}

func TestListMountNodesByFsID(t *testing.T) {

}
