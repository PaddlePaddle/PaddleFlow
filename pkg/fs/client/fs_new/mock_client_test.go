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
package fs

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func getTestMockClient() *MockClient {
	os.MkdirAll("./mock", 0755)
	mockClient := &MockClient{
		pathPrefix: "./mock",
	}
	return mockClient
}

func TestMockClient(t *testing.T) {
	defer os.RemoveAll("./mock")
	client := getTestMockClient()

	newDir1 := "/Dir1/Dir1"
	err := client.MkdirAll(newDir1, 0755)
	assert.Equal(t, nil, err)

	newDir2 := "/Dir1/Dir2"
	err = client.Mkdir(newDir2, 0755)
	assert.Equal(t, nil, err)
	isEmptyDir, err := client.IsEmptyDir(newDir2)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, isEmptyDir)
	isDir, err := client.IsDir(newDir2)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, isDir)

	names, err := client.ListDir("/Dir1")
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(names))
	fmt.Println(names[0])
	fmt.Println(names[1])
	return

	filePath1 := newDir1 + "/file1.txt"
	file, err := client.CreateFile(filePath1, []byte("test create file"))
	assert.Equal(t, nil, err)
	assert.NotNil(t, file)

	err = client.Chmod(filePath1, 0777)
	assert.Equal(t, nil, err)

	_, err = client.Stat(filePath1)
	assert.Equal(t, nil, err)

	size, err := client.Size(filePath1)
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(16), size)

	newFile, err := client.Open(filePath1)
	assert.Equal(t, nil, err)

	err = client.SaveFile(newFile, newDir1, "file2.txt")
	assert.Equal(t, nil, err)
	newFile.Close()

	filePath3 := newDir1 + "/file3.txt"
	err = client.Copy(filePath1, filePath3)
	assert.Equal(t, nil, err)

	newDir3 := "/Dir1/Dir3"
	err = client.Copy(newDir1, newDir3)
	assert.Equal(t, nil, err)

	dirNames, err := client.Readdirnames("/Dir1/Dir3", -1)
	assert.Equal(t, nil, err)
	assert.Equal(t, 3, len(dirNames))

	err = client.Remove(filePath3)
	assert.Equal(t, nil, err)

	err = client.RemoveAll("Dir1")
	assert.Equal(t, nil, err)
}
