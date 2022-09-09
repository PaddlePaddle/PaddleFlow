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

package ufs_new

import (
	"syscall"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/utils"
)

func (fs *localMount) GetAttr(name string) (*base.FileInfo, error) {
	path := fs.GetPath(name)
	var err error = nil
	st := syscall.Stat_t{}
	if name == "" {
		// When GetAttr is called for the toplevel directory, we always want
		// to look through symlinks.
		err = syscall.Stat(path, &st)
	} else {
		err = syscall.Lstat(path, &st)
	}
	if err != nil {
		return nil, err
	}

	fmode := utils.StatModeToFileMode(int(st.Mode))

	return &base.FileInfo{
		Name:  name,
		Path:  path,
		Size:  st.Size,
		Mtime: uint64(st.Mtim.Sec),
		IsDir: fmode.IsDir(),
		Owner: utils.UserName(int(st.Uid)),
		Group: utils.GroupName(int(st.Gid)),
		Mode:  fmode,
		Sys:   st,
	}, nil
}
