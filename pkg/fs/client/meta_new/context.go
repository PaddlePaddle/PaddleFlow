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

package meta_new

import (
	"os"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
)

type Context struct {
	base.Context
	cancel <-chan struct{}
}

func NewContext(cancel <-chan struct{}, Uid, Pid, Gid uint32) *Context {
	ctx := &Context{
		cancel: cancel,
	}
	ctx.Uid = Uid
	ctx.Pid = Pid
	ctx.Gid = Gid
	return ctx
}

func NewEmptyContext() *Context {
	ctx := &Context{
		cancel: nil,
	}
	ctx.Uid = uint32(os.Getuid())
	ctx.Pid = uint32(os.Getpid())
	ctx.Gid = uint32(os.Getgid())
	return ctx
}
