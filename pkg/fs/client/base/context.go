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

package base

import (
	"os"

	"github.com/hanwen/go-fuse/v2/fuse"
)

type Context struct {
	Uid uint32
	Gid uint32
	Pid uint32
}

func NewContext(ctx *fuse.Context) *Context {
	newCtx := &Context{}
	if ctx != nil {
		newCtx.Pid = ctx.Pid
		newCtx.Gid = ctx.Gid
		newCtx.Uid = ctx.Uid
	}
	return newCtx
}

func NewDefaultContext() *Context {
	return &Context{
		Pid: uint32(os.Getpid()),
		Gid: uint32(os.Getgid()),
		Uid: uint32(os.Getuid()),
	}
}
