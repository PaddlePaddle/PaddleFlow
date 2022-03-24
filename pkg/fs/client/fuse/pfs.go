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

package fuse

import (
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/common/config"
	"paddleflow/pkg/fs/client/meta"
	"paddleflow/pkg/fs/client/vfs"
)

const fsName = "PaddleFlowFS"

type PFS struct {
	debug bool
	fuse.RawFileSystem
}

func NewPaddleFlowFileSystem(debug bool) *PFS {
	return &PFS{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
	}
}

func (fs *PFS) String() string {
	return fsName
}

// If called, provide debug output through the log package.
func (fs *PFS) SetDebug(debug bool) {
	log.Debugf("pfs POSIX pfs SetDebug: %t", debug)
	fs.debug = debug
}

// Lookup is called by the kernel when the VFS wants to know
// about a file inside a directory. Many lookup calls can
// occur in parallel, but only one call happens for each (dir,
// name) pair.
func (fs *PFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	log.Debugf("pfs POSIX pfs Lookup: input[%+v] name[%s]", *header, name)
	ctx := meta.NewContext(cancel, header.Uid, header.Pid, header.Gid)
	entry, code := vfs.GetVFS().Lookup(ctx, vfs.Ino(header.NodeId), name)
	if code != 0 {
		return fuse.Status(code)
	}
	fs.replyEntry(entry, out)
	return fuse.OK
}

// Attributes.
func (fs *PFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	log.Debugf("pfs POSIX pfs GetAttr: input[%+v]", *input)
	ctx := meta.NewContext(cancel, input.Uid, input.Pid, input.Gid)
	inode := vfs.Ino(input.NodeId)
	entry, code := vfs.GetVFS().GetAttr(ctx, inode)
	if code != 0 {
		return fuse.Status(code)
	}
	attrToStat(entry.Ino, entry.Attr, &out.Attr)
	return fuse.OK
}

func (fs *PFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	log.Debugf("pfs POSIX SetAttr: input[%+v]", *input)
	ctx := meta.NewContext(cancel, input.Uid, input.Pid, input.Gid)
	entry, code := vfs.GetVFS().SetAttr(ctx, vfs.Ino(input.NodeId), input.Valid, input.Mode, input.Uid, input.Gid,
		int64(input.Atime), int64(input.Mtime), input.Atimensec, input.Mtimensec, input.Size)
	if code != 0 {
		return fuse.Status(code)
	}
	attrToStat(entry.Ino, entry.Attr, &out.Attr)
	log.Debugf("pfs POSIX SetAttr out is [%+v]", *out)
	return fuse.OK
}

// Modifying structure.
func (fs *PFS) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	log.Debugf("pfs POSIX Mknod: input[%+v] name[%s]", *input, name)
	ctx := meta.NewContext(cancel, input.Uid, input.Pid, input.Gid)
	entry, code := vfs.GetVFS().Mknod(ctx, vfs.Ino(input.NodeId), name, input.Mode, input.Rdev)
	if code != 0 {
		return fuse.Status(code)
	}
	fs.replyEntry(entry, out)
	log.Debugf("pfs POSIX Mknod out is [%+v]", *out)
	return fuse.OK
}

func (fs *PFS) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	log.Debugf("pfs POSIX Mkdir: input[%+v] name[%s]", *input, name)
	ctx := meta.NewContext(cancel, input.Uid, input.Pid, input.Gid)
	entry, code := vfs.GetVFS().Mkdir(ctx, vfs.Ino(input.NodeId), name, input.Mode)
	if code != 0 {
		return fuse.Status(code)
	}
	fs.replyEntry(entry, out)
	log.Debugf("pfs POSIX Mkdir out is [%+v]", *out)
	return fuse.OK
}

func (fs *PFS) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	log.Debugf("pfs POSIX Unlink: header[%+v], name[%s]", *header, name)
	ctx := meta.NewContext(cancel, header.Uid, header.Pid, header.Gid)
	code := vfs.GetVFS().Unlink(ctx, vfs.Ino(header.NodeId), name)
	return fuse.Status(code)
}

func (fs *PFS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	log.Debugf("pfs POSIX Rmdir: header[%+v] name[%s]", *header, name)
	ctx := meta.NewContext(cancel, header.Uid, header.Pid, header.Gid)
	code := vfs.GetVFS().Rmdir(ctx, vfs.Ino(header.NodeId), name)
	return fuse.Status(code)
}

func (fs *PFS) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) fuse.Status {
	log.Debugf("pfs POSIX Rename: input[%+v] oldNamename[%s] newName[%s]", *input, oldName, newName)
	ctx := meta.NewContext(cancel, input.Uid, input.Pid, input.Gid)
	code := vfs.GetVFS().Rename(ctx, vfs.Ino(input.NodeId), oldName, vfs.Ino(input.Newdir), newName, input.Flags)
	return fuse.Status(code)
}

func (fs *PFS) Link(cancel <-chan struct{}, input *fuse.LinkIn, filename string, out *fuse.EntryOut) fuse.Status {
	return fuse.ENOSYS
}

func (fs *PFS) Symlink(cancel <-chan struct{}, header *fuse.InHeader, pointedTo string, linkName string, out *fuse.EntryOut) fuse.Status {
	return fuse.ENOSYS
}

func (fs *PFS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) (out []byte, code fuse.Status) {
	return out, fuse.ENOSYS
}

func (fs *PFS) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	log.Debugf("pfs POSIX Access: input[%+v]", *input)
	ctx := meta.NewContext(cancel, input.Uid, input.Pid, input.Gid)
	code := vfs.GetVFS().Access(ctx, vfs.Ino(input.NodeId), input.Mask)
	return fuse.Status(code)
}

// Extended attributes.

// GetXAttr reads an extended attribute, and should return the
// number of bytes. If the buffer is too small, return ERANGE,
// with the required buffer size.
func (fs *PFS) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (uint32, fuse.Status) {
	log.Debugf("pfs POSIX GetXAttr: header[%+v] attr[%s] dest[%s]", *header, attr, string(dest))
	ctx := meta.NewContext(cancel, header.Uid, header.Pid, header.Gid)
	value, code := vfs.GetVFS().GetXAttr(ctx, vfs.Ino(header.NodeId), attr, uint32(len(dest)))
	if code != 0 {
		return 0, fuse.Status(code)
	}
	copy(dest, value)
	return uint32(len(dest)), fuse.Status(code)
}

// ListXAttr lists extended attributes as '\0' delimited byte
// slice, and return the number of bytes. If the buffer is too
// small, return ERANGE, with the required buffer size.
func (fs *PFS) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	log.Debugf("pfs POSIX ListXAttr: header[%+v] dest[%s]", *header, string(dest))
	ctx := meta.NewContext(cancel, header.Uid, header.Pid, header.Gid)
	value, code := vfs.GetVFS().ListXAttr(ctx, vfs.Ino(header.NodeId), uint32(len(dest)))
	if code != 0 {
		return 0, fuse.Status(code)
	}
	copy(dest, value)
	return uint32(len(dest)), fuse.Status(code)
}

// SetAttr writes an extended attribute.
func (fs *PFS) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	log.Debugf("pfs POSIX SetXAttr: input[%+v] attr[%s] data[%s]", *input, attr, string(data))
	ctx := meta.NewContext(cancel, input.Uid, input.Pid, input.Gid)
	code := vfs.GetVFS().SetXAttr(ctx, vfs.Ino(input.NodeId), attr, data, input.Flags)
	return fuse.Status(code)
}

// RemoveXAttr removes an extended attribute.
func (fs *PFS) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status {
	log.Debugf("pfs POSIX RemoveXAttr: header[%+v] atr[%s]", *header, attr)
	ctx := meta.NewContext(cancel, header.Uid, header.Pid, header.Gid)
	code := vfs.GetVFS().RemoveXAttr(ctx, vfs.Ino(header.NodeId), attr)
	return fuse.Status(code)
}

// File handling.
func (fs *PFS) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	log.Debugf("pfs POSIX Create: input[%+v] name[%s]", *input, name)
	ctx := meta.NewContext(cancel, input.Uid, input.Pid, input.Gid)
	entry, fh, code := vfs.GetVFS().Create(ctx, vfs.Ino(input.NodeId), name, input.Mode, 0, input.Flags)
	if code != 0 {
		return fuse.Status(code)
	}
	out.Fh = fh
	fs.replyEntry(entry, &out.EntryOut)
	log.Debugf("pfs POSIX Create out %+v", *out)
	return fuse.Status(code)
}

func (fs *PFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	log.Debugf("pfs POSIX Open: input[%+v]", *input)
	ctx := meta.NewContext(cancel, input.Uid, input.Pid, input.Gid)
	_, fh, code := vfs.GetVFS().Open(ctx, vfs.Ino(input.NodeId), input.Flags)
	if code != 0 {
		return fuse.Status(code)
	}
	out.Fh = fh
	log.Debugf("pfs POSIX Open out %+v", *out)
	return fuse.Status(code)
}

func (fs *PFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	log.Debugf("pfs POSIX Read: input[%+v]", *input)
	ctx := meta.NewContext(cancel, input.Uid, input.Pid, input.Gid)
	n, code := vfs.GetVFS().Read(ctx, vfs.Ino(input.NodeId), buf, input.Offset, input.Fh)
	if code != 0 {
		return nil, fuse.Status(code)
	}
	return fuse.ReadResultData(buf[:n]), fuse.OK
}

// File locking
func (fs *PFS) GetLk(cancel <-chan struct{}, input *fuse.LkIn, out *fuse.LkOut) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *PFS) SetLk(cancel <-chan struct{}, input *fuse.LkIn) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *PFS) SetLkw(cancel <-chan struct{}, input *fuse.LkIn) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *PFS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	log.Debugf("pfs POSIX Write: input[%+v]", *input)
	ctx := meta.NewContext(cancel, input.Uid, input.Pid, input.Gid)
	code := vfs.GetVFS().Write(ctx, vfs.Ino(input.NodeId), data, input.Offset, input.Fh)
	if code != 0 {
		return 0, fuse.Status(code)
	}
	written := uint32(len(data))
	log.Debugf("pfs POSIX Write written[%d]", written)
	return written, fuse.OK
}

func (fs *PFS) CopyFileRange(cancel <-chan struct{}, input *fuse.CopyFileRangeIn) (written uint32, code fuse.Status) {
	return 0, fuse.ENOSYS
}

func (fs *PFS) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	log.Debugf("pfs POSIX Flush: input [%+v]", input)
	ctx := meta.NewContext(cancel, input.Uid, input.Pid, input.Gid)
	code := vfs.GetVFS().Flush(ctx, vfs.Ino(input.NodeId), input.Fh, input.LockOwner)
	return fuse.Status(code)
}

func (fs *PFS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	log.Debugf("pfs POSIX Fsync: input [%+v]", input)
	ctx := meta.NewContext(cancel, input.Uid, input.Pid, input.Gid)
	code := vfs.GetVFS().Fsync(ctx, vfs.Ino(input.NodeId), int(input.FsyncFlags), input.Fh)
	return fuse.Status(code)
}

func (fs *PFS) Fallocate(cancel <-chan struct{}, input *fuse.FallocateIn) fuse.Status {
	log.Debugf("pfs POSIX Fallocate: input [%+v]", input)
	ctx := meta.NewContext(cancel, input.Uid, input.Pid, input.Gid)
	code := vfs.GetVFS().Fallocate(ctx, vfs.Ino(input.NodeId), uint8(input.Mode), int64(input.Offset), int64(input.Length), input.Fh)
	return fuse.Status(code)
}

// Directory handling
func (fs *PFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	log.Debugf("pfs POSIX OpenDir: input [%+v]", input)
	ctx := meta.NewContext(cancel, input.Uid, input.Pid, input.Gid)
	fh, code := vfs.GetVFS().OpenDir(ctx, vfs.Ino(input.NodeId))
	out.Fh = fh
	return fuse.Status(code)
}

func (fs *PFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	log.Debugf("pfs POSIX ReadDir: input [%+v]", input)
	ctx := meta.NewContext(cancel, input.Uid, input.Pid, input.Gid)
	entries, code := vfs.GetVFS().ReadDir(ctx, vfs.Ino(input.NodeId), input.Fh, input.Offset)
	if code != 0 {
		return fuse.Status(code)
	}
	var de fuse.DirEntry
	for _, e := range entries {
		de.Ino = uint64(e.Ino)
		de.Name = e.Name
		de.Mode = e.Attr.Mode
		if !out.AddDirEntry(de) {
			break
		}
	}
	log.Debugf("pfs POSIX ReadDir result %v", code)
	return fuse.Status(code)
}

func (fs *PFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	log.Debugf("pfs POSIX ReadDirPlus: input [%+v]", input)
	ctx := meta.NewContext(cancel, input.Uid, input.Pid, input.Gid)
	entries, code := vfs.GetVFS().ReadDir(ctx, vfs.Ino(input.NodeId), input.Fh, input.Offset)
	if code != 0 {
		return fuse.Status(code)
	}
	var de fuse.DirEntry
	log.Debugf("pfs POSIX ReadDirPlus entry %+v", entries)
	for _, e := range entries {
		de.Ino = uint64(e.Ino)
		de.Name = e.Name
		de.Mode = e.Attr.Mode
		eo := out.AddDirLookupEntry(de)
		if eo == nil {
			break
		}
		if e.Name == "." || e.Name == ".." {
			continue
		}
	}
	log.Debugf("pfs POSIX ReadDirPlus result %v", code)
	return fuse.Status(code)
}

func (fs *PFS) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
	log.Debugf("pfs POSIX Release: input [%+v]", input)
	ctx := meta.NewContext(cancel, input.Uid, input.Pid, input.Gid)
	vfs.GetVFS().Release(ctx, vfs.Ino(input.NodeId), input.Fh)
}

func (fs *PFS) ReleaseDir(input *fuse.ReleaseIn) {
	log.Debugf("pfs POSIX ReleaseDir: input [%+v]", input)
	cancel := make(chan struct{})
	ctx := meta.NewContext(cancel, input.Uid, input.Pid, input.Gid)
	vfs.GetVFS().ReleaseDir(ctx, vfs.Ino(input.NodeId), input.Fh)
	return
}

func (fs *PFS) StatFs(cancel <-chan struct{}, input *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	log.Debugf("pfs POSIX StatFs: input [%+v]", input)
	ctx := meta.NewContext(cancel, input.Uid, input.Pid, input.Gid)
	st, code := vfs.GetVFS().StatFs(ctx)
	if code != 0 {
		return fuse.Status(code)
	}
	out.NameLen = st.NameLen
	out.Bsize = st.Bsize
	out.Blocks = st.Blocks
	out.Bavail = st.Bavail
	out.Bfree = st.Bavail
	out.Files = st.Files
	out.Ffree = st.Ffree
	out.Frsize = st.Bsize
	return fuse.OK
}

func Server(moutpoint string, opt fuse.MountOptions) (*fuse.Server, error) {
	pfs := NewPaddleFlowFileSystem(false)
	opt.SingleThreaded = true
	fssrv, err := fuse.NewServer(pfs, moutpoint, &opt)
	if err != nil {
		return nil, err
	}
	go fssrv.Serve()
	if err := fssrv.WaitMount(); err != nil {
		// we don't shutdown the serve loop. If the mount does
		// not succeed, the loop won't work and exit.
		return nil, err
	}
	return fssrv, nil
}

func (fs *PFS) replyEntry(entry *meta.Entry, out *fuse.EntryOut) {
	log.Debugf("pfs POSIX replyEntry: input [%+v]", *entry)
	out.NodeId = uint64(entry.Ino)
	// todo:: Generation这个配置是干啥的，得在看看
	out.Generation = 1

	out.SetAttrTimeout(time.Duration(config.FuseConf.Fuse.AttrTimeout))
	// todo:: 增加dirEntry配置，目录和目录项超时分开设置
	out.SetEntryTimeout(time.Duration(config.FuseConf.Fuse.EntryTimeout))
	attrToStat(entry.Ino, entry.Attr, &out.Attr)
	if !config.FuseConf.Fuse.RawOwner {
		out.Uid = config.FuseConf.Fuse.Uid
		out.Gid = config.FuseConf.Fuse.Gid
	}
}
