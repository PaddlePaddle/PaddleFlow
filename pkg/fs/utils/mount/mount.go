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

package mount

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"k8s.io/utils/mount"

	"paddleflow/pkg/fs/utils/io"
)

const (
	MountCmdName                        = "mount"
	UMountCmdName                       = "umount"
	MountPointCmdName                   = "mountpoint"
	IsNotMountPoint                     = "is not a mountpoint"
	IsNotMountPointZhCN                 = "不是一个挂载点"
	ErrEndpointNotConnected             = "transport endpoint is not connected"
	TransportEndpointIsNotConnected     = "Transport endpoint is not connected"
	TransportEndpointIsNotConnectedZhCN = "传输端点尚未连接"
	DirectoryNotEmpty                   = "directory not empty"
	DirectoryNotEmptyZhCN               = "目录非空"
	NotMounted                          = "not mounted"

	CmdTimeoutEnv            = "CMD_TIMEOUT_IN_SECOND"
	DefaultCmdTimoutInSecond = 60
	KillPGroupCmd            = "kill -9 -%d"

	readOnly = "ro"
)

func CleanUpMountPoints(paths []string) error {
	var retErr error

	if len(paths) == 0 {
		return nil
	}

	cleanUp := func(path string, cleanAll bool) error {
		isMountPoint, err := IsMountPoint(path)
		if err != nil && !isMountPoint {
			if exist, exErr := io.Exist(path); exErr != nil {
				log.Errorf("check path[%s] exist err: %v", path, exErr)
				if strings.Contains(exErr.Error(), ErrEndpointNotConnected) {
					return ManualUnmount(path)
				}
				return exErr
			} else if !exist {
				return nil
			}
			log.Errorf("check path[%s] mountpoint failed: %v", path, err)
			return err
		}

		if isMountPoint {
			return CleanUpMountPoint(path)
		}
		log.Infof("path [%s] is not a mountpoint, begin to remove path[%s]", path, path)
		if err := os.Remove(path); err != nil {
			log.Errorf("remove path[%s] failed: %v", path, err)
			return err
		}
		return nil
	}

	for _, path := range paths {
		log.Infof("cleanup mountPoint in path[%s] start", path)
		if err := cleanUp(path, false); err != nil {
			log.Errorf("cleanup path[%s] failed: %v", path, err)
			retErr = err
		}
	}
	return retErr
}

func IsLikelyMountPoint(path string) (bool, error) {
	notMountPoint, err := mount.New("").IsLikelyNotMountPoint(path)
	return !notMountPoint, err
}

func IsMountPoint(path string) (bool, error) {
	output, err := ExecCmdWithTimeout(MountPointCmdName, []string{path})
	if err != nil {
		if strings.Contains(string(output), IsNotMountPoint) ||
			strings.Contains(string(output), IsNotMountPointZhCN) {
			return false, nil
		}
		// path is mount point, but the mount point can not access
		if strings.Contains(string(output), TransportEndpointIsNotConnected) ||
			strings.Contains(string(output), TransportEndpointIsNotConnectedZhCN) {
			return true, fmt.Errorf("%s:%s", string(output), err)
		}
		return false, fmt.Errorf("%s:%s", string(output), err)
	}
	return true, nil
}

func CleanUpMountPoint(path string) error {
	// If extensiveMountPointCheck=false, IsLikelyNotMountPoint method will be used,
	// which cannot recognize a mount point generated by linux mount bind command.
	err := mount.CleanupMountPoint(path, mount.New(""), true)
	if err != nil {
		log.Warnf("cleanup mountPoint[%s] failed: %v", path, err)
		if strings.Contains(err.Error(), DirectoryNotEmpty) ||
			strings.Contains(err.Error(), DirectoryNotEmptyZhCN) {
			return err
		}
		log.Infof("CleanUpMountPoint [%s] failed. start force ForceUnmountAndRemove", path)
		return ForceUnmountAndRemove(path)
	}
	return nil
}

func ForceUnmountAndRemove(path string) error {
	log.Errorf("force unmount mountPoint[%s]", path)
	err := ForceUnmount(path)
	if err != nil {
		log.Errorf("force unmount mountPoint[%s] failed: %v", path, err)
		return err
	}
	err = os.Remove(path)
	if err != nil {
		log.Errorf("remove path[%s] failed: %v", path, err)
		return err
	}
	return nil
}

func ManualUnmount(path string) error {
	output, err := ExecCmdWithTimeout(UMountCmdName, []string{path})
	if err != nil {
		log.Errorf("exec cmd[umount %s] failed: [%v], output[%v]", path, err, string(output))
		return ForceUnmount(path)
	}
	return nil
}

func ForceUnmount(path string) error {
	output, err := ExecCmdWithTimeout(UMountCmdName, []string{"-lf", path})
	if err != nil {
		log.Errorf("exec cmd[umount -lf %s] failed: [%v], output[%v]", path, err, string(output))
		return err
	}
	return nil
}

func ExecCmdWithTimeout(name string, args []string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), GetCmdTimeout())
	defer cancel()
	newCmd := exec.CommandContext(ctx, name, args...)
	newCmd.Env = append(os.Environ())
	log.Debugf("newCmd is %s", newCmd.String())

	sysProcAttr := &syscall.SysProcAttr{
		Setpgid: true,
	}
	newCmd.SysProcAttr = sysProcAttr

	waitChan := make(chan struct{}, 1)
	defer close(waitChan)
	// 超时杀掉进程组或正常退出
	go func() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				log.Errorf("exec Command[%s] timeout", newCmd.String())
				pgid := newCmd.Process.Pid
				if name == "su" {
					killCmd := fmt.Sprintf(KillPGroupCmd, pgid)
					output, err := ExecCmdWithTimeout("su", []string{"-c", killCmd})
					if err != nil {
						log.Errorf("kill pgid[%d] failed: %s", pgid, string(output))
					}
				} else {
					if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil {
						log.Errorf("kill pgid[%d] failed: %s", pgid, err.Error())
					}
				}
			}
		case <-waitChan: // 确保协程可以正常退出
		}
	}()

	output, err := newCmd.CombinedOutput()
	if err != nil {
		log.Debugf("exec Command[%s] output:%s", newCmd.String(), string(output))
		return output, err
	}
	return output, nil
}

func GetCmdTimeout() time.Duration {
	timeout := os.Getenv(CmdTimeoutEnv)
	if len(timeout) == 0 {
		return time.Duration(DefaultCmdTimoutInSecond) * time.Second
	}

	timeoutInt, err := strconv.Atoi(timeout)
	if err != nil {
		log.Errorf("failed to get timeout: [%v], use default timeout: [%d]", err, DefaultCmdTimoutInSecond)
		return time.Duration(DefaultCmdTimoutInSecond) * time.Second
	}
	return time.Duration(timeoutInt) * time.Second
}

func ExecMountBind(sourcePath, targetPath string, ReadOnly bool) ([]byte, error) {
	cmdName := MountCmdName
	args := []string{"--bind"}
	if ReadOnly {
		args = append(args, "-o", readOnly)
	}
	args = append(args, sourcePath, targetPath)
	return ExecCmdWithTimeout(cmdName, args)
}

func ExecMount(sourcePath, targetPath string, args []string) ([]byte, error) {
	cmdName := MountCmdName
	args = append(args, sourcePath, targetPath)
	return ExecCmdWithTimeout(cmdName, args)
}

func GetFileInode(path string) (uint64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if sst, ok := fi.Sys().(*syscall.Stat_t); ok {
		return sst.Ino, nil
	}
	return 0, nil
}
