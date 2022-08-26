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

package service

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

var cleanCacheInfo CleanCache

type CleanCache struct {
	Clean   bool
	MetaDir string
	DataDir string
}

func CmdUmount() *cli.Command {
	return &cli.Command{
		Name:      "umount",
		Action:    umount,
		Category:  "SERVICE",
		Usage:     "Unmount a volume",
		ArgsUsage: "MOUNTPOINT",
		Description: `
Examples:
$ pfs-fuse umount /mnt/mount_point`,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "force",
				Aliases: []string{"f"},
				Usage:   "unmount a busy mount point by force",
			},
		},
	}
}

func doUmount(mp string, force bool) error {
	log.Infof("start doUmountr: force:%t clean: %+v", force, cleanCacheInfo)
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		if force {
			cmd = exec.Command("umount", "-f", mp)
		} else {
			cmd = exec.Command("umount", mp)
		}
	case "linux":
		if _, err := exec.LookPath("fusermount"); err == nil {
			if force {
				cmd = exec.Command("fusermount", "-uz", mp)
			} else {
				cmd = exec.Command("fusermount", "-u", mp)
			}
		} else {
			if force {
				cmd = exec.Command("umount", "-l", mp)
			} else {
				cmd = exec.Command("umount", mp)
			}
		}
	default:
		return fmt.Errorf("OS %s is not supported", runtime.GOOS)
	}
	// clean cache if set
	if cleanCacheInfo.Clean {
		log.Infof("start clean cache dir: %+v", cleanCacheInfo)
		if cleanCacheInfo.MetaDir != "" {
			if err := os.RemoveAll(cleanCacheInfo.MetaDir); err != nil {
				err := fmt.Errorf("remove meta cache path[%s] failed: %v", cleanCacheInfo.MetaDir, err)
				log.Warnf(err.Error())
			}
		}
		if cleanCacheInfo.DataDir != "" {
			if err := os.RemoveAll(cleanCacheInfo.DataDir); err != nil {
				err := fmt.Errorf("remove data cache path[%s] failed: %v", cleanCacheInfo.DataDir, err)
				log.Warnf(err.Error())
			}
		}
	}
	log.Infof("start umount. command: %+v", cmd)
	out, err := cmd.CombinedOutput()
	if err != nil && len(out) != 0 {
		err := errors.New(string(out))
		log.Errorf("cmd.CombinedOutput() err: %v", err)
		return err
	}
	return nil
}

func umount(ctx *cli.Context) error {
	mp := ctx.Args().Get(0)
	force := ctx.Bool("force")
	return doUmount(mp, force)
}
