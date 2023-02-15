package csi

import (
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"k8s.io/utils/exec"
	"k8s.io/utils/mount"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

const (
	cfsMountParam = "minorversion=1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport"
	pfsFuseMount  = "/usr/local/bin/pfs-fuse"
)

func mountInCsi(volumeContext map[string]string, mountPath string) error {
	pfsServer := volumeContext[schema.PFSServer]
	fsID := volumeContext[schema.PFSID]
	fuseType := volumeContext[schema.PFSType]
	options := volumeContext[schema.PFSMountOptions]
	var mntCmd string
	var err error
	if fuseType == common.GlusterFSType {
		address := volumeContext[schema.PFSAddress]
		mntCmd = fmt.Sprintf("systemd-run --scope -- mount -t glusterfs %s %s", address, mountPath)
	} else if fuseType == common.CFSType {
		address := volumeContext[schema.PFSAddress]
		mntCmd = fmt.Sprintf("systemd-run --scope -- mount -t nfs4 -o %s %s %s", cfsMountParam, address, mountPath)
	} else {
		mntCmd = fmt.Sprintf("nohup %s mount --mount-point=%s --server=%s --fs-id=%s --log-dir=%s %s > /dev/null 2>&1 &", pfsFuseMount, mountPath, pfsServer, fsID, fuseLogDir, options)
	}
	log.Infof("mntCmd is :%s", mntCmd)
	if err := DoMountInHost(mntCmd); err != nil {
		log.Errorf("DoMountInHost err: %v", err)
		return err
	}
	mounter := &mount.SafeFormatAndMount{
		Interface: mount.New(""),
		Exec:      exec.New(),
	}
	var notMnt bool
	for i := 0; i < 3; i++ {
		notMnt, err = mount.IsNotMountPoint(mounter, mountPath)
		if !notMnt {
			break
		}
		if err != nil {
			log.Errorf("IsNotMountPoint err: %v", err)
		}
		time.Sleep(1 * time.Second)
	}
	if notMnt {
		log.Errorf("mount fail wtih cmd[%s]: %v", mntCmd, err)
		return errors.New("mount fail")
	}
	return nil
}
