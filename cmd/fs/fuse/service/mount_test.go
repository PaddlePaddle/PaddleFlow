package service

import (
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
	"github.com/PaddlePaddle/PaddleFlow/pkg/version"
	"github.com/urfave/cli/v2"
	"os"
	"testing"
	"time"
)

const testMountPoint = "./mock"

func MockMain(args []string) error {
	cli.VersionFlag = &cli.BoolFlag{
		Name: "version", Aliases: []string{"V"},
		Usage: "print only the version",
	}
	app := &cli.App{
		Name:                 "pfs-fuse",
		Usage:                "A POSIX file system built on kv DB and object storage.",
		Version:              version.InfoStr(),
		Copyright:            "Apache License 2.0",
		HideHelpCommand:      true,
		EnableBashCompletion: true,
		Commands: []*cli.Command{
			CmdMount(),
			CmdUmount(),
		},
	}
	return app.Run(args)
}

func mountTemp(t *testing.T) {
	os.MkdirAll(testMountPoint, 0755)
	mountArgs := []string{"", "mount", "-mp", testMountPoint, "--local", "true", "--local-root", testMountPoint, "-d", "true"}

	go func() {
		if err := MockMain(mountArgs); err != nil {
			t.Errorf("mount failed: %s", err)
		}
	}()
	time.Sleep(3 * time.Second)

	inode, err := utils.GetFileInode(testMountPoint)
	if err != nil {
		t.Fatalf("get file inode failed: %s", err)
	}
	if inode != 1 {
		t.Fatalf("mount failed: inode of %s is not 1", testMountPoint)
	} else {
		t.Logf("mount %s success", testMountPoint)
	}
}

func umountTemp(t *testing.T) {
	if err := MockMain([]string{"", "umount", testMountPoint}); err != nil {
		t.Fatalf("umount failed: %s", err)
	}
}

func TestMountBackground(t *testing.T) {
	mountTemp(t)
	defer umountTemp(t)
}
