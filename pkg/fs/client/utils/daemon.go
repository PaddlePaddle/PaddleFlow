package utils

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	stageVar    = "__DAEMON_STAGE"
	fdVarPrefix = "__DAEMON_FD_"
)

// DaemonAttr describes the options that apply to daemonization
type DaemonAttr struct {
	ProgramName   string      // child's os.Args[0]; copied from parent if empty
	CaptureOutput bool        // whether to capture stdout/stderr
	Files         []**os.File // files to keep open in the daemon
	Stdout        *os.File    // redirect stdout/stderr to it
	OnExit        func(stage int) error
}

func MakeDaemon(attrs *DaemonAttr) (io.Reader, io.Reader, error) {
	stage, advanceStage, resetEnv := getStage()

	fatal := func(err error) (io.Reader, io.Reader, error) {
		if stage > 0 {
			os.Exit(1)
		}
		resetErr := resetEnv()
		if err != nil {
			return nil, nil, resetErr
		}
		return nil, nil, err
	}

	fileCount := 3 + len(attrs.Files)
	files := make([]*os.File, fileCount, fileCount+2)

	if stage == 0 {
		nullDev, err := os.OpenFile("/dev/null", 0, 0)
		if err != nil {
			return fatal(err)
		}
		files[0] = nullDev
		if attrs.Stdout != nil {
			files[1], files[2] = attrs.Stdout, attrs.Stdout
		} else {
			files[1], files[2] = nullDev, nullDev
		}

		fd := 3
		for _, fPtr := range attrs.Files {
			files[fd] = *fPtr
			saveFileName(fd, (*fPtr).Name())
			fd++
		}
	} else {
		files[0], files[1], files[2] = os.Stdin, os.Stdout, os.Stderr

		fd := 3
		for _, fPtr := range attrs.Files {
			*fPtr = os.NewFile(uintptr(fd), getFileName(fd))
			syscall.CloseOnExec(fd)
			files[fd] = *fPtr
			fd++
		}
	}

	if stage < 2 {
		procName, err := os.Executable()
		if err != nil {
			return fatal(fmt.Errorf("can't determine full path to executable: %s", err))
		}

		if len(procName) == 0 {
			return fatal(fmt.Errorf("can't determine full path to executable"))
		}

		if stage == 1 && attrs.CaptureOutput {
			files = files[:fileCount+2]

			// stdout: write at fd:1, read at fd:fileCount
			if files[fileCount], files[1], err = os.Pipe(); err != nil {
				return fatal(err)
			}
			// stderr: write at fd:2, read at fd:fileCount+1
			if files[fileCount+1], files[2], err = os.Pipe(); err != nil {
				return fatal(err)
			}
		}

		if err := advanceStage(); err != nil {
			return fatal(err)
		}
		dir, _ := os.Getwd()
		osAttrs := os.ProcAttr{Dir: dir, Env: os.Environ(), Files: files}

		if stage == 0 {
			sysattrs := syscall.SysProcAttr{Setsid: true}
			osAttrs.Sys = &sysattrs
		}

		progName := attrs.ProgramName
		if len(progName) == 0 {
			progName = os.Args[0]
		}
		args := append([]string{progName}, os.Args[1:]...)
		proc, err := os.StartProcess(procName, args, &osAttrs)
		if err != nil {
			return fatal(fmt.Errorf("can't create process %s: %s", procName, err))
		}
		err = proc.Release()
		if err != nil {
			return nil, nil, err
		}
		if attrs.OnExit != nil {
			err := attrs.OnExit(stage)
			if err != nil {
				return nil, nil, err
			}
		}
		os.Exit(0)
	}

	//os.Chdir("/")
	syscall.Umask(0)
	err := resetEnv()
	if err != nil {
		return nil, nil, err
	}

	for fd := 3; fd < fileCount; fd++ {
		resetFileName(fd)
	}
	currStage = DaemonStage(stage)

	var stdout, stderr *os.File
	if attrs.CaptureOutput {
		stdout = os.NewFile(uintptr(fileCount), "stdout")
		stderr = os.NewFile(uintptr(fileCount+1), "stderr")
	}
	return stdout, stderr, nil
}

func saveFileName(fd int, name string) {
	// We encode in hex to avoid issues with filename encoding, and to be able
	// to separate it from the original variable value (if set) that we want to
	// keep. Otherwise, all non-zero characters are valid in the name, and we
	// can't insert a zero in the var as a separator.
	fdVar := fdVarPrefix + fmt.Sprint(fd)
	value := fmt.Sprintf("%s:%s",
		hex.EncodeToString([]byte(name)), os.Getenv(fdVar))

	if err := os.Setenv(fdVar, value); err != nil {
		fmt.Fprintf(os.Stderr, "can't set %s: %s\n", fdVar, err)
		os.Exit(1)
	}
}

func getFileName(fd int) string {
	fdVar := fdVarPrefix + fmt.Sprint(fd)
	value := os.Getenv(fdVar)
	sep := bytes.IndexByte([]byte(value), ':')

	if sep < 0 {
		fmt.Fprintf(os.Stderr, "bad fd var %s\n", fdVar)
		os.Exit(1)
	}
	name, err := hex.DecodeString(value[:sep])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error decoding %s\n", fdVar)
		os.Exit(1)
	}
	return string(name)
}

func resetFileName(fd int) {
	fdVar := fdVarPrefix + fmt.Sprint(fd)
	value := os.Getenv(fdVar)
	sep := bytes.IndexByte([]byte(value), ':')

	if sep < 0 {
		fmt.Fprintf(os.Stderr, "bad fd var %s\n", fdVar)
		os.Exit(1)
	}
	if err := os.Setenv(fdVar, value[sep+1:]); err != nil {
		fmt.Fprintf(os.Stderr, "can't reset %s\n", fdVar)
		os.Exit(1)
	}
}

// DaemonStage tells in what stage in the process we are. See Stage().
type DaemonStage int

// Stages in the daemonizing process.
const (
	StageParent = DaemonStage(iota) // Original process
	StageChild                      // MakeDaemon() called once: first child
	StageDaemon                     // MakeDaemon() run twice: final daemon

	stageUnknown = DaemonStage(-1)
)

var currStage = stageUnknown

func Stage() DaemonStage {
	if currStage == stageUnknown {
		s, _, _ := getStage()
		currStage = DaemonStage(s)
	}
	return currStage
}

func (s DaemonStage) String() string {
	switch s {
	case StageParent:
		return "parent"
	case StageChild:
		return "first child"
	case StageDaemon:
		return "daemon"
	default:
		return "unknown"
	}
}

func getStage() (stage int, advanceStage func() error, resetEnv func() error) {
	var origValue string
	stage = 0

	daemonStage := os.Getenv(stageVar)
	stageTag := strings.SplitN(daemonStage, ":", 2)
	stageInfo := strings.SplitN(stageTag[0], "/", 3)

	if len(stageInfo) == 3 {
		stageStr, tm, check := stageInfo[0], stageInfo[1], stageInfo[2]

		hash := sha1.New()
		hash.Write([]byte(stageStr + "/" + tm + "/"))

		if check != hex.EncodeToString(hash.Sum([]byte{})) {
			// This whole chunk is original data
			origValue = daemonStage
		} else {
			stage, _ = strconv.Atoi(stageStr)

			if len(stageTag) == 2 {
				origValue = stageTag[1]
			}
		}
	} else {
		origValue = daemonStage
	}

	advanceStage = func() error {
		base := fmt.Sprintf("%d/%09d/", stage+1, time.Now().Nanosecond())
		hash := sha1.New()
		hash.Write([]byte(base))
		tag := base + hex.EncodeToString(hash.Sum([]byte{}))

		if err := os.Setenv(stageVar, tag+":"+origValue); err != nil {
			return fmt.Errorf("can't set %s: %s", stageVar, err)
		}
		return nil
	}
	resetEnv = func() error {
		return os.Setenv(stageVar, origValue)
	}

	return stage, advanceStage, resetEnv
}
