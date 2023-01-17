package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/sevlyar/go-daemon"
	log "github.com/sirupsen/logrus"
)

const (
	// LogFilename name of log file
	LogFilename = "/etc/paddleflow/csi-tool/csi_connector.log"
	// PIDFilename name of pid file
	PIDFilename = "/etc/paddleflow/csi-tool/connector.pid"
	// WorkPath workspace
	WorkPath = "./"
	// SocketPath socket path
	SocketPath = "/etc/paddleflow/csi-tool/connector.sock"
)

func main() {
	cntxt := &daemon.Context{
		PidFileName: PIDFilename,
		PidFilePerm: 0644,
		LogFileName: LogFilename,
		LogFilePerm: 0640,
		WorkDir:     WorkPath,
		Umask:       027,
		Args:        []string{"paddleflow.csiplugin.connector"},
	}

	d, err := cntxt.Reborn()
	if err != nil {
		log.Fatalf("Unable to run connector: %s", err.Error())
	}
	if d != nil {
		return
	}
	defer cntxt.Release()
	log.Print("User Space Connector Daemon Is Starting...")

	runUserSpaceProxy()
}

func runUserSpaceProxy() {
	if IsFileExisting(SocketPath) {
		os.Remove(SocketPath)
	} else {
		pathDir := filepath.Dir(SocketPath)
		if !IsFileExisting(pathDir) {
			os.MkdirAll(pathDir, os.ModePerm)
		}
	}
	log.Printf("Socket path is ready: %s", SocketPath)
	ln, err := net.Listen("unix", SocketPath)
	if err != nil {
		log.Fatalf("Server Listen error: %s", err.Error())
	}
	log.Print("Daemon Started ...")
	defer ln.Close()

	// watchdog of UNIX Domain Socket
	var socketsPath []string
	if os.Getenv("WATCHDOG_SOCKETS_PATH") != "" {
		socketsPath = strings.Split(os.Getenv("WATCHDOG_SOCKETS_PATH"), ",")
	}
	socketNotAliveCount := make(map[string]int)
	go func() {
		if len(socketsPath) == 0 {
			return
		}
		for {
			deadSockets := 0
			for _, path := range socketsPath {
				if err := isUnixDomainSocketLive(path); err != nil {
					log.Printf("socket %s is not alive: %v", path, err)
					socketNotAliveCount[path]++
				} else {
					socketNotAliveCount[path] = 0
				}
				if socketNotAliveCount[path] >= 6 {
					deadSockets++
				}
			}
			if deadSockets >= len(socketsPath) {
				log.Printf("watchdog find too many dead sockets, csiplugin-connector will exit(0)")
				os.Exit(0)
			}
			time.Sleep(time.Second * 10)
		}
	}()

	// Handler to process the command
	for {
		fd, err := ln.Accept()
		if err != nil {
			log.Printf("Server Accept error: %s", err.Error())
			continue
		}
		go echoServer(fd)
	}
}

func echoServer(c net.Conn) {
	buf := make([]byte, 2048)
	nr, err := c.Read(buf)
	if err != nil {
		log.Print("Server Read error: ", err.Error())
		return
	}

	cmd := string(buf[0:nr])
	log.Printf("Server Receive csi command: %s", cmd)

	// run command
	if out, err := run(cmd); err != nil {
		reply := "Fail: " + cmd + ", error: " + err.Error()
		_, err = c.Write([]byte(reply))
		log.Print("Server Fail to run cmd:", reply)
	} else {
		out = "Success:" + out
		_, err = c.Write([]byte(out))
		log.Printf("Success: %s", out)
	}
}

func run(cmd string) (string, error) {
	out, err := exec.Command("sh", "-c", cmd).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("Failed to run cmd: " + cmd + ", with out: " + string(out) + ", with error: " + err.Error())
	}
	return string(out), nil
}

// IsFileExisting checks file exist in volume driver or not
func IsFileExisting(filename string) bool {
	_, err := os.Stat(filename)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func isUnixDomainSocketLive(socketPath string) error {
	fileInfo, err := os.Stat(socketPath)
	if err != nil || (fileInfo.Mode()&os.ModeSocket == 0) {
		return fmt.Errorf("socket file %s is invalid", socketPath)
	}
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}
