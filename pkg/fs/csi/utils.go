package csi

import (
	"errors"
	"fmt"
	"net"
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	socketPath = "/etc/paddleflow/csi-tool/connector.sock"
	fuseLogDir = "/home/paddleflow/log"
)

func DoMountInHost(mntCmd string) error {
	out, err := ConnectorRun(mntCmd)
	if err != nil {
		msg := fmt.Sprintf("Mount is failed in host, mntCmd:%s, err: %s, out: %s", mntCmd, err.Error(), out)
		log.Errorf(msg)
		return errors.New(msg)
	}
	return nil
}

// ConnectorRun Run shell command with host connector
// host connector is daemon running in host.
func ConnectorRun(cmd string) (string, error) {
	c, err := net.Dial("unix", socketPath)
	if err != nil {
		log.Errorf("paddleflow connector Dial error: %s", err.Error())
		return err.Error(), err
	}
	defer c.Close()

	_, err = c.Write([]byte(cmd))
	if err != nil {
		log.Errorf("paddleflow connector write error: %s", err.Error())
		return err.Error(), err
	}

	buf := make([]byte, 2048)
	n, err := c.Read(buf[:])
	response := string(buf[0:n])
	if strings.HasPrefix(response, "Success") {
		respstr := response[8:]
		return respstr, nil
	}
	return response, errors.New("Exec command error:" + response)
}
