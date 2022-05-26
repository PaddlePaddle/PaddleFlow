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

package job

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
)

type MsgType string

const (
	HeartbeatMsg MsgType = "HeartbeatMsg"
	DataMsg      MsgType = "DataMsg"
)

type Connection struct {
	ID            string
	WsConnect     *websocket.Conn
	IsClosed      bool
	InformChan    chan []byte
	CloseChan     chan byte
	MuxClose      sync.Mutex
	MuxWrite      sync.Mutex
	Ctx           *logger.RequestContext
	SetupTime     time.Time
	HeartbeatChan chan []byte
}

type InformJob struct {
	JobID      string `json:"jobId"`
	UpdateTime string `json:"updateTime"`
}

func InitConnection(wsConn *websocket.Conn, ctx *logger.RequestContext) (*Connection, error) {
	conn := &Connection{
		WsConnect:     wsConn,
		IsClosed:      false,
		InformChan:    make(chan []byte, 1000),
		CloseChan:     make(chan byte, 1),
		HeartbeatChan: make(chan []byte, 1000),
		Ctx:           ctx,
		SetupTime:     time.Now(),
	}

	go conn.writeHeartbeatLoop()
	go conn.writeDataLoop()
	return conn, nil
}

func (conn *Connection) WriteMessage(data string, msgType MsgType) error {
	switch msgType {
	case HeartbeatMsg:
		select {
		case conn.HeartbeatChan <- []byte(data):
		case <-conn.CloseChan:
			return common.ConnectionClosedError()
		}
	case DataMsg:
		select {
		case conn.InformChan <- []byte(data):
		case <-conn.CloseChan:
			return common.ConnectionClosedError()
		}
	}
	return nil
}

func (conn *Connection) writeHeartbeatLoop() {
	for {
		select {
		case data := <-conn.HeartbeatChan:
			conn.MuxWrite.Lock()
			if err := conn.WsConnect.WriteMessage(websocket.TextMessage, data); err != nil {
				log.Errorf("write heartbeat msg failed, error:[%s]", err.Error())
				return
			}
			conn.MuxWrite.Unlock()
		case <-conn.CloseChan:
			return
		}
	}
}

func (conn *Connection) writeDataLoop() {
	for {
		select {
		case data := <-conn.InformChan:
			conn.MuxWrite.Lock()
			if err := conn.WsConnect.WriteMessage(websocket.TextMessage, data); err != nil {
				log.Errorf("write job data msg failed, error:[%s]", err.Error())
				return
			}
			conn.MuxWrite.Unlock()
		case <-conn.CloseChan:
			return
		}
	}

}

func (conn *Connection) Close() {
	conn.WsConnect.Close()
	// 防止ClosChan被多次关闭
	conn.MuxClose.Lock()
	if !conn.IsClosed {
		log.Infof("ws connectiion closed")
		close(conn.CloseChan)
		conn.IsClosed = true
		WSManager.Exit(conn.ID)
	}
	conn.MuxClose.Unlock()
}
