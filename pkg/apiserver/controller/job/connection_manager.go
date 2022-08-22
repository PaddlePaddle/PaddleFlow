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
	"encoding/json"
	"errors"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

type WebsocketManager struct {
	Connections   map[string]*Connection
	BroadcastChan chan GetJobResponse
}

func (manager *WebsocketManager) Register(connection *Connection, clientID string) {
	if clientID == "" {
		connection.ID = uuid.GenerateID(common.PrefixConnection)
	} else {
		connection.ID = clientID
	}

	WSManager.Connections[connection.ID] = connection
	log.Infof("register connection[%s] to wsmanager", connection.ID)
}

func (manager *WebsocketManager) Exit(id string) {
	delete(WSManager.Connections, id)
	log.Infof("delete connection[%s] from wsmanager", id)
}

func (manager *WebsocketManager) SendGroupData() {
	for {
		data := <-manager.BroadcastChan
		for _, connection := range manager.Connections {
			jobByte, err := json.Marshal(data)
			if err != nil {
				log.Errorf("job[%s] parse to json string faild, error:[%s]", data.ID, err.Error())
				break
			}
			if err := connection.WriteMessage(string(jobByte), DataMsg); err != nil {
				if errors.Is(err, common.ConnectionClosedError()) {
					continue
				}
				log.Errorf("connection[%s] write data error[%s]", connection.ID, err.Error())
				continue
			}
		}
		log.Infof("job[%s] has been send to client", data.ID)
	}
}

func (manager *WebsocketManager) GetGroupData() {
	for {
		time.Sleep(time.Second * 3)
		if len(WSManager.Connections) == 0 {
			for len(WSManager.BroadcastChan) > 0 {
				<-WSManager.BroadcastChan
			}
			continue
		}
		nextTime := time.Now()
		log.Infof("start get data")
		jobList, err := storage.Job.ListJobByUpdateTime(UpdateTime.Format(model.TimeFormat))
		if err != nil {
			log.Errorf("list job failed for websocket to send job")
			continue
		}
		UpdateTime = nextTime
		for _, job := range jobList {
			jobResponse, err := convertJobToResponse(job, true)
			if err != nil {
				log.Errorf("convert model job[%s] to job response failed", job.ID)
				continue
			}
			WSManager.BroadcastChan <- jobResponse
			log.Infof("job[%s] has send to the channel and is ready to send to client", job.ID)
		}

	}
}
