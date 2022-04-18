package job

import (
	"encoding/json"
	"errors"
	"time"

	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/uuid"
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
		jobList, err := models.ListJobByUpdateTime(UpdateTime.Format(models.TimeFormat))
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
