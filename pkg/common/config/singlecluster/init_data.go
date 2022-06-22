package singlecluster

import (
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/queue"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

const (
	DefaultClusterName = "default-cluster"
	DefaultQueueName   = "default-queue"
	DefaultNamespace   = "default"
)

// Init cluster and queue by default
func Init(serverConf *config.ServerConfig) error {
	if !serverConf.Job.IsSingleCluster {
		log.Info("IsSingleCluster is false, pass init cluster and queue")
		return nil
	}
	log.Info("init data for single cluster is starting")

	if database.DB == nil {
		err := fmt.Errorf("please ensure call this function after db is inited")
		log.Errorf("init failed, err: %v", err)
		return err
	}
	if err := initDefaultCluster(); err != nil {
		log.Errorf("initDefaultCluster failed, err: %v", err)
		return err
	}
	if err := initDefaultQueue(); err != nil {
		log.Errorf("initDefaultQueue failed, err: %v", err)
		return err
	}
	log.Info("init data for single cluster completed")

	return nil
}

func initDefaultCluster() error {
	log.Info("starting init data for single cluster: initDefaultCluster")
	if _, err := models.GetClusterByName(DefaultClusterName); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		log.Errorf("GetClusterByName %s failed, err: %v", DefaultClusterName, err)
		return err
	} else if err == nil {
		log.Debug("default cluster has been created")
		return nil
	}
	// create default cluster
	clusterInfo := &models.ClusterInfo{
		Name:        DefaultClusterName,
		Description: "default cluster",
		Endpoint:    "127.0.0.1",
		Source:      "",
		ClusterType: schema.KubernetesType,
		Version:     "1.16+",
		Status:      models.ClusterStatusOnLine,
	}
	if err := models.CreateCluster(clusterInfo); err != nil {
		log.Errorf("create default cluster failed, err: %v", err)
		return err
	}
	return nil
}

func initDefaultQueue() error {
	log.Info("starting init data for single cluster: initDefaultQueue")
	if _, err := models.GetQueueByName(DefaultQueueName); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		log.Errorf("GetQueueByName %s failed, err: %v", DefaultQueueName, err)
		return err
	} else if err == nil {
		log.Debug("default queue has been created")
		return nil
	}
	ctx := &logger.RequestContext{UserName: common.UserRoot}
	// create default cluster
	defaultQueue := &queue.CreateQueueRequest{
		Name:        DefaultQueueName,
		Namespace:   DefaultNamespace,
		ClusterName: DefaultClusterName,
		QuotaType:   schema.TypeVolcanoCapabilityQuota,
		MaxResources: schema.ResourceInfo{
			CPU: "20",
			Mem: "20Gi",
		},
	}
	_, err := queue.CreateQueue(ctx, defaultQueue)
	if err != nil {
		log.Errorf("create default queue[%+v] failed, err: %v", defaultQueue, err)
		return err
	}
	return nil
}
