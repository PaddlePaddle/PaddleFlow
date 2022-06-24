package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-chi/chi"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	_ "go.uber.org/automaxprocs"

	"github.com/PaddlePaddle/PaddleFlow/cmd/server/flag"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/cluster"
	jobCtrl "github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/job"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/pipeline"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/queue"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	router "github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/v1"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
	"github.com/PaddlePaddle/PaddleFlow/pkg/version"
)

var ServerConf *config.ServerConfig

func main() {
	if err := Main(os.Args); err != nil {
		fmt.Println(err)
		gracefullyExit(err)
	}
}

func Main(args []string) error {
	cli.VersionFlag = &cli.BoolFlag{
		Name: "version", Aliases: []string{"V"},
		Usage: "version of PaddleFlow server",
		Value: false,
	}

	if err := initConfig(); err != nil {
		fmt.Println(err)
		gracefullyExit(err)
	}

	compoundFlags := [][]cli.Flag{
		logger.LogFlags(&ServerConf.Log),
		flag.ApiServerFlags(&ServerConf.ApiServer),
		flag.JobFlags(&ServerConf.Job),
		flag.FilesystemFlags(&ServerConf.Fs),
		flag.StorageFlags(&ServerConf.Storage),
	}

	app := &cli.App{
		Name:                 "PaddleFlow",
		Usage:                "pipeline/filesystem/job orchestration services for machine learning",
		Version:              version.InfoStr(),
		Copyright:            "Apache License 2.0",
		HideHelpCommand:      true,
		EnableBashCompletion: true,
		Flags:                flag.ExpandFlags(compoundFlags),
		Action:               act,
	}
	return app.Run(args)
}

func act(c *cli.Context) error {
	setup()
	err := start()
	if err != nil {
		log.Errorf("start server failed. error:%s", err.Error())
	}
	return err
}

func start() error {
	Router := chi.NewRouter()
	router.RegisterRouters(Router, false)
	log.Infof("server addr:%s", fmt.Sprintf(":%d", ServerConf.ApiServer.Port))
	HttpSvr := &http.Server{
		Addr:    fmt.Sprintf(":%d", ServerConf.ApiServer.Port),
		Handler: Router,
	}
	ServerCtx, ServerCancel := context.WithCancel(context.Background())
	defer ServerCancel()

	imageHandler, err := pipeline.InitAndResumeRuns()
	if err != nil {
		log.Errorf("InitAndResumePipeline failed. error: %v", err)
		return err
	}
	go imageHandler.Run()

	globalScheduler := pipeline.GetGlobalScheduler()
	go globalScheduler.Start()

	go jobCtrl.WSManager.SendGroupData()
	go jobCtrl.WSManager.GetGroupData()

	go func() {
		if err := HttpSvr.ListenAndServe(); err != nil && errors.Is(err, http.ErrServerClosed) {
			log.Infof("listen: %s", err)
		}
	}()

	stopSig := make(chan os.Signal, 1)
	signal.Notify(stopSig, syscall.SIGTERM, syscall.SIGINT)
	<-stopSig

	if err := HttpSvr.Shutdown(ServerCtx); err != nil {
		log.Infof("Server forced to shutdown:%s", err.Error())
	}
	log.Info("PaddleFlow server exiting")
	return nil
}

func initConfig() error {
	ServerConf = &config.ServerConfig{}
	if err := config.InitConfigFromYaml(ServerConf, ""); err != nil {
		log.Errorf("InitConfigFromYaml failed. serverConf:[%+v], configPath:[%s] error:[%s]\n", ServerConf, "", err.Error())
		return err
	}

	config.GlobalServerConfig = ServerConf

	// make sure template job yaml file exist
	if filesNum, err := config.FileNumsInDir(ServerConf.Job.DefaultJobYamlDir); err != nil {
		log.Errorf("validate default job yaml dir[%s] failed. error: %s\n", ServerConf.Job.DefaultJobYamlDir, err)
		return err
	} else if filesNum == 0 {
		log.Errorf("validate default job yaml dir[%s] failed. error: yaml files not found", ServerConf.Job.DefaultJobYamlDir)
		return errors.New("yaml files not found")
	}
	return nil
}

// initClusterAndQueue init cluster and queue by default name
func initClusterAndQueue(serverConf *config.ServerConfig) error {
	if !serverConf.Job.IsSingleCluster {
		log.Info("IsSingleCluster is false, pass init cluster and queue")
		return nil
	}
	log.Info("init data for single cluster is starting")
	if storage.DB == nil {
		err := fmt.Errorf("please ensure call this function after db is inited")
		log.Errorf("init failed, err: %v", err)
		return err
	}
	if err := cluster.InitDefaultCluster(); err != nil {
		log.Errorf("initDefaultCluster failed, err: %v", err)
		return err
	}
	if err := queue.InitDefaultQueue(); err != nil {
		log.Errorf("initDefaultQueue failed, err: %v", err)
		return err
	}
	log.Info("init data for single cluster completed")

	return nil
}

func setup() {
	if err := logger.InitStandardFileLogger(&ServerConf.Log); err != nil {
		log.Errorf("InitStandardFileLogger err: %v", err)
		gracefullyExit(err)
	}

	log.Infof("The final server config is: %s ", config.PrettyFormat(ServerConf))

	dbConf := &ServerConf.Storage
	if err := driver.InitStorage(&config.StorageConfig{
		Driver:   dbConf.Driver,
		Host:     dbConf.Host,
		Port:     dbConf.Port,
		User:     dbConf.User,
		Password: dbConf.Password,
		Database: dbConf.Database,
	}, ServerConf.Log.Level); err != nil {
		log.Errorf("init database err: %v", err)
		gracefullyExit(err)
	}

	if err := newAndStartJobManager(); err != nil {
		log.Errorf("create pfjob manager failed, err %v", err)
		gracefullyExit(err)
	}

	if err := config.InitDefaultPV(ServerConf.Fs.DefaultPVPath); err != nil {
		log.Errorf("InitDefaultPV err %v", err)
		gracefullyExit(err)
	}
	if err := config.InitDefaultPVC(ServerConf.Fs.DefaultPVCPath); err != nil {
		log.Errorf("InitDefaultPVC err %v", err)
		gracefullyExit(err)
	}
}

func newAndStartJobManager() error {
	err := initClusterAndQueue(ServerConf)
	if err != nil {
		log.Errorf("init singlecluster data failed, err: %v", err)
		gracefullyExit(err)
	}

	runtimeMgr, err := job.NewJobManagerImpl()
	if err != nil {
		log.Errorf("new job manager failed, error: %v", err)
		return err
	}
	go runtimeMgr.Start(models.ActiveClusters, models.ListQueueJob)
	return nil
}

func gracefullyExit(err error) {
	fmt.Println(err)
	os.Exit(22)
}
