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
	job2 "github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/job"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/run"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	v1 "github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/v1"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database/dbinit"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job"
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
		flag.ApiServerFlags(&ServerConf.ApiServer),
		flag.JobFlags(&ServerConf.Job),
		flag.FilesystemFlags(&ServerConf.Fs),
		logger.LogFlags(&ServerConf.Log),
		database.DatabaseFlags(&ServerConf.Database),
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
	v1.RegisterRouters(Router, false)
	log.Infof("server addr:%s", fmt.Sprintf(":%d", ServerConf.ApiServer.Port))
	HttpSvr := &http.Server{
		Addr:    fmt.Sprintf(":%d", ServerConf.ApiServer.Port),
		Handler: Router,
	}
	ServerCtx, ServerCancel := context.WithCancel(context.Background())
	defer ServerCancel()

	imageHandler, err := run.InitAndResumeRuns()
	if err != nil {
		log.Errorf("InitAndResumePipeline failed. error: %v", err)
		return err
	}
	go imageHandler.Run()

	go job2.WSManager.SendGroupData()
	go job2.WSManager.GetGroupData()

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

func setup() {
	err := logger.InitStandardFileLogger(&ServerConf.Log)
	if err != nil {
		log.Errorf("InitStandardFileLogger err: %v", err)
		gracefullyExit(err)
	}

	log.Infof("The final server config is: %s ", config.PrettyFormat(ServerConf))

	dbConf := &ServerConf.Database

	database.DB, err = dbinit.InitDatabase(&config.DatabaseConfig{
		Driver:   dbConf.Driver,
		Host:     dbConf.Host,
		Port:     dbConf.Port,
		User:     dbConf.User,
		Password: dbConf.Password,
		Database: dbConf.Database,
	}, nil, ServerConf.Log.Level)
	if err != nil {
		log.Errorf("init database err: %v", err)
		gracefullyExit(err)
	}

	if err = newAndStartJobManager(); err != nil {
		log.Errorf("create pfjob manager failed, err %v", err)
		gracefullyExit(err)
	}

	if err = config.InitDefaultPV(ServerConf.Fs.DefaultPVPath); err != nil {
		log.Errorf("InitDefaultPV err %v", err)
		gracefullyExit(err)
	}
	if err = config.InitDefaultPVC(ServerConf.Fs.DefaultPVCPath); err != nil {
		log.Errorf("InitDefaultPVC err %v", err)
		gracefullyExit(err)
	}
}

func newAndStartJobManager() error {
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
