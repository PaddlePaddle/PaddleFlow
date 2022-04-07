package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	v1 "paddleflow/pkg/apiserver/router/v1"
	"syscall"

	"github.com/go-chi/chi"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	_ "go.uber.org/automaxprocs"

	"paddleflow/cmd/server/flag"
	"paddleflow/pkg/apiserver/controller/run"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/database"
	"paddleflow/pkg/common/database/dbinit"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
	"paddleflow/pkg/job"
	"paddleflow/pkg/version"
)

var ServerConf *config.ServerConfig

func main() {
	err := Main(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func Main(args []string) error {
	initConfig()

	cli.VersionFlag = &cli.BoolFlag{
		Name: "version", Aliases: []string{"V"},
		Usage: "print only the version",
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
		Version:              "1.4",
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
		log.Fatalf("server run failed. error:%s", err.Error())
	}
	return nil
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

	go func() {
		if err := HttpSvr.ListenAndServe(); err != nil && errors.Is(err, http.ErrServerClosed) {
			log.Infof("listen: %s", err)
		}
	}()

	stopSig := make(chan os.Signal)
	signal.Notify(stopSig, syscall.SIGTERM, syscall.SIGINT)
	<-stopSig

	if err := HttpSvr.Shutdown(ServerCtx); err != nil {
		log.Infof("Server forced to shutdown:%s", err.Error())
	}
	log.Info("PaddleFlow server exiting")
	return nil
}

func initConfig() {
	ServerConf = &config.ServerConfig{}
	if err := config.InitConfigFromYaml(ServerConf, ""); err != nil {
		fmt.Printf("InitConfigFromYaml failed. serverConf:[%+v], configPath:[%s] error:[%s]\n", ServerConf, "", err.Error())
		os.Exit(22)
	}

	ServerConf.FlavourMap = make(map[string]schema.Flavour)
	for _, f := range ServerConf.Flavour {
		err := schema.ValidateResourceInfo(f.ResourceInfo, ServerConf.Job.ScalarResourceArray)
		if err != nil {
			fmt.Printf("validate resource of flavor[%v] failed. error: %s\n", f, err)
			os.Exit(22)
		}
		ServerConf.FlavourMap[f.Name] = f
	}
	config.GlobalServerConfig = ServerConf

	// make sure template job yaml file exist
	if filesNum, err := config.FileNumsInDir(ServerConf.Job.DefaultJobYamlDir); err != nil {
		fmt.Printf("validate default job yaml dir[%s] failed. error: %s\n", ServerConf.Job.DefaultJobYamlDir, err)
		os.Exit(22)
	} else if filesNum == 0 {
		fmt.Printf("validate default job yaml dir[%s] failed. error: yaml files not found", ServerConf.Job.DefaultJobYamlDir)
		os.Exit(22)
	}
}

func setup() {
	if ServerConf.ApiServer.PrintVersionAndExit {
		version.PrintVersionAndExit()
	}

	err := logger.InitStandardFileLogger(&ServerConf.Log)
	if err != nil {
		log.Errorf("InitStandardFileLogger err: %v", err)
		os.Exit(22)
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
		os.Exit(22)
	}

	if err = NewAndStartJobManager(); err != nil {
		log.Errorf("create pfjob manager failed, err %v", err)
		os.Exit(22)
	}

	if err = config.InitDefaultPV(ServerConf.Fs.DefaultPVPath); err != nil {
		log.Errorf("InitDefaultPV err %v", err)
		os.Exit(22)
	}
	if err = config.InitDefaultPVC(ServerConf.Fs.DefaultPVCPath); err != nil {
		log.Errorf("InitDefaultPVC err %v", err)
		os.Exit(22)
	}
}

func NewAndStartJobManager() error {
	runtimeMgr, err := job.NewJobManagerImpl()
	if err != nil {
		log.Errorf("new job manager failed, error: %v", err)
		return err
	}
	go runtimeMgr.Start(models.ActiveClusters, models.ListQueueJob)
	return nil
}
