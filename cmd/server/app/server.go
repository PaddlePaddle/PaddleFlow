package app

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
	"github.com/spf13/pflag"

	"paddleflow/cmd/server/app/options"
	"paddleflow/pkg/apiserver/controller/run"
	"paddleflow/pkg/apiserver/models"
	v1 "paddleflow/pkg/apiserver/router/v1"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/database"
	dbinit "paddleflow/pkg/common/database/init"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
	"paddleflow/pkg/job"
	"paddleflow/pkg/version"
)

type Server struct {
	Router       *chi.Mux
	HttpSvr      *http.Server
	ServerConf   *config.ServerConfig
	serverOption *options.ServerOption
	ServerCtx    context.Context
	ServerCancel context.CancelFunc
}

func (s *Server) initConfig() {
	s.ServerConf = &config.ServerConfig{}
	if err := config.InitConfigFromYaml(s.ServerConf, ""); err != nil {
		fmt.Printf("InitConfigFromYaml failed. serverConf:[%+v], configPath:[%s] error:[%s]\n", s.ServerConf, "", err.Error())
		os.Exit(22)
	}

	s.ServerConf.FlavourMap = make(map[string]schema.Flavour)
	for _, f := range s.ServerConf.Flavour {
		err := schema.ValidateResourceInfo(f.ResourceInfo, s.ServerConf.Job.ScalarResourceArray)
		if err != nil {
			fmt.Printf("validate resource of flavor[%v] failed. error: %s\n", f, err)
			panic(err)
		}
		s.ServerConf.FlavourMap[f.Name] = f
	}
	config.GlobalServerConfig = s.ServerConf

	// make sure template job yaml file exist
	if filesNum, err := config.FileNumsInDir(s.ServerConf.Job.DefaultJobYamlDir); err != nil {
		fmt.Printf("validate default job yaml dir[%s] failed. error: %s\n", s.ServerConf.Job.DefaultJobYamlDir, err)
		panic(err)
	} else if filesNum == 0 {
		fmt.Printf("validate default job yaml dir[%s] failed. error: yaml files not found", s.ServerConf.Job.DefaultJobYamlDir)
		panic(err)
	}
}

func (s *Server) Run() error {
	imageHandler, err := run.InitAndResumeRuns()
	if err != nil {
		log.Errorf("InitAndResumePipeline failed. error: %v", err)
		return err
	}
	go imageHandler.Run()

	go func() {
		if err := s.HttpSvr.ListenAndServe(); err != nil && errors.Is(err, http.ErrServerClosed) {
			log.Infof("listen: %s", err)
		}
	}()

	stopSig := make(chan os.Signal)
	signal.Notify(stopSig, syscall.SIGTERM, syscall.SIGINT)
	<-stopSig

	s.ServerCancel()
	if err := s.HttpSvr.Shutdown(s.ServerCtx); err != nil {
		log.Infof("Server forced to shutdown:%s", err.Error())
	}
	log.Info("PaddleFlow server exiting")
	return nil
}

func (s *Server) Init() {
	s.initConfig()
	s.serverOption = options.NewServerOption(s.ServerConf)
	s.serverOption.InitFlag(pflag.CommandLine)

	if s.ServerConf.ApiServer.PrintVersionAndExit {
		version.PrintVersionAndExit()
	}

	err := logger.InitStandardFileLogger(&s.ServerConf.Log)
	if err != nil {
		panic("init logger failed.")
	}

	log.Infof("The final server config is: %s ", config.PrettyFormat(s.ServerConf))

	dbConf := &s.ServerConf.Database

	database.DB, err = dbinit.InitDatabase(&config.DatabaseConfig{
		Driver:   dbConf.Driver,
		Host:     dbConf.Host,
		Port:     dbConf.Port,
		User:     dbConf.User,
		Password: dbConf.Password,
		Database: dbConf.Database,
	}, nil, s.ServerConf.Log.Level)
	if err != nil {
		panic("init database failed.")
	}

	if err = NewAndStartJobManager(); err != nil {
		log.Errorf("create pfjob manager failed, err %v", err)
		panic("create pfjob manager failed")
	}

	if err = config.InitDefaultPV(s.ServerConf.Fs.DefaultPVPath); err != nil {
		panic(err)
	}
	if err = config.InitDefaultPVC(s.ServerConf.Fs.DefaultPVCPath); err != nil {
		panic(err)
	}

	s.Router = chi.NewRouter()
	v1.RegisterRouters(s.Router, false)
	log.Infof("server addr:%s", fmt.Sprintf(":%d", s.ServerConf.ApiServer.Port))
	s.HttpSvr = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.ServerConf.ApiServer.Port),
		Handler: s.Router,
	}
	s.ServerCtx, s.ServerCancel = context.WithCancel(context.Background())
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
