package options

import (
	goflag "flag"

	"github.com/spf13/pflag"

	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/database"
	"paddleflow/pkg/common/logger"
)

// ServerOption is the main context object for the controllers.
type ServerOption struct {
	serverConf *config.ServerConfig
}

func NewServerOption(serverConf *config.ServerConfig) *ServerOption {
	return &ServerOption{
		serverConf: serverConf,
	}
}

func (s *ServerOption) AddFlagSet(fs *pflag.FlagSet) {
	if fs == nil {
		fs = pflag.CommandLine
	}
	apiServerConf := &s.serverConf.ApiServer
	fs.StringVar(&apiServerConf.Host, "host", apiServerConf.Host, "host")
	fs.IntVar(&apiServerConf.Port, "port", apiServerConf.Port, "port")
	fs.BoolVarP(&apiServerConf.PrintVersionAndExit, "version", "v", apiServerConf.PrintVersionAndExit, "Version of PaddleFlow server")

	jobConf := &s.serverConf.Job
	fs.IntVar(&jobConf.ClusterSyncPeriod, "cluster-sync-period", jobConf.ClusterSyncPeriod, "The period of job manager get cluster information from database")
	fs.IntVar(&jobConf.QueueExpireTime, "queue-expire-time", jobConf.QueueExpireTime, "The expire time of queue in job manager cache")
	fs.IntVar(&jobConf.QueueCacheSize, "queue-cache-size", jobConf.QueueCacheSize, "The number of queue in job manager cache")
	fs.IntVar(&jobConf.JobLoopPeriod, "job-loop-period", jobConf.JobLoopPeriod, "The loop period for job manager processing job")
	fs.BoolVar(&jobConf.Reclaim.CleanJob, "is-clean-job", jobConf.Reclaim.CleanJob, "CleanJob")
	fs.BoolVar(&jobConf.Reclaim.SkipCleanFailedJob, "is-skip-clean-failed-job", jobConf.Reclaim.SkipCleanFailedJob, "SkipCleanFailedJob")
	fs.IntVar(&jobConf.Reclaim.JobTTLSeconds, "job-ttl-seconds", jobConf.Reclaim.JobTTLSeconds, "JobTTLSeconds")

	fsConf := &s.serverConf.Fs
	fs.StringVar(&fsConf.DefaultPVPath, "pfs-pv-path", fsConf.DefaultPVPath, "The PV config path")
	fs.StringVar(&fsConf.DefaultPVCPath, "pfs-pvc-path", fsConf.DefaultPVCPath, "The PVC config path")
	fs.StringVar(&fsConf.K8sServiceName, "pfs-service-name", fsConf.K8sServiceName, "The fs-server k8s-service name")
	fs.IntVar(&fsConf.K8sServicePort, "pfs-service-port", fsConf.K8sServicePort, "The fs-server k8s-service port")
}

func (s *ServerOption) InitFlag(fs *pflag.FlagSet) {
	if fs == nil {
		fs = pflag.CommandLine
	}
	s.AddFlagSet(fs)
	logger.AddFlagSet(fs, &s.serverConf.Log)
	database.AddFlagSet(fs, &s.serverConf.Database)
	pflag.CommandLine.AddGoFlagSet(goflag.CommandLine)
	pflag.Parse()
}
