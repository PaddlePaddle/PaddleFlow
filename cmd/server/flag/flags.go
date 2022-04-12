package flag

import (
	"github.com/urfave/cli/v2"

	"paddleflow/pkg/common/config"
)

func ApiServerFlags(apiConf *config.ApiServerConfig) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "host",
			Value:       apiConf.Host,
			Usage:       "api server host",
			Destination: &apiConf.Host,
		},
		&cli.IntFlag{
			Name:        "port",
			Value:       apiConf.Port,
			Usage:       "api server port",
			Destination: &apiConf.Port,
		},
		&cli.IntFlag{
			Name:        "token-expire-hour",
			Value:       apiConf.TokenExpirationHour,
			Usage:       "token expire hour",
			Destination: &apiConf.TokenExpirationHour,
		},
	}
}

func JobFlags(jobConf *config.JobConfig) []cli.Flag {
	return []cli.Flag{
		&cli.IntFlag{
			Name:        "cluster-sync-period",
			Value:       jobConf.ClusterSyncPeriod,
			Usage:       "period of job manager get cluster information from database",
			Destination: &jobConf.ClusterSyncPeriod,
		},
		&cli.IntFlag{
			Name:        "queue-expire-time",
			Value:       jobConf.QueueExpireTime,
			Usage:       "expire time of queue in job manager cache",
			Destination: &jobConf.QueueExpireTime,
		},
		&cli.IntFlag{
			Name:        "queue-cache-size",
			Value:       jobConf.QueueCacheSize,
			Usage:       "number of queue in job manager cache",
			Destination: &jobConf.QueueCacheSize,
		},
		&cli.IntFlag{
			Name:        "job-loop-period",
			Value:       jobConf.JobLoopPeriod,
			Usage:       "loop period for job manager processing job",
			Destination: &jobConf.JobLoopPeriod,
		},
		&cli.IntFlag{
			Name:        "failed-job-ttl-seconds",
			Value:       jobConf.Reclaim.FailedJobTTLSeconds,
			Usage:       "failed job TTL Seconds",
			Destination: &jobConf.Reclaim.FailedJobTTLSeconds,
		},
		&cli.IntFlag{
			Name:        "job-ttl-seconds",
			Value:       jobConf.Reclaim.JobTTLSeconds,
			Usage:       "job TTL Seconds",
			Destination: &jobConf.Reclaim.JobTTLSeconds,
		},
		&cli.BoolFlag{
			Name:        "is-clean-job",
			Value:       jobConf.Reclaim.CleanJob,
			Usage:       "clean job",
			Destination: &jobConf.Reclaim.CleanJob,
		},
		&cli.BoolFlag{
			Name:        "skip-clean-failed-job",
			Value:       jobConf.Reclaim.SkipCleanFailedJob,
			Usage:       "skip clean failed job",
			Destination: &jobConf.Reclaim.SkipCleanFailedJob,
		},
	}
}

func FilesystemFlags(fsConf *config.FsServerConf) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "pfs-pv-path",
			Value:       fsConf.DefaultPVPath,
			Usage:       "PV config path",
			Destination: &fsConf.DefaultPVPath,
		},
		&cli.StringFlag{
			Name:        "pfs-pvc-path",
			Value:       fsConf.DefaultPVCPath,
			Usage:       "PVC config path",
			Destination: &fsConf.DefaultPVCPath,
		},
		&cli.StringFlag{
			Name:        "pfs-service-name",
			Value:       fsConf.K8sServiceName,
			Usage:       "fs-server k8s-service name",
			Destination: &fsConf.K8sServiceName,
		},
		&cli.IntFlag{
			Name:        "pfs-service-port",
			Value:       fsConf.K8sServicePort,
			Usage:       "fs-server k8s-service port",
			Destination: &fsConf.K8sServicePort,
		},
	}
}

func ExpandFlags(compoundFlags [][]cli.Flag) []cli.Flag {
	var flags []cli.Flag
	for _, flag := range compoundFlags {
		flags = append(flags, flag...)
	}
	return flags
}
