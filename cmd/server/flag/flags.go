package flag

import (
	"time"

	"github.com/urfave/cli/v2"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
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

func StorageFlags(dbConf *config.StorageConfig) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "db-configPath",
			Value:       dbConf.Driver,
			Usage:       "driver",
			Destination: &dbConf.Driver,
		},
		&cli.StringFlag{
			Name:        "db-host",
			Value:       dbConf.Host,
			Usage:       "host",
			Destination: &dbConf.Host,
		},
		&cli.StringFlag{
			Name:        "db-port",
			Value:       dbConf.Port,
			Usage:       "port",
			Destination: &dbConf.Port,
		},
		&cli.StringFlag{
			Name:        "db-user",
			Value:       dbConf.User,
			Usage:       "user",
			Destination: &dbConf.User,
		},
		&cli.StringFlag{
			Name:        "db-password",
			Value:       dbConf.Password,
			Usage:       "password",
			Destination: &dbConf.Password,
		},
		&cli.StringFlag{
			Name:        "db-database",
			Value:       dbConf.Database,
			Usage:       "database",
			Destination: &dbConf.Database,
		},
		&cli.IntFlag{
			Name:        "db-connect-timeout-in-seconds",
			Value:       dbConf.ConnectTimeoutInSeconds,
			Usage:       "db connect timeout in seconds",
			Destination: &dbConf.ConnectTimeoutInSeconds,
		},
		&cli.IntFlag{
			Name:        "db-lock-timeout-in-milliseconds",
			Value:       dbConf.LockTimeoutInMilliseconds,
			Usage:       "db lock timeout in milliseconds",
			Destination: &dbConf.LockTimeoutInMilliseconds,
		},
		&cli.IntFlag{
			Name:        "db-idle-transaction-timeout-in-milliseconds",
			Value:       dbConf.IdleTransactionTimeoutInMilliseconds,
			Usage:       "db idle transaction timeout in milliseconds",
			Destination: &dbConf.IdleTransactionTimeoutInMilliseconds,
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
		&cli.BoolFlag{
			Name:        "sync-cluster-queue",
			Value:       jobConf.SyncClusterQueue,
			Usage:       "sync cluster queues",
			Destination: &jobConf.SyncClusterQueue,
		},
		&cli.IntFlag{
			Name:        "failed-job-ttl-seconds",
			Value:       jobConf.Reclaim.FailedJobTTLSeconds,
			Usage:       "failed job TTL Seconds",
			Destination: &jobConf.Reclaim.FailedJobTTLSeconds,
		},
		&cli.IntFlag{
			Name:        "succeeded-job-ttl-seconds",
			Value:       jobConf.Reclaim.SucceededJobTTLSeconds,
			Usage:       "succeeded job TTL Seconds",
			Destination: &jobConf.Reclaim.SucceededJobTTLSeconds,
		},
		&cli.IntFlag{
			Name:        "pending-job-ttl-seconds",
			Value:       jobConf.Reclaim.PendingJobTTLSeconds,
			Usage:       "pending job TTL Seconds, when job pull image failed",
			Destination: &jobConf.Reclaim.PendingJobTTLSeconds,
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
		&cli.BoolFlag{
			Name:        "is-single-cluster",
			Value:       jobConf.IsSingleCluster,
			Usage:       "init a cluster named 'default' by default in single cluster mode",
			Destination: &jobConf.IsSingleCluster,
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
		&cli.DurationFlag{
			Name:        "mount-pod-expire",
			Value:       7 * 24 * time.Hour,
			Usage:       "the expiration time when the mount pod needs to be destroyed without used",
			Destination: &fsConf.MountPodExpire,
		},
		&cli.DurationFlag{
			Name:        "clean-pod-interval",
			Value:       30 * time.Second,
			Usage:       "the interval time for clean mount pod",
			Destination: &fsConf.CleanMountPodIntervalTime,
		},
		&cli.DurationFlag{
			Name:        "sync-fscache-stats-interval",
			Value:       10 * time.Second,
			Usage:       "the interval time for sync fscache stats",
			Destination: &fsConf.SyncCacheStatsInterval,
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
