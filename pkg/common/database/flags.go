package database

import (
	"github.com/urfave/cli/v2"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
)

func DatabaseFlags(dbConf *config.DatabaseConfig) []cli.Flag {
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
