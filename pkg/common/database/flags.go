package database

import (
	"github.com/spf13/pflag"
	"github.com/urfave/cli/v2"

	"paddleflow/pkg/common/config"
)

func AddFlagSet(fs *pflag.FlagSet, databaseConfig *config.DatabaseConfig) {
	if fs == nil {
		fs = pflag.CommandLine
	}
	fs.StringVar(&databaseConfig.Driver, "db-configPath", databaseConfig.Driver, "Driver")
	fs.StringVar(&databaseConfig.Host, "db-host", databaseConfig.Host, "Host")
	fs.StringVar(&databaseConfig.Port, "db-port", databaseConfig.Port, "Port")
	fs.StringVar(&databaseConfig.User, "db-user", databaseConfig.User, "User")
	fs.StringVar(&databaseConfig.Password, "db-password", databaseConfig.Password, "Password")
	fs.StringVar(&databaseConfig.Database, "db-database", databaseConfig.Database, "database")
	fs.IntVar(&databaseConfig.ConnectTimeoutInSeconds, "db-connect-timeout-in-seconds",
		databaseConfig.ConnectTimeoutInSeconds, "ConnectTimeoutInSeconds")
	fs.IntVar(&databaseConfig.LockTimeoutInMilliseconds, "db-lock-timeout-in-milliseconds",
		databaseConfig.LockTimeoutInMilliseconds, "LockTimeoutInMilliseconds")
	fs.IntVar(&databaseConfig.IdleTransactionTimeoutInMilliseconds,
		"db-idle-transaction-timeout-in-milliseconds", databaseConfig.IdleTransactionTimeoutInMilliseconds,
		"IdleTransactionTimeoutInMilliseconds")
}

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
