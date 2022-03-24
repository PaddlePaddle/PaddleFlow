/*
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"paddleflow/cmd/fs/fuse/flag"
	"paddleflow/cmd/fs/fuse/service"
	"paddleflow/pkg/common/config"
)

func main() {
	err := Main(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func Main(args []string) error {
	cli.VersionFlag = &cli.BoolFlag{
		Name: "version", Aliases: []string{"V"},
		Usage: "print only the version",
	}
	config.InitFuseConfig()
	compoundFlags := [][]cli.Flag{
		flag.GlobalFlags(&config.FuseConf.Fuse),
		flag.UserFlags(&config.FuseConf.Fuse),
		flag.MetricsFlags(&config.FuseConf.Fuse),
		flag.LogFlags(&config.FuseConf.Log),
	}

	app := &cli.App{
		Name:                 "paddleflow-fuse",
		Usage:                "A POSIX file system built on kv DB and object storage.",
		Version:              "1.4.1",
		Copyright:            "Apache License 2.0",
		HideHelpCommand:      true,
		EnableBashCompletion: true,
		Flags:                flag.ExpandFlags(compoundFlags),
		Commands: []*cli.Command{
			service.CmdMount(),
			//cmdUmount(),
			//cmdStats(),
			//cmdProfile(),
			//cmdBench(),
		},
	}
	return app.Run(args)
}
