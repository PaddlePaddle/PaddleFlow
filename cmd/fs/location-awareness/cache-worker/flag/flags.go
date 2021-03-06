/*
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

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

package flag

import "github.com/urfave/cli/v2"

func CacheWorkerFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:  "fsID",
			Value: "",
			Usage: "file system id",
		},
		&cli.StringFlag{
			Name:  "nodename",
			Value: "",
			Usage: "nodename",
		},
		&cli.StringFlag{
			Name:  "clusterID",
			Value: "",
			Usage: "clusterID",
		},
		&cli.StringFlag{
			Name:  "cacheDir",
			Value: "",
			Usage: "cacheDir",
		},
		&cli.StringFlag{
			Name:  "podCachePath",
			Value: "",
			Usage: "podCachePath",
		},
		&cli.StringFlag{
			Name:  "server",
			Value: "",
			Usage: "server",
		},
		&cli.StringFlag{
			Name:  "username",
			Value: "root",
			Usage: "username",
		},
		&cli.StringFlag{
			Name:  "password",
			Value: "",
			Usage: "password",
		},
	}
}
