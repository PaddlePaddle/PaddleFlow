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

package metric

import (
	"github.com/urfave/cli/v2"
)

func MetricsFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:  "schema",
			Value: "umc",
			Usage: "schema string that controls the output sections (u: usage, f: fuse, m: meta, c: blockcache, o: object, g: go)",
		},
		&cli.UintFlag{
			Name:  "interval",
			Value: 1,
			Usage: "interval in seconds between each update",
		},
		&cli.UintFlag{
			Name:    "verbosity",
			Aliases: []string{"l"},
			Usage:   "verbosity level, 0 or 1 is enough for most cases",
		},
		&cli.BoolFlag{
			Name:  "pprof-enable",
			Value: false,
			Usage: "enable go pprof",
		},
		&cli.IntFlag{
			Name:  "pprof-port",
			Value: 6060,
			Usage: "pprof port",
		},
		&cli.BoolFlag{
			Name:  "metrics-service-on",
			Value: false,
			Usage: "whether to start metrics service",
		},
		&cli.IntFlag{
			Name:  "metrics-service-port",
			Value: 8993,
			Usage: "metrics service port",
		},
	}
}
