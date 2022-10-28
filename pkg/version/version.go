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

package version

import (
	"fmt"
	"runtime"
)

var (
	GitVersion = "v0.0.0"
	GitCommit  = "unknown"
	GitBranch  = "unknown"
	BuildDate  = "unknown"
)

type VerInfo struct {
	GitVersion string `json:"gitVersion"`
	GitCommit  string `json:"gitCommit"`
	GitBranch  string `json:"gitBranch"`
	BuildDate  string `json:"buildDate"`
	GoVersion  string `json:"goVersion"`
	Compiler   string `json:"compiler"`
	Platform   string `json:"platform"`
}

var Info = VerInfo{
	GitVersion: GitVersion,
	GitCommit:  GitCommit,
	GitBranch:  GitBranch,
	BuildDate:  BuildDate,
	GoVersion:  runtime.Version(),
	Compiler:   runtime.Compiler,
	Platform:   fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
}

func info() []string {
	return []string{
		fmt.Sprintf("GitVersion: %v", Info.GitVersion),
		fmt.Sprintf("GitCommit: %v", Info.GitCommit),
		fmt.Sprintf("GitBranch: %v", Info.GitBranch),
		fmt.Sprintf("BuildDate: %v", Info.BuildDate),
		fmt.Sprintf("GoVersion: %v", Info.GoVersion),
		fmt.Sprintf("Compiler: %v", Info.Compiler),
		fmt.Sprintf("Platform: %s", Info.Platform),
	}
}

func InfoStr() string {
	var str string
	for _, i := range info() {
		str += fmt.Sprintf("\n%v", i)
	}
	return str
}
