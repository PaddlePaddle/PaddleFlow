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

package options

import (
	goflag "flag"
	"github.com/spf13/pflag"

	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/logger"
)

// CSIPluginOption is the main context object for the csi plugin.
type CSIPluginOption struct{}

func NewCSIPluginOption() *CSIPluginOption {
	return &CSIPluginOption{}
}

func (csi *CSIPluginOption) AddFlagSet(fs *pflag.FlagSet) {
	if fs == nil {
		fs = pflag.CommandLine
	}
	csiPluginConf := &config.CSIPluginConf.CSIPlugin
	fs.StringVar(&csiPluginConf.UnixEndpoint, "unix-endpoint", csiPluginConf.UnixEndpoint, "CSI endpoint")
	fs.StringVar(&csiPluginConf.NodeID, "node-id", csiPluginConf.NodeID, "node id")
	fs.BoolVar(&csiPluginConf.PprofEnable, "pprof-enable", csiPluginConf.PprofEnable, "pprof debug tool")
	fs.IntVar(&csiPluginConf.PprofPort, "pprof-port", csiPluginConf.PprofPort, "pprof port")
}

func (csi *CSIPluginOption) InitFlag(fs *pflag.FlagSet) {
	if fs == nil {
		fs = pflag.CommandLine
	}
	csi.AddFlagSet(fs)
	logger.AddFlagSet(fs, &config.CSIPluginConf.Log)
	pflag.CommandLine.AddGoFlagSet(goflag.CommandLine)
	pflag.Parse()
}
