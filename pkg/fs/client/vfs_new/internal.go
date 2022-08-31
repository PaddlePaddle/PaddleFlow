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

package vfs

import (
	"bytes"
	"fmt"
	"os"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	log "github.com/sirupsen/logrus"

	meta "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/meta_new"
)

const (
	minInternalNode = 0x7FFFFFFF00000000
	statsInode      = minInternalNode + 1
)

type internalNode struct {
	inode Ino
	name  string
	attr  *Attr
}

var internalNodes = []*internalNode{
	{statsInode, ".stats", &Attr{Mode: 33206}},
}

func initInternalNodes() {
	log.Traceln("init() vfs internal nodes")
	uid := uint32(os.Getuid())
	gid := uint32(os.Getgid())
	now := int64(0)
	for _, v := range internalNodes {
		v.attr.Type = meta.TypeFile
		v.attr.Nlink = 1
		v.attr.Uid = uid
		v.attr.Gid = gid
		v.attr.Atime = now
		v.attr.Mtime = now
		v.attr.Ctime = now
		v.attr.Blksize = 4096
	}
}

func IsSpecialNode(ino Ino) bool {
	return ino >= minInternalNode
}

func IsSpecialName(name string) bool {
	if name[0] != '.' {
		return false
	}
	for _, n := range internalNodes {
		if name == n.name {
			return true
		}
	}
	return false
}

func getInternalNode(ino Ino) *internalNode {
	for _, n := range internalNodes {
		if ino == n.inode {
			return n
		}
	}
	return nil
}

func GetInternalNodeByName(name string) (Ino, *Attr) {
	n := getInternalNodeByName(name)
	if n != nil {
		return n.inode, n.attr
	}
	return 0, nil
}

func getInternalNodeByName(name string) *internalNode {
	if name[0] != '.' {
		return nil
	}
	for _, n := range internalNodes {
		if name == n.name {
			return n
		}
	}
	return nil
}

func collectMetrics(registry *prometheus.Registry) []byte {
	if registry == nil {
		log.Errorf("metrics collectMetrics: registry nil")
		return []byte("")
	}
	log.Tracef("metrics collectMetrics starts")
	mfs, err := registry.Gather()
	if err != nil {
		log.Errorf("collect metrics: %s", err)
		return nil
	}
	w := bytes.NewBuffer(nil)
	format := func(v float64) string {
		return strconv.FormatFloat(v, 'f', -1, 64)
	}
	for _, mf := range mfs {
		for _, m := range mf.Metric {
			var name string = *mf.Name
			for _, l := range m.Label {
				if *l.Name != "mp" && *l.Name != "vol_name" {
					name += "_" + *l.Value
				}
			}
			switch *mf.Type {
			case dto.MetricType_GAUGE:
				_, _ = fmt.Fprintf(w, "%s %s\n", name, format(*m.Gauge.Value))
			case dto.MetricType_COUNTER:
				_, _ = fmt.Fprintf(w, "%s %s\n", name, format(*m.Counter.Value))
			case dto.MetricType_HISTOGRAM:
				_, _ = fmt.Fprintf(w, "%s_total %d\n", name, *m.Histogram.SampleCount)
				_, _ = fmt.Fprintf(w, "%s_sum %s\n", name, format(*m.Histogram.SampleSum))
			case dto.MetricType_SUMMARY:
			}
		}
	}
	// log.Tracef("metrics collectMetrics: %s", w.String())
	return w.Bytes()
}
