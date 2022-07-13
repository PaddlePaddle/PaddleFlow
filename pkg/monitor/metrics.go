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

package monitor

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	log "github.com/sirupsen/logrus"
)

var (
	start = time.Now()
	cpu   = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cpu_usage",
		Help: "Accumulated CPU usage in seconds.",
	}, func() float64 {
		ru := GetRusage()
		return ru.GetStime() + ru.GetUtime()
	})
	memory = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "memory",
		Help: "Used memory in bytes.",
	}, func() float64 {
		_, rss := MemoryUsage()
		return float64(rss)
	})
	uptime = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "uptime",
		Help: "Total running time in seconds.",
	}, func() float64 {
		return time.Since(start).Seconds()
	})
	//usedSpace = prometheus.NewGauge(prometheus.GaugeOpts{
	//	Name: "used_space",
	//	Help: "Total used space in bytes.",
	//})
	//usedInodes = prometheus.NewGauge(prometheus.GaugeOpts{
	//	Name: "used_inodes",
	//	Help: "Total number of inodes.",
	//})
)

func UpdateBaseMetrics( /*m meta.Meta*/ ) {
	prometheus.MustRegister(cpu)
	prometheus.MustRegister(memory)
	prometheus.MustRegister(uptime)
	//prometheus.MustRegister(usedSpace)
	//prometheus.MustRegister(usedInodes)

	//ctx := meta.Background
	for {
		//var totalSpace, availSpace, iused, iavail uint64
		//err := m.StatFS(ctx, &totalSpace, &availSpace, &iused, &iavail)
		//if err == 0 {
		//	usedSpace.Set(float64(totalSpace - availSpace))
		//	usedInodes.Set(float64(iused))
		//}
		time.Sleep(time.Second * 10)
	}
}

func GetMetricValue(col prometheus.Collector) float64 {
	c := make(chan prometheus.Metric, 1) // 1 for metric with no vector
	col.Collect(c)                       // collect current metric value into the channel
	m := dto.Metric{}
	_ = (<-c).Write(&m) // read metric value from the channel
	return *m.Counter.Value
}

func CollectMetrics() []byte {
	if prometheus.DefaultGatherer == nil {
		return []byte("")
	}
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		log.Errorf("gather prometheus metrics err: %s", err)
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
	return w.Bytes()
}

//------------ util funcs ------------//

type Rusage struct {
	syscall.Rusage
}

// GetUtime returns the user time in seconds.
func (ru *Rusage) GetUtime() float64 {
	return float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6
}

// GetStime returns the system time in seconds.
func (ru *Rusage) GetStime() float64 {
	return float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
}

// GetRusage returns CPU usage of current process.
func GetRusage() *Rusage {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return &Rusage{ru}
}

func MemoryUsage() (virt, rss uint64) {
	stat, err := ioutil.ReadFile("/proc/self/stat")
	if err == nil {
		stats := bytes.Split(stat, []byte(" "))
		if len(stats) >= 24 {
			v, _ := strconv.ParseUint(string(stats[22]), 10, 64)
			r, _ := strconv.ParseUint(string(stats[23]), 10, 64)
			return v, r * 4096
		}
	}

	var ru syscall.Rusage
	err = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	if err == nil {
		return uint64(ru.Maxrss), uint64(ru.Maxrss)
	}
	return
}
