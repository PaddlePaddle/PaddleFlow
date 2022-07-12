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
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/consts"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

type JobCollector struct {
	CpuUsageRate     *prometheus.GaugeVec
	MemoryUsage      *prometheus.GaugeVec
	NetReceiveBytes  *prometheus.GaugeVec
	NetTransmitBytes *prometheus.GaugeVec
	DiskUsage        *prometheus.GaugeVec
	DiskReadRate     *prometheus.GaugeVec
	DiskWriteRate    *prometheus.GaugeVec
	GpuUtil          *prometheus.GaugeVec
	GpuMemUtil       *prometheus.GaugeVec
}

func newJobCollectManager() *JobCollector {
	cpuUsageRate := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: consts.MetricCpuUsageRate,
		Help: consts.MetricCpuUsageRate,
	}, []string{"jobID", "pod"},
	)
	memoryUsage := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: consts.MetricMemoryUsageRate,
		Help: consts.MetricMemoryUsageRate,
	}, []string{"jobID", "pod"},
	)
	netReceiveBytes := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: consts.MetricNetReceiveBytes,
		Help: consts.MetricNetReceiveBytes,
	}, []string{"jobID", "pod"},
	)
	netTransmitBytes := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: consts.MetricNetSendBytes,
		Help: consts.MetricNetSendBytes,
	}, []string{"jobID", "pod"},
	)
	diskUsage := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: consts.MetricDiskUsage,
		Help: consts.MetricDiskUsage,
	}, []string{"jobID", "pod"},
	)
	diskReadRate := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: consts.MetricDiskReadRate,
		Help: consts.MetricDiskReadRate,
	}, []string{"jobID", "pod"},
	)
	diskWriteRate := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: consts.MetricDiskWriteRate,
		Help: consts.MetricDiskWriteRate,
	}, []string{"jobID", "pod"},
	)
	gpuUtil := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: consts.MetricGpuUtil,
		Help: consts.MetricGpuUtil,
	}, []string{"jobID", "pod"},
	)
	gpuMemUtil := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: consts.MetricGpuMemoryUtil,
		Help: consts.MetricGpuMemoryUtil,
	}, []string{"jobID", "pod"},
	)
	return &JobCollector{
		CpuUsageRate:     cpuUsageRate,
		MemoryUsage:      memoryUsage,
		NetReceiveBytes:  netReceiveBytes,
		NetTransmitBytes: netTransmitBytes,
		DiskUsage:        diskUsage,
		DiskReadRate:     diskReadRate,
		DiskWriteRate:    diskWriteRate,
		GpuUtil:          gpuUtil,
		GpuMemUtil:       gpuMemUtil,
	}
}

func (j *JobCollector) Describe(ch chan<- *prometheus.Desc) {
	j.CpuUsageRate.Describe(ch)
	j.MemoryUsage.Describe(ch)
	j.NetReceiveBytes.Describe(ch)
	j.NetTransmitBytes.Describe(ch)
	j.DiskUsage.Describe(ch)
	j.DiskReadRate.Describe(ch)
	j.DiskWriteRate.Describe(ch)
	j.GpuUtil.Describe(ch)
	j.GpuMemUtil.Describe(ch)
}

func (j *JobCollector) Collect(ch chan<- prometheus.Metric) {
	err := j.CollectPodMetrics(consts.MetricCpuUsageRate)
	if err != nil {
		log.Errorf("collect podMetrics[%s] failed, error:[%s]", consts.MetricCpuUsageRate, err.Error())
		return
	}
	j.CpuUsageRate.Collect(ch)
	err = j.CollectPodMetrics(consts.MetricMemoryUsageRate)
	if err != nil {
		log.Errorf("collect podMetrics[%s] failed, error:[%s]", consts.MetricMemoryUsageRate, err.Error())
		return
	}
	j.MemoryUsage.Collect(ch)
	err = j.CollectPodMetrics(consts.MetricNetReceiveBytes)
	if err != nil {
		log.Errorf("collect podMetrics[%s] failed, error:[%s]", consts.MetricNetReceiveBytes, err.Error())
		return
	}
	j.NetReceiveBytes.Collect(ch)
	err = j.CollectPodMetrics(consts.MetricNetSendBytes)
	if err != nil {
		log.Errorf("collect podMetrics[%s] failed, error:[%s]", consts.MetricNetSendBytes, err.Error())
		return
	}
	j.NetTransmitBytes.Collect(ch)
	err = j.CollectPodMetrics(consts.MetricDiskUsage)
	if err != nil {
		log.Errorf("collect podMetrics[%s] failed, error:[%s]", consts.MetricDiskUsage, err.Error())
		return
	}
	j.DiskUsage.Collect(ch)
	err = j.CollectPodMetrics(consts.MetricDiskReadRate)
	if err != nil {
		log.Errorf("collect podMetrics[%s] failed, error:[%s]", consts.MetricDiskReadRate, err.Error())
		return
	}
	j.DiskReadRate.Collect(ch)
	err = j.CollectPodMetrics(consts.MetricDiskWriteRate)
	if err != nil {
		log.Errorf("collect podMetrics[%s] failed, error:[%s]", consts.MetricDiskWriteRate, err.Error())
		return
	}
	j.DiskWriteRate.Collect(ch)
	err = j.CollectPodMetrics(consts.MetricGpuUtil)
	if err != nil {
		log.Errorf("collect podMetrics[%s] failed, error:[%s]", consts.MetricGpuUtil, err.Error())
		return
	}
	j.GpuUtil.Collect(ch)
	err = j.CollectPodMetrics(consts.MetricGpuMemoryUtil)
	if err != nil {
		log.Errorf("collect podMetrics[%s] failed, error:[%s]", consts.MetricGpuMemoryUtil, err.Error())
		return
	}
	j.GpuMemUtil.Collect(ch)
}

func (j *JobCollector) CollectPodMetrics(metricName string) error {
	jobs := models.ListJobByStatus(schema.StatusJobRunning)
	for _, value := range jobs {
		podNameList := make([]string, 0)
		if err := getPodNameList(&podNameList, value); err != nil {
			log.Errorf("job[%s] get pod name list error %s", value.ID, err.Error())
			return err
		}
		result, err := callPrometheusAPI(metricName, value.ID)
		if err != nil {
			log.Errorf("call prometheus query api error %s", err.Error())
			return err
		}
		data, ok := result.(model.Vector)
		if !ok {
			log.Errorf("convert result to vector failed")
			return err
		}
		for _, metric := range data {
			for _, podName := range podNameList {
				if podName == string(metric.Metric["pod"]) {
					j.CpuUsageRate.With(prometheus.Labels{"jobID": value.ID, "pod": podName}).Set(float64(metric.Value))
				}
			}
		}
	}
	return nil
}

func callPrometheusAPI(metricName, jobID string) (model.Value, error) {
	ctxP, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	query := getQuerySql(metricName)
	result, _, err := PrometheusClientAPI.Query(ctxP, query, time.Now())
	if err != nil {
		log.Errorf("job[%s] prometheus query range api error %s", jobID, err.Error())
		return nil, err
	}
	return result, nil
}

func getQuerySql(metricName string) string {
	var querySql string
	switch metricName {
	case consts.MetricCpuUsageRate:
		querySql = QueryCPUUsageRateQl
		return querySql
	case consts.MetricMemoryUsageRate:
		querySql = QueryMEMUsageRateQl
		return querySql
	case consts.MetricNetReceiveBytes:
		querySql = QueryNetReceiveQl
		return querySql
	case consts.MetricNetSendBytes:
		querySql = QueryNetTransmitQl
		return querySql
	case consts.MetricDiskUsage:
		querySql = QueryDiskUsageQl
		return querySql
	case consts.MetricDiskReadRate:
		querySql = QueryDiskReadQl
		return querySql
	case consts.MetricDiskWriteRate:
		querySql = QueryDiskWriteQl
		return querySql
	case consts.MetricGpuUtil:
		querySql = QueryGpuUtilQl
		return querySql
	case consts.MetricGpuMemoryUtil:
		querySql = QueryGpuMemUtilQl
		return querySql
	default:
		return ""
	}
}

func getPodNameList(podNameList *[]string, job models.Job) error {
	names, err := getTaskName(job.ID)
	if err != nil {
		log.Errorf("get job[%s] tasks failed, error:[%s]", job.ID, err.Error())
		return err
	}
	*podNameList = append(*podNameList, names...)
	return nil
}

func getTaskName(jobID string) ([]string, error) {
	taskNameList := make([]string, 0)
	tasks, err := models.ListByJobID(jobID)
	if err != nil {
		log.Errorf("list job[%s] tasks failed, error:[%s]", jobID, err.Error())
		return taskNameList, err
	}
	for _, task := range tasks {
		taskNameList = append(taskNameList, task.Name)
	}
	return taskNameList, nil

}
