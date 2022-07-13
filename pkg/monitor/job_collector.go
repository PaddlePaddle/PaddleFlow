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
	CpuUsageRate *prometheus.GaugeVec
	MemoryUsage  *prometheus.GaugeVec
}

func newJobCollectManager() *JobCollector {
	cpuUsageRate := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: consts.MetricCpuUsageRate,
		Help: consts.MetricCpuUsageRate,
	}, []string{"jobID", "pod"},
	)
	memoryUsage := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: consts.MetricMemoryUsage,
		Help: consts.MetricMemoryUsage,
	}, []string{"jobID", "pod"},
	)
	return &JobCollector{
		CpuUsageRate: cpuUsageRate,
		MemoryUsage:  memoryUsage,
	}
}

func (j *JobCollector) Describe(ch chan<- *prometheus.Desc) {
	j.CpuUsageRate.Describe(ch)
	j.MemoryUsage.Describe(ch)
}

func (j *JobCollector) Collect(ch chan<- prometheus.Metric) {
	err := j.CollectPodMetrics(consts.MetricCpuUsageRate)
	if err != nil {
		log.Errorf("collect podMetrics[%s] failed, error:[%s]", consts.MetricCpuUsageRate, err.Error())
		return
	}
	j.CpuUsageRate.Collect(ch)
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
	switch metricName {
	case consts.MetricCpuUsageRate:
		querySql := QueryCPUUsageRateQl
		return querySql
		// TODO add more metric sql
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
