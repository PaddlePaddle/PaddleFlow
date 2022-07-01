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
	"context"
	"time"

	"github.com/PaddlePaddle/PaddleFlow/pkg/monitor"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

type JobCollector struct {
	CpuUsageRate *prometheus.GaugeVec
	MemoryUsage  *prometheus.GaugeVec
}

func newJobCollectManager() *JobCollector {
	cpuUsageRate := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: common.MetricJobCpuUsageRate,
		Help: common.MetricJobCpuUsageRate,
	}, []string{"jobID", "pod"},
	)
	memoryUsage := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: common.MetricJobMemoryUsage,
		Help: common.MetricJobMemoryUsage,
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
	err := j.CollectPodMetrics(common.MetricJobCpuUsageRate)
	if err != nil {
		log.Errorf("collect podMetrics[%s] failed, error:[%s]", common.MetricJobCpuUsageRate, err.Error())
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
		ctxP, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		query := getQuerySql(metricName)
		result, _, err := monitor.PrometheusClientAPI.Query(ctxP, query, time.Now())
		if err != nil {
			log.Errorf("job[%s] prometheus query range api error %s", value.ID, err.Error())
			return err
		}
		data := result.(model.Vector)
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

func getQuerySql(metricName string) string {
	switch metricName {
	case common.MetricJobCpuUsageRate:
		querySql := "sum(rate(container_cpu_usage_seconds_total{image!=\"\"，pod=~\".*job.*\"}[1m])) by (pod) * 100"
		return querySql
		// TODO add more metric sql
	default:
		return ""
	}
}

func getPodNameList(podNameList *[]string, job models.Job) error {
	names, err := getTaskName(job.ID)
	if err != nil {
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
