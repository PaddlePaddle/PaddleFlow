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
	"fmt"
	"strings"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/consts"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

type KubernetesMetric struct {
	PrometheusClientAPI v1.API
}

func NewKubernetesMetric(clientAPI v1.API) MetricInterface {
	return &KubernetesMetric{
		PrometheusClientAPI: clientAPI,
	}
}

func (km *KubernetesMetric) GetJobAvgMetrics(metricName, jobID string) (float64, error) {
	job, err := storage.Job.GetJobByID(jobID)
	if err != nil {
		log.Errorf("job[%s] find error %s", jobID, err.Error())
		return 0.0, err
	}
	var start, end int64
	if job.ActivatedAt.Valid {
		start = job.ActivatedAt.Time.Unix()
	} else {
		return 0.0, nil
	}
	if schema.IsImmutableJobStatus(job.Status) {
		end = job.UpdatedAt.Unix()
	} else {
		end = time.Now().Unix()
	}
	result, err := km.GetJobSequenceMetrics(metricName, jobID, start, end, 30)
	if err != nil {
		log.Errorf("job[%s] get prometheus sequence data error %s", jobID, err.Error())
		return 0.0, err
	}
	data, ok := result.(model.Matrix)
	if !ok {
		return 0.0, fmt.Errorf("convert result to matrix failed")
	}
	sum := 0.0
	count := 0
	for _, value := range data {
		for _, rangeValue := range value.Values {
			sum += float64(rangeValue.Value)
			count += 1
		}
	}
	if count != 0 {
		return sum / float64(count), nil
	}
	return sum, nil
}

func (km *KubernetesMetric) GetJobSequenceMetrics(metricName, jobID string, start, end, step int64) (model.Value, error) {
	tasks, err := storage.Job.ListByJobID(jobID)
	if err != nil {
		log.Errorf("job[%s] get task error %s", jobID, err.Error())
		return nil, err
	}
	podNameList := make([]string, 0)
	for _, task := range tasks {
		podNameList = append(podNameList, task.Name)
	}
	podNames := strings.Join(podNameList, "|")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	queryPromql := getQuerySqlByMetric(metricName, podNames)
	r := v1.Range{
		Start: time.Unix(start, 0),
		End:   time.Unix(end, 0),
		Step:  time.Duration(step) * time.Second,
	}
	result, _, err := km.PrometheusClientAPI.QueryRange(ctx, queryPromql, r)
	if err != nil {
		log.Errorf("job[%s] prometheus query range api error %s", jobID, err.Error())
		return nil, err
	}
	return result, nil
}

func getQuerySqlByMetric(metricName, podNames string) string {
	switch metricName {
	case consts.MetricCpuUsageRate:
		return fmt.Sprintf(QueryCPUUsageRateQl, podNames, podNames)
	case consts.MetricMemoryUsageRate:
		return fmt.Sprintf(QueryMEMUsageRateQl, podNames, podNames)
	case consts.MetricMemoryUsage:
		return fmt.Sprintf(QueryMEMUsageQl, podNames)
	case consts.MetricDiskUsage:
		return fmt.Sprintf(QueryDiskUsageQl, podNames)
	case consts.MetricNetReceiveBytes:
		return fmt.Sprintf(QueryNetReceiveQl, podNames)
	case consts.MetricNetSendBytes:
		return fmt.Sprintf(QueryNetTransmitQl, podNames)
	case consts.MetricDiskReadRate:
		return fmt.Sprintf(QueryDiskReadQl, podNames)
	case consts.MetricDiskWriteRate:
		return fmt.Sprintf(QueryDiskWriteQl, podNames)
	case consts.MetricGpuUtil:
		return fmt.Sprintf(QueryGpuUtilQl, podNames)
	case consts.MetricGpuMemoryUtil:
		return fmt.Sprintf(QueryGpuMemUtilQl, podNames, podNames)
	case consts.MetricGpuMemoryUsage:
		return fmt.Sprintf(QueryGpuMemUsageQl, podNames)
	default:
		return ""
	}
}
