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
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
)

type KubernetesMetric struct {
	PrometheusClientAPI v1.API
}

func NewKubernetesMetric(clientAPI v1.API) MetricInterface {
	return &KubernetesMetric{
		PrometheusClientAPI: clientAPI,
	}
}

func (km *KubernetesMetric) GetJobAvgMetrics(metricName, jobID string) (model.Value, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	queryPromql := fmt.Sprintf("avg(%s{jobID=\"%s\"}) without (podID)", metricName, jobID)
	result, _, err := km.PrometheusClientAPI.Query(ctx, queryPromql, time.Now())
	if err != nil {
		log.Errorf("job[%s] prometheus query api error %s", jobID, err.Error())
		return nil, err
	}
	return result, nil
}

func (km *KubernetesMetric) GetJobSequenceMetrics(metricName, jobID string, start, end, step int64) (model.Value, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	queryPromql := fmt.Sprintf("%s{jobID=\"%s\"}", metricName, jobID)
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