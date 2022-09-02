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

/*
 kube-stat-metric must be setup
*/

package metrics

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"

	prometheus_model "github.com/prometheus/common/model"
)

const (
	QueryTimeout = time.Second * 1
)

const (
	PromQLQueryPodAnnotations = "kube_pod_annotations{pod~=\"%s\"}"
	PromQLQueryPodLabels      = "kube_pod_labels{pod~=\"%s\"}"
)

// GetQueryLabelsFromPrometheus return query labels from prometheus
func GetQueryLabelsFromPrometheus(query string) map[string]string {
	ctx, cancel := context.WithTimeout(context.Background(), QueryTimeout)
	defer cancel()
	val, _, err := PromAPIClient.Query(
		ctx,
		query,
		time.Now(),
	)
	if err != nil {
		log.Errorf("failed to get pod annotation by promethues query: %s", err)
		return nil
	}
	log.Infof("query labels from prometheus values: %s", val.String())
	vec, ok := val.(prometheus_model.Vector)
	if !ok {
		log.Errorf("failed to get query: cast failed")
		return nil
	}
	if len(vec) == 0 {
		return nil
	}

	labels := map[string]string{}
	// only take the first vec
	for labelName, labelValue := range vec[0].Metric {
		if labelName.IsValid() {
			labels[string(labelName)] = string(labelValue)
		}
	}
	return labels

}
