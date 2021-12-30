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

package framework

import (
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	SparkApp = "sparkapp"
	VCJob    = "vcjob"
	BatchJob = "batchjob"

	WorkerSuffix   = "-worker"
	LauncherSuffix = "-launcher"

	SparkDriverPodNameKey = "spark.kubernetes.driver.pod.name"
	// SparkRoleLabel is the driver/executor label set by the operator/spark-distribution on the driver/executors Pods.
	SparkRoleLabel = "spark-role"
	// SparkExecutorRole is the value of the spark-role label for the executors.
	SparkExecutorRole = "executor"
	// SparkLabelPrefix is the prefix of every labels and annotations added by the controller.
	SparkLabelPrefix = "sparkoperator.k8s.io/"
	// SparkAppNameLabel is the name of the label for the SparkApplication object name.
	SparkAppNameLabel = SparkLabelPrefix + "app-name"
	// SparkSubmissionIDLabel is the label that records the submission ID of the current run of an application.
	SparkSubmissionIDLabel = SparkLabelPrefix + "submission-id"

	BatchJobNameLabel = "job-name"
)

type Request struct {
	Namespace string
	Name      string
	JobStatus string
}

type FinishedJobInfo struct {
	GVK       schema.GroupVersionKind
	Namespace string
	Name      string
	OwnerName string
}

func FindOwnerReferenceName(ownerReference []v1.OwnerReference) string {
	return ""
}
