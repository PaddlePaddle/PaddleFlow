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

package csiconfig

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	NodeName   = ""
	Namespace  = ""
	PodName    = ""
	MountImage = ""
	HostMntDir = ""

	CSIPod = corev1.Pod{}
)

const (
	PodTypeKey = "app.kubernetes.io/name"
	PodMount   = "pfs-mount"
)

func GeneratePodTemplate() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: Namespace,
			Labels: map[string]string{
				PodTypeKey: PodMount,
			},
			Annotations: make(map[string]string),
		},
		Spec: corev1.PodSpec{
			Containers:         []corev1.Container{},
			NodeName:           NodeName,
			HostNetwork:        CSIPod.Spec.HostNetwork,
			HostAliases:        CSIPod.Spec.HostAliases,
			HostPID:            CSIPod.Spec.HostPID,
			HostIPC:            CSIPod.Spec.HostIPC,
			DNSConfig:          CSIPod.Spec.DNSConfig,
			DNSPolicy:          CSIPod.Spec.DNSPolicy,
			ServiceAccountName: CSIPod.Spec.ServiceAccountName,
			ImagePullSecrets:   CSIPod.Spec.ImagePullSecrets,
			PreemptionPolicy:   CSIPod.Spec.PreemptionPolicy,
			Tolerations:        CSIPod.Spec.Tolerations,
		},
	}
}
