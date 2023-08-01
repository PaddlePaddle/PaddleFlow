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

package main

import (
	"context"
	"fmt"

	"github.com/PaddlePaddle/PaddleFlow/go-sdk/service"
	v1 "github.com/PaddlePaddle/PaddleFlow/go-sdk/service/apiserver/v1"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

func main() {
	config := &core.PaddleFlowClientConfiguration{
		Host:                       "",
		Port:                       8999,
		ConnectionTimeoutInSeconds: 1,
	}
	pfClient, err := service.NewForClient(config)
	if err != nil {
		panic(err)
	}
	data, err := pfClient.APIV1().User().Login(context.TODO(), &v1.LoginInfo{
		UserName: "",
		Password: "",
	})
	if err != nil {
		panic(err)
	}
	token := data.Authorization

	queueName := "test-queue"

	// Create single job
	createResult, err := pfClient.APIV1().Job().Create(context.TODO(), &v1.CreateSingleJobRequest{
		CommonJobInfo: v1.CommonJobInfo{
			Name: "test-single-job",
			Labels: map[string]string{
				"job-type": "single",
			},
			SchedulingPolicy: v1.SchedulingPolicy{
				Queue: queueName,
			},
		},
		JobSpec: v1.JobSpec{
			Flavour: schema.Flavour{
				Name: "customFlavour",
				ResourceInfo: schema.ResourceInfo{
					CPU: "4",
					Mem: "8Gi",
					ScalarResources: map[schema.ResourceName]string{
						"nvidia.com/gpu": "1",
					},
				},
			},
			Image: "xxx",
		},
	}, nil, nil, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("create single job result %v\n", createResult)

	// Create distributed job
	createResult, err = pfClient.APIV1().Job().Create(context.TODO(), nil, &v1.CreateDisJobRequest{
		CommonJobInfo: v1.CommonJobInfo{
			Name: "test-distributed-paddle-job",
			Labels: map[string]string{
				"job-type": "distributed",
			},
			SchedulingPolicy: v1.SchedulingPolicy{
				Queue: queueName,
			},
		},
		Framework: schema.FrameworkPaddle,
		Members: []v1.MemberSpec{
			{
				JobSpec: v1.JobSpec{
					Flavour: schema.Flavour{
						Name: "customFlavour",
						ResourceInfo: schema.ResourceInfo{
							CPU: "4",
							Mem: "8Gi",
							ScalarResources: map[schema.ResourceName]string{
								"nvidia.com/gpu": "1",
							},
						},
					},
					Image: "xxx",
				},
				Role:     "worker",
				Replicas: 2,
			},
		},
	}, nil, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("create distributed job result %v\n", createResult)

	jobID := createResult.ID
	getResult, err := pfClient.APIV1().Job().Get(context.TODO(), jobID, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("get job result %v\n", getResult)

	listResult, err := pfClient.APIV1().Job().List(context.TODO(), &v1.ListJobRequest{
		Queue:     queueName,
		Timestamp: 1653292737721,
		Labels: map[string]string{
			"job-type": "single",
		},
		MaxKeys: 100,
	}, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("list job result %v\n", listResult)

	err = pfClient.APIV1().Job().Update(context.TODO(), jobID, &v1.UpdateJobRequest{
		Labels: map[string]string{
			"key1": "value1",
		},
		Annotations: map[string]string{
			"anno1": "value1",
		},
	}, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("update job %s ok\n", jobID)

	err = pfClient.APIV1().Job().Stop(context.TODO(), jobID, token)
	if err != nil {
		panic(err)
	}
	fmt.Printf("stop job %s ok\n", jobID)

	err = pfClient.APIV1().Job().Delete(context.TODO(), jobID, token)
	if err != nil {
		panic(err)
	}
	fmt.Println("delete job ok")
}
