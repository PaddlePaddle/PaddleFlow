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

package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	log "github.com/sirupsen/logrus"
	apiv1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

// maximum number of lines loaded from the apiserver
var lineReadLimit int64 = 5000

// maximum number of bytes loaded from the apiserver
var byteReadLimit int64 = 500000

func getKubernetesLogs(client kubernetes.Interface, jobLogRequest schema.JobLogRequest) (schema.JobLogInfo, error) {
	jobLogInfo := schema.JobLogInfo{
		JobID: jobLogRequest.JobID,
	}
	labelSelector := metav1.LabelSelector{}
	switch schema.JobType(jobLogRequest.JobType) {
	case schema.TypeSingle, schema.TypeDistributed, schema.TypeWorkflow:
		labelSelector.MatchLabels = map[string]string{
			schema.JobIDLabel: jobLogRequest.JobID,
		}
	default:
		log.Errorf("unknown job type %s, skip get log for job[%s]", jobLogRequest.JobType, jobLogRequest.JobID)
		return schema.JobLogInfo{}, errors.New("unknown job type")
	}
	labelMap, err := metav1.LabelSelectorAsMap(&labelSelector)
	if err != nil {
		log.Errorf("job[%s] parse selector to map failed", jobLogRequest.JobID)
		return schema.JobLogInfo{}, err
	}
	listOptions := metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labelMap).String(),
	}
	podList, err := getPodList(client, jobLogRequest.Namespace, listOptions)
	if err != nil {
		log.Errorf("job[%s] get pod list failed", jobLogRequest.JobID)
		return schema.JobLogInfo{}, err
	}
	taskLogInfoList := make([]schema.TaskLogInfo, 0)
	for _, pod := range podList.Items {
		itemLogInfoList, err := constructTaskLog(client, jobLogRequest.Namespace, pod.Name, jobLogRequest.LogFilePosition, jobLogRequest.LogPageSize, jobLogRequest.LogPageNo)
		if err != nil {
			log.Errorf("job[%s] construct task[%s] log failed", jobLogRequest.JobID, pod.Name)
			return schema.JobLogInfo{}, err
		}
		taskLogInfoList = append(taskLogInfoList, itemLogInfoList...)
	}
	jobLogInfo.TaskList = taskLogInfoList
	return jobLogInfo, nil
}

func constructTaskLog(client kubernetes.Interface, namespace, name, logFilePosition string, pageSize, pageNo int) ([]schema.TaskLogInfo, error) {
	taskLogInfoList := make([]schema.TaskLogInfo, 0)
	pod, err := getPod(client, namespace, name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return []schema.TaskLogInfo{}, nil
	} else if err != nil {
		return []schema.TaskLogInfo{}, err
	}
	for _, c := range pod.Spec.Containers {
		podLogOptions := mapToLogOptions(c.Name, logFilePosition)
		logContent, length, err := getContainerLog(client, namespace, name, podLogOptions)
		if err != nil {
			return []schema.TaskLogInfo{}, err
		}
		startIndex := -1
		endIndex := -1
		hasNextPage := false
		truncated := false
		limitFlag := isReadLimitReached(int64(len(logContent)), int64(length), logFilePosition)
		overFlag := false
		// 判断开始位置是否已超过日志总行数，若超过overFlag为true；
		// 如果是logFilePPosition为end，则看下startIndex是否已经超过0，若超过则置startIndex为-1（从最开始获取），并检查日志是否被截断
		// 如果是logFilePPosition为begin，则判断末尾index是否超过总长度，若超过endIndex为-1（直到末尾），并检查日志是否被截断
		if (pageNo-1)*pageSize+1 <= length {
			switch logFilePosition {
			case common.EndFilePosition:
				startIndex = length - pageSize*pageNo
				endIndex = length - (pageNo-1)*pageSize
				if startIndex <= 0 {
					startIndex = -1
					truncated = limitFlag
				} else {
					hasNextPage = true
				}
				if endIndex == length {
					endIndex = -1
				}
			case common.BeginFilePosition:
				startIndex = (pageNo - 1) * pageSize
				if pageNo*pageSize < length {
					endIndex = pageNo * pageSize
					hasNextPage = true
				} else {
					truncated = limitFlag
				}
			}
		} else {
			overFlag = true
		}

		taskLogInfo := schema.TaskLogInfo{
			TaskID: fmt.Sprintf("%s_%s", pod.GetUID(), c.Name),
			Info: schema.LogInfo{
				LogContent:  splitLog(logContent, startIndex, endIndex, overFlag),
				HasNextPage: hasNextPage,
				Truncated:   truncated,
			},
		}
		taskLogInfoList = append(taskLogInfoList, taskLogInfo)
	}
	return taskLogInfoList, nil

}

func getContainerLog(client kubernetes.Interface, namespace, name string, logOptions *apiv1.PodLogOptions) (string, int, error) {
	readCloser, err := client.CoreV1().RESTClient().Get().
		Namespace(namespace).
		Name(name).
		Resource("pods").
		SubResource("log").
		VersionedParams(logOptions, scheme.ParameterCodec).Stream(context.TODO())
	if err != nil {
		log.Errorf("pod[%s] get log stream failed. error: %s", name, err.Error())
		return err.Error(), 0, nil
	}

	defer readCloser.Close()

	// logOptions: begin LimitBytes 500000; end TailLines 5000
	result, err := io.ReadAll(readCloser)
	if err != nil {
		log.Errorf("pod[%s] read content failed; error: %s", name, err.Error())
		return "", 0, err
	}

	return string(result), len(strings.Split(strings.TrimRight(string(result), "\n"), "\n")), nil
}

func getPod(client kubernetes.Interface, namespace, name string, getOptions metav1.GetOptions) (*apiv1.Pod, error) {
	return client.CoreV1().Pods(namespace).Get(context.TODO(), name, getOptions)
}

func getPodList(client kubernetes.Interface, namespace string, listOptions metav1.ListOptions) (*apiv1.PodList, error) {
	return client.CoreV1().Pods(namespace).List(context.TODO(), listOptions)
}

func mapToLogOptions(container, logFilePosition string) *apiv1.PodLogOptions {
	logOptions := &apiv1.PodLogOptions{
		Container:  container,
		Follow:     false,
		Timestamps: true,
	}

	if logFilePosition == common.BeginFilePosition {
		logOptions.LimitBytes = &byteReadLimit
	} else {
		logOptions.TailLines = &lineReadLimit
	}

	return logOptions
}

func splitLog(logContent string, startIndex, endIndex int, overFlag bool) string {
	if overFlag || logContent == "" {
		return ""
	}
	logContent = strings.TrimRight(logContent, "\n")
	logLines := make([]string, 0)
	if startIndex == -1 && endIndex == -1 {
		logLines = strings.Split(logContent, "\n")[:]
	} else if startIndex == -1 {
		logLines = strings.Split(logContent, "\n")[:endIndex]
	} else if endIndex == -1 {
		logLines = strings.Split(logContent, "\n")[startIndex:]
	} else {
		logLines = strings.Split(logContent, "\n")[startIndex:endIndex]
	}
	return strings.Join(logLines, "\n") + "\n"
}

func isReadLimitReached(bytesLoaded int64, linesLoaded int64, logFilePosition string) bool {
	return (logFilePosition == common.BeginFilePosition && bytesLoaded >= byteReadLimit) ||
		(logFilePosition == common.EndFilePosition && linesLoaded >= lineReadLimit)
}
