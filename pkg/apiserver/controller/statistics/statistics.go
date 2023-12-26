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

package statistics

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	prometheusModel "github.com/prometheus/common/model"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/consts"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/monitor"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

const (
	CaseType1 = "case1"
	CaseType2 = "case2"
	CaseType3 = "case3"
	CaseType4 = "case4"
)

var metricNameList = [...]string{
	consts.MetricCpuUsageRate, consts.MetricMemoryUsageRate,
	consts.MetricMemoryUsage, consts.MetricDiskUsage,
	consts.MetricNetReceiveBytes, consts.MetricNetSendBytes,
	consts.MetricDiskReadRate, consts.MetricDiskWriteRate,
	consts.MetricGpuUtil, consts.MetricGpuMemoryUtil,
	consts.MetricGpuMemoryUsage}

type JobStatisticsResponse struct {
	MetricsInfo map[string]string `json:"metricsInfo"`
}

type JobDetailStatisticsResponse struct {
	Result      []TaskStatistics `json:"result"`
	TaskNameMap map[string]int   `json:"-"`
	Truncated   bool             `json:"truncated"`
}

type TaskStatistics struct {
	TaskName string       `json:"taskName"`
	TaskInfo []MetricInfo `json:"taskInfo"`
}

type MetricInfo struct {
	MetricName string       `json:"metric"`
	Values     [][2]float64 `json:"values"`
}

type GetCardTimeBatchRequest struct {
	StartTime  string   `json:"startTime"`
	EndTime    string   `json:"endTime"`
	QueueNames []string `json:"queueNames"`
}

type GetCardTimeResponse struct {
	QueueName string      `json:"queueName"`
	CardTime  float64     `json:"cardTime"`
	Detail    []JobDetail `json:"detail"`
}

type JobStatusDataForCardTime struct {
	JobID      string  `json:"jobId"`
	CardTime   float64 `json:"cardTime"`
	CreateTime string  `json:"createTime"`
	StartTime  string  `json:"startTime"`
	FinishTime string  `json:"finishTime"`
	GpuCount   int     `json:"gpuCount"`
}

type JobDetail struct {
	UserName      string                     `json:"userName"`
	JobInfoList   []JobStatusDataForCardTime `json:"jobInfoList"`
	JobCount      int                        `json:"jobCount"`
	TotalCardTime float64                    `json:"totalCardTime"`
}

type CardTimeInfo struct {
	QueueName string      `json:"queueName"`
	CardTime  float64     `json:"cardTime"`
	Detail    []JobDetail `json:"detail"`
}

func GetJobStatistics(ctx *logger.RequestContext, jobID string) (*JobStatisticsResponse, error) {
	response := &JobStatisticsResponse{
		MetricsInfo: make(map[string]string),
	}
	clusterType, _, err := getClusterTypeByJob(ctx, jobID)
	if err != nil {
		ctx.Logging().Errorf("get metric type failed, error: %s", err.Error())
		return nil, err
	}
	metric, err := getMetricByType(clusterType)
	if err != nil {
		ctx.Logging().Errorf("get metric by type[%s] failed, error: %s", clusterType, err.Error())
		return nil, err
	}

	for _, value := range metricNameList {
		result, err := metric.GetJobAvgMetrics(value, jobID)
		if err != nil {
			ctx.Logging().Errorf("query metric[%s] failed, error: %s", value, err.Error())
			return nil, err
		}
		convertResultToResponse(response, result, value)
	}

	return response, nil
}

func GetJobDetailStatistics(ctx *logger.RequestContext, jobID string, start, end, step int64) (*JobDetailStatisticsResponse, error) {
	response := &JobDetailStatisticsResponse{
		Result:      make([]TaskStatistics, 0),
		TaskNameMap: make(map[string]int),
	}

	clusterType, job, err := getClusterTypeByJob(ctx, jobID)
	if err != nil {
		ctx.Logging().Errorf("get metric type failed, error: %s", err.Error())
		return nil, err
	}

	if start == 0 {
		if job.ActivatedAt.Valid {
			start = job.ActivatedAt.Time.Unix()
		} else {
			return response, nil
		}
	}
	if end == 0 {
		if schema.IsImmutableJobStatus(job.Status) {
			end = job.UpdatedAt.Unix()
		} else {
			end = start + 60*10
		}
	}

	metric, err := getMetricByType(clusterType)
	if err != nil {
		ctx.Logging().Errorf("get metric by type[%s] failed, error: %s", clusterType, err.Error())
		return nil, err
	}

	for _, value := range metricNameList {
		result, err := metric.GetJobSequenceMetrics(value, jobID, start, end, step)
		if err != nil {
			ctx.Logging().Errorf("query range metric[%s] failed, error: %s", value, err.Error())
			return nil, err
		}
		err = convertResultToDetailResponse(ctx, result, response, value)
		if err != nil {
			ctx.Logging().Errorf("convert metric[%s] result to detail response failed, error: %s", value, err.Error())
			return nil, err
		}
	}
	return response, nil
}

func getMetricByType(metricType string) (monitor.MetricInterface, error) {
	var metric monitor.MetricInterface
	switch metricType {
	case schema.KubernetesType:
		metric = monitor.NewKubernetesMetric(monitor.PrometheusClientAPI)
	default:
		return nil, fmt.Errorf("metric type[%s] is not support", metricType)
	}
	return metric, nil
}

func getClusterTypeByJob(ctx *logger.RequestContext, jobID string) (string, *model.Job, error) {
	job, err := storage.Job.GetJobByID(jobID)
	if err != nil {
		ctx.ErrorCode = common.JobNotFound
		ctx.Logging().Errorln(err.Error())
		return "", nil, common.NotFoundError(common.ResourceTypeJob, jobID)
	}
	if ok := checkJobPermission(ctx, &job); !ok {
		ctx.ErrorCode = common.AccessDenied
		ctx.Logging().Errorf("get the job[%s] auth failed.", jobID)
		return "", nil, common.NoAccessError(ctx.UserName, common.ResourceTypeJob, jobID)
	}

	queue, err := storage.Queue.GetQueueByID(job.QueueID)
	if err != nil {
		ctx.ErrorCode = common.QueueNameNotFound
		ctx.Logging().Errorln(err.Error())
		return "", nil, common.NotFoundError(common.ResourceTypeQueue, job.QueueID)
	}
	cluster, err := storage.Cluster.GetClusterById(queue.ClusterId)
	if err != nil {
		ctx.ErrorCode = common.ClusterNotFound
		ctx.Logging().Errorln(err.Error())
		return "", nil, common.NotFoundError(common.ResourceTypeCluster, queue.ClusterId)
	}
	return cluster.ClusterType, &job, nil
}

func convertResultToDetailResponse(ctx *logger.RequestContext, result prometheusModel.Value, response *JobDetailStatisticsResponse, metricName string) error {
	data, ok := result.(prometheusModel.Matrix)
	if !ok {
		ctx.Logging().Errorf("convert result to matrix failed")
		return fmt.Errorf("convert result to matrix failed")
	}
	for _, value := range data {
		taskValues := make([][2]float64, 0)
		if len(value.Values) > common.StsMaxSeqData {
			value.Values = value.Values[:common.StsMaxSeqData]
			response.Truncated = true
		}
		for _, rangeValue := range value.Values {
			taskValues = append(taskValues, [2]float64{float64(rangeValue.Timestamp.Unix()), float64(rangeValue.Value)})
		}
		taskName := string(value.Metric[common.Pod])
		metricInfo := MetricInfo{
			MetricName: metricName,
			Values:     taskValues,
		}
		if _, ok := response.TaskNameMap[taskName]; ok {
			response.Result[response.TaskNameMap[taskName]].TaskInfo = append(response.Result[response.TaskNameMap[taskName]].TaskInfo, metricInfo)
		} else {
			task := TaskStatistics{
				TaskName: taskName,
				TaskInfo: make([]MetricInfo, 0),
			}
			task.TaskInfo = append(task.TaskInfo, metricInfo)
			response.TaskNameMap[taskName] = len(response.Result)
			response.Result = append(response.Result, task)
		}
	}
	return nil
}

func convertResultToResponse(response *JobStatisticsResponse, result float64, metricName string) {
	switch metricName {
	case consts.MetricCpuUsageRate, consts.MetricMemoryUsageRate, consts.MetricGpuUtil, consts.MetricGpuMemoryUtil:
		response.MetricsInfo[metricName] = fmt.Sprintf("%.2f%%", result*100)
	case consts.MetricNetReceiveBytes, consts.MetricNetSendBytes, consts.MetricDiskReadRate, consts.MetricDiskWriteRate:
		response.MetricsInfo[metricName] = fmt.Sprintf("%.2f(B/s)", result)
	case consts.MetricDiskUsage, consts.MetricMemoryUsage, consts.MetricGpuMemoryUsage:
		response.MetricsInfo[metricName] = fmt.Sprintf("%.2f(Bytes)", result)
	}
}

func checkJobPermission(ctx *logger.RequestContext, job *model.Job) bool {
	return common.IsRootUser(ctx.UserName) || ctx.UserName == job.UserName
}

func GetCardTimeByQueue(ctx *logger.RequestContext, queueName string, startTimeStr string, endTimeStr string) (*GetCardTimeResponse, error) {
	// parse start time and end time
	startTime, err := time.Parse(model.TimeFormat, startTimeStr)
	if err != nil {
		ctx.Logging().Errorf("[GetCardTime] startTime parse failed, err:%s", err.Error())
		return nil, err
	}
	endTime, err := time.Parse(model.TimeFormat, endTimeStr)
	if err != nil {
		ctx.Logging().Errorf("[GetCardTime] endTime parse failed, err:%s", err.Error())
		return nil, err
	}
	if startTime.After(endTime) {
		return nil, errors.New("[GetCardTime] startTime must be before than endTime")
	}
	// get queue info by queue name
	queue, err := storage.Queue.GetQueueByName(queueName)
	if err != nil {
		ctx.ErrorMessage = err.Error()
		ctx.Logging().Errorf("get queue by name failed. queuerName:[%s]", queueName)
		return nil, err
	}
	detailInfo, cardTimeData, err := GetCardTimeByQueueID(startTime, endTime, queue.ID, 0)
	if err != nil {
		ctx.ErrorMessage = err.Error()
		ctx.Logging().Errorf("get cardTime failed. queuerName:[%s]", queueName)
		return nil, err
	}
	cardTimeInfo := &CardTimeInfo{
		QueueName: queue.Name,
		CardTime:  cardTimeData,
		Detail:    detailInfo,
	}

	return &GetCardTimeResponse{QueueName: cardTimeInfo.QueueName,
		CardTime: cardTimeInfo.CardTime,
		Detail:   cardTimeInfo.Detail}, nil
}

func GetCardTimeBatch(ctx *logger.RequestContext, queueNames []string, startTimeStr string, endTimeStr string) ([]*CardTimeInfo, error) {
	var cardTimeInfoList []*CardTimeInfo
	startTime, err := time.Parse(model.TimeFormat, startTimeStr)
	if err != nil {
		ctx.Logging().Errorf("[GetCardTime] startTime parse failed, err:%s", err.Error())
		return nil, err
	}
	endTime, err := time.Parse(model.TimeFormat, endTimeStr)
	if err != nil {
		ctx.Logging().Errorf("[GetCardTime] endTime parse failed, err:%s", err.Error())
		return nil, err
	}
	if startTime.After(endTime) {
		return nil, errors.New("[GetCardTime] startTime must be before than endTime")
	}

	for _, groupName := range queueNames {
		queue, err := storage.Queue.GetQueueByName(groupName)
		if err != nil {
			ctx.ErrorMessage = err.Error()
			ctx.Logging().Errorf("get queue by name failed. queuerName:[%s]", groupName)
			return nil, err
		}
		detailInfo, cardTime, err := GetCardTimeByQueueID(startTime, endTime, queue.ID, 0)
		if err != nil {
			ctx.ErrorMessage = err.Error()
			ctx.Logging().Errorf("get cardTime failed. queuerName:[%s]", queue.Name)
			return nil, err
		}
		cardTimeInfo := &CardTimeInfo{
			QueueName: queue.Name,
			CardTime:  cardTime,
			Detail:    detailInfo,
		}
		cardTimeInfoList = append(cardTimeInfoList, cardTimeInfo)
	}
	return cardTimeInfoList, nil
}

func GetCardTimeByQueueID(startDate time.Time, endDate time.Time,
	queueID string, minDuration time.Duration) ([]JobDetail, float64, error) {
	// endDate 原本为 20xx-xx-xx xx:59:59  加一秒
	endDate = endDate.Add(time.Second)
	period := endDate.Sub(startDate)
	// 若指定的时间段长度小于最小任务运行时间，报错
	logger.Logger().Infof("[GetCardTimeFromQueueID]groupID:%v, period:%#v, startDate:%v, endDate:%v",
		queueID, period, startDate, endDate)
	if period < minDuration {
		return nil, 0, fmt.Errorf("time period less than minDuration")
	}

	// 初始化detailInfo,map的key为userName，value为[]PaddleJobStatusDataForCardTime
	detailInfoMap := make(map[string][]JobStatusDataForCardTime)

	// case 1: 任务开始运行时间 < start_date， 任务结束时间 > end_date 或者 任务尚未结束
	jobStatusForCase1, err := storage.Job.ListJobStat(startDate, endDate, queueID, CaseType1, minDuration)
	if err != nil {
		logger.Logger().Errorf("[GetCardTimeFromQueueID] list job status for case1 failed, error: %s", err.Error())
	}
	cardTimeCalculation1 := func(jobStatus *model.Job, startDate, endDate time.Time, gpuCards int) float64 {
		return float64(gpuCards) * period.Seconds()
	}
	detailInfoMap, err = FulfillDetailInfo(startDate, endDate, detailInfoMap, jobStatusForCase1, cardTimeCalculation1)
	if err != nil {
		logger.Logger().Errorf("processCardTimeCase error: %s", err.Error())
	}

	// case 2: 任务开始运行时间 < start_date，任务结束时间 <= end_date
	jobStatusForCase2, err := storage.Job.ListJobStat(startDate, endDate, queueID, CaseType2, minDuration)
	if err != nil {
		logger.Logger().Errorf("[GetCardTimeFromQueueID] list job status for case2 failed, error: %s", err.Error())
	}
	cardTimeCalculation2 := func(jobStatus *model.Job, startDate, endDate time.Time, gpuCards int) float64 {
		return jobStatus.UpdatedAt.Sub(startDate).Seconds() * float64(gpuCards)
	}
	detailInfoMap, err = FulfillDetailInfo(startDate, endDate, detailInfoMap, jobStatusForCase2, cardTimeCalculation2)
	if err != nil {
		logger.Logger().Errorf("processCardTimeCase error: %s", err.Error())
	}

	// case 3: 任务开始运行时间 >= start_date，任务结束时间 <= end_date
	jobStatusForCase3, err := storage.Job.ListJobStat(startDate, endDate, queueID, CaseType3, minDuration)
	if err != nil {
		logger.Logger().Errorf("[GetCardTimeFromQueueID] list job status for case3 failed, error: %s", err.Error())
	}
	cardTimeCalculation3 := func(jobStatus *model.Job, startDate, endDate time.Time, gpuCards int) float64 {
		return jobStatus.UpdatedAt.Sub((*jobStatus).ActivatedAt.Time).Seconds() * float64(gpuCards)
	}
	detailInfoMap, err = FulfillDetailInfo(startDate, endDate, detailInfoMap, jobStatusForCase3, cardTimeCalculation3)
	if err != nil {
		logger.Logger().Errorf("processCardTimeCase error: %s", err.Error())
	}

	// case 4: 任务开始运行时间 >= start_date， 任务结束时间 > end_date 或者 任务尚未结束
	jobStatusForCase4, err := storage.Job.ListJobStat(startDate, endDate, queueID, CaseType4, minDuration)
	if err != nil {
		logger.Logger().Errorf("[GetCardTimeFromQueueID] list job status for case4 failed, error: %s", err.Error())
	}
	cardTimeCalculation4 := func(jobStatus *model.Job, startDate, endDate time.Time, gpuCards int) float64 {
		return endDate.Sub((*jobStatus).ActivatedAt.Time).Seconds() * float64(gpuCards)
	}
	detailInfoMap, err = FulfillDetailInfo(startDate, endDate, detailInfoMap, jobStatusForCase4, cardTimeCalculation4)
	if err != nil {
		logger.Logger().Errorf("processCardTimeCase error: %s", err.Error())
	}

	cardTimeForGroup := float64(0)
	var detailInfo []JobDetail
	// 遍历detailInfoMap，计算每个用户的卡时以及该group的总卡时
	for userName, jobInfoList := range detailInfoMap {
		var totalCardTime float64 = 0
		for _, jobStatusData := range jobInfoList {
			totalCardTime += jobStatusData.CardTime
		}
		totalCardTime = common.Floor2decimal(totalCardTime)
		detailInfo = append(detailInfo, JobDetail{
			UserName:      userName,
			JobInfoList:   jobInfoList,
			JobCount:      len(jobInfoList),
			TotalCardTime: totalCardTime,
		})
		cardTimeForGroup += totalCardTime
	}
	return detailInfo, cardTimeForGroup, nil
}

func in(target string, str_array []string) bool {
	sort.Strings(str_array)
	index := sort.SearchStrings(str_array, target)
	if index < len(str_array) && str_array[index] == target {
		return true
	}
	return false
}

// TODO: 从job中获取gpu卡数
func GetGpuCards(jobStatus *model.Job) int {
	jobID := jobStatus.ID
	queueID := jobStatus.QueueID
	resourceJsonStr := jobStatus.ResourceJson
	var resourceJsonMap map[string]interface{}
	err := json.Unmarshal([]byte(resourceJsonStr), &resourceJsonMap)
	if err != nil {
		logger.LoggerForJob(jobID).Errorf("queueID:%v, jobid is %v, unmarshal resourceJson error: %v", queueID, jobID, err)
		return 0
	}
	//jobResourceList 存放所有的资源类型
	jobResourceList := []string{"k8s", "slurm", "k8s-new", "aistudio", "kubernetes"}
	var gpuCards int = 0
	for resourceKey, resourceValue := range resourceJsonMap {
		if in(resourceKey, jobResourceList) == true {
			gpuCards += resourceValue.(int)
		}
	}
	return gpuCards
}

func FulfillDetailInfo(startTime time.Time, endTime time.Time, detailInfo map[string][]JobStatusDataForCardTime,
	jobStatusCase []*model.Job, cardTimeCalculation func(*model.Job, time.Time, time.Time, int) float64) (map[string][]JobStatusDataForCardTime, error) {
	for _, jobStatus := range jobStatusCase {
		// TODO：用GetGpuCards(jobStatus)获得GPU卡数，此处暂时mock
		gpuCards := 1
		cardTime := cardTimeCalculation(jobStatus, startTime, endTime, gpuCards) / 3600
		cardTime = common.Floor2decimal(cardTime)
		_, ok := detailInfo[jobStatus.UserName]
		if ok {
			detailInfo[jobStatus.UserName] = append(detailInfo[jobStatus.UserName], JobStatusDataForCardTime{
				JobID:      jobStatus.ID,
				CardTime:   cardTime,
				CreateTime: jobStatus.CreatedAt.Format(model.TimeFormat),
				StartTime:  jobStatus.ActivatedAt.Time.Format(model.TimeFormat),
				FinishTime: jobStatus.UpdatedAt.Format(model.TimeFormat),
				GpuCount:   gpuCards,
			})
		} else {
			var jobStatusDataForCardTimeList []JobStatusDataForCardTime
			jobStatusDataForCardTimeList = append(jobStatusDataForCardTimeList, JobStatusDataForCardTime{
				JobID:      jobStatus.ID,
				CardTime:   cardTime,
				CreateTime: jobStatus.CreatedAt.Format(model.TimeFormat),
				StartTime:  jobStatus.ActivatedAt.Time.Format(model.TimeFormat),
				FinishTime: jobStatus.UpdatedAt.Format(model.TimeFormat),
				GpuCount:   gpuCards,
			})
			detailInfo[jobStatus.UserName] = jobStatusDataForCardTimeList
		}
	}
	return detailInfo, nil
}
