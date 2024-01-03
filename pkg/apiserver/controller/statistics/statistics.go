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
	"errors"
	"fmt"
	"strings"
	"time"

	prometheusModel "github.com/prometheus/common/model"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/consts"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/monitor"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

// cardNameList 存放所有的资源类型
var (
	cardNameList   = []string{"nvidia.com/gpu", "_cgpu"}
	metricNameList = [...]string{
		consts.MetricCpuUsageRate, consts.MetricMemoryUsageRate,
		consts.MetricMemoryUsage, consts.MetricDiskUsage,
		consts.MetricNetReceiveBytes, consts.MetricNetSendBytes,
		consts.MetricDiskReadRate, consts.MetricDiskWriteRate,
		consts.MetricGpuUtil, consts.MetricGpuMemoryUtil,
		consts.MetricGpuMemoryUsage}
)

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
	QueueCardTimeInfo []*QueueCardTimeInfo `json:"queueCardTimeInfo"`
}

type QueueCardTimeInfo struct {
	QueueName  string      `json:"queueName"`
	CardTime   float64     `json:"cardTime"`
	DeviceType string      `json:"deviceType"`
	Detail     []JobDetail `json:"detail"`
}

type JobCardTimeInfo struct {
	JobID       string  `json:"jobId"`
	CardTime    float64 `json:"cardTime"`
	CreateTime  string  `json:"createTime"`
	StartTime   string  `json:"startTime"`
	FinishTime  string  `json:"finishTime"`
	DeviceCount int     `json:"deviceCount"`
}

type JobDetail struct {
	UserName      string            `json:"userName"`
	JobInfoList   []JobCardTimeInfo `json:"jobInfoList"`
	JobCount      int               `json:"jobCount"`
	TotalCardTime float64           `json:"totalCardTime"`
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

// GetCardTimeInfo 函数从给定的队列中获取卡片时间信息
// ctx: 请求上下文
// queueNames: 队列名称列表
// startTimeStr: 开始时间字符串
// endTimeStr: 结束时间字符串
// 返回值：
// []*GetCardTimeResponse: 卡片时间信息列表
// error: 错误信息
func GetCardTimeInfo(ctx *logger.RequestContext, queueNames []string, startTimeStr string, endTimeStr string) (*GetCardTimeResponse, error) {
	var cardTimeInfoList []*QueueCardTimeInfo
	startTime, err := time.Parse(model.TimeFormat, startTimeStr)
	if err != nil {
		ctx.Logging().Errorf("[GetCardTimeInfo] startTime parse failed, err:%s", err.Error())
		return nil, err
	}
	endTime, err := time.Parse(model.TimeFormat, endTimeStr)
	if err != nil {
		ctx.Logging().Errorf("[GetCardTimeInfo] endTime parse failed, err:%s", err.Error())
		return nil, err
	}
	if startTime.After(endTime) {
		return nil, errors.New("[GetCardTimeInfo] startTime must be before than endTime")
	}
	minDuration := time.Second * 0
	// 若指定的时间段长度小于最小任务运行时间，报错
	logger.Logger().Infof("[GetCardTimeInfo] queueNames:%s, startDate:%v, endDate:%v", queueNames, startTime, endTime)
	if endTime.Sub(startTime) < minDuration {
		return nil, fmt.Errorf("time period less than minDuration")
	}
	for _, queueName := range queueNames {
		queue, err := storage.Queue.GetQueueByName(queueName)
		if err != nil {
			ctx.ErrorMessage = err.Error()
			ctx.Logging().Errorf("get queue by name failed. queuerName:[%s]", queueName)
			continue
		}
		logger.Logger().Infof("[GetCardTimeInfo] queueName:%s, queueID:%s", queueName, queue.ID)
		detailInfo, cardTime, err := GetCardTimeByQueueID(startTime, endTime, queue.ID, minDuration)
		if err != nil {
			ctx.ErrorMessage = err.Error()
			ctx.Logging().Errorf("get cardTime failed. queuerName:[%s]", queue.Name)
			continue
		}
		cardTimeInfo := &QueueCardTimeInfo{
			QueueName: queue.Name,
			CardTime:  cardTime,
			Detail:    detailInfo,
		}
		cardTimeInfoList = append(cardTimeInfoList, cardTimeInfo)
	}
	return &GetCardTimeResponse{cardTimeInfoList}, nil
}

// GetCardTimeByQueueID 根据队列ID获取任务卡片运行时间
//
// 参数：
// startDate: 开始时间，格式为20xx-xx-xx
// endDate: 结束时间，格式为20xx-xx-xx
// queueID: 队列ID
// minDuration: 最小任务运行时间
//
// 返回值：
// []JobDetail: 任务详情列表
// float64: 任务卡片运行时间总和
// error: 错误信息，若成功则为nil
func GetCardTimeByQueueID(startDate time.Time, endDate time.Time,
	queueID string, minDuration time.Duration) ([]JobDetail, float64, error) {
	// endDate 原本为 20xx-xx-xx xx:59:59  加一秒
	endDate = endDate.Add(time.Second)
	logger.Logger().Infof("[GetCardTimeFromQueueID] startDate:%s, endDate:%s,queueID: %s", startDate, endDate, queueID)
	// 初始化detailInfo,map的key为userName，value为[]PaddleJobStatusDataForCardTime
	// TODO: 优化大数据量时的查询方案
	limit, offset := 5000, 0
	jobStats, err := storage.Job.ListJobStat(startDate, endDate, queueID, minDuration, limit, offset)
	logger.Logger().Debugf("[GetCardTimeFromQueueID] job stats: %v", jobStats)
	if err != nil {
		logger.Logger().Errorf("[GetCardTimeFromQueueID] list job status failed, error: %s", err.Error())
		return nil, 0, err
	}
	detailInfoMap := make(map[string][]JobCardTimeInfo)
	for caseType, jobStat := range jobStats {
		detailInfoMap, err = FulfillDetailInfo(startDate, endDate, detailInfoMap, jobStat, caseType, minDuration)
		if err != nil {
			logger.Logger().Errorf("processCardTimeCase error: %s", err.Error())
		}
	}
	logger.Logger().Infof("[GetCardTimeFromQueueID] detail info map: %v", detailInfoMap)
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
	logger.Logger().Infof("[GetCardTimeFromQueueID] detail info: %v", detailInfo)
	return detailInfo, cardTimeForGroup, nil
}

// containsStr 判断目标字符串是否包含切片中的任意一个字符串
// 参数 target: 目标字符串
// 参数 strSlice: 字符串切片
// 返回值: 如果目标字符串包含切片中的任意一个字符串，返回 true，否则返回 false
func containsStr(target string, strSlice []string) bool {
	for _, str := range strSlice {
		if strings.HasSuffix(target, str) {
			return true
		}
	}
	return false
}

// GetFlavourCards 根据传入的 Flavour 参数和设备卡类型列表，返回符合条件的资源数量
func GetFlavourCards(flavour schema.Flavour, deviceCardTypes []string) int {
	res, err := resources.NewResourceFromMap(flavour.ToMap())
	if err != nil {
		logger.Logger().Errorf("[GetFlavourCards] NewResourceFromMap failed, error: %s", err.Error())
		return 0
	}
	for rName, rValue := range res.ScalarResources("") {
		if containsStr(rName, deviceCardTypes) && rValue > 0 {
			return int(rValue)
		}
	}
	return 0
}

// GetGpuCards 函数用于获取指定任务状态下的GPU卡数量
// 参数jobStatus为任务状态，类型为model.Job指针
// 返回值为int类型，表示GPU卡数量
func GetGpuCards(jobStatus *model.Job) int {
	members := jobStatus.Members
	var gpuCards int = 0
	for _, member := range members {
		gpuCards += GetFlavourCards(member.Flavour, cardNameList) * member.Replicas
	}
	return gpuCards
}

// FulfillDetailInfo 函数用于填充任务详情信息
//
// 参数：
// startTime: 开始时间
// endTime: 结束时间
// detailInfo: 任务详情信息
// jobStatusCase: 任务状态列表
// caseType: 任务状态类型
// minDuration: 最小持续时间
//
// 返回值：
// map[string][]JobCardTimeInfo: 更新后的任务详情信息
// error: 错误信息
func FulfillDetailInfo(startTime time.Time, endTime time.Time, detailInfo map[string][]JobCardTimeInfo,
	jobStatusCase []*model.Job, caseType string, minDuration time.Duration) (map[string][]JobCardTimeInfo, error) {
	logger.Logger().Infof("[FulfillDetailInfo] case type:%s, job status cases:%v", caseType, jobStatusCase)
	var cardTimeCalculation = func(jobStatus *model.Job, startDate, endDate time.Time, gpuCards int) (time.Duration, float64) {
		var cardTime float64 = 0
		var jobDuration time.Duration
		switch caseType {
		case "case1":
			// 任务开始运行时间 < start_date， 任务结束时间 > end_date 或者 任务尚未结束
			jobDuration = endDate.Sub(startDate)
			cardTime = float64(gpuCards) * endDate.Sub(startDate).Seconds()
		case "case2":
			// 任务开始运行时间 < start_date，任务结束时间 <= end_date
			jobDuration = jobStatus.FinishedAt.Time.Sub(startDate)
			cardTime = jobDuration.Seconds() * float64(gpuCards)
		case "case3":
			// 任务开始运行时间 >= start_date，任务结束时间 <= end_date
			jobDuration = jobStatus.FinishedAt.Time.Sub((*jobStatus).ActivatedAt.Time)
			cardTime = jobDuration.Seconds() * float64(gpuCards)
		case "case4":
			// 任务开始运行时间 >= start_date， 任务结束时间 > end_date 或者 任务尚未结束
			jobDuration = endDate.Sub((*jobStatus).ActivatedAt.Time)
			cardTime = jobDuration.Seconds() * float64(gpuCards)
		}
		return jobDuration, cardTime
	}

	for _, jobStatus := range jobStatusCase {
		gpuCards := GetGpuCards(jobStatus)
		logger.Logger().Infof("[FulfillDetailInfo] jobID: %s,gpu cards:%v", jobStatus.ID, gpuCards)
		jobDuration, cardTime := cardTimeCalculation(jobStatus, startTime, endTime, gpuCards)
		if jobDuration < minDuration {
			continue
		}
		cardTime = cardTime / 3600
		cardTime = common.Floor2decimal(cardTime)
		_, ok := detailInfo[jobStatus.UserName]
		if ok {
			detailInfo[jobStatus.UserName] = append(detailInfo[jobStatus.UserName], JobCardTimeInfo{
				JobID:       jobStatus.ID,
				CardTime:    cardTime,
				CreateTime:  jobStatus.CreatedAt.Format(model.TimeFormat),
				StartTime:   jobStatus.ActivatedAt.Time.Format(model.TimeFormat),
				FinishTime:  jobStatus.FinishedAt.Time.Format(model.TimeFormat),
				DeviceCount: gpuCards,
			})
		} else {
			var jobStatusDataForCardTimeList []JobCardTimeInfo
			jobStatusDataForCardTimeList = append(jobStatusDataForCardTimeList, JobCardTimeInfo{
				JobID:       jobStatus.ID,
				CardTime:    cardTime,
				CreateTime:  jobStatus.CreatedAt.Format(model.TimeFormat),
				StartTime:   jobStatus.ActivatedAt.Time.Format(model.TimeFormat),
				FinishTime:  jobStatus.FinishedAt.Time.Format(model.TimeFormat),
				DeviceCount: gpuCards,
			})
			detailInfo[jobStatus.UserName] = jobStatusDataForCardTimeList
		}
	}
	return detailInfo, nil
}
