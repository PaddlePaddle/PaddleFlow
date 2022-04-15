package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	c "paddleflow-go-sdk/client"
	"paddleflow-go-sdk/util"
)

var (
	jobCache = make(map[string]JobInfo, 0)
)

type JobCache struct {
	Cache util.CacheProcess
}

func (j *JobCache) ClearCache() {
	jobCache = make(map[string]JobInfo, 0)
}

func (j *JobCache) WriteCache(data []byte) {
	var job JobInfo
	err := json.Unmarshal(data, &job)
	if err != nil {
		return
	}
	jobCache[job.ID] = job
}

func (j *JobCache) GetCache() map[string]string {
	m := make(map[string]string)
	for k, v := range jobCache {
		b, _ := json.Marshal(v)
		m[k] = string(b)
	}
	return m
}

func Login(client *c.PFClient, username, password string) error {
	data := LoginResponse{}
	uri := c.LoginUri
	requestBody := LoginRequest{
		Username: username,
		Password: password,
	}
	response, err := client.SendPostRequest(requestBody, uri)
	if err != nil {
		return err
	}
	if err := response.ParseAndCloseJsonBody(&data); err != nil {
		return err
	}
	client.AuthHeader = http.Header{}
	client.AuthHeader.Add(c.HeaderAuthKey, data.Authorization)
	client.AuthHeader.Add(c.HeaderClientIDKey, util.GenerateID(c.PrefixConnection))
	if err := c.BuildWSConnection(client); err != nil {
		return err
	}
	return nil
}

func CreateJob(client *c.PFClient, request CreateJobRequest) (CreateJobResponse, error) {
	data := CreateJobResponse{}
	uri := c.JobUriPrefix[:len(c.JobUriPrefix)-1]
	response, err := client.SendPostRequest(request, uri)
	if err != nil {
		return CreateJobResponse{}, err
	}
	if err := response.ParseAndCloseJsonBody(&data); err != nil {
		return CreateJobResponse{}, err
	}
	return data, nil
}

func GetJob(client *c.PFClient, jobID string) (JobInfo, error) {
	// check if ws healthy
	if client.Connection != nil && client.HealthCheck {
		//if ws healthy then get job in cache
		if job, ok := jobCache[jobID]; ok {
			return job, nil
		}
		// if not found in jobCache send request && write in cache
		jobInfo, err := getJobFromHttp(client, jobID)
		if err != nil {
			return JobInfo{}, err
		}
		jobCache[jobID] = jobInfo
		return jobInfo, nil
	}
	// if ws unhealthy then make connection
	err := c.BuildWSConnection(client)
	if err != nil {
		return JobInfo{}, err
	}
	jobInfo, err := getJobFromHttp(client, jobID)
	if err != nil {
		return JobInfo{}, err
	}
	jobCache[jobID] = jobInfo
	return jobInfo, nil
}

func getJobFromHttp(client *c.PFClient, jobID string) (JobInfo, error) {
	job := JobInfo{}
	uri := c.JobUriPrefix + jobID
	response, err := client.SendGetRequest(uri, nil)
	if err != nil {
		return JobInfo{}, err
	}
	if err := response.ParseAndCloseJsonBody(&job); err != nil {
		return JobInfo{}, err
	}
	return job, nil
}

func ListJob(client *c.PFClient, options ListJobOptions) (ListJobResponse, error) {
	jobs := ListJobResponse{}
	uri := c.JobUriPrefix[:len(c.JobUriPrefix)-1]
	m, err := convertToMap(options)
	if err != nil {
		return ListJobResponse{}, err
	}
	response, err := client.SendGetRequest(uri, &m)
	if err != nil {
		return ListJobResponse{}, err
	}
	if err := response.ParseAndCloseJsonBody(&jobs); err != nil {
		return ListJobResponse{}, err
	}
	return jobs, nil
}

func convertToMap(options ListJobOptions) (map[string]string, error) {
	m := make(map[string]string)
	b, err := json.Marshal(options)
	if err != nil {
		return m, err
	}
	var tmp map[string]interface{}
	err = json.Unmarshal(b, &tmp)
	if err != nil {
		return m, err
	}
	for k, v := range tmp {
		switch v.(type) {
		case float64:
			m[k] = strconv.FormatInt(int64(v.(float64)), 10)
		case map[string]interface{}:
			vb, _ := json.Marshal(v)
			m[k] = string(vb)
		default:
			m[k] = v.(string)
		}
	}
	return m, nil
}

func UpdateJob(client *c.PFClient, request CreateJobRequest) error {
	uri := c.JobUriPrefix + request.ID
	_, err := client.SendPutRequest(request, uri)
	return err
}

func StopJob(client *c.PFClient, jobID string) error {
	uri := fmt.Sprintf("%s%s?stop", c.JobUriPrefix, jobID)
	_, err := client.SendPutRequest(nil, uri)
	return err
}

func DeleteJob(client *c.PFClient, jobID string) error {
	uri := c.JobUriPrefix + jobID
	_, err := client.SendDeleteRequest(uri)
	return err
}
