package services

import (
	"paddleflow-go-sdk/client"
	"paddleflow-go-sdk/services/api"
)

type PFClient interface {
	// Login user login
	Login(username, password string) error

	// CreateJob create job
	CreateJob(request api.CreateJobRequest) (api.CreateJobResponse, error)

	// GetJob get job
	GetJob(jobID string) (api.JobInfo, error)

	// ListJob list job
	ListJob(listOption api.ListJobOptions) (api.ListJobResponse, error)

	// UpdateJob update job
	UpdateJob(request api.CreateJobRequest) error

	// StopJob stop job
	StopJob(jobID string) error

	// DeleteJob delete job
	DeleteJob(jobID string) error
}

type Client struct {
	*client.PFClient
}

func NewClient(config client.ClientConfig) *Client {
	client := &Client{
		PFClient: client.NewPFClient(&config),
	}
	client.PFClient.JobCache = &api.JobCache{}
	return client
}

func (c *Client) Login(username, password string) error {
	err := api.Login(c.PFClient, username, password)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) CreateJob(request api.CreateJobRequest) (api.CreateJobResponse, error) {
	response, err := api.CreateJob(c.PFClient, request)
	if err != nil {
		return api.CreateJobResponse{}, err
	}
	return response, nil
}

func (c *Client) GetJob(jobID string) (api.JobInfo, error) {
	jobInfo, err := api.GetJob(c.PFClient, jobID)
	if err != nil {
		return api.JobInfo{}, err
	}
	return jobInfo, nil
}

func (c *Client) ListJob(options api.ListJobOptions) (api.ListJobResponse, error) {
	jobList, err := api.ListJob(c.PFClient, options)
	if err != nil {
		return api.ListJobResponse{}, err
	}
	return jobList, nil
}

func (c *Client) UpdateJob(request api.CreateJobRequest) error {
	err := api.UpdateJob(c.PFClient, request)
	return err
}

func (c *Client) StopJob(jobID string) error {
	err := api.StopJob(c.PFClient, jobID)
	return err
}

func (c *Client) DeleteJob(jobID string) error {
	err := api.DeleteJob(c.PFClient, jobID)
	return err
}
