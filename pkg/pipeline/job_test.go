package pipeline

import (
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/job"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
)

func TestStopJob(t *testing.T) {
	pfj := NewPaddleFlowJob("abc", "abc:qe", "root", make(chan<- WorkflowEvent), nil, nil, "paddle", nil)

	assert.Equal(t, "root", pfj.userName)

	patches := gomonkey.ApplyMethod(reflect.TypeOf(pfj), "Validate", func(_ *PaddleFlowJob) error {
		return nil
	})
	defer patches.Reset()

	patches2 := gomonkey.ApplyFunc(job.StopJob, func(ctx *logger.RequestContext, id string) error {
		assert.Equal(t, ctx.UserName, pfj.userName)
		return nil
	})
	defer patches2.Reset()

	pfj.Stop()
}

func TestGenerateCreateJobInfo(t *testing.T) {
	testCases := []struct {
		name      string
		image     string
		userName  string
		mainFS    *schema.FsMount
		extraFs   []schema.FsMount
		framework string
		members   []schema.Member
		wantRes   job.CreateJobInfo
	}{
		{
			name:      "CreateJobInfo for distributed paddle job",
			image:     "paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7",
			userName:  "root",
			mainFS:    &schema.FsMount{Name: "xd"},
			extraFs:   []schema.FsMount{},
			framework: "paddle",
			members: []schema.Member{
				{
					Replicas: 2,
					Role:     "pworker",
					Conf: schema.Conf{
						QueueName: "train-queue",
						Command:   "echo worker",
						Image:     "paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7",
						Env:       map[string]string{"Worker": "1"},
						Flavour:   schema.Flavour{Name: "flavour1"},
						Priority:  "high",
					},
				},
				{
					Replicas: 2,
					Role:     "pserver",
					Conf: schema.Conf{
						QueueName: "train-queue",
						Command:   "echo server",
						Image:     "paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7",
						Env:       map[string]string{"PS": "1"},
						Flavour:   schema.Flavour{Name: "flavour1"},
						Priority:  "high",
					},
				},
			},
			wantRes: job.CreateJobInfo{
				CommonJobInfo: job.CommonJobInfo{
					Name:     "CreateJobInfo for distributed paddle job",
					UserName: "root",
					SchedulingPolicy: job.SchedulingPolicy{
						Queue: "default-queue",
					},
				},
				Framework: "paddle",
				Type:      schema.TypeDistributed,
				Members: []job.MemberSpec{
					{
						CommonJobInfo: job.CommonJobInfo{
							Name:             "CreateJobInfo for distributed paddle job",
							UserName:         "root",
							SchedulingPolicy: job.SchedulingPolicy{Queue: "train-queue", Priority: "high"},
						},
						Replicas: 2,
						Role:     "pworker",
						JobSpec: job.JobSpec{Image: "paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7", Flavour: schema.Flavour{Name: "flavour1"}, Command: "echo worker",
							Env: map[string]string{"Worker": "1"}, FileSystem: schema.FileSystem{Name: "xd"}, ExtraFileSystems: []schema.FileSystem{}},
					},
					{
						CommonJobInfo: job.CommonJobInfo{
							Name:             "CreateJobInfo for distributed paddle job",
							UserName:         "root",
							SchedulingPolicy: job.SchedulingPolicy{Queue: "train-queue", Priority: "high"},
						},
						Replicas: 2,
						Role:     "pserver",
						JobSpec: job.JobSpec{Image: "paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7", Flavour: schema.Flavour{Name: "flavour1"}, Command: "echo server",
							Env: map[string]string{"PS": "1"}, FileSystem: schema.FileSystem{Name: "xd"}, ExtraFileSystems: []schema.FileSystem{}},
					},
				},
			},
		},
		{
			name:      "CreateJobInfo for distributed job",
			image:     "paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7",
			userName:  "root",
			mainFS:    &schema.FsMount{Name: "xd"},
			extraFs:   []schema.FsMount{},
			framework: "paddle",
			members: []schema.Member{
				{
					Replicas: 2,
					Role:     "pworker",
					Conf: schema.Conf{
						QueueName:  "default-queue",
						FileSystem: schema.FileSystem{Name: "xd"},
					},
				},
				{
					Replicas: 2,
					Role:     "pserver",
					Conf: schema.Conf{
						QueueName:  "default-queue",
						FileSystem: schema.FileSystem{Name: "xd"},
					},
				},
			},
			wantRes: job.CreateJobInfo{
				CommonJobInfo: job.CommonJobInfo{
					Name:     "CreateJobInfo for distributed job",
					UserName: "root",
				},
				Framework: "paddle",
				Type:      schema.TypeDistributed,
				Members: []job.MemberSpec{
					{
						CommonJobInfo: job.CommonJobInfo{
							Name:             "CreateJobInfo for distributed job",
							UserName:         "root",
							SchedulingPolicy: job.SchedulingPolicy{Queue: "default-queue"},
						},
						Replicas: 2,
						Role:     "pworker",
						JobSpec:  job.JobSpec{Image: "paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7", FileSystem: schema.FileSystem{Name: "xd"}, ExtraFileSystems: []schema.FileSystem{}},
					},
					{
						CommonJobInfo: job.CommonJobInfo{
							Name:             "CreateJobInfo for distributed job",
							UserName:         "root",
							SchedulingPolicy: job.SchedulingPolicy{Queue: "default-queue"},
						},
						Replicas: 2,
						Role:     "pserver",
						JobSpec:  job.JobSpec{Image: "paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7", FileSystem: schema.FileSystem{Name: "xd"}, ExtraFileSystems: []schema.FileSystem{}},
					},
				},
			},
		},
		{
			name:      "single",
			image:     "paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7",
			userName:  "root",
			mainFS:    &schema.FsMount{Name: "xd"},
			extraFs:   []schema.FsMount{},
			framework: "paddle",
			wantRes: job.CreateJobInfo{
				CommonJobInfo: job.CommonJobInfo{
					Name:     "single",
					UserName: "root",
					SchedulingPolicy: job.SchedulingPolicy{
						Queue: "default-queue",
					},
				},
				Framework: schema.FrameworkStandalone,
				Type:      schema.TypeSingle,

				Members: []job.MemberSpec{
					{
						CommonJobInfo: job.CommonJobInfo{
							Name:     "single",
							UserName: "root",
						},
						Replicas: 1,
						Role:     string(schema.RoleWorker),
						JobSpec:  job.JobSpec{Image: "paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7", Flavour: schema.Flavour{}, FileSystem: schema.FileSystem{Name: "xd"}, ExtraFileSystems: []schema.FileSystem{}},
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pfj := NewPaddleFlowJob(tc.name, tc.image, tc.userName, make(chan<- WorkflowEvent), tc.mainFS, tc.extraFs, schema.Framework(tc.framework), tc.members)
			jobInfo := pfj.generateCreateJobInfo()
			tc.wantRes.ID = jobInfo.ID
			for index, member := range tc.wantRes.Members {
				member.ID = tc.wantRes.ID
				tc.wantRes.Members[index] = member
			}
			assert.Equal(t, tc.wantRes.Members, jobInfo.Members)
		})
	}
}
