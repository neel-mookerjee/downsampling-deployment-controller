package main

import (
	"errors"
	"fmt"
	batch_v1 "k8s.io/api/batch/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	"testing"
)

type JobHandlerTestSuite struct {
	JobHandler JobHandler
}

func NewJobHandlerTestSuite(config *Config, jobs FakeJobNames) *JobHandler {
	return &JobHandler{jobClient: &FakeJobClient{jobs}, templateParser: &MockTemplateParser{}, config: config}
}

type FakeJobNames struct {
	job_get_name    string
	job_list_name   string
	job_create_name string
}

// FakeJobs implements JobInterface
type FakeJobClient struct {
	jobNames FakeJobNames
}

func (c *FakeJobClient) Get(name string, options v1.GetOptions) (result *batch_v1.Job, err error) {
	j := &batch_v1.Job{}
	if name == c.jobNames.job_get_name {
		j.Name = c.jobNames.job_get_name
	}

	return j, err
}

func (c *FakeJobClient) List(opts v1.ListOptions) (result *batch_v1.JobList, err error) {
	list := &batch_v1.JobList{}
	if opts.LabelSelector != "purpose="+SIMULATION_STACK_PREFIX {
		return list, errors.New("Wrong label selector")
	}
	j := batch_v1.Job{}
	j.Name = c.jobNames.job_list_name
	list.Items = append(list.Items, j)
	return list, nil
}

func (c *FakeJobClient) Watch(opts v1.ListOptions) (watch.Interface, error) {
	return nil, nil
}

func (c *FakeJobClient) Create(job *batch_v1.Job) (result *batch_v1.Job, err error) {
	return nil, err
}

func (c *FakeJobClient) Update(job *batch_v1.Job) (result *batch_v1.Job, err error) {
	return nil, err
}

func (c *FakeJobClient) UpdateStatus(job *batch_v1.Job) (*batch_v1.Job, error) {
	return nil, nil
}

func (c *FakeJobClient) Delete(name string, options *v1.DeleteOptions) error {
	return nil
}

func (c *FakeJobClient) DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error {
	return nil
}

func (c *FakeJobClient) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *batch_v1.Job, err error) {
	return nil, err
}

type MockTemplateParser struct {
}

func (t *MockTemplateParser) LoadTemplate(path string, filler interface{}) (string, error) {
	return "{}", nil
}

func Test_JobHandler_CheckIfJobExists(t *testing.T) {
	tc := NewJobHandlerTestSuite(&Config{}, FakeJobNames{job_get_name: "job-to-exist"})
	bool, _ := tc.CheckIfJobExists("job-to-exist")
	if !bool {
		t.Error(fmt.Sprintf("Expected job %s to exist but returned false", "job-to-exist"))
	}
}

func Test_JobHandler_CheckIfJobExists_False(t *testing.T) {
	tc := NewJobHandlerTestSuite(&Config{}, FakeJobNames{job_get_name: "job-to-exist"})
	bool, _ := tc.CheckIfJobExists("job-to-exist-false")
	if bool {
		t.Error(fmt.Sprintf("Expected false but returned true"))
	}
}

func Test_JobHandler_CreateJob(t *testing.T) {
	tc := NewJobHandlerTestSuite(&Config{}, FakeJobNames{job_get_name: "job-to-exist"})
	ind, err := tc.CreateJob("job-new", &DefaultFiller{})
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but received. %v", err))
	}
	if !ind {
		t.Error(fmt.Sprintf("Job %s should be created but returned false", "job-new"))
	}
}

func Test_JobHandler_CreateJob_AlreadyExists(t *testing.T) {
	tc := NewJobHandlerTestSuite(&Config{}, FakeJobNames{job_get_name: "job-to-exist"})
	ind, err := tc.CreateJob("job-to-exist", &DefaultFiller{})
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but received. %v", err))
	}
	if ind {
		t.Error(fmt.Sprintf("Job %s should not have been created but returned true", "job-to-exist"))
	}
}

func Test_JobHandler_GetAllDeploymentJobs(t *testing.T) {
	tc := NewJobHandlerTestSuite(&Config{}, FakeJobNames{job_list_name: "job-1"})
	list, err := tc.GetAllDeploymentJobs()
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but received. %v", err))
	}
	if list.Items[0].Name != "job-1" {
		t.Error(fmt.Sprintf("Job %s should be returned but wasn't.", "job-1"))
	}
}
