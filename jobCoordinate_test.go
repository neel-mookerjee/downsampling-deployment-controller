package main

import (
	"errors"
	"fmt"
	batchv1 "k8s.io/api/batch/v1"
	"strings"
	"testing"
)

type CoordinatorJobTestSuite struct {
	CoordinatorJob      CoordinatorJob
	FakeQueryAssertData FakeQueryAssertData
}

func NewCoordinatorJobTestSuite(config *Config, data FakeQueryAssertData, jobName string, operation string) *CoordinatorJobTestSuite {
	return &CoordinatorJobTestSuite{CoordinatorJob{k8JobHandler: &FakeJobHandler{jobName, operation}, itemHandler: &DownsamplingItemHandler{db: NewMockDb(data), config: config}, config: config}, data}
}

type FakeJobHandler struct {
	JobName   string
	Operation string
}

func (j *FakeJobHandler) CheckIfJobExists(jobName string) (bool, error) {
	return false, nil
}

func NewFakeJobHandler() (*FakeJobHandler, error) {
	return &FakeJobHandler{}, nil
}

func (j *FakeJobHandler) GetAllDeploymentJobs() (*batchv1.JobList, error) {
	list := &batchv1.JobList{}
	job := batchv1.Job{}
	job.Name = ""
	list.Items = append(list.Items, job)
	return list, nil
}

func (j *FakeJobHandler) GetJob(jobName string) (*batchv1.Job, error) {
	job := batchv1.Job{}
	return &job, nil
}

func (j *FakeJobHandler) CreateJob(jobName string, config *DefaultFiller) (bool, error) {
	if jobName != j.JobName {
		return false, errors.New("wrong job name")
	}
	return true, nil
}

func (j *FakeJobHandler) GetConfigForJob(operation string, jobName string, queryId string, params PARAM) (*DefaultFiller, error) {
	if jobName != j.JobName {
		return nil, errors.New("wrong job name")
	}
	if operation != j.Operation {
		return nil, errors.New("wrong operation")
	}
	return nil, nil
}

func Test_CoordinatorJob_Execute_PreviewPending(t *testing.T) {
	tc := NewCoordinatorJobTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45, Environment: "test"}, FakeQueryAssertData{dsList: []DownsamplingObject{DownsamplingObject{QueryId: "query1", QueryState: "PREVIEW_PENDING"}}}, "downsample-controller-test-simulate-query1", "simulate")
	err := tc.CoordinatorJob.Execute(PARAM{OPERATION_SIMULATE: "simulate"})
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected here but received - %v", err))
	}
}

func Test_CoordinatorJob_Execute_Pending(t *testing.T) {
	tc := NewCoordinatorJobTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45, Environment: "test"}, FakeQueryAssertData{dsList: []DownsamplingObject{DownsamplingObject{QueryId: "query2", QueryState: "PENDING"}}}, "downsample-controller-test-deploy-query2", "deploy")
	err := tc.CoordinatorJob.Execute(PARAM{OPERATION_DEPLOY: "deploy"})
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected here but received - %v", err))
	}
}

func Test_CoordinatorJob_Execute_Deleted(t *testing.T) {
	tc := NewCoordinatorJobTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45, Environment: "test"}, FakeQueryAssertData{dsList: []DownsamplingObject{DownsamplingObject{QueryId: "query3", QueryState: "DELETED"}}}, "downsample-controller-test-delete-query3", "delete")
	err := tc.CoordinatorJob.Execute(PARAM{OPERATION_DELETE: "delete"})
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected here but received - %v", err))
	}
}

func Test_CoordinatorJob_Execute_Error(t *testing.T) {
	tc := NewCoordinatorJobTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45, Environment: "test"}, FakeQueryAssertData{dsList: []DownsamplingObject{DownsamplingObject{QueryId: "query3", QueryState: "DEPLOYED"}}}, "", "deploy")
	err := tc.CoordinatorJob.Execute(PARAM{OPERATION_DEPLOY: "deploy"})
	if err == nil || !strings.Contains(err.Error(), "Invalid option") {
		t.Error(fmt.Sprintf("Error was expected but not received accordingly - %v", err))
	}
}
