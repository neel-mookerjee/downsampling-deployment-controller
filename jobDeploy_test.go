package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

type DeployDownsamplingJobTestSuite struct {
	DeployDownsamplingJob DeployDownsamplingJob
	FakeQueryAssertData   FakeQueryAssertData
}

func NewDeployDownsamplingJobTestSuite(config *Config, data FakeQueryAssertData) *DeployDownsamplingJobTestSuite {
	return &DeployDownsamplingJobTestSuite{DeployDownsamplingJob{flinkJobHandler: &FakeFlinkJobHandlerForDeploy{}, itemHandler: &DownsamplingItemHandler{db: NewMockDb(data), config: config}, config: config}, data}
}

type FakeFlinkJobHandlerForDeploy struct {
}

func (f *FakeFlinkJobHandlerForDeploy) DeployFlinkJobForSimulation(query DownsamplingObject, influxdbBaseUrl string, offsets KafkaPartitionOffsets) error {
	return nil
}

func (f *FakeFlinkJobHandlerForDeploy) DeployFlinkJob(query DownsamplingObject) error {
	return nil
}

func (f *FakeFlinkJobHandlerForDeploy) CancelFlinkJob(queryId string, mode string) (int, error) {
	if mode != FLINK_ALL {
		return 0, errors.New("wrong mode")
	}
	return 0, nil
}

func (f *FakeFlinkJobHandlerForDeploy) HandleOldFlinkJobs() (int, error) {
	return 0, nil
}

func Test_DeployDownsamplingJob_Execute_Success(t *testing.T) {
	tc := NewDeployDownsamplingJobTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45, Environment: "test"}, FakeQueryAssertData{dsList: []DownsamplingObject{DownsamplingObject{QueryId: "query1", QueryState: "PENDING"}}})
	err := tc.DeployDownsamplingJob.Execute(PARAM{queryId: "query1"})
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected here but received - %v", err))
	}
}

func Test_DeployDownsamplingJob_Execute_NoItem(t *testing.T) {
	tc := NewDeployDownsamplingJobTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45, Environment: "test"}, FakeQueryAssertData{dsList: []DownsamplingObject{{}}})
	err := tc.DeployDownsamplingJob.Execute(PARAM{queryId: "query1"})
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but received - %v", err))
	}
}

func Test_DeployDownsamplingJob_Execute_DiffStatus(t *testing.T) {
	tc := NewDeployDownsamplingJobTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45, Environment: "test"}, FakeQueryAssertData{dsList: []DownsamplingObject{{QueryId: "query1", QueryState: "DEPLOY"}}})
	err := tc.DeployDownsamplingJob.Execute(PARAM{queryId: "query1"})
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but received - %v", err))
	}
}

func Test_DeployDownsamplingJob_Execute_Error_NoQueryId(t *testing.T) {
	tc := NewDeployDownsamplingJobTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45, Environment: "test"}, FakeQueryAssertData{dsList: []DownsamplingObject{DownsamplingObject{QueryId: "query1", QueryState: "DELETE"}}})
	err := tc.DeployDownsamplingJob.Execute(PARAM{})
	if err == nil || !strings.Contains(err.Error(), "queryId not received") {
		t.Error(fmt.Sprintf("Error was expected but not received accordingly - %v", err))
	}
}
