package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

type DeleteDownsamplingJobTestSuite struct {
	DeleteDownsamplingJob DeleteDownsamplingJob
	FakeQueryAssertData   FakeQueryAssertData
}

func NewDeleteDownsamplingJobTestSuite(config *Config, data FakeQueryAssertData) *DeleteDownsamplingJobTestSuite {
	return &DeleteDownsamplingJobTestSuite{DeleteDownsamplingJob{flinkJobHandler: &FakeFlinkJobHandler{}, itemHandler: &DownsamplingItemHandler{db: NewMockDb(data), config: config}, config: config}, data}
}

type FakeFlinkJobHandler struct {
}

func (f *FakeFlinkJobHandler) DeployFlinkJobForSimulation(query DownsamplingObject, influxdbBaseUrl string, offsets KafkaPartitionOffsets) error {
	return nil
}

func (f *FakeFlinkJobHandler) DeployFlinkJob(query DownsamplingObject) error {
	return nil
}

func (f *FakeFlinkJobHandler) CancelFlinkJob(queryId string, mode string) (int, error) {
	if mode != FLINK_ALL {
		return 0, errors.New("wrong mode")
	}
	return 0, nil
}

func (f *FakeFlinkJobHandler) HandleOldFlinkJobs() (int, error) {
	return 0, nil
}

func Test_DeleteDownsamplingJob_Execute_Success(t *testing.T) {
	tc := NewDeleteDownsamplingJobTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45, Environment: "test"}, FakeQueryAssertData{dsList: []DownsamplingObject{DownsamplingObject{QueryId: "query1", QueryState: "DELETED"}}})
	err := tc.DeleteDownsamplingJob.Execute(PARAM{queryId: "query1"})
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected here but received - %v", err))
	}
}

func Test_DeleteDownsamplingJob_Execute_NoItem(t *testing.T) {
	tc := NewDeleteDownsamplingJobTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45, Environment: "test"}, FakeQueryAssertData{dsList: []DownsamplingObject{{}}})
	err := tc.DeleteDownsamplingJob.Execute(PARAM{queryId: "query1"})
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but received - %v", err))
	}
}

func Test_DeleteDownsamplingJob_Execute_DiffStatus(t *testing.T) {
	tc := NewDeleteDownsamplingJobTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45, Environment: "test"}, FakeQueryAssertData{dsList: []DownsamplingObject{{QueryId: "query1", QueryState: "DEPLOY"}}})
	err := tc.DeleteDownsamplingJob.Execute(PARAM{queryId: "query1"})
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but received - %v", err))
	}
}

func Test_DeleteDownsamplingJob_Execute_Error_NoQueryId(t *testing.T) {
	tc := NewDeleteDownsamplingJobTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45, Environment: "test"}, FakeQueryAssertData{dsList: []DownsamplingObject{DownsamplingObject{QueryId: "query1", QueryState: "DELETE"}}})
	err := tc.DeleteDownsamplingJob.Execute(PARAM{})
	if err == nil || !strings.Contains(err.Error(), "queryId not received") {
		t.Error(fmt.Sprintf("Error was expected but not received accordingly - %v", err))
	}
}
