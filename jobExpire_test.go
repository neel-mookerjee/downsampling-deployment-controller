package main

import (
	"fmt"
	"testing"
)

type DeletePreviewJobTestSuite struct {
	DeletePreviewJob DeletePreviewJob
}

func NewDeletePreviewJobTestSuite(config *Config, data FakeQueryAssertData) *DeletePreviewJobTestSuite {
	return &DeletePreviewJobTestSuite{DeletePreviewJob{flinkJobHandler: &FakeFlinkJobHandler{}, itemHandler: &DownsamplingItemHandler{db: NewMockDb(data), config: config}, config: config, k8DeploymentHandler: &FakeDeploymentHandler{}, k8IngressHandler: &FakeIngressHandler{}, k8ServiceHandler: &FakeServiceHandler{}}}
}

func Test_DeletePreviewJob_Execute_Success(t *testing.T) {
	tc := NewDeletePreviewJobTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45, Environment: "test"}, FakeQueryAssertData{})
	err := tc.DeletePreviewJob.Execute(PARAM{})
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected here but received - %v", err))
	}
}
