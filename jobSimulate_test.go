package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type DeployPreviewJobTestSuite struct {
	DeployPreviewJob    DeployPreviewJob
	FakeQueryAssertData FakeQueryAssertData
}

func NewDeployPreviewJobTestSuite(config *Config, data FakeQueryAssertData) *DeployPreviewJobTestSuite {
	return &DeployPreviewJobTestSuite{DeployPreviewJob{flinkJobHandler: &FakeFlinkJobHandlerForPreview{}, itemHandler: &DownsamplingItemHandler{db: NewMockDb(data), config: config}, config: config, k8DeploymentHandler: &FakeDeploymentHandler{}, k8IngressHandler: &FakeIngressHandler{}, k8ServiceHandler: &FakeServiceHandler{}, kafkaClient: &FakeKafkaClient{}}, data}
}

type FakeFlinkJobHandlerForPreview struct {
}

func (f *FakeFlinkJobHandlerForPreview) DeployFlinkJobForSimulation(query DownsamplingObject, influxdbBaseUrl string, offsets KafkaPartitionOffsets) error {
	return nil
}

func (f *FakeFlinkJobHandlerForPreview) DeployFlinkJob(query DownsamplingObject) error {
	return nil
}

func (f *FakeFlinkJobHandlerForPreview) CancelFlinkJob(queryId string, mode string) (int, error) {
	return 0, nil
}

func (f *FakeFlinkJobHandlerForPreview) HandleOldFlinkJobs() (int, error) {
	return 0, nil
}

type FakeDeploymentHandler struct {
}

func (d *FakeDeploymentHandler) CreateDeployment(params PARAM) error {
	return nil
}

func (d *FakeDeploymentHandler) HandleOldDeployments() (int, error) {
	return 0, nil
}

type FakeIngressHandler struct {
}

func (d *FakeIngressHandler) CreateIngress(params PARAM) (string, string, error) {
	h := http.NewServeMux()
	h.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	})
	h.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("{\"status\":\"success\"}"))
	})
	ispy := httptest.NewServer(h)
	//defer ispy.Close()
	return ispy.URL, ispy.URL, nil
}

func (d *FakeIngressHandler) HandleOldIngresses() (int, error) {
	return 0, nil
}

type FakeServiceHandler struct {
}

func (d *FakeServiceHandler) CreateService(params PARAM) error {
	return nil
}

func (d *FakeServiceHandler) HandleOldServices() error {
	return nil
}

type FakeKafkaClient struct {
}

func (d *FakeKafkaClient) GetDesiredOffsets(topic string) (KafkaPartitionOffsets, error) {
	return nil, nil
}

func Test_DeployPreviewJob_Execute_Success(t *testing.T) {
	var ds DownsamplingObject
	json.Unmarshal([]byte(`{
	  "createdAt": "2017-11-20T18:58:31Z",
	  "db": "sca",
	  "fields": [
		{
		  "alias": "sum_count",
		  "field": "count",
		  "func": "SUM"
		}
	  ],
	  "interval": 60,
	  "measurement": "request_count",
	  "nickname": "proficient-porpoise",
	  "previewExpiresAt": null,
	  "queryHash": "d6a0cf17",
	  "queryId": "query1",
	  "queryState": "PREVIEW_PENDING",
	  "rp": "autogen",
	  "tags": [
		"app_name",
		"scenario",
		"service"
	  ],
	  "targetMeasurement": "request_count_by_scenario",
	  "targetRp": "downsample",
	  "updatedAt": null
	}`), &ds)

	tc := NewDeployPreviewJobTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45, Environment: "test"}, FakeQueryAssertData{dsList: []DownsamplingObject{ds}})
	err := tc.DeployPreviewJob.Execute(PARAM{queryId: "query1"})
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected here but received - %v", err))
	}
}

func Test_DeployPreviewJob_Execute_NoItem(t *testing.T) {
	tc := NewDeployPreviewJobTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45, Environment: "test"}, FakeQueryAssertData{dsList: []DownsamplingObject{{}}})
	err := tc.DeployPreviewJob.Execute(PARAM{queryId: "query1"})
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but received - %v", err))
	}
}

func Test_DeployPreviewJob_Execute_DiffStatus(t *testing.T) {
	tc := NewDeployPreviewJobTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45, Environment: "test"}, FakeQueryAssertData{dsList: []DownsamplingObject{{QueryId: "query1", QueryState: "DEPLOY"}}})
	err := tc.DeployPreviewJob.Execute(PARAM{queryId: "query1"})
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but received - %v", err))
	}
}

func Test_DeployPreviewJob_Execute_Error_NoQueryId(t *testing.T) {
	tc := NewDeployPreviewJobTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45, Environment: "test"}, FakeQueryAssertData{dsList: []DownsamplingObject{DownsamplingObject{QueryId: "query1", QueryState: "DELETE"}}})
	err := tc.DeployPreviewJob.Execute(PARAM{})
	if err == nil || !strings.Contains(err.Error(), "queryId not received") {
		t.Error(fmt.Sprintf("Error was expected but not received accordingly - %v", err))
	}
}
