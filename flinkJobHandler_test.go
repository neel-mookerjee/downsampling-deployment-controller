package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

type FlinkJobHandlerTestSuite struct {
	FlinkJobHandler FlinkJobHandler
}

func NewFlinkJobHandlerTestSuite(config *Config) *FlinkJobHandlerTestSuite {
	return &FlinkJobHandlerTestSuite{FlinkJobHandler{config: config, flink: NewMockFlinkOperations()}}
}

type MockFlinkOperations struct {
}

func NewMockFlinkOperations() *MockFlinkOperations {
	return &MockFlinkOperations{}
}

func (f *MockFlinkOperations) GetRunningFlinkJobs() (JobDetails, error) {
	var jobDetails JobDetails
	body := []byte(`{
			"jobs": [
				{
					"jid": "e0a668956185483b933d9b77820eacbd",
					"name": "downsample:197601d5:omni:nsa_duration:60",
					"state": "RUNNING",
					"start-time": 1512754993173,
					"end-time": -1,
					"duration": 354889492,
					"last-modification": 1512755081997,
					"tasks": {
						"total": 3,
						"pending": 0,
						"running": 3,
						"finished": 0,
						"canceling": 0,
						"canceled": 0,
						"failed": 0
					}
				},
				{
					"jid": "e0a668956185483b933d9b77820eacbd",
					"name": "simulate:b86f3721:omni:nsa_duration:60",
					"state": "RUNNING",
					"start-time": 1512754993173,
					"end-time": -1,
					"duration": 354889492,
					"last-modification": 1512755081997,
					"tasks": {
						"total": 3,
						"pending": 0,
						"running": 3,
						"finished": 0,
						"canceling": 0,
						"canceled": 0,
						"failed": 0
					}
				},
				{
					"jid": "e0a668956185483b933d9b77820eacbd",
					"name": "downsample:197601d5:omni:nsa_duration:60",
					"state": "RUNNING",
					"start-time": 1512754993173,
					"end-time": -1,
					"duration": 354889492,
					"last-modification": 1512755081997,
					"tasks": {
						"total": 3,
						"pending": 0,
						"running": 3,
						"finished": 0,
						"canceling": 0,
						"canceled": 0,
						"failed": 0
					}
				},
				{
					"jid": "437549e832223e9f815e927613707c33",
					"name": "simulate:b86f3721:sca:request_count:60",
					"state": "RUNNING",
					"start-time": 1512760251113,
					"end-time": -1,
					"duration": 349631552,
					"last-modification": 1512760251486,
					"tasks": {
						"total": 3,
						"pending": 0,
						"running": 3,
						"finished": 0,
						"canceling": 0,
						"canceled": 0,
						"failed": 0
					}
				},
				{
					"jid": "437549e832223e9f815e927613707c33",
					"name": "simulate:b86f3732:sca:request_count:60",
					"state": "RUNNING",
					"start-time": 1512760251113,
					"end-time": -1,
					"duration": 349631552,
					"last-modification": 1512760251486,
					"tasks": {
						"total": 3,
						"pending": 0,
						"running": 3,
						"finished": 0,
						"canceling": 0,
						"canceled": 0,
						"failed": 0
					}
				}
			]
		}`)

	err := json.Unmarshal(body, &jobDetails)
	return jobDetails, err
}

func (f *MockFlinkOperations) CreatelJob(jarId string, param string) error {
	return nil
}

func (f *MockFlinkOperations) CancelJob(jobId string) error {
	return nil
}

func (f *MockFlinkOperations) GetLatestFlinkJarId() (string, error) {
	return "972ab84e-dad5-4af8-944d-d1c96183f3bf_flink-line-protocol-downsampler-assembly-0.10.0-SNAPSHOT.jar", nil
}

func (f *MockFlinkOperations) ProduceJobCancelUrl(jobId string) string {
	return ""
}

func (f *MockFlinkOperations) ProduceJobCreationUrl(jarId string) string {
	return ""
}

func Test_FlinkJobHandler_CancelFlinkJob_Success(t *testing.T) {
	tc := NewFlinkJobHandlerTestSuite(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}})
	count, err := tc.FlinkJobHandler.CancelFlinkJob("b86f3721", FLINKL_SIMULATION)
	if err != nil {
		t.Error(fmt.Sprintf("Error occurred but wasn't expected: %v", err))
	}
	if count != 2 {
		t.Error(fmt.Sprintf("Expected cancel simulate job count as %d found %d", 2, count))
	}
}

func Test_FlinkJobHandler_CancelFlinkJob_Success_2(t *testing.T) {
	tc := NewFlinkJobHandlerTestSuite(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}})
	count, err := tc.FlinkJobHandler.CancelFlinkJob("197601d5", FLINK_ACTUAL)
	if err != nil {
		t.Error(fmt.Sprintf("Error occurred but wasn't expected: %v", err))
	}
	if count != 2 {
		t.Error(fmt.Sprintf("Expected cancel downsample job count as %d found %d", 2, count))
	}
}

func Test_FlinkJobHandler_CancelFlinkJob_Success_3(t *testing.T) {
	tc := NewFlinkJobHandlerTestSuite(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}})
	count, err := tc.FlinkJobHandler.CancelFlinkJob("197601d5", FLINK_ALL)
	if err != nil {
		t.Error(fmt.Sprintf("Error occurred but wasn't expected: %v", err))
	}
	if count != 2 {
		t.Error(fmt.Sprintf("Expected cancel job count as %d found %d", 2, count))
	}
}

func Test_FlinkJobHandler_HandleOldFlinkJobs_Success(t *testing.T) {
	tc := NewFlinkJobHandlerTestSuite(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}})
	count, err := tc.FlinkJobHandler.HandleOldFlinkJobs()
	if err != nil {
		t.Error(fmt.Sprintf("Error occurred but wasn't expected: %v", err))
	}
	if count != 3 {
		t.Error(fmt.Sprintf("Expected cancel simulate job count as %d found %d", 3, count))
	}
}

func Test_FlinkJobHandler_CheckForExistingJob_Success(t *testing.T) {
	tc := NewFlinkJobHandlerTestSuite(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}})
	bool, err := tc.FlinkJobHandler.CheckForExistingJob("b86f3721", FLINKL_SIMULATION)
	if err != nil {
		t.Error(fmt.Sprintf("Error occurred but wasn't expected: %v", err))
	}
	if bool != true {
		t.Error(fmt.Sprintf("Expected job but could not find: %s", "b86f3721"))
	}
}

func Test_FlinkJobHandler_CheckForExistingJob_Success_2(t *testing.T) {
	tc := NewFlinkJobHandlerTestSuite(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}})
	bool, err := tc.FlinkJobHandler.CheckForExistingJob("197601d5", FLINK_ACTUAL)
	if err != nil {
		t.Error(fmt.Sprintf("Error occurred but wasn't expected: %v", err))
	}
	if bool != true {
		t.Error(fmt.Sprintf("Expected job but could not find: %s", "197601d5"))
	}
}

func Test_FlinkJobHandler_CheckForExistingJob_Success_3(t *testing.T) {
	tc := NewFlinkJobHandlerTestSuite(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}})
	bool, err := tc.FlinkJobHandler.CheckForExistingJob("197601d5", FLINK_ALL)
	if err != nil {
		t.Error(fmt.Sprintf("Error occurred but wasn't expected: %v", err))
	}
	if bool != true {
		t.Error(fmt.Sprintf("Expected job but could not find: %s", "197601d5"))
	}
}

func Test_FlinkJobHandler_ModifyQueryObject(t *testing.T) {
	var ds DownsamplingObject
	json.Unmarshal([]byte(`{
	  "createdAt": "2017-11-20T18:58:31Z",
	  "db": "sca",
	  "fields": [
		{
		  "alias": "sum_count",
		  "field": "count",
		  "func": "SUM"
		},
		{
		  "alias": "min_count",
		  "field": "count",
		  "func": "MIN"
		},
		{
		  "alias": "sum_count2",
		  "field": "count2",
		  "func": "SUM"
		}
	  ],
	  "interval": 60,
	  "measurement": "request_count",
	  "nickname": "proficient-porpoise",
	  "previewExpiresAt": null,
	  "queryHash": "d6a0cf17",
	  "queryId": "a7bd0e1c",
	  "queryState": "PREVIEW_PENDING",
	  "rp": "autogen",
	  "tags": [
		"app_name",
		"scenario",
		"service"
	  ],
	  "targetMeasurement": "request_count_by_scenario",
	  "targetRp": "downsample",
	  "updatedAt": null,
	  "isHistoricDownsampling": false
	}`), &ds)

	tc := NewFlinkJobHandlerTestSuite(&Config{})
	dsNew := tc.FlinkJobHandler.ModifyQueryObject(ds)
	indx1, indx2 := 0, 0
	if dsNew.Fields[0].Field == "count" {
		indx2 = 1
	} else {
		indx1 = 1
	}
	if dsNew.Fields[indx1].Field != "count" || dsNew.Fields[indx2].Field != "count2" ||
		dsNew.Fields[indx1].Alias != "metricsAGG_count" || dsNew.Fields[indx2].Alias != "metricsAGG_count2" ||
		dsNew.Fields[indx1].Function != "metricsAGG" || dsNew.Fields[indx2].Function != "metricsAGG" {
		t.Error(fmt.Sprintf("Modified object was received wrong %v", dsNew))
	}
}

func Test_FlinkJobHandler_CreateDownsampleJobConfig(t *testing.T) {
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
	  "queryId": "a7bd0e1c",
	  "queryState": "DEPLOYED",
	  "rp": "autogen",
	  "tags": [
		"app_name",
		"scenario",
		"service"
	  ],
	  "targetMeasurement": "request_count_by_scenario",
	  "targetRp": "downsample",
	  "updatedAt": null,
	  "isHistoricDownsampling": false
	}`), &ds)

	tc := NewFlinkJobHandlerTestSuite(&Config{KafkaConfig: &KafkaConfig{Source: "kafka.corenonprod.r53.arghanil.net:9092", Sink: "kafka.corenonprod.r53.arghanil.net:9092"}})
	str, _ := tc.FlinkJobHandler.CreateDownsampleJobConfig(ds)
	expected := "--jobConfig eyJuaWNrbmFtZSI6InByb2ZpY2llbnQtcG9ycG9pc2UiLCJxdWVyeUlkIjoiYTdiZDBlMWMiLCJxdWVyeUhhc2giOiJkNmEwY2YxNyIsImNyZWF0ZWRBdCI6IjIwMTctMTEtMjBUMTg6NTg6MzFaIiwidXBkYXRlZEF0IjoiIiwiZGIiOiJzY2EiLCJycCI6ImF1dG9nZW4iLCJtZWFzdXJlbWVudCI6InJlcXVlc3RfY291bnQiLCJ0YXJnZXRScCI6ImRvd25zYW1wbGUiLCJ0YXJnZXRNZWFzdXJlbWVudCI6InJlcXVlc3RfY291bnRfYnlfc2NlbmFyaW8iLCJwcmV2aWV3RXhwaXJlc0F0IjoiIiwicXVlcnlTdGF0ZSI6IkRFUExPWUVEIiwibGlzdEZpZWxkRnVuYyI6W3siYWxpYXMiOiJtZXRyaWNzQUdHX2NvdW50IiwiZmllbGQiOiJjb3VudCIsImZ1bmMiOiJtZXRyaWNzQUdHIn1dLCJ0YWdzIjpbImFwcF9uYW1lIiwic2NlbmFyaW8iLCJzZXJ2aWNlIl0sImludGVydmFsIjo2MCwiaXNIaXN0b3JpY0Rvd25zYW1wbGluZyI6ZmFsc2V9 --sourceTopic sca-influx-metrics --sourceCluster kafka.corenonprod.r53.arghanil.net:9092 --consumerGroupId downsample-a7bd0e1c --sinkCluster kafka.corenonprod.r53.arghanil.net:9092 --sinkTopic sca-downsampling-influx-metrics --jobName downsample:a7bd0e1c:sca:request_count:60"
	if str != expected {
		t.Error(fmt.Sprintf("Downsample job config was expected %s found %s", expected, str))
	}
}

func Test_FlinkJobHandler_CreateSimulationJobConfig(t *testing.T) {
	var ds DownsamplingObject
	json.Unmarshal([]byte(`{
	  "createdAt": "2017-11-20T18:58:31Z",
	  "db": "sca",
	  "fields": [
		{
		  "alias": "sum_count",
		  "field": "count",
		  "func": "SUM"
		},
		{
		  "alias": "min_count",
		  "field": "count",
		  "func": "MIN"
		},
		{
		  "alias": "sum_count2",
		  "field": "count2",
		  "func": "SUM"
		}
	  ],
	  "interval": 60,
	  "measurement": "request_count",
	  "nickname": "proficient-porpoise",
	  "previewExpiresAt": null,
	  "queryHash": "d6a0cf17",
	  "queryId": "a7bd0e1c",
	  "queryState": "PREVIEW_PENDING",
	  "rp": "autogen",
	  "tags": [
		"app_name",
		"scenario",
		"service"
	  ],
	  "targetMeasurement": "request_count_by_scenario",
	  "targetRp": "downsample",
	  "updatedAt": null,
	  "isHistoricDownsampling": false
	}`), &ds)

	offset := KafkaPartitionOffsets{{partitionId: 0, DesiredOffset: 1000}, {partitionId: 1, DesiredOffset: 2000}}
	influxUrl := "http://downsamplr-preview-b4a3ef6c.grafana.platform.r53.arghanil.net"

	tc := NewFlinkJobHandlerTestSuite(&Config{KafkaConfig: &KafkaConfig{Source: "kafka.corenonprod.r53.arghanil.net:9092"}})
	str, _ := tc.FlinkJobHandler.CreateSimulationJobConfig(ds, influxUrl, offset)
	expected := "--jobConfig eyJuaWNrbmFtZSI6InByb2ZpY2llbnQtcG9ycG9pc2UiLCJxdWVyeUlkIjoiYTdiZDBlMWMiLCJxdWVyeUhhc2giOiJkNmEwY2YxNyIsImNyZWF0ZWRBdCI6IjIwMTctMTEtMjBUMTg6NTg6MzFaIiwidXBkYXRlZEF0IjoiIiwiZGIiOiJzY2EiLCJycCI6ImF1dG9nZW4iLCJtZWFzdXJlbWVudCI6InJlcXVlc3RfY291bnQiLCJ0YXJnZXRScCI6ImRvd25zYW1wbGUiLCJ0YXJnZXRNZWFzdXJlbWVudCI6InJlcXVlc3RfY291bnRfYnlfc2NlbmFyaW8iLCJwcmV2aWV3RXhwaXJlc0F0IjoiIiwicXVlcnlTdGF0ZSI6IlBSRVZJRVdfUEVORElORyIsImxpc3RGaWVsZEZ1bmMiOlt7ImFsaWFzIjoibWV0cmljc0FHR19jb3VudCIsImZpZWxkIjoiY291bnQiLCJmdW5jIjoibWV0cmljc0FHRyJ9LHsiYWxpYXMiOiJtZXRyaWNzQUdHX2NvdW50MiIsImZpZWxkIjoiY291bnQyIiwiZnVuYyI6Im1ldHJpY3NBR0cifV0sInRhZ3MiOlsiYXBwX25hbWUiLCJzY2VuYXJpbyIsInNlcnZpY2UiXSwiaW50ZXJ2YWwiOjYwLCJpc0hpc3RvcmljRG93bnNhbXBsaW5nIjpmYWxzZX0= --sourceTopic sca-influx-metrics --sourceCluster kafka.corenonprod.r53.arghanil.net:9092 --consumerGroupId downsample-simulation-a7bd0e1c --influxdbUrl aHR0cDovL2Rvd25zYW1wbHItcHJldmlldy1iNGEzZWY2Yy5ncmFmYW5hLmRhdGFsZW5zLnBsYXRmb3JtLnI1My5ub3Jkc3Ryb20ubmV0OjgwL3dyaXRlP2RiPXNjYSZycD1kb3duc2FtcGxlJnByZWNpc2lvbj11cw== --previewMode true --jobName simulate:a7bd0e1c:sca:request_count:60 --topicOffsets 0:1000,1:2000"
	if str != expected {
		t.Error(fmt.Sprintf("Simulation job config was expected %s found %s", expected, str))
	}
}

func Test_FlinkJobHandler_DeployFlinkJobForSimulation(t *testing.T) {
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
	  "queryId": "a7bd0e1c",
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

	offset := KafkaPartitionOffsets{{partitionId: 0, DesiredOffset: 1000}, {partitionId: 1, DesiredOffset: 2000}}
	influxUrl := "http://downsamplr-preview-b4a3ef6c.grafana.platform.r53.arghanil.net"

	tc := NewFlinkJobHandlerTestSuite(&Config{KafkaConfig: &KafkaConfig{Source: "kafka.corenonprod.r53.arghanil.net:9092"}})
	err := tc.FlinkJobHandler.DeployFlinkJobForSimulation(ds, influxUrl, offset)
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but found %v", err))
	}
}

func Test_FlinkJobHandler_DeployFlinkJob(t *testing.T) {
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
	  "queryId": "a7bd0e1c",
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

	tc := NewFlinkJobHandlerTestSuite(&Config{KafkaConfig: &KafkaConfig{Source: "kafka.corenonprod.r53.arghanil.net:9092", Sink: "kafka.corenonprod.r53.arghanil.net:9092"}})
	err := tc.FlinkJobHandler.DeployFlinkJob(ds)
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but found %v", err))
	}
}

type MockFlinkOperationsForError struct {
}

func NewMockFlinkOperationsForError() *MockFlinkOperationsForError {
	return &MockFlinkOperationsForError{}
}

func (f *MockFlinkOperationsForError) GetRunningFlinkJobs() (JobDetails, error) {
	var jobDetails JobDetails
	body := []byte(`{
			"jobs": [
				{
					"jid": "e0a668956185483b933d9b77820eacbd",
					"name": "downsample:a7bd0e1c:sca:request_count:60",
					"state": "RUNNING",
					"start-time": 1512754993173,
					"end-time": -1,
					"duration": 354889492,
					"last-modification": 1512755081997,
					"tasks": {
						"total": 3,
						"pending": 0,
						"running": 3,
						"finished": 0,
						"canceling": 0,
						"canceled": 0,
						"failed": 0
					}
				},
				{
					"jid": "437549e832223e9f815e927613707c33",
					"name": "simulate:a7bd0e1c:sca:request_count:60",
					"state": "RUNNING",
					"start-time": 1512760251113,
					"end-time": -1,
					"duration": 349631552,
					"last-modification": 1512760251486,
					"tasks": {
						"total": 3,
						"pending": 0,
						"running": 3,
						"finished": 0,
						"canceling": 0,
						"canceled": 0,
						"failed": 0
					}
				}
			]
		}`)

	err := json.Unmarshal(body, &jobDetails)
	return jobDetails, err
}

func (f *MockFlinkOperationsForError) CreatelJob(jarId string, param string) error {
	return errors.New("Should not reach here")
}

func (f *MockFlinkOperationsForError) CancelJob(jobId string) error {
	return errors.New("Should not reach here")
}

func (f *MockFlinkOperationsForError) GetLatestFlinkJarId() (string, error) {
	return "", errors.New("Should not reach here")
}

func Test_FlinkJobHandler_DeployFlinkJobForSimulation_Skip_Condition(t *testing.T) {
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
	  "queryId": "a7bd0e1c",
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

	offset := KafkaPartitionOffsets{{partitionId: 0, DesiredOffset: 1000}, {partitionId: 1, DesiredOffset: 2000}}
	influxUrl := "http://downsamplr-preview-b4a3ef6c.grafana.platform.r53.arghanil.net"

	tc := NewFlinkJobHandlerTestSuite(&Config{KafkaConfig: &KafkaConfig{Source: "kafka.corenonprod.r53.arghanil.net:9092"}})
	tc.FlinkJobHandler.flink = NewMockFlinkOperationsForError()
	err := tc.FlinkJobHandler.DeployFlinkJobForSimulation(ds, influxUrl, offset)
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but found %v", err))
	}
}

func Test_FlinkJobHandler_DeployFlinkJob_Skip_Condition(t *testing.T) {
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
	  "queryId": "a7bd0e1c",
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

	tc := NewFlinkJobHandlerTestSuite(&Config{KafkaConfig: &KafkaConfig{Source: "kafka.corenonprod.r53.arghanil.net:9092", Sink: "kafka.corenonprod.r53.arghanil.net:9092"}})
	tc.FlinkJobHandler.flink = NewMockFlinkOperationsForError()
	err := tc.FlinkJobHandler.DeployFlinkJob(ds)
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but found %v", err))
	}
}
