package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type FlinkClientTestSuite struct {
	label             string
	flinkCallHandler  func(w http.ResponseWriter, r *http.Request)
	expectedErrorCode int
	expectedMessage   string
}

var FlinkClientTestsCases = []FlinkClientTestSuite{
	{
		"Should Return 200 And No Error Message",
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(200)
		},
		200,
		"",
	},
	{
		"Should Return 400 And Error Message",
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(400)
			w.Write([]byte("error message1"))
		},
		400,
		"error message1",
	},
}

func (suite *FlinkClientTestSuite) AssertErrorNotExpected(httpCode int, err error, t *testing.T) {
	if err != nil {
		t.Error(fmt.Sprintf("%s: Recieved error that should have not been received.\n\tresponse code: %d\tactual %s", suite.label, httpCode, err.Error()))
	}
}

func (suite *FlinkClientTestSuite) AssertUnexpected(httpCode int, body string, err error, t *testing.T) {
	if httpCode != suite.expectedErrorCode && !strings.Contains(body, suite.expectedMessage) {
		t.Error(fmt.Sprintf("%s: Recieved unexpected message.\n\texpected: %s\n\tactual: %s", suite.label, suite.expectedMessage, body))
	}
}

func Test_FlinkClient_MakeHttpCall(t *testing.T) {
	for _, testCase := range FlinkClientTestsCases {
		t.Run(testCase.label, func(t *testing.T) {
			flinkSpy := httptest.NewServer(http.HandlerFunc(testCase.flinkCallHandler))
			defer flinkSpy.Close()
			sut := &FlinkClient{}
			code, body, err := sut.MakeHttpCall(http.MethodPost, flinkSpy.URL)
			testCase.AssertErrorNotExpected(code, err, t)
			testCase.AssertUnexpected(code, string(body), err, t)
		})
	}
}

func Test_FlinkClient_MakeHttpCall_Error(t *testing.T) {
	sut := &FlinkClient{}
	_, _, err := sut.MakeHttpCall(http.MethodPost, "junkurl")
	if err == nil {
		t.Error(fmt.Sprintf("Was expecting error but not received.\n\texpected: %s\n\tactual: %v", "error message", err))
	}
}

type FlinkFunctionsTestSuite struct {
	FlinkFunctions FlinkFunctions
}

func NewFlinkFunctionsTestSuite(config *Config, f func(w http.ResponseWriter, r *http.Request)) *FlinkFunctionsTestSuite {
	return &FlinkFunctionsTestSuite{FlinkFunctions{config: config, flinkClient: NewFlinkMockClient(f)}}
}

func NewFlinkFunctionsTestSuiteWithUrl(config *Config, url string) *FlinkFunctionsTestSuite {
	return &FlinkFunctionsTestSuite{FlinkFunctions{config: config, flinkClient: &FlinkMockClient{url: url}}}
}

type FlinkMockClient struct {
	url string
}

func NewFlinkMockClient(f func(w http.ResponseWriter, r *http.Request)) *FlinkMockClient {
	flinkSpy := httptest.NewServer(http.HandlerFunc(f))
	return &FlinkMockClient{flinkSpy.URL}
}

func (f *FlinkMockClient) MakeHttpCall(method string, url string) (int, []byte, error) {
	return NewFlinkClient().MakeHttpCall(http.MethodPost, f.url)
}

func Test_FlinkFunctions_CancelFlinkJob_Success(t *testing.T) {
	tc := NewFlinkFunctionsTestSuite(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}}, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
	err := tc.FlinkFunctions.CancelJob("jobid")
	if err != nil {
		t.Error(fmt.Sprintf("Error occurred  but wasn't expected: %v", err))
	}
}

func Test_FlinkFunctions_CancelFlinkJob_Failure_No_Error(t *testing.T) {
	tc := NewFlinkFunctionsTestSuite(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}}, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte("error message1"))
	})
	err := tc.FlinkFunctions.CancelJob("jobid")
	if err == nil {
		t.Error(fmt.Sprintf("Error was expected but didn't receive."))
	}
}

func Test_FlinkFunctions_CancelFlinkJob_Failure_With_Error(t *testing.T) {
	tc := NewFlinkFunctionsTestSuiteWithUrl(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}}, "junkurl")
	err := tc.FlinkFunctions.CancelJob("jobid")
	if err == nil {
		t.Error(fmt.Sprintf("Error was expected but didn't receive."))
	}
}

func Test_FlinkFunctions_ProduceJobCancelUrl(t *testing.T) {
	tc := NewFlinkFunctionsTestSuiteWithUrl(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: "http://cancel-job"}}, "")
	url := tc.FlinkFunctions.ProduceJobCancelUrl("jobid")
	if url != "http://cancel-job/jobid/cancel" {
		t.Error(fmt.Sprintf("Flink job cancel url was expected %s found %s.", "http://cancel-job/jobid/cancel", url))
	}
}

func Test_FlinkFunctions_GetLatestFlinkJarId_Success(t *testing.T) {
	tc := NewFlinkFunctionsTestSuite(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}}, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(`{
			"address": "http://ip-172-16-228-176.us-west-2.compute.internal:8081",
			"files": [
				{
					"id": "972ab84e-dad5-4af8-944d-d1c96183f3bf_flink-line-protocol-downsampler-assembly-0.10.0-SNAPSHOT.jar",
					"name": "flink-line-protocol-downsampler-assembly-0.10.0-SNAPSHOT.jar",
					"uploaded": 1513106987000,
					"entry": [
						{
							"name": "com.arghanil.Job",
							"description": "No description provided"
						}
					]
				},
				{
					"id": "972ab84e-dad5-4af8-944d-d1c96183f3bg_flink-line-protocol-downsampler-assembly-0.10.0-SNAPSHOT.jar",
					"name": "flink-line-protocol-downsampler-assembly-0.10.0-SNAPSHOT.jar",
					"uploaded": 1513106987000,
					"entry": [
						{
							"name": "com.arghanil.Job",
							"description": "No description provided"
						}
					]
				},
				{
					"id": "972ab84e-dad5-4af8-944d-d1c96183f3bh_flink-line-protocol-downsampler-assembly-0.10.0-SNAPSHOT.jar",
					"name": "flink-line-protocol-downsampler-assembly-0.10.0-SNAPSHOT.jar",
					"uploaded": 1513106987000,
					"entry": [
						{
							"name": "com.arghanil.Job",
							"description": "No description provided"
						}
					]
				}
			]
		}`))
	})
	jarId, err := tc.FlinkFunctions.GetLatestFlinkJarId()
	if err != nil {
		t.Error(fmt.Sprintf("Error occurred  but wasn't expected: %v", err))
	}
	jarIdExpected := "972ab84e-dad5-4af8-944d-d1c96183f3bf_flink-line-protocol-downsampler-assembly-0.10.0-SNAPSHOT.jar"
	if jarId != jarIdExpected {
		t.Error(fmt.Sprintf("Jar Id was expected %s found %s", jarIdExpected, jarId))
	}
}

func Test_FlinkFunctions_GetLatestFlinkJarId_OtherJars(t *testing.T) {
	tc := NewFlinkFunctionsTestSuite(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}}, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(`{
			"address": "http://ip-172-16-228-176.us-west-2.compute.internal:8081",
			"files": [
				{
					"id": "972ab84e-dad5-4af8-944d-d1c96183f3bf_crap-assembly-0.10.0-SNAPSHOT.jar",
					"name": "crap-assembly-0.10.0-SNAPSHOT.jar",
					"uploaded": 1513106987000,
					"entry": [
						{
							"name": "com.arghanil.Job",
							"description": "No description provided"
						}
					]
				},
				{
					"id": "972ab84e-dad5-4af8-944d-d1c96183f3bg_crap-assembly-0.10.0-SNAPSHOT.jar",
					"name": "crap-assembly-0.10.0-SNAPSHOT.jar",
					"uploaded": 1513106987000,
					"entry": [
						{
							"name": "com.arghanil.Job",
							"description": "No description provided"
						}
					]
				},
				{
					"id": "972ab84e-dad5-4af8-944d-d1c96183f3bh_crap-0.10.0-SNAPSHOT.jar",
					"name": "crap-assembly-0.10.0-SNAPSHOT.jar",
					"uploaded": 1513106987000,
					"entry": [
						{
							"name": "com.arghanil.Job",
							"description": "No description provided"
						}
					]
				}
			]
		}`))
	})
	_, err := tc.FlinkFunctions.GetLatestFlinkJarId()
	if err == nil {
		t.Error(fmt.Sprintf("Error was expected but didn't receive."))
	}
}

func Test_FlinkFunctions_GetLatestFlinkJarId_Failure_No_Error(t *testing.T) {
	tc := NewFlinkFunctionsTestSuite(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}}, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte("message1"))
	})
	_, err := tc.FlinkFunctions.GetLatestFlinkJarId()
	if err == nil {
		t.Error(fmt.Sprintf("Error was expected but didn't receive."))
	}
}

func Test_FlinkFunctions_GetLatestFlinkJarId_Failure_With_Error(t *testing.T) {
	tc := NewFlinkFunctionsTestSuiteWithUrl(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}}, "junkurl")
	_, err := tc.FlinkFunctions.GetLatestFlinkJarId()
	if err == nil {
		t.Error(fmt.Sprintf("Error was expected but didn't receive."))
	}
}

func Test_FlinkFunctions_GetLatestFlinkJarId_No_Jar(t *testing.T) {
	tc := NewFlinkFunctionsTestSuite(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}}, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(`{
			"address": "http://ip-172-16-228-236.us-west-2.compute.internal:8081",
			"files": []
		}`))
	})
	_, err := tc.FlinkFunctions.GetLatestFlinkJarId()
	if err == nil || !strings.Contains(err.Error(), "Jar id not found") {
		t.Error(fmt.Sprintf("Error was expected but didn't receive."))
	}
}

func Test_FlinkFunctions_GetRunningFlinkJobs_Success(t *testing.T) {
	tc := NewFlinkFunctionsTestSuite(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}}, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(`{
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
					"jid": "437549e832223e9f815e927613707c33",
					"name": "downsample:b86f3721:sca:request_count:60",
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
		}`))
	})
	jobDetails, err := tc.FlinkFunctions.GetRunningFlinkJobs()
	if err != nil {
		t.Error(fmt.Sprintf("Error occurred  but wasn't expected: %v", err))
	}
	if len(jobDetails.Jobs) != 2 {
		t.Error(fmt.Sprintf("Job count were expected %d found %d", 2, len(jobDetails.Jobs)))
	}
	if jobDetails.Jobs[0].Name != "downsample:197601d5:omni:nsa_duration:60" || jobDetails.Jobs[0].JobId != "e0a668956185483b933d9b77820eacbd" || jobDetails.Jobs[0].StartTs != 1512754993173 {
		t.Error(fmt.Sprintf("Job details were expected %s,%s,%d found %v", "downsample:197601d5:omni:nsa_duration:60", "e0a668956185483b933d9b77820eacbd", 1512754993173, jobDetails.Jobs[0]))
	}
}

func Test_FlinkFunctions_GetRunningFlinkJobs_Failure_No_Error(t *testing.T) {
	tc := NewFlinkFunctionsTestSuite(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}}, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte("message1"))
	})
	_, err := tc.FlinkFunctions.GetRunningFlinkJobs()
	if err == nil {
		t.Error(fmt.Sprintf("Error was expected but didn't receive."))
	}
}

func Test_FlinkFunctions_GetRunningFlinkJobs_Failure_With_Error(t *testing.T) {
	tc := NewFlinkFunctionsTestSuiteWithUrl(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}}, "junkurl")
	_, err := tc.FlinkFunctions.GetRunningFlinkJobs()
	if err == nil {
		t.Error(fmt.Sprintf("Error was expected but didn't receive."))
	}
}

func Test_FlinkFunctions_GetRunningFlinkJobs_No_Job(t *testing.T) {
	tc := NewFlinkFunctionsTestSuite(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}}, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(`{
			"jobs": []
			}
		`))
	})
	_, err := tc.FlinkFunctions.GetRunningFlinkJobs()
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but received. %v", err))
	}
}
func Test_FlinkFunctions_CreatelJob_Success(t *testing.T) {
	tc := NewFlinkFunctionsTestSuite(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}}, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(`{
			"jobid": "437549e832223e9f815e927613707c33"
		}`))
	})
	err := tc.FlinkFunctions.CreatelJob("jarid", "param")
	if err != nil {
		t.Error(fmt.Sprintf("Error occurred but wasn't expected: %v", err))
	}
}

func Test_FlinkFunctions_CreatelJob_Failure_No_Error(t *testing.T) {
	tc := NewFlinkFunctionsTestSuite(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}}, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte("message1"))
	})
	err := tc.FlinkFunctions.CreatelJob("jarid", "param")
	if err == nil {
		t.Error(fmt.Sprintf("Error was expected but didn't receive."))
	}
}

func Test_FlinkFunctions_CreatelJob_Failure_With_Error(t *testing.T) {
	tc := NewFlinkFunctionsTestSuiteWithUrl(&Config{FlinkConfig: &FlinkConfig{FlinkJobDeleteUrl: ""}}, "junkurl")
	err := tc.FlinkFunctions.CreatelJob("jarid", "param")
	if err == nil {
		t.Error(fmt.Sprintf("Error was expected but didn't receive."))
	}
}

func Test_FlinkFunctions_ProduceJobCreationUrl(t *testing.T) {
	tc := NewFlinkFunctionsTestSuiteWithUrl(&Config{FlinkConfig: &FlinkConfig{FlinkJarsUrl: "http://create-job/"}}, "")
	url := tc.FlinkFunctions.ProduceJobCreationUrl("jarid")
	if url != "http://create-job/jarid/run" {
		t.Error(fmt.Sprintf("Flink job creation url was expected %s found %s.", "http://create-job/jarid/run", url))
	}
}
