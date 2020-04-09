package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type GrafanaClientTestSuite struct {
	label              string
	grafanaCallHandler func(w http.ResponseWriter, r *http.Request)
	expectedErrorCode  int
	expectedMessage    string
}

var GrafanaClientTestsCases = []GrafanaClientTestSuite{
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

func (suite *GrafanaClientTestSuite) AssertErrorNotExpected(httpCode int, err error, t *testing.T) {
	if err != nil {
		t.Error(fmt.Sprintf("%s: Recieved error that should have not been received.\n\tresponse code: %d\tactual %s", suite.label, httpCode, err.Error()))
	}
}

func (suite *GrafanaClientTestSuite) AssertUnexpected(httpCode int, body string, err error, t *testing.T) {
	if httpCode != suite.expectedErrorCode && !strings.Contains(body, suite.expectedMessage) {
		t.Error(fmt.Sprintf("%s: Recieved unexpected message.\n\texpected: %s\n\tactual: %s", suite.label, suite.expectedMessage, body))
	}
}

func Test_GrafanaClient_MakeHttpCall(t *testing.T) {
	for _, testCase := range FlinkClientTestsCases {
		t.Run(testCase.label, func(t *testing.T) {
			flinkSpy := httptest.NewServer(http.HandlerFunc(testCase.flinkCallHandler))
			defer flinkSpy.Close()
			sut := NewGrafanaClient("admin", "admin")
			code, body, err := sut.MakeHttpCall(http.MethodPost, flinkSpy.URL, "", false)
			testCase.AssertErrorNotExpected(code, err, t)
			testCase.AssertUnexpected(code, string(body), err, t)
		})
	}
}

func Test_GrafanaClient_MakeHttpCall_Error(t *testing.T) {
	sut := NewGrafanaClient("admin", "admin")
	_, _, err := sut.MakeHttpCall(http.MethodPost, "junkurl", "", false)
	if err == nil {
		t.Error(fmt.Sprintf("Was expecting error but not received.\n\texpected: %s\n\tactual: %v", "error message", err))
	}
}

func Test_GrafanaClient_MakeHttpCall_BasicAuth(t *testing.T) {
	flinkSpy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ind := r.BasicAuth()
		if ind != true || u != "admin" || p != "admin" {
			w.WriteHeader(403)
			w.Write([]byte("Unauthorized"))
		} else {
			w.WriteHeader(200)
			w.Write([]byte("message1"))
		}
	}))
	defer flinkSpy.Close()
	sut := NewGrafanaClient("admin", "admin")
	code, _, _ := sut.MakeHttpCall(http.MethodPost, flinkSpy.URL, "", true)
	if code != 200 {
		t.Error(fmt.Sprintf("Was not expecting error but received.\n\texpected: %d\n\tactual: %d", 200, code))
	}
}

type GrafanaTestSuite struct {
	label              string
	grafanaCallHandler func(w http.ResponseWriter, r *http.Request)
	expectError        bool
	expectedErrorMsg   string
}

var GrafanaTestsCasesDataSource = []GrafanaTestSuite{
	{
		"Success - Should Return With No Error",
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(200)
		},
		false,
		"",
	},
	{
		"Conflict - Should Return with no error",
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(409)
			w.Write([]byte("error message1"))
		},
		false,
		"",
	},
	{
		"Error - Should Return with no error",
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(502)
			w.Write([]byte("error message1"))
		},
		true,
		"error message1",
	},
}

var GrafanaTestsCasesDashboard = []GrafanaTestSuite{
	{
		"Success - Should Return With No Error",
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(200)
		},
		false,
		"",
	},
	{
		"Conflict - Should Return with no error",
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(412)
			w.Write([]byte("name-exists"))
		},
		false,
		"",
	},
	{
		"Error - Should Return with no error",
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(502)
			w.Write([]byte("error message1"))
		},
		true,
		"error message1",
	},
}

type GrafanaMockClient struct {
	url string
}

func NewGrafanaMockClient(f func(w http.ResponseWriter, r *http.Request)) *GrafanaMockClient {
	flinkSpy := httptest.NewServer(http.HandlerFunc(f))
	return &GrafanaMockClient{flinkSpy.URL}
}

func (f *GrafanaMockClient) MakeHttpCall(method string, url string, body interface{}, isJson bool) (int, []byte, error) {
	return NewGrafanaClient("admin", "admin").MakeHttpCall(http.MethodPost, f.url, body, isJson)
}

func (suite *GrafanaTestSuite) AssertErrorNotExpected(err error, t *testing.T) {
	if err != nil && !suite.expectError {
		t.Error(fmt.Sprintf("%s: Recieved error that should have not been received - %s", suite.label, err.Error()))
	}
}

func (suite *GrafanaTestSuite) AssertErrorExpected(err error, t *testing.T) {
	if (err == nil || !strings.Contains(err.Error(), suite.expectedErrorMsg)) && suite.expectError {
		t.Error(fmt.Sprintf("%s: Didn't recieve error that should have been received - %s", suite.label, err.Error()))
	}
}

func Test_Grafana_CreateDatasource(t *testing.T) {
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
	for _, testCase := range GrafanaTestsCasesDataSource {
		t.Run(testCase.label, func(t *testing.T) {
			sut := &Grafana{grafanaClient: NewGrafanaMockClient(testCase.grafanaCallHandler)}
			err := sut.CreateDatasource(ds)
			testCase.AssertErrorNotExpected(err, t)
			testCase.AssertErrorExpected(err, t)
		})
	}
}

func Test_Grafana_CreateDashboard(t *testing.T) {
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
	for _, testCase := range GrafanaTestsCasesDashboard {
		t.Run(testCase.label, func(t *testing.T) {
			sut := &Grafana{grafanaClient: NewGrafanaMockClient(testCase.grafanaCallHandler), templateParser: NewTemplateParser()}
			err := sut.CreateDashboard(ds)
			testCase.AssertErrorNotExpected(err, t)
			testCase.AssertErrorExpected(err, t)
		})
	}
}
