package main

import (
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type FlinkClientInterface interface {
	MakeHttpCall(method string, url string) (int, []byte, error)
}

type FlinkClient struct {
	flinkClient http.Client
}

func NewFlinkClient() *FlinkClient {
	httpClient := http.Client{
		Timeout: time.Second * 30,
	}
	return &FlinkClient{flinkClient: httpClient}
}

func (f *FlinkClient) MakeHttpCall(method string, url string) (int, []byte, error) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}

	log.Printf("Request: %s %s", method, url)
	res, err := f.flinkClient.Do(req)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}

	log.Printf("Response Code: %v \nResponse Body: %s", res.StatusCode, string(body))
	return res.StatusCode, body, nil

}

type JobDetails struct {
	Jobs []struct {
		JobId   string `json:"jid"`
		Name    string `json:"name"`
		StartTs int64  `json:"start-time"`
	} `json:"jobs"`
}

type JarDetails struct {
	Files []struct {
		Id   string `json:"id"`
		Name string `json:"name"`
	} `json:"files"`
}

type FlinkFunctionsInterface interface {
	GetRunningFlinkJobs() (JobDetails, error)
	CancelJob(jobId string) error
	GetLatestFlinkJarId() (string, error)
	CreatelJob(jarId string, param string) error
}

type FlinkFunctions struct {
	config      *Config
	flinkClient FlinkClientInterface
	Metrics     *Metrics
}

func NewFlinkFunctions(config *Config, metrics *Metrics) *FlinkFunctions {
	return &FlinkFunctions{flinkClient: NewFlinkClient(), config: config, Metrics: metrics}
}

func (f *FlinkFunctions) GetRunningFlinkJobs() (JobDetails, error) {
	var jobDetails JobDetails
	code, body, err := f.flinkClient.MakeHttpCall(http.MethodGet, f.config.FlinkConfig.FlinkJobsUrl)
	if err != nil {
		return jobDetails, err
	} else if code != http.StatusOK || strings.Contains(string(body), "error") {
		return jobDetails, errors.New(string(body))
	}

	err = json.Unmarshal(body, &jobDetails)
	return jobDetails, err
}

func (f *FlinkFunctions) CreatelJob(jarId string, param string) error {
	flinkJarUrl := f.ProduceJobCreationUrl(jarId)

	log.Printf("Sending to Flink – Endpoint: %s\nUrl Params: %s", flinkJarUrl, param)

	data := url.Values{
		"program-args": {param},
	}
	code, body, err := f.flinkClient.MakeHttpCall(http.MethodPost, flinkJarUrl+"?"+data.Encode())
	if err != nil {
		return err
	}
	bodyStr := string(body)

	if code != http.StatusOK || strings.Contains(bodyStr, "error") {
		return errors.New("Received error while submitting flink job")
	}

	return nil
}

func (f *FlinkFunctions) CancelJob(jobId string) error {
	log.Printf("Cancelling Flink job: %s", jobId)
	// returns 200 even if job is not present
	code, body, err := f.flinkClient.MakeHttpCall(http.MethodDelete, f.ProduceJobCancelUrl(jobId))
	if err != nil {
		return err
	} else if code != http.StatusOK || strings.Contains(string(body), "error") {
		return errors.New(string(body))
	}

	return nil
}

func (f *FlinkFunctions) GetLatestFlinkJarId() (string, error) {
	jarId := ""
	code, body, err := f.flinkClient.MakeHttpCall(http.MethodGet, f.config.FlinkConfig.FlinkJarsUrl)
	if err != nil {
		return jarId, err
	} else if code != http.StatusOK || strings.Contains(string(body), "error") {
		return jarId, errors.New(string(body))
	}

	var jarDetails JarDetails
	err = json.Unmarshal(body, &jarDetails)
	if err != nil {
		return jarId, err
	}

	if len(jarDetails.Files) > 0 {
		for _, jar := range jarDetails.Files {
			if strings.Contains(jar.Name, "flink-line-protocol-downsampler") {
				jarId = jar.Id
				break
			}
		}
	}

	if jarId == "" {
		return jarId, errors.New("Jar id not found, response: " + string(body))
	}
	return jarId, nil
}

func (f *FlinkFunctions) ProduceJobCancelUrl(jobId string) string {
	return fmt.Sprintf("%s/%s/%s", f.config.FlinkConfig.FlinkJobDeleteUrl, jobId, "cancel")
}

func (f *FlinkFunctions) ProduceJobCreationUrl(jarId string) string {
	return fmt.Sprintf("%s%s/%s", f.config.FlinkConfig.FlinkJarsUrl, jarId, "run")
}
