package main

import (
	"bytes"
	"encoding/json"
	"errors"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

type GrafanaClientInterface interface {
	MakeHttpCall(method string, path string, body interface{}, isJson bool) (int, []byte, error)
}

type GrafanaClient struct {
	grafanaClient http.Client
	User          string
	Password      string
}

func NewGrafanaClient(user string, password string) *GrafanaClient {
	httpClient := http.Client{
		Timeout: time.Second * 30,
	}
	return &GrafanaClient{grafanaClient: httpClient, User: user, Password: password}
}

func (f *GrafanaClient) MakeHttpCall(method string, url string, body interface{}, isJson bool) (int, []byte, error) {
	var data []byte
	var err error
	if body != nil {
		if isJson {
			data, err = json.Marshal(body)
		} else {
			data = []byte(body.(string))
		}
	}
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}

	req, err := http.NewRequest(method, url, bytes.NewBuffer(data))
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}
	req.SetBasicAuth(f.User, f.Password)
	req.Header.Add("Content-Type", "application/json")

	log.Printf("Request: %s %s %v", method, url, data)
	res, err := f.grafanaClient.Do(req)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}

	bodyOutput, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}

	log.Printf("Response Code : %v \nResponse Body: %s", res.StatusCode, string(bodyOutput))
	return res.StatusCode, bodyOutput, nil
}

type DataSource struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	URL       string `json:"url"`
	Access    string `json:"access"`
	Database  string `json:"database,omitempty"`
	IsDefault bool   `json:"isDefault"`
	BasicAuth bool   `json:"basicAuth"`
}

type DashboardTemplateFiller struct {
	Database    string
	Measurement string
	Tag         string
	Field       string
}

type GrafanaInterface interface {
	CreateDatasource(dsObject DownsamplingObject) error
	CreateDashboard(dsObject DownsamplingObject) error
}

type Grafana struct {
	templateParser TemplateParserInterface
	influxdbUrl    string
	grafanaBaseURL string
	grafanaClient  GrafanaClientInterface
}

func NewGrafanaApiClient(influxdbUrl string, baseUrl string, user string, password string) (*Grafana, error) {
	grafana := &Grafana{influxdbUrl: influxdbUrl, templateParser: NewTemplateParser(), grafanaBaseURL: baseUrl, grafanaClient: NewGrafanaClient(user, password)}
	grafana.checkUpStatus()
	return grafana, nil
}

func (g *Grafana) CreateDatasource(dsObject DownsamplingObject) error {
	log.Println("Creating grafana datasource...")
	ds := &DataSource{dsObject.Db, "influxdb", g.influxdbUrl, "proxy", dsObject.Db, true, false}
	code, body, err := g.grafanaClient.MakeHttpCall(http.MethodPost, g.grafanaBaseURL+"/api/datasources", ds, true)
	if err != nil || code != http.StatusOK || strings.Contains(string(body), "error") {
		if code == http.StatusConflict {
			log.Println("Datasource already exists, skipping...")
			return nil
		} else if err == nil {
			err = errors.New(string((body)))
		}
		return err
	}
	return nil
}

func (g *Grafana) CreateDashboard(dsObject DownsamplingObject) error {
	log.Println("Creating grafana dashboard...")
	filler := &DashboardTemplateFiller{dsObject.Db, dsObject.TargetMeasurement, dsObject.Tags[0], dsObject.Fields[0].Alias}
	str, err := g.templateParser.LoadTemplate("templates/dashboard.json", filler)
	if err != nil {
		return err
	}
	code, body, err := g.grafanaClient.MakeHttpCall(http.MethodPost, g.grafanaBaseURL+"/api/dashboards/db", str, false)
	if err != nil || code != http.StatusOK || strings.Contains(string(body), "error") {
		if code == http.StatusPreconditionFailed && strings.Contains(string(body), "name-exists") {
			log.Println("Dashboard already exists, skipping...")
			return nil
		} else if err == nil {
			err = errors.New(string((body)))
		}
		return err
	}
	return nil
}

func (g *Grafana) checkUpStatus() {
	for {
		log.Printf("Checking if Grafana is up at %s", g.grafanaBaseURL+"/api/admin/stats")
		code, body, err := g.grafanaClient.MakeHttpCall(http.MethodGet, g.grafanaBaseURL+"/api/admin/stats", nil, false)
		if err != nil || code != http.StatusOK || strings.Contains(string(body), "error") {
			log.Printf("Got Response: %v, %v, %v\nRetrying after 5s...", code, body, err)
			time.Sleep(5 * time.Second)
		} else {
			return
		}
	}
}
