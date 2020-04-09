package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"sort"
	"strings"
	"time"
)

const FLINK_ALL = "all"
const FLINKL_SIMULATION = "simulation"
const FLINK_ACTUAL = "actual"

type DownsamplingObjectForFlink struct {
	Nickname          string `json:"nickname"`
	QueryId           string `json:"queryId"`
	QueryHash         string `json:"queryHash"`
	CreatedAt         string `json:"createdAt"`
	UpdatedAt         string `json:"updatedAt"`
	Db                string `json:"db"`
	Rp                string `json:"rp"`
	Measurement       string `json:"measurement"`
	TargetRp          string `json:"targetRp"`
	TargetMeasurement string `json:"targetMeasurement"`
	PreviewExpiresAt  string `json:"previewExpiresAt"`
	QueryState        string `json:"queryState"`
	Fields            []struct {
		Alias    string `json:"alias"`
		Field    string `json:"field"`
		Function string `json:"func"`
	} `json:"listFieldFunc"`
	Tags                   []string `json:"tags"`
	Interval               int      `json:"interval"`
	IsHistoricDownsampling bool     `json:"isHistoricDownsampling"`
}

type FlinkJobHandlerInterface interface {
	DeployFlinkJob(query DownsamplingObject) error
	DeployFlinkJobForSimulation(query DownsamplingObject, influxdbBaseUrl string, offsets KafkaPartitionOffsets) error
	CancelFlinkJob(queryId string, mode string) (int, error)
	HandleOldFlinkJobs() (int, error)
}

type FlinkJobHandler struct {
	config  *Config
	flink   FlinkFunctionsInterface
	Metrics *Metrics
}

func NewFlinkJobHandler(config *Config, metrics *Metrics) *FlinkJobHandler {
	return &FlinkJobHandler{flink: NewFlinkFunctions(config, metrics), config: config, Metrics: metrics}
}

func (f *FlinkJobHandler) DeployFlinkJobForSimulation(query DownsamplingObject, influxdbBaseUrl string, offsets KafkaPartitionOffsets) error {
	ind, err := f.CheckForExistingJob(query.QueryId, FLINKL_SIMULATION)
	if err != nil {
		return err
	}
	if ind {
		log.Println("Already a Flink job is running for this. Skipping...")
		return nil
	}

	log.Println("No existing Flink job found.\nGetting latest flink jar uri...")
	jarId, err := f.flink.GetLatestFlinkJarId()
	if err != nil {
		return err
	}

	log.Println("Submitting Flink job for simulation...")
	flinkUrlParamStr, err := f.CreateSimulationJobConfig(query, influxdbBaseUrl, offsets)
	if err != nil {
		return err
	}

	err = f.flink.CreatelJob(jarId, flinkUrlParamStr)
	return err
}

func (f *FlinkJobHandler) CreateSimulationJobConfig(query DownsamplingObject, influxdbBaseUrl string, offsets KafkaPartitionOffsets) (string, error) {
	queryStr, err := json.Marshal(f.ModifyQueryObject(query))
	if err != nil {
		return "", err
	}

	var arr []string
	for _, p := range offsets {
		arr = append(arr, fmt.Sprintf("%d:%d", p.partitionId, p.DesiredOffset))
	}
	offsetsStr := strings.Join(arr, ",")

	influxDbUrl := influxdbBaseUrl + ":80/write?db=" + query.Db + "&rp=downsample&precision=us"
	flinkUrlParamStr := fmt.Sprintf("--jobConfig %s --sourceTopic %s --sourceCluster %s --consumerGroupId %s --influxdbUrl %s --previewMode true --jobName %s --topicOffsets %s",
		base64.StdEncoding.EncodeToString([]byte(queryStr)),
		f.config.GetSourceKafkaTopic(query),
		f.config.KafkaConfig.Source,
		"downsample-simulation-"+query.QueryId,
		base64.StdEncoding.EncodeToString([]byte(influxDbUrl)),
		"simulate"+":"+query.QueryId+":"+query.Db+":"+query.Measurement+":"+fmt.Sprintf("%d", query.Interval),
		offsetsStr,
	)
	return flinkUrlParamStr, nil
}

func (f *FlinkJobHandler) DeployFlinkJob(query DownsamplingObject) error {
	ind, err := f.CheckForExistingJob(query.QueryId, FLINK_ACTUAL)
	if err != nil {
		return err
	}
	if ind {
		log.Println("Already a Flink job is runnung for this. Skippping...")
		return nil
	}

	log.Println("No existing Flink job found.\nGetting latest flink jar uri...")
	jarId, err := f.flink.GetLatestFlinkJarId()
	if err != nil {
		return err
	}

	log.Println("Submitting Flink job for actual downsampling...")
	flinkUrlParamStr, err := f.CreateDownsampleJobConfig(query)
	if err != nil {
		return err
	}

	err = f.flink.CreatelJob(jarId, flinkUrlParamStr)
	return err
}

func (f *FlinkJobHandler) CreateDownsampleJobConfig(query DownsamplingObject) (string, error) {
	queryStr, err := json.Marshal(f.ModifyQueryObject(query))
	if err != nil {
		return "", err
	}

	flinkUrlParamStr := fmt.Sprintf("--jobConfig %s --sourceTopic %s --sourceCluster %s --consumerGroupId %s --sinkCluster %s --sinkTopic %s --jobName %s",
		base64.StdEncoding.EncodeToString([]byte(queryStr)),
		f.config.GetSourceKafkaTopic(query),
		f.config.KafkaConfig.Source,
		"downsample-"+query.QueryId,
		f.config.KafkaConfig.Sink,
		f.config.GetSinkKafkaTopic(query),
		"downsample"+":"+query.QueryId+":"+query.Db+":"+query.Measurement+":"+fmt.Sprintf("%d", query.Interval),
	)
	return flinkUrlParamStr, nil
}

func (f *FlinkJobHandler) ModifyQueryObject(q DownsamplingObject) DownsamplingObjectForFlink {
	queryNew := DownsamplingObjectForFlink{q.Nickname, q.QueryId, q.QueryHash,
		q.CreatedAt, q.UpdatedAt, q.Db, q.Rp, q.Measurement,
		q.TargetRp, q.TargetMeasurement, q.PreviewExpiresAt,
		q.QueryState, nil, q.Tags, q.Interval, q.IsHistoricDownsampling}

	m := make(map[string]string)
	for _, field := range q.Fields {
		m[field.Field] = field.Field
	}
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		queryNew.Fields = append(queryNew.Fields, struct {
			Alias    string `json:"alias"`
			Field    string `json:"field"`
			Function string `json:"func"`
		}{Alias: "metricsAGG_" + k, Field: k, Function: "metricsAGG"})

	}

	return queryNew
}

func (f *FlinkJobHandler) CheckForExistingJob(queryId string, mode string) (bool, error) {
	log.Printf("Flink Job check mode: %s", mode)
	prefix := ""
	switch mode {
	case FLINK_ALL:
		prefix = ""
	case FLINK_ACTUAL:
		prefix = "downsample"
	case FLINKL_SIMULATION:
		prefix = "simulate"
	default:
		return false, errors.New("Wrong mode, aborting...")
	}
	log.Println("Getting running flink jobs...")
	jobDetails, err := f.flink.GetRunningFlinkJobs()
	if err != nil {
		return false, err
	}

	log.Println("Checking if a Flink job is already running for the query id...")
	for _, job := range jobDetails.Jobs {
		if strings.HasPrefix(job.Name, prefix) && strings.Contains(job.Name, queryId) {
			log.Printf("Found marching Flink job: %s", job.JobId)
			return true, nil
		}
	}

	return false, nil
}

func (f *FlinkJobHandler) CancelFlinkJob(queryId string, mode string) (int, error) {
	log.Printf("Flink Job Cancel mode: %s", mode)
	prefix := ""
	switch mode {
	case FLINK_ALL:
		prefix = ""
	case FLINK_ACTUAL:
		prefix = "downsample"
	case FLINKL_SIMULATION:
		prefix = "simulate"
	default:
		return 0, errors.New("Wrong cancel mode, aborting...")
	}
	log.Println("Getting running flink jobs...")
	jobDetails, err := f.flink.GetRunningFlinkJobs()
	if err != nil {
		return 0, err
	}

	count := 0
	log.Println("Checking if a Flink job is already running for the query id...")
	for _, job := range jobDetails.Jobs {
		if strings.HasPrefix(job.Name, prefix) && strings.Contains(job.Name, queryId) {
			log.Printf("Cancelling Flink job: %s", job.JobId)
			err = f.flink.CancelJob(job.JobId)
			if err != nil {
				return count, err
			}
			count++
		}
	}

	return count, nil
}

func (f *FlinkJobHandler) HandleOldFlinkJobs() (int, error) {
	log.Println("Getting running flink jobs...")
	jobDetails, err := f.flink.GetRunningFlinkJobs()
	if err != nil {
		return 0, err
	}

	count := 0
	for _, job := range jobDetails.Jobs {
		ts := job.StartTs / 1000
		now := time.Now().Unix()
		if strings.HasPrefix(job.Name, "simulate:") && float64(now-ts) > f.config.ExpireAfterMinute*60 {
			err = f.flink.CancelJob(job.JobId)
			if err != nil {
				return count, err
			}
			count++
		}
	}

	return count, nil
}
