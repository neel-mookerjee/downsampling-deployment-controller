package main

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Environment       string
	Namespace         string
	DbTablePrefix     string
	ExpireAfterMinute float64
	Mode              string //local/in-cluster
	DeploymentConfig  *DeploymentConfig
	MetricsConfig     *MetricsConfig
	KafkaConfig       *KafkaConfig
	FlinkConfig       *FlinkConfig
}
type DeploymentConfig struct {
	AwsRole string
	Image   string
}

type MetricsConfig struct {
	Host     string
	Database string
	Username string
	Password string
}

type KafkaConfig struct {
	Source string
	Sink   string
}

type FlinkConfig struct {
	FlinkJarsUrl      string
	FlinkJobsUrl      string
	FlinkJobDeleteUrl string
}

func NewConfig(mode string) (*Config, error) {
	ExpireAfterMinute, err := strconv.ParseFloat(os.Getenv("EXPIRE_AFTER_MINUTE"), 64)
	if err != nil {
		return nil, err
	}

	c := &Config{
		os.Getenv("ENVIRONMENT"),
		os.Getenv("NAMESPACE"),
		os.Getenv("DB_TABLE_PREFIX"),
		ExpireAfterMinute,
		mode,
		&DeploymentConfig{
			os.Getenv("AWS_ROLE"),
			os.Getenv("POD_IMAGE"),
		},
		&MetricsConfig{
			Host:     os.Getenv("METRICS_HOST"),
			Database: os.Getenv("METRICS_DATABASE"),
			Username: os.Getenv("METRICS_USERNAME"),
			Password: os.Getenv("METRICS_PASSWORD"),
		},
		&KafkaConfig{
			Source: os.Getenv("SOURCE_CLUSTER"),
			Sink:   os.Getenv("SINK_CLUSTER"),
		},
		&FlinkConfig{
			os.Getenv("FLINK_JARS_URL"),
			os.Getenv("FLINK_JOBS_URL"),
			os.Getenv("FLINK_JOB_DELETE_URL"),
		},
	}

	return c, nil
}

func (c *Config) GetSourceKafkaTopic(query DownsamplingObject) string {
	return strings.ToLower(query.Db) + "-influx-metrics"
}

func (c *Config) GetSinkKafkaTopic(query DownsamplingObject) string {
	return strings.ToLower(query.Db) + "-downsampling-influx-metrics"
}
