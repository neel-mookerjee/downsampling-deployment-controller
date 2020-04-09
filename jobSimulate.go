package main

import (
	"errors"
	log "github.com/sirupsen/logrus"
)

const SIMULATION_STACK_PREFIX = "downsamplr-preview"

type DeployPreviewJob struct {
	flinkJobHandler     FlinkJobHandlerInterface
	k8DeploymentHandler DeploymentHandlerInterface
	k8ServiceHandler    ServiceHandlerInterface
	k8IngressHandler    IngressHandlerInterface
	itemHandler         DownsamplingItemHandlerInterface
	config              *Config
	metrics             *Metrics
	kafkaClient         KafkaClientInterface
}

func NewDeployPreviewJob(config *Config, metrics *Metrics) (*DeployPreviewJob, error) {
	k8client, err := NewK8Client(config)
	if err != nil {
		return nil, err
	}
	k8DeploymentHandler, err := NewDeploymentHandler(k8client, config, metrics)
	if err != nil {
		return nil, err
	}
	k8ServiceHandler, err := NewServiceHandler(k8client, config, metrics)
	if err != nil {
		return nil, err
	}
	k8IngressHandler, err := NewIngressHandler(k8client, config, metrics)
	if err != nil {
		return nil, err
	}
	previewHandler, err := NewDownsamplingItemHandler(config, metrics)
	if err != nil {
		return nil, err
	}
	flinkJobHandler := NewFlinkJobHandler(config, metrics)
	if err != nil {
		return nil, err
	}

	kafkaClient, err := NewKafkaClient(config)
	if err != nil {
		return nil, err
	}

	return &DeployPreviewJob{flinkJobHandler, k8DeploymentHandler, k8ServiceHandler, k8IngressHandler,
		previewHandler, config, metrics, kafkaClient}, nil
}

func (d *DeployPreviewJob) Execute(params PARAM) error {
	if params.queryId == "" {
		return errors.New("queryId not received, erroring out")
	}

	query, err := d.itemHandler.GetDownsamplingItem(params.queryId)
	if err != nil {
		return err
	}

	if query.QueryId == "" {
		log.Println("No item found. Exiting...")
		return nil
	}

	log.Printf("query object: %v", query)
	if query.QueryState != "PREVIEW_PENDING" {
		log.Printf("Query %s must have status as PREVIEW_PENDING, found %s", query.QueryId, query.QueryState)
		return nil
	}

	err = d.k8DeploymentHandler.CreateDeployment(params)
	if err != nil {
		return err
	}

	err = d.k8ServiceHandler.CreateService(params)
	if err != nil {
		return err
	}

	influxdbIngressUrl, grafanaIngressUrl, err := d.k8IngressHandler.CreateIngress(params)
	if err != nil {
		return err
	}

	influx, err := NewInfluxdb(influxdbIngressUrl, "admin", "admin")
	if err != nil {
		return err
	}

	err = influx.CreateDatabaseAndRp(query.Db, "downsample")
	if err != nil {
		return err
	}

	grafana, err := NewGrafanaApiClient(influxdbIngressUrl, grafanaIngressUrl, "admin", "admin")
	if err != nil {
		return err
	}

	err = grafana.CreateDatasource(query)
	if err != nil {
		return err
	}

	err = grafana.CreateDashboard(query)
	if err != nil {
		return err
	}

	offsets, err := d.kafkaClient.GetDesiredOffsets(d.config.GetSourceKafkaTopic(query))
	if err != nil {
		return err
	}
	err = d.flinkJobHandler.DeployFlinkJobForSimulation(query, influxdbIngressUrl, offsets)
	if err != nil {
		return err
	}

	err = d.itemHandler.DeployDownsamplingPendingSimulationItem(query.QueryId)
	if err != nil {
		return err
	}

	return nil
}
