package main

import (
	"errors"
	log "github.com/sirupsen/logrus"
)

type DeployDownsamplingJob struct {
	flinkJobHandler FlinkJobHandlerInterface
	itemHandler     DownsamplingItemHandlerInterface
	config          *Config
	metrics         *Metrics
}

func NewDeployDownsamplingJob(config *Config, metrics *Metrics) (*DeployDownsamplingJob, error) {
	itemHanlder, err := NewDownsamplingItemHandler(config, metrics)
	if err != nil {
		return nil, err
	}

	flinkJobHandler := NewFlinkJobHandler(config, metrics)

	return &DeployDownsamplingJob{flinkJobHandler, itemHanlder, config, metrics}, nil
}

func (d *DeployDownsamplingJob) Execute(params PARAM) error {
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
	if query.QueryState != "PENDING" {
		log.Printf("Query %s must have status as PENDING, found %s", query.QueryId, query.QueryState)
		return nil
	}

	log.Println("Deploying Flink job...")
	err = d.flinkJobHandler.DeployFlinkJob(query)
	if err != nil {
		return err
	}

	log.Println("Updating status as deployed...")
	err = d.itemHandler.DeployDownsamplingItem(query)
	if err != nil {
		return err
	}

	return nil
}
