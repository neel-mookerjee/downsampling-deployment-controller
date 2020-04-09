package main

import (
	"errors"
	log "github.com/sirupsen/logrus"
)

type DeleteDownsamplingJob struct {
	flinkJobHandler FlinkJobHandlerInterface
	itemHandler     DownsamplingItemHandlerInterface
	config          *Config
	Metrics         *Metrics
}

func NewDeleteDownsamplingJob(config *Config, metrics *Metrics) (*DeleteDownsamplingJob, error) {
	itemHanlder, err := NewDownsamplingItemHandler(config, metrics)
	if err != nil {
		return nil, err
	}

	flinkJobHandler := NewFlinkJobHandler(config, metrics)
	return &DeleteDownsamplingJob{flinkJobHandler, itemHanlder, config, metrics}, nil
}

func (d *DeleteDownsamplingJob) Execute(params PARAM) error {
	if params.queryId == "" {
		return errors.New("queryId not received, erroring out...")
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
	if query.QueryState != "DELETED" {
		log.Printf("Query %s must have status as DELETE, found %s", query.QueryId, query.QueryState)
		return nil
	}

	log.Println("Cancelling Flink job...")
	count, err := d.flinkJobHandler.CancelFlinkJob(query.QueryId, FLINK_ALL)
	if err != nil {
		return err
	}
	log.Printf("%d jobs were cancel attempted.", count)

	log.Println("Deleting from database...")
	err = d.itemHandler.DeleteDownsamplingItem(query)
	if err != nil {
		return err
	}

	return nil
}
