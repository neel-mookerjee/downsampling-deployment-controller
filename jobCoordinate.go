package main

import (
	"errors"
	log "github.com/sirupsen/logrus"
)

type CoordinatorJob struct {
	k8JobHandler JobHandlerInterface
	itemHandler  DownsamplingItemHandlerInterface
	config       *Config
	Metrics      *Metrics
}

func NewCoordinatorJob(config *Config, metrics *Metrics) (*CoordinatorJob, error) {
	k8client, err := NewK8Client(config)
	if err != nil {
		return nil, err
	}
	k8JobHandler, err := NewJobHandler(k8client, config, metrics)
	if err != nil {
		return nil, err
	}
	itemHandler, err := NewDownsamplingItemHandler(config, metrics)
	if err != nil {
		return nil, err
	}

	return &CoordinatorJob{k8JobHandler, itemHandler, config, metrics}, nil
}

func (d *CoordinatorJob) Execute(params PARAM) error {
	dsList, err := d.itemHandler.GetAllDownsamplingItems()
	if err != nil {
		return err
	}

	log.Printf("Received pending items: %v", dsList)
	operation := ""
	jobName := ""
	const CONTROLLER_NAME_PREFIX = "downsample-controller-"
	for _, query := range dsList {
		log.Printf("Trying query object: %v", query)
		switch query.QueryState {
		case "PREVIEW_PENDING":
			jobName = CONTROLLER_NAME_PREFIX + d.config.Environment + "-" + params.OPERATION_SIMULATE + "-" + query.QueryId
			operation = params.OPERATION_SIMULATE
		case "PENDING":
			jobName = CONTROLLER_NAME_PREFIX + d.config.Environment + "-" + params.OPERATION_DEPLOY + "-" + query.QueryId
			operation = params.OPERATION_DEPLOY
		case "DELETED":
			jobName = CONTROLLER_NAME_PREFIX + d.config.Environment + "-" + params.OPERATION_DELETE + "-" + query.QueryId
			operation = params.OPERATION_DELETE
		default:
			err = errors.New("Invalid option, must not have reached here!")
		}
		if err != nil {
			return err
		}

		config, err := d.k8JobHandler.GetConfigForJob(operation, jobName, query.QueryId, params)
		if err != nil {
			return err
		}

		log.Printf("Creating job %s with config: %v", jobName, config)
		ind, err := d.k8JobHandler.CreateJob(jobName, config)
		if err != nil {
			return err
		}
		log.Printf("Job created: %t", ind)

		log.Println("Job creation process complete.")
	}

	return nil
}
