package main

import (
	log "github.com/sirupsen/logrus"
)

type DeletePreviewJob struct {
	flinkJobHandler     FlinkJobHandlerInterface
	k8DeploymentHandler DeploymentHandlerInterface
	k8ServiceHandler    ServiceHandlerInterface
	k8IngressHandler    IngressHandlerInterface
	itemHandler         DownsamplingItemHandlerInterface
	config              *Config
	Metrics             *Metrics
}

func NewDeletePreviewJob(config *Config, metrics *Metrics) (*DeletePreviewJob, error) {
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

	return &DeletePreviewJob{flinkJobHandler, k8DeploymentHandler, k8ServiceHandler, k8IngressHandler, previewHandler, config, metrics}, nil
}

func (d *DeletePreviewJob) Execute(params PARAM) error {
	log.Println("Cancelling old deployments...")
	countDeps, err := d.k8DeploymentHandler.HandleOldDeployments()
	if err != nil {
		return err
	}
	log.Printf("%d k8 deployments were cancelled.", countDeps)

	log.Println("Cancelling old services...")
	err = d.k8ServiceHandler.HandleOldServices()
	if err != nil {
		return err
	}

	log.Println("Cancelling old ingresses...")
	countIngress, err := d.k8IngressHandler.HandleOldIngresses()
	if err != nil {
		return err
	}
	log.Printf("%d k8 ingresses were cancelled.", countIngress)

	log.Println("Cancelling old Flink jobs...")
	countFlinkJobs, err := d.flinkJobHandler.HandleOldFlinkJobs()
	if err != nil {
		return err
	}
	log.Printf("%d Flink jobs were cancelled.", countFlinkJobs)

	log.Println("Deleting expired simulation queries...")
	countQueries, err := d.itemHandler.HandleExpiredSimulations()
	if err != nil {
		return err
	}
	log.Printf("%d queries were deleted.", countQueries)

	return nil
}
