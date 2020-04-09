package main

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

var PARAMS = GetParams()

type PARAM struct {
	mode                 string
	operation            string
	queryId              string
	MODE_LOCAL           string
	MODE_IN_CLUSTER      string
	OPERATION_COORDINATE string
	OPERATION_SIMULATE   string
	OPERATION_DEPLOY     string
	OPERATION_DELETE     string
	OPERATION_EXPIRE     string
}

func GetParams() *PARAM {
	var params PARAM
	for index, p := range os.Args {
		switch index {
		case 1:
			params.mode = p
		case 2:
			params.operation = p
		case 3:
			params.queryId = p
		}
	}
	params.MODE_IN_CLUSTER = "in-cluster"
	params.MODE_LOCAL = "local"
	params.OPERATION_COORDINATE = "coordinate"
	params.OPERATION_SIMULATE = "simulate"
	params.OPERATION_DEPLOY = "deploy"
	params.OPERATION_DELETE = "delete"
	params.OPERATION_EXPIRE = "expire"
	return &params
}

func main() {
	// set logger
	log.SetFormatter(&log.JSONFormatter{})

	log.Println("Starting controller...")

	var CONFIG *Config
	var METRICS *Metrics

	log.Printf("Received Params – mode: %s, operation: %s, queryId: %s", PARAMS.mode, PARAMS.operation, PARAMS.queryId)
	Error(ValidateParams())

	CONFIG, err1 := NewConfig(PARAMS.mode)
	Error(err1)

	METRICS = NewMetrics(*CONFIG, *PARAMS)
	METRICS.SetRunning()
	started := time.Now().Unix()

	job, err := DefineJob(CONFIG, METRICS)
	handleError(err, METRICS)

	log.Println("Executing " + PARAMS.operation + " job...")
	err = job.Execute(*PARAMS)
	METRICS.SetDuration(started)
	handleError(err, METRICS)

	log.Println("Controller executed successfully")
	time.Sleep(7 * time.Second)
	log.Println("Exiting...")
	os.Exit(0)
}

func ValidateParams() error {
	if PARAMS.mode != PARAMS.MODE_LOCAL && PARAMS.mode != PARAMS.MODE_IN_CLUSTER {
		return errors.New(fmt.Sprintf("invalid mode, must be - %s, %s", PARAMS.MODE_LOCAL, PARAMS.MODE_IN_CLUSTER))
	}
	if PARAMS.operation != PARAMS.OPERATION_COORDINATE && PARAMS.operation != PARAMS.OPERATION_SIMULATE && PARAMS.operation != PARAMS.OPERATION_DEPLOY && PARAMS.operation != PARAMS.OPERATION_DELETE && PARAMS.operation != PARAMS.OPERATION_EXPIRE {
		return errors.New(fmt.Sprintf("invalid operation, must be - $s/%s/%s/%s/%s", PARAMS.OPERATION_COORDINATE, PARAMS.OPERATION_SIMULATE, PARAMS.OPERATION_DEPLOY, PARAMS.OPERATION_DELETE, PARAMS.OPERATION_EXPIRE))
	}
	return nil
}

func DefineJob(CONFIG *Config, METRICS *Metrics) (DownsampleJobInterface, error) {
	var job DownsampleJobInterface
	var err error
	switch PARAMS.operation {
	case PARAMS.OPERATION_COORDINATE:
		job, err = NewCoordinatorJob(CONFIG, METRICS)
	case PARAMS.OPERATION_DEPLOY:
		job, err = NewDeployDownsamplingJob(CONFIG, METRICS)
	case PARAMS.OPERATION_DELETE:
		job, err = NewDeleteDownsamplingJob(CONFIG, METRICS)
	case PARAMS.OPERATION_SIMULATE:
		job, err = NewDeployPreviewJob(CONFIG, METRICS)
	case PARAMS.OPERATION_EXPIRE:
		job, err = NewDeletePreviewJob(CONFIG, METRICS)
	default:
		err = errors.New("Invalid option " + PARAMS.operation + ", must not reach here!")
	}
	return job, err
}

func handleError(err error, metrics *Metrics) {
	if err != nil {
		metrics.ReportError()
		Error(err)
	}
}

func Error(err error) {
	if err != nil {
		log.Printf("An error has occurred. Details as follows – %v", err)
		time.Sleep(7 * time.Second)
		log.Println("Exiting...")
		os.Exit(1)
	}
}
