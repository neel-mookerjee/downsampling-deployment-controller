package main

import (
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	//"gopkg.in/yaml.v2"
	"encoding/json"
	errors2 "errors"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes/typed/batch/v1"
)

type JobHandlerInterface interface {
	GetAllDeploymentJobs() (*batchv1.JobList, error)
	CheckIfJobExists(jobName string) (bool, error)
	GetJob(jobName string) (*batchv1.Job, error)
	CreateJob(jobName string, config *DefaultFiller) (bool, error)
	GetConfigForJob(operation string, jobName string, queryId string, params PARAM) (*DefaultFiller, error)
}

type JobHandler struct {
	jobClient      v1.JobInterface
	templateParser TemplateParserInterface
	config         *Config
	Metrics        *Metrics
}

func (j *JobHandler) CheckIfJobExists(jobName string) (bool, error) {
	job, err := j.GetJob(jobName)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Println("Job with the same name doesn't exist.")
			return false, nil
		} else {
			return false, err
		}
	}
	if job.Name != "" {
		log.Printf("Job is already present: %v", job)
		return true, nil
	}

	return false, nil
}

func NewJobHandler(k8client K8ClientInterface, config *Config, metrics *Metrics) (*JobHandler, error) {
	jobClient := k8client.GetClientSet().BatchV1().Jobs(config.Namespace)
	return &JobHandler{jobClient, NewTemplateParser(), config, metrics}, nil
}

func (j *JobHandler) GetAllDeploymentJobs() (*batchv1.JobList, error) {
	jobs, err := j.jobClient.List(metav1.ListOptions{LabelSelector: "purpose=" + SIMULATION_STACK_PREFIX})
	return jobs, err
}

func (j *JobHandler) GetJob(jobName string) (*batchv1.Job, error) {
	job, err := j.jobClient.Get(jobName, metav1.GetOptions{})
	return job, err
}

func (j *JobHandler) CreateJob(jobName string, config *DefaultFiller) (bool, error) {
	jobCreated := false
	log.Printf("Checking if a job already exists with name %s", jobName)
	present, err := j.CheckIfJobExists(jobName)
	if err != nil {
		return jobCreated, err
	}

	if present {
		log.Println("Skipping...")
		return jobCreated, nil
	}

	str, err := j.templateParser.LoadTemplate("templates/controller-job.json", config)
	if err != nil {
		return jobCreated, err
	}

	log.Printf("Job spec: %s", str)
	var spec *batchv1.Job
	err = json.Unmarshal([]byte(str), &spec)
	if err != nil {
		return jobCreated, err
	}

	// Create Job
	log.Println("Creating job...")
	result, err := j.jobClient.Create(spec)
	log.Printf("Output: %v", result)
	if err != nil {
		return jobCreated, err
	}
	jobCreated = true

	return jobCreated, nil
}

func (j *JobHandler) GetConfigForJob(operation string, jobName string, queryId string, params PARAM) (*DefaultFiller, error) {
	var f *DefaultFiller
	switch operation {
	case params.OPERATION_DEPLOY:
		f = j.getFiller(jobName, "./main", "\"in-cluster\",\"deploy\",\""+queryId+"\"")
	case params.OPERATION_DELETE:
		f = j.getFiller(jobName, "./main", "\"in-cluster\",\"delete\",\""+queryId+"\"")
	case params.OPERATION_SIMULATE:
		f = j.getFiller(jobName, "./main", "\"in-cluster\",\"simulate\",\""+queryId+"\"")
	default:
		return f, errors2.New("wrong case option " + operation)
	}
	//f = j.getFiller(jobName, "sleep", "\"900\"")
	return f, nil
}

func (j *JobHandler) getFiller(stackName string, Command string, Argument string) *DefaultFiller {
	return &DefaultFiller{stackName, Command, Argument, *j.config}
}
