package main

import (
	"encoding/json"
	"k8s.io/api/apps/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientv1beta1 "k8s.io/client-go/kubernetes/typed/apps/v1beta1"
	//"gopkg.in/yaml.v2"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/errors"
	"time"
)

type DeploymentHandlerInterface interface {
	HandleOldDeployments() (int, error)
	CreateDeployment(params PARAM) error
}

type DeploymentHandler struct {
	deploymentClient clientv1beta1.DeploymentInterface
	templateParser   TemplateParserInterface
	config           *Config
	Metrics          *Metrics
}

func NewDeploymentHandler(k8client K8ClientInterface, config *Config, metrics *Metrics) (*DeploymentHandler, error) {
	deploymentsClient := k8client.GetClientSet().AppsV1beta1().Deployments(config.Namespace)
	return &DeploymentHandler{deploymentsClient, NewTemplateParser(), config, metrics}, nil
}

func (d *DeploymentHandler) CreateDeployment(params PARAM) error {
	yamlStr, err := d.templateParser.LoadTemplate("templates/simulation-deployment.json", &DefaultFiller{StackName: SIMULATION_STACK_PREFIX + "-" + params.queryId})
	if err != nil {
		return err
	}

	log.Printf("Deployment spec: %s", yamlStr)
	var spec *v1beta1.Deployment
	err = json.Unmarshal([]byte(yamlStr), &spec)
	if err != nil {
		return err
	}

	// Create Deployment
	created, err := d.CreateK8Deployment(spec)
	log.Printf("Deployment created: %t", created)
	return err
}

func (d *DeploymentHandler) CreateK8Deployment(spec *v1beta1.Deployment) (bool, error) {
	created := false
	// Create Deployment
	log.Printf("Creating deployment %s", spec.Name)
	result, err := d.deploymentClient.Create(spec)
	log.Printf("Response: %v", result)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			log.Println("Deployment already exists. Skipping...")
			return created, nil
		} else {
			return created, err
		}
	}

	created = true
	return created, nil
}

func (d *DeploymentHandler) HandleOldDeployments() (int, error) {
	count := 0
	log.Println("Getting old k8 deployments...")
	deps, err := d.deploymentClient.List(metav1.ListOptions{LabelSelector: "purpose=" + SIMULATION_STACK_PREFIX})
	if err != nil {
		return count, err
	}

	log.Printf("Got k8 deployments with tag purpose=%s: %v", SIMULATION_STACK_PREFIX, deps)
	deletePolicy := metav1.DeletePropagationForeground
	for _, item := range deps.Items {
		ts := item.GetCreationTimestamp()
		now := time.Now()
		diff := now.Sub(ts.Time).Minutes()
		if diff > d.config.ExpireAfterMinute {
			log.Printf("Deleting k8 deployment: %s", item.Name)
			if err := d.deploymentClient.Delete(item.Name, &metav1.DeleteOptions{
				PropagationPolicy: &deletePolicy,
			}); err != nil {
				return count, err
			}
			d.Metrics.IncrementExpiredCount()
			count++
		}
	}

	return count, nil
}
