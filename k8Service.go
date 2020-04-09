package main

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientv1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"time"
)

type ServiceHandlerInterface interface {
	HandleOldServices() error
	CreateService(params PARAM) error
}

type ServiceHandler struct {
	servicesClient clientv1core.ServiceInterface
	templateParser TemplateParserInterface
	config         *Config
	Metrics        *Metrics
}

func NewServiceHandler(k8client K8ClientInterface, config *Config, metrics *Metrics) (*ServiceHandler, error) {
	ServicesClient := k8client.GetClientSet().CoreV1().Services(config.Namespace)
	return &ServiceHandler{ServicesClient, NewTemplateParser(), config, metrics}, nil
}

func (s *ServiceHandler) CreateService(params PARAM) error {
	yamlStr, err := s.templateParser.LoadTemplate("templates/simulation-service.json", &DefaultFiller{StackName: SIMULATION_STACK_PREFIX + "-" + params.queryId})
	if err != nil {
		return err
	}

	log.Printf("Service spec: %s", yamlStr)
	var spec *v1.Service
	err = json.Unmarshal([]byte(yamlStr), &spec)
	if err != nil {
		return err
	}

	// Create service
	log.Printf("Creating service %s", spec.Name)
	result, err := s.servicesClient.Create(spec)
	log.Printf("Response: %v", result)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			log.Printf("Service already exists. Skipping...")
			return nil
		} else {
			return err
		}
	}

	return nil
}

func (h *ServiceHandler) HandleOldServices() error {
	log.Println("Getting old k8 services...")
	deps, err := h.servicesClient.List(metav1.ListOptions{LabelSelector: "purpose=" + SIMULATION_STACK_PREFIX})
	if err != nil {
		return err
	}

	log.Printf("Got k8 services with tag purpose=%s: %v", SIMULATION_STACK_PREFIX, deps)
	deletePolicy := metav1.DeletePropagationForeground
	for _, item := range deps.Items {
		ts := item.GetCreationTimestamp()
		now := time.Now()
		diff := now.Sub(ts.Time).Minutes()
		if diff > h.config.ExpireAfterMinute {
			log.Printf("Deleting k8 service: %s", item.Name)
			if err := h.servicesClient.Delete(item.Name, &metav1.DeleteOptions{
				PropagationPolicy: &deletePolicy,
			}); err != nil {
				return err
			}
		}
	}

	return nil
}
