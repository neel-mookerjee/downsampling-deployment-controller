package main

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"k8s.io/api/extensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientv1betaextn "k8s.io/client-go/kubernetes/typed/extensions/v1beta1"
	"time"
)

type IngressHandlerInterface interface {
	HandleOldIngresses() (int, error)
	CreateIngress(params PARAM) (string, string, error)
}

type IngressHandler struct {
	ingressesClient clientv1betaextn.IngressInterface
	templateParser  TemplateParserInterface
	config          *Config
	Metrics         *Metrics
}

func NewIngressHandler(k8client K8ClientInterface, config *Config, metrics *Metrics) (*IngressHandler, error) {
	ingressesClient := k8client.GetClientSet().ExtensionsV1beta1().Ingresses(config.Namespace)
	return &IngressHandler{ingressesClient, NewTemplateParser(), config, metrics}, nil
}

func (s *IngressHandler) CreateIngress(params PARAM) (string, string, error) {
	influxdbIngressUrl := "http://" + SIMULATION_STACK_PREFIX + "-" + params.queryId + ".influx.platform.r53.arghanil.net"
	grafanaIngressUrl := "http://" + SIMULATION_STACK_PREFIX + "-" + params.queryId + ".grafana.platform.r53.arghanil.net"
	yamlStr, err := s.templateParser.LoadTemplate("templates/simulation-ingress.json", &DefaultFiller{StackName: SIMULATION_STACK_PREFIX + "-" + params.queryId})
	if err != nil {
		return influxdbIngressUrl, grafanaIngressUrl, err
	}

	log.Printf("Ingress spec: %s", yamlStr)
	var spec *v1beta1.Ingress
	err = json.Unmarshal([]byte(yamlStr), &spec)
	if err != nil {
		return influxdbIngressUrl, grafanaIngressUrl, err
	}

	// Create ingress
	ingressCreated, err := s.CreateK8Ingress(spec)
	log.Printf("Ingress created: %t", ingressCreated)
	return influxdbIngressUrl, grafanaIngressUrl, err
}

func (s *IngressHandler) CreateK8Ingress(spec *v1beta1.Ingress) (bool, error) {
	ingressCreated := false
	// Create ingress
	log.Printf("Creating ingress %s", spec.Name)
	result, err := s.ingressesClient.Create(spec)
	log.Printf("Response: %v", result)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			log.Println("Ingress already exists. Skipping...")
			return ingressCreated, nil
		} else {
			return ingressCreated, err
		}
	}

	ingressCreated = true
	return ingressCreated, nil
}

func (h *IngressHandler) HandleOldIngresses() (int, error) {
	count := 0
	log.Println("Getting old k8 ingresses...")
	deps, err := h.ingressesClient.List(metav1.ListOptions{LabelSelector: "purpose=" + SIMULATION_STACK_PREFIX})
	if err != nil {
		return count, err
	}

	log.Printf("Got k8 ingresses with tag purpose=%s: %v", SIMULATION_STACK_PREFIX, deps)
	deletePolicy := metav1.DeletePropagationForeground
	for _, item := range deps.Items {
		ts := item.GetCreationTimestamp()
		now := time.Now()
		diff := now.Sub(ts.Time).Minutes()
		if diff > h.config.ExpireAfterMinute {
			log.Printf("Deleting k8 ingress: %s", item.Name)
			if err := h.ingressesClient.Delete(item.Name, &metav1.DeleteOptions{
				PropagationPolicy: &deletePolicy,
			}); err != nil {
				return count, err
			}
			count++
		}
	}

	return count, nil
}
