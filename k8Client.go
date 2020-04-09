package main

import (
	"flag"
	log "github.com/sirupsen/logrus"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"path/filepath"
)

type K8ClientInterface interface {
	GetClientSet() *kubernetes.Clientset
}

type K8Client struct {
	clientset *kubernetes.Clientset
	config    *Config
}

func NewK8Client(config *Config) (*K8Client, error) {
	k8client := &K8Client{}
	var clientset *kubernetes.Clientset
	var err error

	if config.Mode == "in-cluster" {
		clientset, err = k8client.createInClusterClientSet()
	} else {
		clientset, err = k8client.createLocalClientSet()
	}
	if err != nil {
		return k8client, err
	}

	k8client.clientset = clientset
	k8client.config = config

	return k8client, nil
}

func (k8 K8Client) GetClientSet() *kubernetes.Clientset {
	return k8.clientset
}

func (k8 K8Client) createLocalClientSet() (*kubernetes.Clientset, error) {
	log.Println("Getting local k8 client...")

	var kubeconfig *string
	homeDir := os.Getenv("HOME")

	if home := homeDir; home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	// use the current context in kubeconfig
	currentconfig, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(currentconfig)

	return clientset, err
}

func (k8 K8Client) createInClusterClientSet() (*kubernetes.Clientset, error) {
	log.Println("Getting in-cluster k8 client...")

	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)

	return clientset, err
}
