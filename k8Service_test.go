package main

import (
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
)

type ServiceHandlerTestSuite struct {
	ServiceHandler ServiceHandler
}

func NewServiceHandlerTestSuite(config *Config, jobs FakeJobNames) *ServiceHandler {
	return &ServiceHandler{templateParser: &MockTemplateParser2{}, config: config}
}

// FakeServices implements ServiceInterface
type FakeService struct {
}

func (c *FakeService) Get(name string, options metav1.GetOptions) (result *v1.Service, err error) {
	return nil, nil
}

func (c *FakeService) List(opts metav1.ListOptions) (result *v1.ServiceList, err error) {
	return nil, nil
}

func (c *FakeService) Watch(opts metav1.ListOptions) (watch.Interface, error) {
	return nil, nil

}

func (c *FakeService) Create(service *v1.Service) (result *v1.Service, err error) {
	return nil, nil
}

func (c *FakeService) Update(service *v1.Service) (result *v1.Service, err error) {
	return nil, nil
}

func (c *FakeService) UpdateStatus(service *v1.Service) (result *v1.Service, err error) {
	return nil, nil
}

func (c *FakeService) Delete(name string, options *metav1.DeleteOptions) error {
	return nil
}

func (c *FakeService) DeleteCollection(options *metav1.DeleteOptions, listOptions metav1.ListOptions) error {
	return nil
}

func (c *FakeService) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1.Service, err error) {
	return nil, err
}

type MockTemplateParser2 struct {
}

func (t *MockTemplateParser2) LoadTemplate(path string, filler interface{}) (string, error) {
	return "{}", nil
}
