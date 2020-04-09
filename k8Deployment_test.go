package main

import (
	"encoding/json"
	"fmt"
	"k8s.io/api/apps/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"testing"
	"time"
)

type DeploymentHandlerTestSuite struct {
	DeploymentHandler DeploymentHandler
}

func NewDeploymentHandlerTestSuite(config *Config, data FakeDeploymentAssertData) *DeploymentHandler {
	return &DeploymentHandler{deploymentClient: &FakeDeployments{data}, templateParser: &MockTemplateParserK8DepTest{}, config: config, Metrics: NewMetrics(*config, PARAM{})}
}

type FakeDeploymentAssertData struct {
	errorToExpect error
}

type FakeDeployments struct {
	FakeDeploymentAssertData FakeDeploymentAssertData
}

func (c *FakeDeployments) Get(name string, options v1.GetOptions) (result *v1beta1.Deployment, err error) {
	return nil, err
}

func (c *FakeDeployments) List(opts v1.ListOptions) (result *v1beta1.DeploymentList, err error) {
	list := &v1beta1.DeploymentList{}
	if opts.LabelSelector != "purpose="+SIMULATION_STACK_PREFIX {
		err = errors.NewBadRequest("wrong label selector")
		return list, err
	}
	i := v1beta1.Deployment{}
	i.Name = "test-deployment"
	i.CreationTimestamp.Time = time.Now().Add(-46 * time.Minute)
	list.Items = append(list.Items, i)
	i2 := v1beta1.Deployment{}
	i2.Name = "test-deployment2"
	i2.CreationTimestamp.Time = time.Now().Add(-40 * time.Minute)
	list.Items = append(list.Items, i2)
	return list, nil
}

func (c *FakeDeployments) Watch(opts v1.ListOptions) (watch.Interface, error) {
	return nil, nil

}

func (c *FakeDeployments) Create(deployment *v1beta1.Deployment) (result *v1beta1.Deployment, err error) {
	return nil, c.FakeDeploymentAssertData.errorToExpect
}

func (c *FakeDeployments) Update(deployment *v1beta1.Deployment) (result *v1beta1.Deployment, err error) {
	return nil, err
}

func (c *FakeDeployments) UpdateStatus(deployment *v1beta1.Deployment) (*v1beta1.Deployment, error) {
	return nil, nil
}

func (c *FakeDeployments) Delete(name string, options *v1.DeleteOptions) error {
	return nil
}

func (c *FakeDeployments) DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error {
	return nil
}

func (c *FakeDeployments) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1beta1.Deployment, err error) {
	return nil, err
}

type MockTemplateParserK8DepTest struct {
}

func (t *MockTemplateParserK8DepTest) LoadTemplate(path string, filler interface{}) (string, error) {
	return "{}", nil
}

func Test_DeploymentHandler_CreateDeployment(t *testing.T) {
	tc := NewDeploymentHandlerTestSuite(&Config{MetricsConfig: &MetricsConfig{}}, FakeDeploymentAssertData{})
	err := tc.CreateDeployment(PARAM{queryId: "qwertyu"})
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but received. %v", err))
	}
}

func Test_DeploymentHandler_CreateK8Deployment_Success(t *testing.T) {
	tc := NewDeploymentHandlerTestSuite(&Config{MetricsConfig: &MetricsConfig{}}, FakeDeploymentAssertData{})
	var spec *v1beta1.Deployment
	json.Unmarshal([]byte("{}"), &spec)
	bool, err := tc.CreateK8Deployment(spec)
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but received. %v", err))
	}
	if !bool {
		t.Error(fmt.Sprintf("Deployment not present, was expecting true (created), but got false."))
	}
}

func Test_DeploymentHandler_CreateK8Deployment_AlreadyExists(t *testing.T) {
	tc := NewDeploymentHandlerTestSuite(&Config{MetricsConfig: &MetricsConfig{}}, FakeDeploymentAssertData{errors.NewAlreadyExists(schema.GroupResource{}, "")})
	var spec *v1beta1.Deployment
	json.Unmarshal([]byte("{}"), &spec)
	bool, err := tc.CreateK8Deployment(spec)
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but received. %v", err))
	}
	if bool {
		t.Error(fmt.Sprintf("Deployment already present, was expecting false, but got true."))
	}
}

func Test_DeploymentHandler_HandleOldDeployments_Success(t *testing.T) {
	tc := NewDeploymentHandlerTestSuite(&Config{ExpireAfterMinute: 45, MetricsConfig: &MetricsConfig{}}, FakeDeploymentAssertData{})
	count, err := tc.HandleOldDeployments()
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but received. %v", err))
	}
	if count != 1 {
		t.Error(fmt.Sprintf("Only one expired deployment was expected."))
	}
}
