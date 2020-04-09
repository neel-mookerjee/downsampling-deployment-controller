package main

import (
	"encoding/json"
	"fmt"
	v1beta1 "k8s.io/api/extensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	"testing"
	"time"
)

type IngressHandlerTestSuite struct {
	IngressHandler IngressHandler
}

func NewIngressHandlerTestSuite(config *Config, data FakeIngressAssertData) *IngressHandler {
	return &IngressHandler{ingressesClient: &FakeIngresses{data}, templateParser: &MockTemplateParser3{}, config: config}
}

type FakeIngressAssertData struct {
	errorToExpect error
}

type FakeIngresses struct {
	FakeIngressAssertData FakeIngressAssertData
}

func (c *FakeIngresses) Get(name string, options v1.GetOptions) (result *v1beta1.Ingress, err error) {
	return nil, err
}

func (c *FakeIngresses) List(opts v1.ListOptions) (result *v1beta1.IngressList, err error) {
	list := &v1beta1.IngressList{}
	if opts.LabelSelector != "purpose="+SIMULATION_STACK_PREFIX {
		err = errors.NewBadRequest("wrong label selector")
		return list, err
	}
	i := v1beta1.Ingress{}
	i.Name = "test-ingress"
	i.CreationTimestamp.Time = time.Now().Add(-46 * time.Minute)
	list.Items = append(list.Items, i)
	i2 := v1beta1.Ingress{}
	i2.Name = "test-ingress2"
	i2.CreationTimestamp.Time = time.Now().Add(-40 * time.Minute)
	list.Items = append(list.Items, i2)
	return list, nil
}

func (c *FakeIngresses) Watch(opts v1.ListOptions) (watch.Interface, error) {
	return nil, nil

}

func (c *FakeIngresses) Create(ingress *v1beta1.Ingress) (result *v1beta1.Ingress, err error) {
	return nil, c.FakeIngressAssertData.errorToExpect
}

func (c *FakeIngresses) Update(ingress *v1beta1.Ingress) (result *v1beta1.Ingress, err error) {
	return nil, err
}

func (c *FakeIngresses) UpdateStatus(ingress *v1beta1.Ingress) (*v1beta1.Ingress, error) {
	return nil, nil
}

func (c *FakeIngresses) Delete(name string, options *v1.DeleteOptions) error {
	return nil
}

func (c *FakeIngresses) DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error {
	return nil
}

func (c *FakeIngresses) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1beta1.Ingress, err error) {
	return nil, err
}

type MockTemplateParser3 struct {
}

func (t *MockTemplateParser3) LoadTemplate(path string, filler interface{}) (string, error) {
	return "{}", nil
}

func Test_IngressHandler_CreateIngress(t *testing.T) {
	tc := NewIngressHandlerTestSuite(&Config{}, FakeIngressAssertData{})
	_, _, err := tc.CreateIngress(PARAM{queryId: "qwertyu"})
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but received. %v", err))
	}
}

func Test_IngressHandler_CreateK8Ingress_Success(t *testing.T) {
	tc := NewIngressHandlerTestSuite(&Config{}, FakeIngressAssertData{})
	var spec *v1beta1.Ingress
	json.Unmarshal([]byte("{}"), &spec)
	bool, err := tc.CreateK8Ingress(spec)
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but received. %v", err))
	}
	if !bool {
		t.Error(fmt.Sprintf("Ingress not present, was expecting true (created), but got false."))
	}
}

func Test_IngressHandler_CreateK8Ingress_AlreadyExists(t *testing.T) {
	tc := NewIngressHandlerTestSuite(&Config{}, FakeIngressAssertData{errors.NewAlreadyExists(schema.GroupResource{}, "")})
	var spec *v1beta1.Ingress
	json.Unmarshal([]byte("{}"), &spec)
	bool, err := tc.CreateK8Ingress(spec)
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but received. %v", err))
	}
	if bool {
		t.Error(fmt.Sprintf("Ingress already present, was expecting false, but got true."))
	}
}

func Test_IngressHandler_HandleOldIngresses_Success(t *testing.T) {
	tc := NewIngressHandlerTestSuite(&Config{ExpireAfterMinute: 45}, FakeIngressAssertData{})
	count, err := tc.HandleOldIngresses()
	if err != nil {
		t.Error(fmt.Sprintf("Error was not expected but received. %v", err))
	}
	if count != 1 {
		t.Error(fmt.Sprintf("Ingress already present, was expecting false, but got true."))
	}
}
