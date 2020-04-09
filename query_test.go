package main

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

type DownsamplingItemHandlerTestSuite struct {
	DownsamplingItemHandler DownsamplingItemHandler
	FakeQueryAssertData     FakeQueryAssertData
}

func NewDownsamplingItemHandlerTestSuite(config *Config, data FakeQueryAssertData) *DownsamplingItemHandlerTestSuite {
	return &DownsamplingItemHandlerTestSuite{DownsamplingItemHandler{db: NewMockDb(data), config: config}, data}
}

type FakeQueryAssertData struct {
	dsList         []DownsamplingObject
	objectToExpect DownsamplingObject
	errorToExpect  error
}

type MockDb struct {
	FakeQueryAssertData FakeQueryAssertData
}

func NewMockDb(FakeQueryAssertData FakeQueryAssertData) *MockDb {
	return &MockDb{FakeQueryAssertData}
}

func (d *MockDb) GetPendingDownsamplePreviewItems() ([]DownsamplingObject, error) {
	return d.FakeQueryAssertData.dsList, nil
}

func (d *MockDb) GetDeployedDownsamplePreviewItems() ([]DownsamplingObject, error) {
	return d.FakeQueryAssertData.dsList, nil
}

func (d *MockDb) GetPendingDownsampleItems() ([]DownsamplingObject, error) {
	return d.FakeQueryAssertData.dsList, nil
}

func (d *MockDb) GetDeletedDownsampleItems() ([]DownsamplingObject, error) {
	return d.FakeQueryAssertData.dsList, nil
}

func (d *MockDb) GetAllToDoItems() ([]DownsamplingObject, error) {
	return d.FakeQueryAssertData.dsList, nil
}

func (d *MockDb) getDownsampleItems(state string) ([]DownsamplingObject, error) {
	return d.FakeQueryAssertData.dsList, nil
}

func (d *MockDb) DeleteDownsamplingItem(queryId string) error {
	return nil
}

func (d *MockDb) GetDownsamplingItem(queryId string) (DownsamplingObject, error) {
	return d.FakeQueryAssertData.dsList[0], nil
}

func (d *MockDb) UpdateDownsamplingItem(DownsampleObject DownsamplingObject) (string, error) {
	d.FakeQueryAssertData.objectToExpect = DownsampleObject
	if d.FakeQueryAssertData.errorToExpect != nil {
		return "failure", d.FakeQueryAssertData.errorToExpect
	}
	if d.FakeQueryAssertData.objectToExpect.QueryId != DownsampleObject.QueryId || d.FakeQueryAssertData.objectToExpect.QueryState != DownsampleObject.QueryState ||
		d.FakeQueryAssertData.objectToExpect.PreviewExpiresAt != DownsampleObject.PreviewExpiresAt {
		return "failure", errors.New("Error occurred, mismatch. ")
	}
	return "success", nil
}

func Test_DownsamplingItemHandler_GetNextPendingDownsamplingItem(t *testing.T) {
	tc := NewDownsamplingItemHandlerTestSuite(&Config{MetricsConfig: &MetricsConfig{}}, FakeQueryAssertData{dsList: []DownsamplingObject{DownsamplingObject{QueryId: "query1", CreatedAt: "2017-12-08T21:46:28Z"}, DownsamplingObject{QueryId: "query2", CreatedAt: "2017-12-07T21:46:28Z"}}})
	ds, err := tc.DownsamplingItemHandler.GetNextPendingDownsamplingItem()
	if err != nil {
		t.Error(fmt.Sprintf("Error wasn't expected here"))
	}
	if ds.QueryId != "query2" {
		t.Error(fmt.Sprintf("%s expected to be %s but found %s", "ds.QueryId", "query2", ds.QueryId))
	}
}

func Test_DownsamplingItemHandler_DeployDownsamplingPendingSimulationItem(t *testing.T) {
	tc := NewDownsamplingItemHandlerTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45}, FakeQueryAssertData{dsList: []DownsamplingObject{DownsamplingObject{QueryId: "query1", QueryState: "PREVIEW_PENDING"}}, objectToExpect: DownsamplingObject{QueryId: "query1", QueryState: "PREVIEW_DEPLOYED"}})
	err := tc.DownsamplingItemHandler.DeployDownsamplingPendingSimulationItem("query1")
	if err != nil {
		t.Error(fmt.Sprintf("Error wasn't expected here - %v", err))
	}
}

func Test_DownsamplingItemHandler_DeployDownsamplingPendingSimulationItem_Deployed(t *testing.T) {
	tc := NewDownsamplingItemHandlerTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45}, FakeQueryAssertData{dsList: []DownsamplingObject{DownsamplingObject{QueryId: "query1", QueryState: "PREVIEW_DEPLOYED"}}, errorToExpect: errors.New("No update should have happened")})
	err := tc.DownsamplingItemHandler.DeployDownsamplingPendingSimulationItem("query1")
	if err != nil {
		t.Error(fmt.Sprintf("Error wasn't expected here - %v", err))
	}
}

func Test_DownsamplingItemHandler_DeployDownsamplingPendingSimulationItem_NotFound(t *testing.T) {
	tc := NewDownsamplingItemHandlerTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45}, FakeQueryAssertData{dsList: []DownsamplingObject{DownsamplingObject{QueryId: "", CreatedAt: "2017-12-08T21:00:00Z", QueryState: "PREVIEW_DEPLOYED"}}, errorToExpect: errors.New("No update should have happened")})
	err := tc.DownsamplingItemHandler.DeployDownsamplingPendingSimulationItem("query1")
	if err != nil {
		t.Error(fmt.Sprintf("Error wasn't expected here - %v", err))
	}
}

func Test_DownsamplingItemHandler_DeployDownsamplingItem(t *testing.T) {
	tc := NewDownsamplingItemHandlerTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45}, FakeQueryAssertData{dsList: []DownsamplingObject{DownsamplingObject{QueryId: "query1", QueryState: "PENDING"}}, objectToExpect: DownsamplingObject{QueryId: "query1", QueryState: "DEPLOYED"}})

	err := tc.DownsamplingItemHandler.DeployDownsamplingItem(DownsamplingObject{QueryId: "query1"})
	if err != nil {
		t.Error(fmt.Sprintf("Error wasn't expected here - %v", err))
	}
}

func Test_DownsamplingItemHandler_DeployDownsamplingItem_Deployed(t *testing.T) {
	tc := NewDownsamplingItemHandlerTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45}, FakeQueryAssertData{dsList: []DownsamplingObject{DownsamplingObject{QueryId: "query1", QueryState: "DEPLOYED"}}})
	err := tc.DownsamplingItemHandler.DeployDownsamplingItem(DownsamplingObject{QueryId: "query1"})
	if err.Error() != "status changed" {
		t.Error(fmt.Sprintf("Error was expected here but not received"))
	}
}

func Test_DownsamplingItemHandler_DeployDownsamplingItem_NotFound(t *testing.T) {
	tc := NewDownsamplingItemHandlerTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45}, FakeQueryAssertData{dsList: []DownsamplingObject{DownsamplingObject{QueryId: "", QueryState: "DEPLOYED"}}})
	err := tc.DownsamplingItemHandler.DeployDownsamplingItem(DownsamplingObject{QueryId: ""})
	if err.Error() != "object not found" {
		t.Error(fmt.Sprintf("Error was expected here but not received"))
	}
}

func Test_DownsamplingItemHandler_HandleExpiredSimulations(t *testing.T) {
	tc := NewDownsamplingItemHandlerTestSuite(&Config{MetricsConfig: &MetricsConfig{}, ExpireAfterMinute: 45}, FakeQueryAssertData{dsList: []DownsamplingObject{DownsamplingObject{QueryId: "query1", PreviewExpiresAt: time.Now().Add(-1 * time.Minute).Format(time.RFC3339), QueryState: "PREVIEW_DEPLOYED"}, DownsamplingObject{QueryId: "query2", PreviewExpiresAt: time.Now().Add(1 * time.Minute).Format(time.RFC3339), QueryState: "PREVIEW_DEPLOYED"}}})
	count, err := tc.DownsamplingItemHandler.HandleExpiredSimulations()
	if err != nil {
		t.Error(fmt.Sprintf("Error wasn't expected here - %v", err))
	}
	if count != 1 {
		t.Error(fmt.Sprintf("Only one expired query was expected."))
	}
}
