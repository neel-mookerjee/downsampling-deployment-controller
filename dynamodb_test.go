package main

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"testing"
)

type DynamodbTestSuite struct {
	Dynamodb DbInterface
}

func NewDynamodbTestSuite() *DynamodbTestSuite {
	return &DynamodbTestSuite{&Dynamodb{&Config{}, &mockDynamoDBClient{}}}
}

// Define a mock struct to be used in your unit tests of functions.
type mockDynamoDBClient struct {
	dynamodbiface.DynamoDBAPI
}

type DynamodbTestCase struct {
	Object DownsamplingObject
}

var DynamodbTestCases = map[string]DynamodbTestCase{
	"PREVIEW_PENDING":  {DownsamplingObject{QueryState: "PREVIEW_PENDING"}},
	"PENDING":          {DownsamplingObject{QueryState: "PENDING"}},
	"PREVIEW_DEPLOYED": {DownsamplingObject{QueryState: "PREVIEW_DEPLOYED"}},
	"DELETED":          {DownsamplingObject{QueryState: "DELETED"}},
}

func (m *mockDynamoDBClient) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	var av map[string]*dynamodb.AttributeValue
	var err error
	av, err = dynamodbattribute.ConvertToMap(DownsamplingObject{QueryId: "abcdefg"})
	op := &dynamodb.GetItemOutput{Item: av}
	return op, err
}

func (m *mockDynamoDBClient) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	if *input.Item["queryId"].S != "update" {
		return &dynamodb.PutItemOutput{}, errors.New("An error encountered")
	}
	return &dynamodb.PutItemOutput{}, nil
}

func (m *mockDynamoDBClient) DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	if *input.Key["queryId"].S != "abcdefgh" {
		return &dynamodb.DeleteItemOutput{}, errors.New("An error encountered")
	}
	return &dynamodb.DeleteItemOutput{}, nil
}

func (m *mockDynamoDBClient) Scan(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	var al []map[string]*dynamodb.AttributeValue
	var av map[string]*dynamodb.AttributeValue
	var err error
	for _, v := range input.ExpressionAttributeValues {
		av, err = dynamodbattribute.ConvertToMap(DynamodbTestCases[*v.S].Object)
		if err != nil {
			break
		}
		al = append(al, av)
	}
	op := &dynamodb.ScanOutput{Items: al}
	return op, err
}

func Test_Dynamodb_GetPendingDownsamplePreviewItems(t *testing.T) {
	tc := NewDynamodbTestSuite()
	ds, _ := tc.Dynamodb.GetPendingDownsamplePreviewItems()
	if ds[0].QueryState != "PREVIEW_PENDING" {
		t.Error(fmt.Sprintf("%s expected to be %s but found %s", "QueryState", "PREVIEW_PENDING", ds[0].QueryState))
	}
}

func Test_Dynamodb_GetPendingDownsampleItems(t *testing.T) {
	tc := NewDynamodbTestSuite()
	ds, _ := tc.Dynamodb.GetPendingDownsampleItems()
	if ds[0].QueryState != "PENDING" {
		t.Error(fmt.Sprintf("%s expected to be %s but found %s", "QueryState", "PENDING", ds[0].QueryState))
	}
}

func Test_Dynamodb_GetDeployedDownsamplePreviewItems(t *testing.T) {
	tc := NewDynamodbTestSuite()
	ds, _ := tc.Dynamodb.GetDeployedDownsamplePreviewItems()
	if ds[0].QueryState != "PREVIEW_DEPLOYED" {
		t.Error(fmt.Sprintf("%s expected to be %s but found %s", "QueryState", "PREVIEW_DEPLOYED", ds[0].QueryState))
	}
}

func Test_Dynamodb_GetDeletedDownsampleItems(t *testing.T) {
	tc := NewDynamodbTestSuite()
	ds, _ := tc.Dynamodb.GetDeletedDownsampleItems()
	if ds[0].QueryState != "DELETED" {
		t.Error(fmt.Sprintf("%s expected to be %s but found %s", "QueryState", "DELETED", ds[0].QueryState))
	}
}

func Test_Dynamodb_GetAllPendingItems(t *testing.T) {
	tc := NewDynamodbTestSuite()
	ds, _ := tc.Dynamodb.GetAllToDoItems()
	if len(ds) != 3 {
		t.Error(fmt.Sprintf("%s expected to be %d but found %d", "Length of query objects", 3, len(ds)))
	}
	var statuses = map[string]string{"PREVIEW_PENDING": "PREVIEW_PENDING", "PENDING": "PENDING", "DELETED": "DELETED"}
	for _, item := range ds {
		delete(statuses, item.QueryState)
	}
	if len(statuses) != 0 {
		t.Error(fmt.Sprintf("%s expected to be %d but found %d", "Length of the remaining status map", 0, len(ds)))
	}
}

func Test_Dynamodb_GetDownsamplingItem(t *testing.T) {
	tc := NewDynamodbTestSuite()
	ds, _ := tc.Dynamodb.GetDownsamplingItem("abcdefg")
	if ds.QueryId != "abcdefg" {
		t.Error(fmt.Sprintf("%s expected to be %s but found %s", "QueryId", "abcdefg", ds.QueryId))
	}
}

func Test_Dynamodb_UpdateDownsamplingItem(t *testing.T) {
	tc := NewDynamodbTestSuite()
	_, err := tc.Dynamodb.UpdateDownsamplingItem(DownsamplingObject{QueryId: "update"})
	if err != nil {
		t.Error(fmt.Sprintf("Error occurred during test: %v", err))
	}
}

func Test_Dynamodb_DeleteDownsamplingItem(t *testing.T) {
	tc := NewDynamodbTestSuite()
	err := tc.Dynamodb.DeleteDownsamplingItem("abcdefgh")
	if err != nil {
		t.Error(fmt.Sprintf("Error occurred during test: %v", err))
	}
}
