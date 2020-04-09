package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type DbInterface interface {
	GetAllToDoItems() ([]DownsamplingObject, error)
	GetPendingDownsamplePreviewItems() ([]DownsamplingObject, error)
	GetPendingDownsampleItems() ([]DownsamplingObject, error)
	GetDeployedDownsamplePreviewItems() ([]DownsamplingObject, error)
	GetDeletedDownsampleItems() ([]DownsamplingObject, error)
	DeleteDownsamplingItem(queryId string) error
	GetDownsamplingItem(queryId string) (DownsamplingObject, error)
	UpdateDownsamplingItem(DownsampleObject DownsamplingObject) (string, error)
}

func (d *Dynamodb) formTableName(table string) *string {
	return aws.String(fmt.Sprintf("%s%s", d.Configs.DbTablePrefix, table))
}

type Dynamodb struct {
	Configs *Config
	Svc     dynamodbiface.DynamoDBAPI
}

func NewDynamodb(config *Config) (*Dynamodb, error) {
	session, err := session.NewSession(&aws.Config{
		Region: aws.String("us-west-2"),
	})
	if err != nil {
		return nil, err
	}

	return &Dynamodb{Svc: dynamodb.New(session), Configs: config}, nil
}

func (d *Dynamodb) GetPendingDownsamplePreviewItems() ([]DownsamplingObject, error) {
	return d.getDownsampleItems("PREVIEW_PENDING")
}

func (d *Dynamodb) GetDeployedDownsamplePreviewItems() ([]DownsamplingObject, error) {
	return d.getDownsampleItems("PREVIEW_DEPLOYED")
}

func (d *Dynamodb) GetPendingDownsampleItems() ([]DownsamplingObject, error) {
	return d.getDownsampleItems("PENDING")
}

func (d *Dynamodb) GetDeletedDownsampleItems() ([]DownsamplingObject, error) {
	return d.getDownsampleItems("DELETED")
}

func (d *Dynamodb) GetAllToDoItems() ([]DownsamplingObject, error) {
	input := &dynamodb.ScanInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":queryStatePending": {
				S: aws.String("PENDING"),
			},
			":queryStatePreviewPending": {
				S: aws.String("PREVIEW_PENDING"),
			},
			":queryStateDeleted": {
				S: aws.String("DELETED"),
			},
		},
		FilterExpression: aws.String("queryState IN (:queryStatePending, :queryStatePreviewPending, :queryStateDeleted)"),
		TableName:        d.formTableName("metrics_downsample_queries"),
	}
	var ds []DownsamplingObject
	result, err := d.Svc.Scan(input)
	if err != nil {
		return ds, err
	}
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &ds)
	if err != nil {
		return ds, err
	}
	return ds, nil
}

func (d *Dynamodb) getDownsampleItems(state string) ([]DownsamplingObject, error) {
	input := &dynamodb.ScanInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":queryState": {
				S: aws.String(state),
			},
		},
		FilterExpression: aws.String("queryState = :queryState"),
		TableName:        d.formTableName("metrics_downsample_queries"),
	}
	var ds []DownsamplingObject
	result, err := d.Svc.Scan(input)
	if err != nil {
		return ds, err
	}

	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &ds)
	if err != nil {
		return ds, err
	}
	return ds, nil
}

func (d *Dynamodb) DeleteDownsamplingItem(queryId string) error {
	_, err := d.Svc.DeleteItem(
		&dynamodb.DeleteItemInput{
			TableName: d.formTableName("metrics_downsample_queries"),
			Key: map[string]*dynamodb.AttributeValue{
				"queryId": {
					S: aws.String(queryId),
				},
			},
		})

	return err
}

func (d *Dynamodb) GetDownsamplingItem(queryId string) (DownsamplingObject, error) {
	input := &dynamodb.GetItemInput{
		TableName: d.formTableName("metrics_downsample_queries"),
		Key: map[string]*dynamodb.AttributeValue{
			"queryId": {
				S: aws.String(queryId),
			},
		},
	}
	var query DownsamplingObject
	result, err := d.Svc.GetItem(input)
	if err != nil {
		return query, err
	}

	err = dynamodbattribute.UnmarshalMap(result.Item, &query)
	if err != nil {
		return query, err
	}
	return query, nil
}

func (d *Dynamodb) UpdateDownsamplingItem(DownsampleObject DownsamplingObject) (string, error) {
	ds, err := dynamodbattribute.MarshalMap(DownsampleObject)
	if err != nil {
		return "Failure", err
	}

	_, err = d.Svc.PutItem(
		&dynamodb.PutItemInput{
			TableName: d.formTableName("metrics_downsample_queries"),
			Item:      ds,
		})
	if err != nil {
		return "Failure", err
	}

	status := "Success"
	return status, nil
}
