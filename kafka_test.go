package main

import (
	"errors"
	"gopkg.in/Shopify/sarama.v1"
	"log"
	"testing"
)

type KafkaClientTestSuite struct {
	KafkaClient KafkaClient
}

type SaramaMockClient struct {
	isError bool
	moldest map[int32]int64
	mnewest map[int32]int64
}

func NewKafkaClientTestSuite(bool bool) *KafkaClientTestSuite {
	client, _ := NewSaramaMockClient(bool)
	return &KafkaClientTestSuite{KafkaClient{SaramaClient: client}}
}

func NewSaramaMockClient(bool bool) (*SaramaMockClient, error) {
	mnewest := map[int32]int64{0: 29699366, 1: 29699296, 2: 29709419, 3: 29700971, 4: 29697439, 5: 29702429, 6: 29702036, 7: 29701840}
	moldest := map[int32]int64{0: 27722971, 1: 27740352, 2: 27731954, 3: 27724846, 4: 27735642, 5: 27727105, 6: 27725023, 7: 27741339}
	return &SaramaMockClient{bool, moldest, mnewest}, nil
}

func (s *SaramaMockClient) Partitions(topic string) ([]int32, error) {
	if s.isError {
		return nil, errors.New("an error occurred")
	}
	return []int32{0, 1, 2, 3, 4, 5, 6, 7}, nil
}

func (s *SaramaMockClient) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	if time == sarama.OffsetOldest {
		return s.moldest[partitionID], nil
	} else if time == sarama.OffsetNewest {
		return s.mnewest[partitionID], nil
	} else {
		return 0, errors.New("wrong argument")
	}
}

func Test_KafkaClient_GetDesiredOffsets(t *testing.T) {
	testError := false
	tc := NewKafkaClientTestSuite(testError)
	offsets, err := tc.KafkaClient.GetDesiredOffsets("test-influx-metrics")
	mdesired := map[int32]int64{0: 29405525, 1: 29405455, 2: 29415578, 3: 29407130, 4: 29403598, 5: 29408588, 6: 29408195, 7: 29407999}
	if offsets == nil || err != nil {
		t.Errorf("Test failed, wan't expecting error or empty values")
	} else {
		for _, v := range offsets {
			if v.DesiredOffset != mdesired[v.partitionId] {
				t.Errorf("Expected desired offset for partition %d as %d, but found %d", v.partitionId, mdesired[v.partitionId], v.DesiredOffset)
			}
		}
	}
}

func Test_KafkaClient_GetDesiredOffsets_Error(t *testing.T) {
	testError := true
	tc := NewKafkaClientTestSuite(testError)
	_, err := tc.KafkaClient.GetDesiredOffsets("test-influx-metrics")
	if err == nil {
		t.Errorf("Test failed, was expecting error but received none.")
	}
}

func Test_GetAllOffsets_Integration(t *testing.T) {
	tc := KafkaClientTestSuite{}
	client, _ := tc.GetKafkaClient()
	offsets, err := client.GetDesiredOffsets("test-influx-metrics")
	if offsets == nil || err != nil {
		log.Println("Integration test to get kafka offsets failed")
	} else {
		log.Println(offsets)
	}
}

func (tc *KafkaClientTestSuite) GetKafkaClient() (*KafkaClient, error) {
	return NewKafkaClient(&Config{KafkaConfig: &KafkaConfig{Source: "kafka.nonprod.r53.arghanil.net:9092"}})
}
