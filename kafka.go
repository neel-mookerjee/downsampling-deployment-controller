package main

import (
	"errors"
	"gopkg.in/Shopify/sarama.v1"
)

type SaramaClientInterface interface {
	Partitions(topic string) ([]int32, error)
	GetOffset(topic string, partitionID int32, time int64) (int64, error)
}

type SaramaClient struct {
	Client sarama.Client
}

func NewSaramaClient(config *Config) (*SaramaClient, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V0_11_0_0
	client, err := sarama.NewClient([]string{config.KafkaConfig.Source}, saramaConfig)
	if err != nil {
		return nil, err
	}
	return &SaramaClient{Client: client}, nil
}

func (s *SaramaClient) Partitions(topic string) ([]int32, error) {
	return s.Client.Partitions(topic)
}

func (s *SaramaClient) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	return s.Client.GetOffset(topic, partitionID, time)
}

type KafkaClientInterface interface {
	GetDesiredOffsets(topic string) (KafkaPartitionOffsets, error)
}

type KafkaClient struct {
	Config       *Config
	SaramaClient SaramaClientInterface
}

type KafkaPartitionOffset struct {
	partitionId   int32
	OldestOffset  int64
	NewestOffset  int64
	DesiredOffset int64
}

type KafkaPartitionOffsets []KafkaPartitionOffset

func NewKafkaClient(config *Config) (*KafkaClient, error) {
	client, err := NewSaramaClient(config)
	if err != nil {
		return nil, err
	}
	return &KafkaClient{Config: config, SaramaClient: client}, nil
}

func (k *KafkaClient) getAllOffsets(topic string) (KafkaPartitionOffsets, error) {
	if partitionIds, err := k.SaramaClient.Partitions(topic); err != nil {
		return nil, err
	} else {
		if len(partitionIds) == 0 {
			return nil, errors.New("Received no partition for the topic, which is unusual")
		}
		partitions := KafkaPartitionOffsets{}
		for id := range partitionIds {
			offsetOldest, err := k.SaramaClient.GetOffset(topic, int32(id), sarama.OffsetOldest)
			if err != nil {
				return nil, err
			}
			offsetNewest, err := k.SaramaClient.GetOffset(topic, int32(id), sarama.OffsetNewest)
			if err != nil {
				return nil, err
			}
			partitions = append(partitions, KafkaPartitionOffset{int32(id), offsetOldest, offsetNewest, 0})
		}
		return partitions, nil
	}
}

func (k *KafkaClient) GetDesiredOffsets(topic string) (KafkaPartitionOffsets, error) {
	offsets, err := k.getAllOffsets(topic)
	if err != nil {
		return nil, err
	}
	var minDepth int64
	for _, f := range offsets {
		if minDepth == 0 || f.NewestOffset-f.OldestOffset < minDepth {
			minDepth = f.NewestOffset - f.OldestOffset
		}
	}
	minDepth = minDepth * 15 / 100
	for i := range offsets {
		offsets[i].DesiredOffset = offsets[i].NewestOffset - minDepth
	}
	return offsets, nil
}
