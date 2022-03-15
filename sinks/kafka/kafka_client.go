package kafka

import (
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

type KafkaClient struct {
	brokers []string
	client  sarama.Client
	logger *zap.Logger
}

const (
	maxRetry                 = 10
	TopicOrPartitionNotExist = -10
)

// brokers: ip:port,ip:port,...

func NewKafkaClient(brokers []string, logger *zap.Logger) (*KafkaClient, error) {
	if len(brokers) == 0 {
		logger.Error("broker IP/port is required to create KafkaClient")
		return nil, fmt.Errorf("broker IP/port is required to create KafkaClient")
	}

	kconfig := sarama.NewConfig()
	return NewKafkaClientFromConfig(brokers, kconfig, logger)
}

func NewKafkaClientFromConfig(brokers []string, config *sarama.Config, logger *zap.Logger) (*KafkaClient, error) {
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		logger.Error("Failed to create KafkaClient", zap.Error(err))
		return nil, err
	}

	return &KafkaClient{
		brokers: brokers,
		client:  client,
		logger: logger,
	}, nil
}

func (client *KafkaClient) BrokerIPs() []string {
	return client.brokers
}

func (client *KafkaClient) Topics() ([]string, error) {
	return client.client.Topics()
}

func (client *KafkaClient) RefreshMetadata(topics ...string) error {
	return client.client.RefreshMetadata(topics...)
}

func (client *KafkaClient) TopicPartitions(topics []string) (map[string][]int, error) {
	var err error
	if topics == nil {
		topics, err = client.Topics()
		if err != nil {
			client.logger.Error("Failed to get topics from Kafka", zap.Error(err))
			return nil, err
		}
	}

	topicPartitions := make(map[string][]int, len(topics))
	for _, topic := range topics {
		if topic == "" {
			continue
		}

		partitions, err := client.client.Partitions(topic)
		if err != nil {
			client.logger.Error("Failed to get partitions", zap.String("topic", topic), zap.Error(err))
			continue
		}

		if _, ok := topicPartitions[topic]; !ok {
			topicPartitions[topic] = make([]int, 0, len(partitions))
		}

		for _, partition := range partitions {
			topicPartitions[topic] = append(topicPartitions[topic], int(partition))
		}
	}

	if len(topicPartitions) == 0 {
		return nil, err
	}
	return topicPartitions, nil
}

func (client *KafkaClient) GetConsumerOffset(consumerGroup string,
	topic string, partition int) (int64, error) {
	// 1. Use consumerGroup to get the offset coordinator broker
	// 2. Talk to the coordinator to get the current offset for consumerGroup
	coordinator, err := client.client.Coordinator(consumerGroup)
	if err != nil {
		client.logger.Error("Failed to get coordinator", zap.String("consumer_group", consumerGroup), zap.Error(err))
		return 0, err
	}

	req := sarama.OffsetFetchRequest{
		ConsumerGroup: consumerGroup,
		Version:       1,
	}

	req.AddPartition(topic, int32(partition))
	resp, err := coordinator.FetchOffset(&req)
	if err != nil {
		client.logger.Error("Failed to get offset", zap.String("consumer_group", consumerGroup), zap.String("topic", topic), zap.Int("partition", partition), zap.Error(err))
		return 0, err
	}

	offset := resp.Blocks[topic][int32(partition)].Offset
	if offset == sarama.OffsetNewest {
		// When consumer group doesn't exist, Kafka server
		// returns sarama.OffsetNewest, but we want OffsetOldest
		offset = sarama.OffsetOldest
	}
	return offset, nil
}

// Return: OffsetNotExist
func (client *KafkaClient) GetProducerOffset(topic string, partition int) (int64, error) {
	leader, err := client.Leader(topic, partition)
	if err != nil || leader == nil {
		return TopicOrPartitionNotExist, err
	}

	ofreq := &sarama.OffsetRequest{}
	ofreq.AddBlock(topic, int32(partition), time.Now().UnixNano(), 10)

	oresp, err := leader.GetAvailableOffsets(ofreq)
	if err != nil {
		client.logger.Error("Failed to get the available offset", zap.String("topic", topic), zap.Int("partition", partition), zap.Error(err))
		return 0, err
	}

	// offsets are returned in desc order already
	return oresp.GetBlock(topic, int32(partition)).Offsets[0], nil
}

// @Return (position, nil) if no errors
// (nil, nil) if topic or partition etc doesn't exist
// (nil, err) for other errors
func (client *KafkaClient) GetLastBlock(topic string, partition int) ([]byte, error) {
	/*
	lastOffset, err := client.GetProducerOffset(topic, partition)
	if lastOffset == TopicOrPartitionNotExist {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	leader, err := client.Leader(topic, partition)
	if err != nil {
		return nil, err
	}

	freq := &sarama.FetchRequest{
		MaxWaitTime: 1000, // millisec
		MinBytes:    1,
	}

	if lastOffset > 0 {
		lastOffset -= 1
	}

	freq.AddBlock(topic, int32(partition), lastOffset, 1024)
	fresp, err := leader.Fetch(freq)
	if err != nil {
		client.logger.Error("Failed to get data", zap.String("topic", topic), zap.Int("partition", partition), zap.Error(err))
		return nil, err
	}

	msgBlocks := fresp.Blocks[topic][int32(partition)].MsgSet.Messages
	for i := 0; i < len(msgBlocks); i++ {
		block := msgBlocks[i]
		if block.Offset == lastOffset {
			return block.Msg.Value, nil
		}
	}
	*/
	return nil, nil
}

func (client *KafkaClient) Leader(topic string, partition int) (*sarama.Broker, error) {
	var leader *sarama.Broker
	var err error

	for i := 0; i < maxRetry; i++ {
		leader, err = client.client.Leader(topic, int32(partition))
		if err != nil {
			client.logger.Error("Failed to get leader", zap.String("topic", topic), zap.Int("partition", partition), zap.Error(err))
			// Fast break out if topic doesn't exist
			if strings.Contains(err.Error(), "does not exist") {
				return nil, nil
			}

			time.Sleep(time.Second)
		} else {
			return leader, err
		}
	}
	return leader, err
}

func (client *KafkaClient) Close() {
	client.client.Close()
}

func (client *KafkaClient) Client() sarama.Client {
	return client.client
}

func (client *KafkaClient) Config() *sarama.Config {
	return client.client.Config()
}

func (client *KafkaClient) OldestOffset(topic string, partition int) (int64, error) {
	return client.client.GetOffset(topic, int32(partition), sarama.OffsetOldest)
}

func (client *KafkaClient) NewestOffset(topic string, partition int) (int64, error) {
	return client.client.GetOffset(topic, int32(partition), sarama.OffsetNewest)
}
