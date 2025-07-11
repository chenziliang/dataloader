package kafka

import (
	"bytes"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"

	"gitlab.com/chenziliang/dataloader/models"
	"gitlab.com/chenziliang/dataloader/sinks"
)

func (writer *kafkaWriter) loadMetricData(source *models.Source, wg *sync.WaitGroup) {
	if source.Settings.Topic == "" {
		source.Settings.Topic = "device_utils"
	}

	if err := writer.newDeviceTopic(source); err != nil {
		return
	}

	writer.devLocations = sinks.GenerateLocations(source, true)

	for i := 0; i < int(source.Settings.Concurrency); i++ {
		wg.Add(1)
		go writer.doLoadMetricData(source, wg, i)
	}
}

func (writer *kafkaWriter) doLoadMetricData(source *models.Source, wg *sync.WaitGroup, i int) {
	defer wg.Done()

	var currentIteration int32
	batchSize := int(source.Settings.BatchSize)

	writer.logger.Info("target topic", zap.String("topic", source.Settings.Topic))

	start := time.Now().UnixNano()
	prev := start

	// records := models.GenerateMetrics(source.Settings.TotalEntities, writer.devLocations)
	for {
		records := models.GenerateMetrics(source.Settings.TotalEntities, writer.devLocations)

		atomic.AddUint64(&writer.ingested, (uint64)(len(records)))
		atomic.AddUint64(&writer.ingested_total, (uint64)(len(records)))

		batch := records
		for n := 0; n < len(batch); n += batchSize {
			pos := n + batchSize
			if pos > len(batch) {
				pos = len(batch)
			}

			/// now := time.Now().UnixNano()
			writer.doMetricInsert(batch[n:pos], source.Settings.Topic, source.Type)
			/// atomic.AddUint64(&writer.duration, uint64(time.Now().UnixNano()-now))
			/// atomic.AddUint64(&writer.duration_total, uint64(time.Now().UnixNano()-now))

			currentIteration += 1
		}

		now := time.Now().UnixNano()
		if now-prev >= 2*1000*1000*1000 && i == 0 {
			current_duration_ms := uint64((now - prev) / 1000000)
			current_ingested := atomic.LoadUint64(&writer.ingested)

			ingested_total := atomic.LoadUint64(&writer.ingested_total)
			duration_total_ms := uint64((now - start) / 1000000)

			/// reset to 0
			atomic.StoreUint64(&writer.ingested, 0)

			writer.logger.Info("ingest metrics", zap.Uint64("ingested", current_ingested), zap.Uint64("duration_ms", current_duration_ms), zap.Uint64("eps", (current_ingested*1000)/current_duration_ms), zap.Uint64("ingested_total", ingested_total), zap.Uint64("duration_total_ms", duration_total_ms), zap.Uint64("overall_eps", (ingested_total*1000)/duration_total_ms))

			prev = now
		}

		if source.Settings.Iteration > 0 && currentIteration >= source.Settings.Iteration {
			break
		}

		if source.Settings.Interval > 0 {
			time.Sleep(time.Duration(source.Settings.Interval) * time.Millisecond)
		}
	}
}

func (writer *kafkaWriter) doMetricInsert(records []models.Metric, topic, typ string) error {
	var data [][]byte
	for _, record := range records {
		payload, err := json.Marshal(record)
		if err != nil {
			writer.logger.Error("Failed to marshal base.Data object", zap.Error(err))
			return err
		}

		data = append(data, payload)
	}

	batch_payload := bytes.Join(data, []byte("\n"))

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   nil,
		Value: sarama.StringEncoder(batch_payload),
	}

	writer.write(msg)

	return nil
}

func (writer *kafkaWriter) newDeviceTopic(source *models.Source) error {
	topicExists := false
	topic := source.Settings.Topic
	metadata, err := writer.admin.DescribeTopics([]string{topic})
	if err != nil {
		writer.logger.Error("Failed to describe topic", zap.String("topic", topic), zap.Error(err))
	} else if metadata[0].Err != sarama.ErrNoError {
		if metadata[0].Err != sarama.ErrUnknownTopicOrPartition {
			writer.logger.Error("Failed to describe topic", zap.String("topic", topic), zap.String("error", metadata[0].Err.Error()))
		}
	} else {
		topicExists = true
	}

	if topicExists && !source.Settings.CleanBeforeLoad {
		writer.logger.Info("Topic already exists", zap.String("topic", topic))
		return nil
	}

	if source.Settings.CleanBeforeLoad && topicExists {
		// Topic exists, we need clean it up before data load
		err = writer.admin.DeleteTopic(topic)
		if err != nil {
			writer.logger.Error("Failed to delete topic", zap.String("topic", topic), zap.Error(err))
			return err
		} else {
			writer.logger.Info("Successfully delete topic", zap.String("topic", topic))
		}
	}

	err = writer.admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     source.Settings.NumPartitions,
		ReplicationFactor: source.Settings.ReplicationFactor,
	}, false)

	if err != nil {
		writer.logger.Error("Failed to create topic", zap.String("topic", topic), zap.Error(err))
	} else {
		writer.logger.Info("Successfully create topic", zap.String("topic", topic))
	}

	return err
}
