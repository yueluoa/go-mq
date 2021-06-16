package go_mq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

var (
	ConsumerConsumeErr = fmt.Errorf("%v", "消费者消费失败")
)

type HandleEventFunc func(*ConsumerEvent) error

type Consumer struct {
	consumers     int // 消费者数量
	brokers       []string
	topics        []string
	wg            sync.WaitGroup
	ctx           context.Context
	consumer      sarama.Consumer
	consumerGroup sarama.ConsumerGroup
	config        *consumerConfig
	err           HandleErrorFunc
}

func NewConsumerGroup(brokers []string, group string) (GroupPuller, error) {
	return newConsumerGroup(brokers, group)
}

func newConsumerGroup(brokers []string, group string) (GroupPuller, error) {
	consumer := &Consumer{}
	cfg := consumer.config.getConfig()
	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		return nil, err
	}
	consumerGroup, err := sarama.NewConsumerGroupFromClient(group, client)
	if err != nil {
		return nil, err
	}
	consumer.brokers = brokers
	consumer.ctx = context.Background()
	consumer.consumerGroup = consumerGroup
	consumer.consumers = getConsumers(brokers)

	go consumer.monitoring()

	return consumer, nil
}

func NewConsumer(brokers []string) (Puller, error) {
	return newConsumer(brokers)
}

func newConsumer(brokers []string) (Puller, error) {
	cfg := sarama.NewConfig()
	c, err := sarama.NewConsumer(brokers, cfg)
	if err != nil {
		return nil, err
	}
	consumer := &Consumer{}
	consumer.brokers = brokers
	consumer.ctx = context.Background()
	consumer.consumer = c

	return consumer, nil
}

func (c *Consumer) Pull(topic string, eventFunc HandleEventFunc) {
	defer func() {
		if err := c.consumer.Close(); err != nil {
			fmt.Printf("error closing client, err: %v\n", err)
		}
	}()
	partitions, err := c.consumer.Partitions(topic)
	if err != nil {
		fmt.Printf("failed to start consumer partition, err: %v\n", err)
		return
	}
	c.handlePartitions(partitions, topic, eventFunc)
}

func (c *Consumer) GroupPull(topic string, eventFunc HandleEventFunc) {
	c.topics = append(c.topics, topic)
	c.wg.Add(c.consumers)
	for i := 0; i < c.consumers; i++ {
		go c.consume(eventFunc)
	}
	c.wg.Wait()
}

func (c *Consumer) Close() {
	if err := c.consumerGroup.Close(); err != nil {
		fmt.Printf("error closing client, err: %v\n", err)
	}
}

func (c *Consumer) HandleErrorFunc(err HandleErrorFunc) {
	c.err = err
}

func (c *Consumer) handlePartitions(partitions []int32, topic string, eventFunc HandleEventFunc) {
	c.wg.Add(len(partitions))
	for partition := range partitions {
		pc, err := c.consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			return
		}
		// 异步从每个分区消费信息
		go c.partitionConsume(pc, eventFunc)
	}
	c.wg.Wait()
}

func (c *Consumer) partitionConsume(pc sarama.PartitionConsumer, eventFunc HandleEventFunc) {
	defer func() {
		c.wg.Done()
		pc.AsyncClose()
	}()
	for message := range pc.Messages() {
		ce := &ConsumerEvent{}
		if err := json.Unmarshal(message.Value, ce); err != nil {
			fmt.Println("json.Unmarshal err: ", err)
		}
		ce.Topic = message.Topic
		ce.Partition = message.Partition
		ce.Offset = message.Offset

		if err := eventFunc(ce); err != nil {
			return
		}
	}
}

func (c *Consumer) consume(eventFunc HandleEventFunc) {
	defer func() {
		c.wg.Done()
	}()
	handler := consumerGroupHandler{eventFunc: eventFunc}
	for {
		err := c.consumerGroup.Consume(c.ctx, c.topics, handler)
		if err != nil {
			fmt.Printf("%v, err: %v\n", ConsumerConsumeErr, err)
		} // 阻塞在这里
		if c.ctx.Err() != nil {
			return
		}
	}
}

func (c *Consumer) monitoring() {
	for {
		select { // 这里不太好模拟，暂时执行不到
		case err, ok := <-c.consumerGroup.Errors():
			if ok {
				c.err(err)
			}
		}
		time.Sleep(time.Second)
	}
}

type consumerGroupHandler struct {
	eventFunc HandleEventFunc
}

func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		ce := &ConsumerEvent{}
		if err := json.Unmarshal(message.Value, ce); err != nil {
			fmt.Printf("json.Unmarshal err: %v\n", err)
		}
		ce.Topic = message.Topic
		ce.Partition = message.Partition
		ce.Offset = message.Offset
		ce.Time = time.Now().Format("2006-01-02 15:04:05")

		// 此处错误被写入channel
		if err := h.eventFunc(ce); err != nil {
			return err
		}
		// 更新位移
		sess.MarkMessage(message, "")
	}

	return nil
}

type ConsumerEvent struct {
	Topic     string          `json:"topic,omitempty"`
	Partition int32           `json:"partition,omitempty"`
	Offset    int64           `json:"offset,omitempty"`
	Type      string          `json:"type,omitempty"`
	Time      string          `json:"time,omitempty"`
	From      string          `json:"from,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
}

func getConsumers(brokers []string) int {
	defaultNum := 10
	num := len(brokers)
	switch {
	case num <= 3:
		return 3
	case num <= 6:
		return 6
	case num <= 9:
		return 9
	default:
		return defaultNum
	}
}

type consumerConfig struct{}

func (*consumerConfig) getConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	cfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange

	return cfg
}
