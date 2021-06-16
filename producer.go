package go_mq

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

var version = sarama.V0_10_2_0

type HandleErrorFunc func(error)

var (
	ProducerClosedErr = fmt.Errorf("%v", "生产者启动失败")
	SendMsgFailedErr  = fmt.Errorf("%v", "发送消息失败")
)

func NewAsyncProducer(brokers []string) (AsyncPusher, error) {
	return newAsyncProducer(brokers)
}

func newAsyncProducer(brokers []string) (AsyncPusher, error) {
	producer := &AsyncProducer{}
	cfg := producer.config.getConfig()
	p, err := sarama.NewAsyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("%v, err: %v", ProducerClosedErr, err)
	}
	producer.producer = p

	go producer.monitoring()

	return producer, nil
}

func NewSyncProducer(brokers []string) (SyncPusher, error) {
	return newSyncProducer(brokers)
}

func newSyncProducer(brokers []string) (SyncPusher, error) {
	producer := &SyncProducer{}
	cfg := producer.config.getConfig()
	p, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("%v, err: %v", ProducerClosedErr, err)
	}
	producer.producer = p

	return producer, nil
}

type producerConfig struct{}

func (*producerConfig) getConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Net.KeepAlive = 60 * time.Second
	cfg.Producer.Return.Successes = true // 同步推送需要这个配置
	cfg.Producer.Return.Errors = true
	cfg.Version = version
	cfg.Producer.Flush.Frequency = time.Second
	cfg.Producer.Flush.MaxMessages = 10

	return cfg
}

type AsyncProducer struct {
	producer sarama.AsyncProducer
	err      HandleErrorFunc
	config   *producerConfig
}

func (ap *AsyncProducer) AsyncPush(topic, eventType string, data interface{}) {
	msg := newEventProducer(eventType, data)
	b, _ := json.Marshal(msg)
	ap.send(topic, b)
}

func (ap *AsyncProducer) Close() error {
	return ap.producer.Close()
}

func (ap *AsyncProducer) HandleErrorFunc(err HandleErrorFunc) {
	ap.err = err
}

func (ap *AsyncProducer) send(topic string, data []byte) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}
	ap.producer.Input() <- msg
}

type SyncProducer struct {
	producer sarama.SyncProducer
	config   *producerConfig
}

func (sp *SyncProducer) SyncPush(topic, eventType string, data interface{}) {
	msg := newEventProducer(eventType, data)
	b, _ := json.Marshal(msg)
	sp.send(topic, b)
}

func (sp *SyncProducer) Close() error {
	return sp.producer.Close()
}

func (sp *SyncProducer) send(topic string, data []byte) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}
	_, _, err := sp.producer.SendMessage(msg)
	if err != nil {
		fmt.Printf("%v, err: %v", SendMsgFailedErr, err)
		return
	}
}

type producerEvent struct {
	Type string      `json:"type,omitempty"`
	From string      `json:"from,omitempty"`
	Time string      `json:"time,omitempty"`
	Data interface{} `json:"data,omitempty"`
}

func newEventProducer(eventType string, data interface{}) *producerEvent {
	return &producerEvent{
		Type: eventType,
		Time: time.Now().Format("2006-01-02 15:04:05"),
		From: InternalIP(),
		Data: data,
	}
}

func (ap *AsyncProducer) monitoring() {
	for {
		select {
		case _, _ = <-ap.producer.Successes(): // 此处不做处理
		case err, ok := <-ap.producer.Errors():
			if ok {
				ap.err(err)
			}
		}
		time.Sleep(time.Second)
	}
}
