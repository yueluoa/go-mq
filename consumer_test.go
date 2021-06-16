package go_mq

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func Test_ConsumerGroup(t *testing.T) {
	consumer, err := NewConsumerGroup([]string{"139.155.53.10:9092"}, "group3")
	if err != nil {
		fmt.Println(err)
	}
	defer consumer.Close()
	consumer.HandleErrorFunc(func(err error) {
		fmt.Printf("[HandleErrorFunc]err: %v", err)
	})
	for {
		consumer.GroupPull("med_dts_b_convert", func(event *ConsumerEvent) error {
			h, _ := json.Marshal(event)
			fmt.Println(string(h))
			return nil
		})
		time.Sleep(time.Second)
	}
}

func Test_Consumer(t *testing.T) {
	consumer, err := NewConsumer([]string{"139.155.53.10:9092"})
	if err != nil {
		fmt.Println(err)
	}
	for true {
		consumer.Pull("med_dts_b_convert", func(event *ConsumerEvent) error {
			h, _ := json.Marshal(event)
			fmt.Println(string(h))
			return nil

		})
		time.Sleep(time.Second)
	}
}
