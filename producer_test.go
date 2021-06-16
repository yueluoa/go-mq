package go_mq

import (
	"fmt"
	"testing"
)

func Test_AsyncProducer(t *testing.T) {
	producer, err := NewAsyncProducer([]string{"139.155.53.10:9092"})
	if err != nil {
		fmt.Println(err)
	}
	defer producer.Close()
	producer.HandleErrorFunc(func(err error) {
		fmt.Printf("[HandleErrorFunc]err: %v", err)
	})
	for i := 0; i < 5; i++ {
		producer.AsyncPush("med_dts_b_convert", "test", "qwertyuio")
	}
}

func Test_SyncProducer(t *testing.T) {
	producer, err := NewSyncProducer([]string{"139.155.53.10:9092"})
	if err != nil {
		fmt.Println(err)
	}
	defer producer.Close()
	producer.SyncPush("med_dts_b_convert", "test", "fjalasjfjafklafa")
}
