package go_mq

// 异步推送
type AsyncPusher interface {
	AsyncPush(topic, eventType string, data interface{})
	HandleErrorFunc(err HandleErrorFunc)
	Close() error // 使用的时候一定要记得关闭
}

// 同步推送
type SyncPusher interface {
	SyncPush(topic, eventType string, data interface{})
	Close() error
}

// 消费者组
type GroupPuller interface {
	GroupPull(topic string, eventFunc HandleEventFunc)
	HandleErrorFunc(err HandleErrorFunc)
	Close()
}

// 消费者,如果向topic添加新分区,则不会通知消费者
type Puller interface {
	Pull(topic string, eventFunc HandleEventFunc)
}
