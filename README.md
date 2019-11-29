# kafka_cpp
a kafka cpp wrapper of librdkafka

---
> librdkafka是个非常优秀的c/c++的kafka client，但是笔者本人使用起来，感觉接口层面还是可以更简单点，故而基于librdkafka写个个cpp wrapper, 使得用户使用起来更加方便，接口更加简单友好, 下面简单描述下封装好的接口

### 1. topic消费者 kafka_consumer

#### 1.1 消费者事件回调
```
    class kafka_consumer_event_handler
    {
    public:
        virtual ~kafka_consumer_event_handler() {}

    public:
        /** events */
        /** all brokers down notify */
        virtual void    on_consume_all_brokers_down_notify() {}

        /** Event is an error condition */
        virtual void    on_consume_error(const std::string& event_str, const std::string& error_desc) {}

        /** Event is a statistics JSON document */
        virtual void    on_consume_status(const std::string& status) {}

        /** Event is a log message */
        virtual void    on_consume_log(int32_t log_level, const std::string& fac, const std::string& msg) {}

        /** Event is a throttle level signaling from the broker */
        virtual void    on_consume_throttle(int32_t throttle_time, const std::string& broker_name, int32_t broker_id) {}

        /** unknow event */
        virtual void    on_consume_uknow_event(int32_t event_type, const std::string& event_str, const std::string& error_desc) {}

        /** consume events */
        /** on consume time out */

        /** on consume msg */
        virtual void    on_consume_msg(RdKafka::Message* message){}

        /** on partition eof(last message) */
        virtual void    on_consume_partition_eof(int32_t parition, int32_t parition_count){}

        /** on consume failed */
        virtual void    on_consume_failed(const std::string& error_desc) {}
    };
```

我们需要重载kafka_consumer_event_handler， 然后重写相应的事件函数, 注意是实现 "on_consume_msg(RdKafka::Message* message)" 接口， 这个是表示收到了kafka的消息

#### 1.2 消费者接口
```
	/**   
     * @brief 设置消费者事件回调函数
     */
    void    set_event_handler(kafka_consumer_event_handler* handler);

    /** 
     * @brief 订阅topic
     */
    bool    subscribe(const std::string& topic_name, const consume_msg_handler& msg_handler);

    /** 
     * @brief 订阅多个topic
     */
    bool    subscribe(const std::vector<std::string>& topic_list,  const std::vector<consume_msg_handler>& msg_handler_list);
```

> 有一点要注意的是，kafka_consumer的订阅是覆盖式的，不是增量式的；比如开始订阅了a、b两个topic，之后又订阅了c、d两个topic，那么这个消费者，最后订阅的topic只有c、d，而不是a、b、c、d

### 2. topic生产者 kafka_producer

#### 2.1 生产者事件回调
```
class kafka_producer_event_handler
{
public:
    virtual ~kafka_producer_event_handler(){}

public:
    /** events */

    /** all brokers down notify */
    virtual void    on_produce_all_brokers_down_notify(){}

    /** Event is an error condition */
    virtual void    on_produce_error(const std::string& event_str, const std::string& error_desc){}

    /** Event is a statistics JSON document */
    virtual void    on_produce_status(const std::string& status) {}

    /** Event is a log message */
    virtual void    on_produce_log(int32_t log_level, const std::string& fac, const std::string& msg){}

    /** Event is a throttle level signaling from the broker */
    virtual void    on_produce_throttle(int32_t throttle_time, const std::string& broker_name, int32_t broker_id) {}

    /** unknow event */
    virtual void    on_produce_uknow_event(int32_t event_type, const std::string& event_str, const std::string& error_desc) {}

    /** event message delivery*/
    virtual void    on_produce_msg_delivered(RdKafka::Message& message){}
};
```
用户需要重载kafka_producer_event_handler接口

#### 2.2 生产者接口
```
	/**
     * @brief 设置生产者的事件回调
     */
    void    set_event_handler(kafka_producer_event_handler* handler);
   
    /** 
     * @brief 生产一个消息
     */
    bool    produce_msg(const std::string& topic_name, const std::string& msg, const std::string* key, std::string* err_string);
    
	/** 
     * @brief 生产一个消息
     */
	bool    produce_msg(const std::string& topic_name, int32_t partition, const std::string& msg, const std::string* key, std::string* err_string);
    
	/** 
     * @brief 获取所有topic元数据
     */
	bool    get_all_topic_metadata(RdKafka::Metadata** metadata, std::string* err_string);
    
	/**
     * @brief 获取自定topic的元数据
	 */
	bool    get_topic_metadata(const std::string& topic_name, class RdKafka::Metadata** metadata, std::string* err_string);

    /**
     * @brief 获取当前发送队列里面还有多个消息
     */
    int32_t out_queue_len();

```

### 3. 使用例子
使用实例详见 examples/test_1.cpp