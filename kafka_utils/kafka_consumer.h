/**
 * @brief kafka consumer
 *
 * librdkafka consumer wrapper
 *
 * @author  :   yandaren1220@126.com
 * @date    :   2019-04-26
 */

#ifndef __utility_common_kafka_consumer_h__
#define __utility_common_kafka_consumer_h__

#include "kafka_common.h"
#include <stdarg.h>
#include <string>
#include <vector>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <rdkafkacpp.h>

namespace utility {

class kafka_consumer_event_handler;
class kafka_thread_pool;
struct kafka_consumer_options
{
    std::string broker_list;
    bool        use_sasl;
    std::string sasl_username;
    std::string sasl_password;
    std::string group_id;
    std::string debug;

    kafka_consumer_options() : use_sasl(false) {
    }
};

class kafka_consumer :
    public RdKafka::EventCb,
    public RdKafka::RebalanceCb
{
public:
    typedef std::function<void(const std::string& topic_name, int32_t partition, int64_t offset, const std::string* key, const char* msg, int32_t msg_len)> consume_msg_handler;
    typedef std::shared_ptr<consume_msg_handler> consume_msg_handler_ptr;

protected:
    enum {
        max_log_len = 1023,
    };

protected:
    /* <topic_name, msg_handler > */
    typedef std::unordered_map<std::string, consume_msg_handler_ptr> consume_msg_handler_map_type;

    kafka_thread_pool*              m_work_thread_pool;
    kafka_consumer_event_handler*   m_event_handler;
    kafka_consumer_options          m_options;
    RdKafka::Conf*                  m_global_conf;
    RdKafka::Conf*                  m_default_topic_conf;
    RdKafka::KafkaConsumer*         m_consumer;
    int32_t                         m_total_partition_count;
    std::mutex                      m_mtx;
    consume_msg_handler_map_type    m_consume_msg_handler_map;

public:
    kafka_consumer(const kafka_consumer_options& options, int32_t work_thread_count = 1);
    ~kafka_consumer();

public:
    void    set_event_handler(kafka_consumer_event_handler* handler);
    bool    subscribe(const std::string& topic_name, const consume_msg_handler& msg_handler);
    bool    subscribe(const std::vector<std::string>& topic_list,  const std::vector<consume_msg_handler>& msg_handler_list);
    void    start();
    void    stop();
    void    wait_for_stop();

protected:
    /** implement the interface from EventCb */
    void    event_cb(RdKafka::Event &event) override;

    /** implement the interface from RebalanceCb */
    void    rebalance_cb(RdKafka::KafkaConsumer *consumer,
        RdKafka::ErrorCode err,
        std::vector<RdKafka::TopicPartition*> &partitions) override;

protected:
    bool    msg_consume(RdKafka::Message* message, void* opaque);
    consume_msg_handler_ptr get_topic_handler(const std::string& topic_name);
    bool    tick_func();
    void    log_msg(int32_t log_level, const char* format, ...);
};

} // end namespace utility

#endif