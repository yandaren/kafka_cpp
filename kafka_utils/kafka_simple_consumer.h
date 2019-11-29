/**
 * @brief kafka simple consumer
 *
 * librdkafka simple consumer wrapper
 *
 * @author  :   yandaren1220@126.com
 * @date    :   2019-04-26
 */

#ifndef __utility_common_kafka_simple_consumer_h__
#define __utility_common_kafka_simple_consumer_h__

#include "kafka_common.h"
#include <string>
#include <vector>
#include <rdkafkacpp.h>

namespace utility
{
class kafka_consumer_event_handler;
class kafka_thread_pool;
struct kafka_simple_consumer_options
{
    std::string broker_list;
    bool        use_sasl;
    std::string sasl_username;
    std::string sasl_password;
    std::string group_id;
    std::string topic_name;
    int64_t     start_offset;
    int32_t     partition;
    std::string debug;

    kafka_simple_consumer_options() 
        : use_sasl(false)
        , start_offset(RdKafka::Topic::OFFSET_INVALID)
        , partition(RdKafka::Topic::PARTITION_UA){
    }
};

class kafka_simple_consumer :
    public RdKafka::EventCb
{
protected:
    kafka_thread_pool*              m_work_thread_pool;
    kafka_consumer_event_handler*   m_event_handler;
    kafka_simple_consumer_options   m_options;
    RdKafka::Conf*                  m_global_conf;
    RdKafka::Conf*                  m_default_topic_conf;
    RdKafka::Consumer*              m_consumer;
    RdKafka::Topic*                 m_topic;
    int32_t                         m_total_partition_count;

public:
    kafka_simple_consumer(const kafka_simple_consumer_options& options, int32_t work_thread_count = 1);
    ~kafka_simple_consumer();

public:
    void    set_event_handler(kafka_consumer_event_handler* handler);
    void    start();
    void    stop();
    void    wait_for_stop();

protected:
    /** implement the interface from EventCb */
    void    event_cb(RdKafka::Event &event) override;

protected:
    bool    msg_consume(RdKafka::Message* message, void* opaque);
    bool    tick_func();

};

} // end namespace utility

#endif