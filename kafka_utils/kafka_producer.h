/**
 * @brief kafka producer
 *
 * librdkafka producer wrapper
 *
 * @author  :   yandaren1220@126.com
 * @date    :   2019-04-26
 */

#ifndef __utility_common_kafka_producer_h__
#define __utility_common_kafka_producer_h__

#include "kafka_common.h"
#include <string>
#include <rdkafkacpp.h>
#include <string>

namespace utility {

class kafka_producer_event_handler;
class kafka_thread_pool;
struct kafka_producer_options {
    std::string broker_list;
    bool        use_sasl;
    std::string sasl_username;
    std::string sasl_password;
    std::string debug;
    RdKafka::PartitionerCb* partitioner_cb;

    kafka_producer_options() : use_sasl(false), partitioner_cb(nullptr){
    }
};

class kafka_producer : 
    public RdKafka::EventCb,
    public RdKafka::DeliveryReportCb
{
protected:
    kafka_thread_pool*              m_work_thread_pool;
    kafka_producer_event_handler*   m_event_handler;
    kafka_producer_options          m_options;
    RdKafka::Conf*                  m_global_conf;
    RdKafka::Conf*                  m_default_topic_conf;
    RdKafka::Producer*              m_producer;

public:
    static std::string error_to_string(int32_t error_code);

public:
    kafka_producer(const kafka_producer_options& options, int32_t work_thread_count = 1);
    ~kafka_producer();

public:
    void    set_event_handler(kafka_producer_event_handler* handler);
    bool    produce_msg(const std::string& topic_name, const std::string& msg, const std::string* key, std::string* err_string);
    bool    produce_msg(const std::string& topic_name, int32_t partition, const std::string& msg, const std::string* key, std::string* err_string);
    bool    get_all_topic_metadata(RdKafka::Metadata** metadata, std::string* err_string);
    bool    get_topic_metadata(const std::string& topic_name, class RdKafka::Metadata** metadata, std::string* err_string);
    int32_t out_queue_len();
    void    start();
    void    stop();
    void    wait_for_stop();

protected:
    /* implement the interface from DeliveryReportCb **/
    void    dr_cb(RdKafka::Message& message) override;

    /** implenet the interface from EventCb */
    void    event_cb(RdKafka::Event &event) override;

private:
    bool    tick_func();
};

} // end namespace utility

#endif