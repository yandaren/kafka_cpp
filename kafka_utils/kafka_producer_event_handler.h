/**
 * @brief kafka producer
 *
 * librdkafka producer wrapper
 *
 * @author  :   yandaren1220@126.com
 * @date    :   2019-04-26
 */

#ifndef __utility_common_kafka_producer_event_handler_h__
#define __utility_common_kafka_producer_event_handler_h__

#include "kafka_common.h"
#include <rdkafkacpp.h>
#include <string>

namespace utility
{
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
}

#endif