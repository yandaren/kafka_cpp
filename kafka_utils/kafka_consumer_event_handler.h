/**
* @brief kafka consumer event handler
*
* librdkafka consumer event handler
*
* @author  :   yandaren1220@126.com
* @date    :   2019-04-26
*/

#ifndef __utility_common_kafka_consumer_event_handler_h__
#define __utility_common_kafka_consumer_event_handler_h__

#include "kafka_common.h"
#include <rdkafkacpp.h>

namespace utility
{
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
}

#endif