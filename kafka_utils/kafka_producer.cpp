#include "kafka_producer.h"
#include "kafka_producer_event_handler.h"
#include "kafka_thread_pool.hpp"
#include "kafka_ip_utils.hpp"

namespace utility
{

std::string kafka_producer::error_to_string(int32_t error_code){
    return RdKafka::err2str((RdKafka::ErrorCode)error_code);
}

kafka_producer::kafka_producer(const kafka_producer_options& options, int32_t work_thread_count)
    : m_event_handler(nullptr)
    , m_options(options)
    , m_global_conf(nullptr)
    , m_default_topic_conf(nullptr)
    , m_producer(nullptr){
    m_global_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    m_default_topic_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

    m_options.broker_list = utility::broker_list_from_domain(m_options.broker_list);

    std::string err_string;

    if (m_options.partitioner_cb) {
        m_default_topic_conf->set("partitioner_cb", m_options.partitioner_cb, err_string);
    }

    m_global_conf->set("metadata.broker.list", m_options.broker_list, err_string);

    if (m_options.use_sasl) {
        m_global_conf->set("security.protocol", "sasl_plaintext", err_string);
        m_global_conf->set("sasl.mechanisms", "PLAIN", err_string);
        m_global_conf->set("sasl.username", m_options.sasl_username.c_str(), err_string);
        m_global_conf->set("sasl.password", m_options.sasl_password.c_str(), err_string);
    }

    if (!m_options.debug.empty()) {
        m_global_conf->set("debug", m_options.debug, err_string);
    }

    m_global_conf->set("default_topic_conf", m_default_topic_conf, err_string);
    m_global_conf->set("dr_cb", (RdKafka::DeliveryReportCb*)this, err_string);
    m_global_conf->set("event_cb", (RdKafka::EventCb*)this, err_string);

    m_producer = RdKafka::Producer::create(m_global_conf, err_string);

    m_work_thread_pool = new kafka_thread_pool(std::bind(&kafka_producer::tick_func, this), work_thread_count);
}

kafka_producer::~kafka_producer() {
    if (m_producer) {
        delete m_producer;
        m_producer = nullptr;
    }

    if (m_global_conf) {
        delete m_global_conf;
        m_global_conf = nullptr;
    }

    if (m_default_topic_conf) {
        delete m_default_topic_conf;
        m_default_topic_conf = nullptr;
    }
}

void    kafka_producer::set_event_handler(kafka_producer_event_handler* handler) {
    m_event_handler = handler;
}

bool kafka_producer::produce_msg(const std::string& topic_name, const std::string& msg, const std::string* key, std::string* err_string) {
    return produce_msg(topic_name, RdKafka::Topic::PARTITION_UA, msg, key, err_string);
}

bool kafka_producer::produce_msg(const std::string& topic_name, int32_t partition, const std::string& msg, const std::string* key, std::string* err_string) {
    if (!m_producer) {
        return false;
    }

    auto res = m_producer->produce(topic_name, partition,
        RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
        /* Value */
        const_cast<char *>(msg.c_str()), msg.size(),
        /* Key */
        key ? key->c_str() : NULL, key ? key->size() : 0,
        /* Timestamp (defaults to now) */
        0,
        /* Message headers, if any */
        NULL,
        /* Per-message opaque value passed to
        * delivery report */
        NULL);

    if (res != RdKafka::ERR_NO_ERROR) {
        if (err_string) {
            *err_string = RdKafka::err2str(res);
        }

        return false;
    }

    return true;
}

void    kafka_producer::dr_cb(RdKafka::Message& message) {
    if (m_event_handler) {
        m_event_handler->on_produce_msg_delivered(message);
    }
}

void    kafka_producer::event_cb(RdKafka::Event &event) {
    switch (event.type())
    {
    case RdKafka::Event::EVENT_ERROR: 
    {
        //if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN) {
        //    run = false;
        //}

        if (m_event_handler) {
            if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN) {
                m_event_handler->on_produce_all_brokers_down_notify();
            }

            std::string event_str = std::move(event.str());
            std::string error_desc = std::move(RdKafka::err2str(event.err()));

            m_event_handler->on_produce_error(event_str, error_desc);
        }

        break;
    }
    case RdKafka::Event::EVENT_STATS:
    {
        if (m_event_handler) {
            std::string event_str(std::move(event.str()));

            m_event_handler->on_produce_status(event_str);
        }

        break;
    }

    case RdKafka::Event::EVENT_LOG:
    {
        if (m_event_handler) {
            std::string fac(std::move(event.fac()));
            std::string msg(std::move(event.str()));

            m_event_handler->on_produce_log((int32_t)event.severity(), fac, msg);
        }

        break;
    }
    case RdKafka::Event::EVENT_THROTTLE:
    {
        if (m_event_handler) {
            m_event_handler->on_produce_throttle((int32_t)event.throttle_time(), event.broker_name(), event.broker_id());
        }

        break;
    }
    default:
    {
        if (m_event_handler) {
            std::string event_str(std::move(event.str()));
            std::string error_desc(std::move(RdKafka::err2str(event.err())));

            m_event_handler->on_produce_uknow_event(event.type(), event_str, error_desc);
        }

        break;
    }
    }
}


bool    kafka_producer::get_all_topic_metadata(RdKafka::Metadata** metadata, std::string* err_string) {
    auto res = m_producer->metadata(true, NULL, metadata, 5000);

    if (res != RdKafka::ERR_NO_ERROR && err_string) {
        *err_string = RdKafka::err2str(res);
    }

    return res == RdKafka::ERR_NO_ERROR;
}

bool    kafka_producer::get_topic_metadata(const std::string& topic_name, class RdKafka::Metadata** metadata, std::string* err_string) {
    std::string err_string_inner;
    RdKafka::Topic* topic = nullptr;
    bool ret = true;

    do {
        topic = RdKafka::Topic::create(m_producer, topic_name, m_default_topic_conf, err_string_inner);
        if (!topic) {
            ret = false;
            break;
        }

        auto res = m_producer->metadata(false, topic,
            metadata, 5000);

        if (res != RdKafka::ERR_NO_ERROR) {
            ret = false;
            err_string_inner = RdKafka::err2str(res);
        }
    } while (0);

    if (topic) {
        delete topic;
    }

    if (err_string) {
        *err_string = err_string_inner;
    }

    return ret;
}

int32_t kafka_producer::out_queue_len() {
    return (int32_t)m_producer->outq_len();
}

void    kafka_producer::start() {
    m_work_thread_pool->start();
}

void    kafka_producer::stop() {
    m_work_thread_pool->stop();
}

void    kafka_producer::wait_for_stop() {
    m_work_thread_pool->join_all();
}

bool    kafka_producer::tick_func() {
    int32_t event_count = m_producer->poll(0);
    return event_count > 0;
}

}   // end namespace utility

