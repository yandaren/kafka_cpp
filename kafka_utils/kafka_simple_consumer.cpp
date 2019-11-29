#include "kafka_simple_consumer.h"
#include "kafka_consumer_event_handler.h"
#include "kafka_thread_pool.hpp"
#include "kafka_ip_utils.hpp"

namespace utility
{

kafka_simple_consumer::kafka_simple_consumer(const kafka_simple_consumer_options& options, int32_t work_thread_count)
    : m_event_handler(nullptr)
    , m_options(options)
    , m_global_conf(nullptr)
    , m_default_topic_conf(nullptr)
    , m_work_thread_pool(nullptr)
    , m_total_partition_count(0) {

    m_global_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    m_default_topic_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

    m_options.broker_list = utility::broker_list_from_domain(m_options.broker_list);

    std::string err_string;
    m_global_conf->set("metadata.broker.list", m_options.broker_list, err_string);

    if (m_options.group_id != "") {
        m_global_conf->set("group.id", m_options.group_id, err_string);
        m_default_topic_conf->set("auto.offset.reset", "smallest", err_string);
    }

    if (m_options.use_sasl) {
        m_global_conf->set("security.protocol", "sasl_plaintext", err_string);
        m_global_conf->set("sasl.mechanisms", "PLAIN", err_string);
        m_global_conf->set("sasl.username", m_options.sasl_username.c_str(), err_string);
        m_global_conf->set("sasl.password", m_options.sasl_password.c_str(), err_string);
    }

    if (!m_options.debug.empty()) {
        m_global_conf->set("debug", m_options.debug, err_string);
    }

    m_global_conf->set("dr_cb", (RdKafka::DeliveryReportCb*)this, err_string);
    m_global_conf->set("event_cb", (RdKafka::EventCb*)this, err_string);

    m_consumer = RdKafka::Consumer::create(m_global_conf, err_string);
    m_topic = RdKafka::Topic::create(m_consumer, m_options.topic_name, m_default_topic_conf, err_string);

    m_work_thread_pool = new kafka_thread_pool(std::bind(&kafka_simple_consumer::tick_func, this), work_thread_count);
}

kafka_simple_consumer::~kafka_simple_consumer() {
    stop();

    if (m_work_thread_pool) {
        delete m_work_thread_pool;
        m_work_thread_pool = nullptr;
    }

    if (m_consumer) {
        delete m_consumer;
        m_consumer = nullptr;
    }

    if (m_topic) {
        delete m_topic;
        m_topic = nullptr;
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

void    kafka_simple_consumer::set_event_handler(kafka_consumer_event_handler* handler) {
    m_event_handler = handler;
}

void    kafka_simple_consumer::start() {
    m_consumer->start(m_topic, m_options.partition, m_options.start_offset);
    m_work_thread_pool->start();
}

void    kafka_simple_consumer::stop() {
    m_work_thread_pool->stop();
}

void    kafka_simple_consumer::wait_for_stop() {
    return m_work_thread_pool->join_all();
}

void    kafka_simple_consumer::event_cb(RdKafka::Event &event) {
    switch (event.type())
    {
    case RdKafka::Event::EVENT_ERROR:
    {
        //if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN) {
        //    run = false;
        //}

        if (m_event_handler) {
            if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN) {
                m_event_handler->on_consume_all_brokers_down_notify();
            }

            std::string event_str = std::move(event.str());
            std::string error_desc = std::move(RdKafka::err2str(event.err()));

            m_event_handler->on_consume_error(event_str, error_desc);
        }

        break;
    }
    case RdKafka::Event::EVENT_STATS:
    {
        if (m_event_handler) {
            std::string event_str(std::move(event.str()));

            m_event_handler->on_consume_status(event_str);
        }

        break;
    }

    case RdKafka::Event::EVENT_LOG:
    {
        if (m_event_handler) {
            std::string fac(std::move(event.fac()));
            std::string msg(std::move(event.str()));

            m_event_handler->on_consume_log((int32_t)event.severity(), fac, msg);
        }

        break;
    }
    case RdKafka::Event::EVENT_THROTTLE:
    {
        if (m_event_handler) {
            m_event_handler->on_consume_throttle((int32_t)event.throttle_time(), event.broker_name(), event.broker_id());
        }

        break;
    }
    default:
    {
        if (m_event_handler) {
            std::string event_str(std::move(event.str()));
            std::string error_desc(std::move(RdKafka::err2str(event.err())));

            m_event_handler->on_consume_uknow_event(event.type(), event_str, error_desc);
        }

        break;
    }
    }
}

bool    kafka_simple_consumer::msg_consume(RdKafka::Message* message, void* opaque) {
    bool ret = false;
    switch (message->err())
    {
    case RdKafka::ERR__TIMED_OUT:
    {
        //if (m_event_handler) {
        //    m_event_handler->on_consume_time_out();
        //}

        break;
    }

    case RdKafka::ERR_NO_ERROR:
    {
        ret = true;

        if (m_event_handler) {
            m_event_handler->on_consume_msg(message);
        }

        break;
    }

    case RdKafka::ERR__PARTITION_EOF:
    {
        ret = true;

        if (m_event_handler) {
            m_event_handler->on_consume_partition_eof(message->partition(), m_total_partition_count);
        }

        break;
    }

    case RdKafka::ERR__UNKNOWN_TOPIC:
    case RdKafka::ERR__UNKNOWN_PARTITION:
    {
        if (m_event_handler) {
            std::string error_desc(std::move(message->errstr()));
            m_event_handler->on_consume_failed(error_desc);
        }

        break;
    }
    default:
    {
        /* Errors */
        if (m_event_handler) {
            std::string error_desc(std::move(message->errstr()));
            m_event_handler->on_consume_failed(error_desc);
        }

        break;
    }
    }

    return ret;
}

bool    kafka_simple_consumer::tick_func() {
    RdKafka::Message *msg = m_consumer->consume(m_topic, m_options.partition, 2000);
    bool ret = msg_consume(msg, NULL);
    delete msg;

    return ret;
}

} // end namespace utility