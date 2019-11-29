#include "kafka_consumer.h"
#include "kafka_consumer_event_handler.h"
#include "kafka_thread_pool.hpp"
#include "kafka_ip_utils.hpp"

#ifdef _WIN32
#define snprintf _snprintf
#endif

namespace utility
{

kafka_consumer::kafka_consumer(const kafka_consumer_options& options, int32_t work_thread_count)
    : m_event_handler(nullptr)
    , m_options(options)
    , m_global_conf(nullptr)
    , m_default_topic_conf(nullptr)
    , m_total_partition_count(0){

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

    m_global_conf->set("default_topic_conf", m_default_topic_conf, err_string);
    m_global_conf->set("dr_cb", (RdKafka::DeliveryReportCb*)this, err_string);
    m_global_conf->set("event_cb", (RdKafka::EventCb*)this, err_string);
    m_global_conf->set("rebalance_cb", (RdKafka::RebalanceCb*)this, err_string);

    m_consumer = RdKafka::KafkaConsumer::create(m_global_conf, err_string);
    m_work_thread_pool = new kafka_thread_pool(std::bind(&kafka_consumer::tick_func, this), work_thread_count);
}

kafka_consumer::~kafka_consumer() {
    stop();

    if (m_work_thread_pool) {
        delete m_work_thread_pool;
        m_work_thread_pool = nullptr;
    }

    if (m_consumer) {
        delete m_consumer;
        m_consumer = nullptr;
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

void    kafka_consumer::set_event_handler(kafka_consumer_event_handler* handler) {
    m_event_handler = handler;
}

bool    kafka_consumer::subscribe(const std::string& topic_name, const consume_msg_handler& msg_handler) {
    std::vector<std::string> topic_list;
    topic_list.push_back(topic_name);

    // save the topc handler first
    {
        std::lock_guard<std::mutex> locker(m_mtx);
        m_consume_msg_handler_map[topic_name] = std::make_shared<consume_msg_handler>(msg_handler);
    }

    auto res = m_consumer->subscribe(topic_list);
    return res == RdKafka::ERR_NO_ERROR;
}

bool    kafka_consumer::subscribe(const std::vector<std::string>& topic_list, const std::vector<consume_msg_handler>& msg_handler_list) {

    if (topic_list.size() != msg_handler_list.size()) {
        return false;
    }

    // save the topc handler first
    {
        std::lock_guard<std::mutex> locker(m_mtx);

        for (int32_t i = 0; i < topic_list.size(); ++i) {
            m_consume_msg_handler_map[topic_list[i]] = std::make_shared<consume_msg_handler>(msg_handler_list[i]);
        }
    }

    auto res = m_consumer->subscribe(topic_list);
    return res == RdKafka::ERR_NO_ERROR;
}

kafka_consumer::consume_msg_handler_ptr kafka_consumer::get_topic_handler(const std::string& topic_name) {
    std::lock_guard<std::mutex> locker(m_mtx);

    auto iter = m_consume_msg_handler_map.find(topic_name);
    if (iter != m_consume_msg_handler_map.end()) {
        return iter->second;
    }

    return consume_msg_handler_ptr();
}

void    kafka_consumer::start() {
    m_work_thread_pool->start();
}

void    kafka_consumer::stop() {
    m_work_thread_pool->stop();
}

void    kafka_consumer::wait_for_stop() {
    m_work_thread_pool->join_all();
}

bool    kafka_consumer::tick_func() {
    RdKafka::Message *msg = m_consumer->consume(1000);
    bool ret = msg_consume(msg, NULL);
    delete msg;

    return ret;
}

bool    kafka_consumer::msg_consume(RdKafka::Message* message, void* opaque) {
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

        std::string topic_name(std::move(message->topic_name()));

        auto msg_handler = get_topic_handler(topic_name);
        if (msg_handler) {
            (*msg_handler)(topic_name, message->partition(), message->offset(), 
                message->key(),
                static_cast<const char *>(message->payload()), static_cast<int32_t>(message->len()));
            break;
        }

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

void    kafka_consumer::event_cb(RdKafka::Event &event) {
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


void    kafka_consumer::rebalance_cb(RdKafka::KafkaConsumer *consumer,
    RdKafka::ErrorCode err,
    std::vector<RdKafka::TopicPartition*> &partitions) {

    log_msg(RdKafka::Event::EVENT_SEVERITY_INFO, "rebalance_cb: %s, partitions_count: %d",
        RdKafka::err2str(err).c_str(), (int32_t)partitions.size());

    for (int32_t i = 0; i < partitions.size(); ++i) {
        auto& part = partitions[i];

        log_msg(RdKafka::Event::EVENT_SEVERITY_INFO, "topic[%s] partition[%d] offset[%lld]",
            part->topic().c_str(), part->partition(), part->offset());
    }

    int64_t default_start_offset = RdKafka::Topic::OFFSET_STORED;

    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
        for (unsigned int i = 0; i < partitions.size(); i++) {
            auto& tpp = partitions[i];
            tpp->set_offset(default_start_offset);

            log_msg(RdKafka::Event::EVENT_SEVERITY_INFO, "set topic[%s] partition[%d] offset as [%lld]",
                tpp->topic().c_str(), tpp->partition(), tpp->offset());
        }

        consumer->assign(partitions);

        m_total_partition_count = (int32_t)partitions.size();
    }
    else {
        consumer->unassign();
    }
}

void    kafka_consumer::log_msg(int32_t log_level, const char* format, ...) {
    char buffer[max_log_len + 1];
    char* log_buff = buffer;
    va_list ap;
    va_start(ap, format);
    int32_t len = vsnprintf(0, 0, format, ap);
    va_end(ap);

    if (len > max_log_len) {
        log_buff = (char*)malloc(len + 1);
    }
    va_start(ap, format);
    len = vsprintf(log_buff, format, ap);
    va_end(ap);

    if (m_event_handler) {
        std::string fac = "kafka_consumer";
        std::string msg(buffer, len);
        m_event_handler->on_consume_log(log_level, fac, msg);
    }

    if (log_buff != buffer) {
        free(log_buff);
    }
}

} // end namespace utility