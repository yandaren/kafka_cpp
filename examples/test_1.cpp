#include "kafka_utils/kafka_simple_consumer.h"
#include "kafka_utils/kafka_consumer.h"
#include "kafka_utils/kafka_producer.h"
#include "kafka_utils/kafka_consumer_event_handler.h"
#include "kafka_utils/kafka_producer_event_handler.h"
#include "kafka_utils/kafka_default_define.hpp"
#include <stdio.h>
#include <stdint.h>
#include <iostream>
#include <string>

namespace test1 {
    static std::string broker_list = "192.168.94.92:9092,192.168.94.92:9093,192.168.94.92:9094";
    static std::string username = "alice";
    static std::string password = "alice-secret";

    static void metadata_print(const std::string &topic,
        const RdKafka::Metadata *metadata) {

        if (!metadata) {
            printf("try metadata_print for topic: %s failed.\n", topic.empty() ? "all topic" : topic.c_str());
            return;
        }
        printf("Metadata for %s ( from broker %d:%s)\n",
            topic.empty() ? "all topic" : topic.c_str(),
            metadata->orig_broker_id(), metadata->orig_broker_name().c_str());

        /* Iterate brokers */
        printf("brokers(%d):\n", (int32_t)metadata->brokers()->size());
        RdKafka::Metadata::BrokerMetadataIterator ib;
        for (ib = metadata->brokers()->begin();
            ib != metadata->brokers()->end();
            ++ib) {
            printf("broker[%d] at %s:%d\n", (*ib)->id(), (*ib)->host().c_str(), (*ib)->port());
        }
        /* Iterate topics */
        printf("topics(%d):\n", (int32_t)metadata->topics()->size());
        RdKafka::Metadata::TopicMetadataIterator it;
        for (it = metadata->topics()->begin();
            it != metadata->topics()->end();
            ++it) {

            printf("    topic\"%s\" with %d partitions:",
                (*it)->topic().c_str(), (int32_t)(*it)->partitions()->size());

            if ((*it)->err() != RdKafka::ERR_NO_ERROR) {
                printf("  %s", err2str((*it)->err()).c_str());
                if ((*it)->err() == RdKafka::ERR_LEADER_NOT_AVAILABLE)
                    printf(" (try again)");
            }
            printf("\n");

            /* Iterate topic's partitions */
            RdKafka::TopicMetadata::PartitionMetadataIterator ip;
            for (ip = (*it)->partitions()->begin();
                ip != (*it)->partitions()->end();
                ++ip) {
                printf("      partition %d, leader %d, replicas:", (*ip)->id(), (*ip)->leader());

                /* Iterate partition's replicas */
                RdKafka::PartitionMetadata::ReplicasIterator ir;
                for (ir = (*ip)->replicas()->begin();
                    ir != (*ip)->replicas()->end();
                    ++ir) {

                    printf("%s%d", (ir == (*ip)->replicas()->begin() ? "" : ","), *ir);
                }

                /* Iterate partition's ISRs */
                printf(", isrs: ");
                RdKafka::PartitionMetadata::ISRSIterator iis;
                for (iis = (*ip)->isrs()->begin(); iis != (*ip)->isrs()->end(); ++iis)
                    printf("%s%d", (iis == (*ip)->isrs()->begin() ? "" : ","), *iis);

                if ((*ip)->err() != RdKafka::ERR_NO_ERROR)
                    printf(", %s\n", RdKafka::err2str((*ip)->err()).c_str());
                else
                    printf("\n");
            }
        }
    }

    class producer_msg_handler : public utility::kafka_producer_event_handler {
    public:
        producer_msg_handler(){}

    public:
        /** all brokers down notify */
        virtual void    on_produce_all_brokers_down_notify() override{
            printf("producer_msg_handler::on_produce_all_brokers_down_notify\n");
        }

        /** Event is an error condition */
        virtual void    on_produce_error(const std::string& event_str, const std::string& error_desc) override{
            printf("producer_msg_handler::on_produce_error, event_str[%s] error_desc[%s]\n",
                event_str.c_str(), error_desc.c_str());
        }

        /** Event is a statistics JSON document */
        virtual void    on_produce_status(const std::string& status) override{
            printf("producer_msg_handler::on_produce_status {%s}\n", status.c_str());
        }

        /** Event is a log message */
        virtual void    on_produce_log(int32_t log_level, const std::string& fac, const std::string& msg) override{
            printf("producer_msg_handler::on_produce_log, log_level[%d][%s] %s\n",
                log_level, fac.c_str(), msg.c_str());
        }

        /** Event is a throttle level signaling from the broker */
        virtual void    on_produce_throttle(int32_t throttle_time, const std::string& broker_name, int32_t broker_id) override{
            printf("producer_msg_handler::on_produce_throttle, throttle_time[%d] broker_name[%s] broker_id[%d]\n",
                throttle_time, broker_name.c_str(), broker_id);
        }

        /** unknow event */
        virtual void    on_produce_uknow_event(int32_t event_type, const std::string& event_str, const std::string& error_desc) override{
            printf("producer_msg_handler::on_produce_uknow_event, event_type[%d] event_str[%s] error_desc[%s]\n",
                event_type, event_str.c_str(), error_desc.c_str());
        }

        /** event message delivery*/
        virtual void    on_produce_msg_delivered(RdKafka::Message& message) override {
            std::string content((const char*)message.payload(), message.len());
            printf("producer_msg_handler::on_produce_msg_delivered,  delivery %d bytes, error: %s, key: %s, content[%s] len[%d]\n",
                (int32_t)message.len(), message.errstr().c_str(), message.key() ? message.key()->c_str() : "", content.c_str(), (int32_t)content.size());
        }
    };


    void metadate_test() {

        printf("metadate_test\n");

        utility::custom_hash_partitioner hash_partitioner;
        producer_msg_handler event_handler;

        utility::kafka_producer_options options;
        options.broker_list = broker_list;
        options.use_sasl = true;
        options.sasl_username = username;
        options.sasl_password = password;
        options.partitioner_cb = &hash_partitioner;

        utility::kafka_producer producer(options);
        producer.set_event_handler(&event_handler);

        printf("ls                  - print all topics metadata\n");
        printf("lst [topic_name]    - print the specific topic metadata\n");

        std::string cmd;
        while (std::cin >> cmd) {
            if (cmd == "ls") {
                class RdKafka::Metadata *metadata;

                std::string err_string;
                if (!producer.get_all_topic_metadata(&metadata, &err_string)) {
                    printf("try get all topic metadata error, err_desc[%s].\n", err_string.c_str());
                    continue;
                }

                std::string topic_name;
                metadata_print(topic_name, metadata);

                delete metadata;
            }
            else if (cmd == "lst") {
                std::string topic_name;
                std::cin >> topic_name;

                class RdKafka::Metadata *metadata;
                std::string err_string;

                if (!producer.get_topic_metadata(topic_name, &metadata, &err_string)) {
                    printf("try get topic[%s] metadata error, err_desc[%s].\n", 
                        topic_name.c_str(), err_string.c_str());
                    continue;
                }

                metadata_print(topic_name, metadata);

                delete metadata;
            }
            else {
                printf("unsupport cmd[%s]\n", cmd.c_str());
            }
        }
    }

    void produer_test() {
        printf("produer_test\n");

        utility::custom_hash_partitioner hash_partitioner;
        producer_msg_handler event_handler;

        utility::kafka_producer_options options;
        options.broker_list = broker_list;
        options.use_sasl = true;
        options.sasl_username = username;
        options.sasl_password = password;
        options.partitioner_cb = &hash_partitioner;
        //options.debug = "all";
        //options.debug = "topic,msg";
        //options.debug = "topic,msg,queue";

        utility::kafka_producer producer(options);
        producer.set_event_handler(&event_handler);
        producer.start();

        std::string err_string;
        std::string topic_name;
        while (true) {
            printf("input topic to create:\n");
            std::cin >> topic_name;

            printf(">");
            for (std::string line; std::getline(std::cin, line); ) {
                if (line.empty()) {
                    continue;
                }

                if (line == "quit_topic") {
                    break;
                }

                std::string key = line;

                if (!producer.produce_msg(topic_name, line, &key, &err_string)) {
                    printf("try produce msg error, error_desc[%s].\n", err_string.c_str());
                    continue;
                }

                printf("produced msg, msg[%s] bytes[%d], out_queue_len[%d].\n", 
                    line.c_str(), (int32_t)line.size(), producer.out_queue_len());

                printf(">");
            }
        }
    }

    class consumer_msg_hander : public utility::kafka_consumer_event_handler
    {
    public:
        consumer_msg_hander(){}

    public:
        /** events */
        /** all brokers down notify */
        virtual void    on_consume_all_brokers_down_notify() override {
            printf("consumer_msg_hander::on_consume_all_brokers_down_notify.\n");
        }

        /** Event is an error condition */
        virtual void    on_consume_error(const std::string& event_str, const std::string& error_desc) override {
            printf("consumer_msg_hander::on_consume_error, event_str[%s] error_desc[%s].\n",
                event_str.c_str(), error_desc.c_str());
        }

        /** Event is a statistics JSON document */
        virtual void    on_consume_status(const std::string& status) override {
            printf("consumer_msg_hander::on_consume_status, status[%s].\n",
                status.c_str());
        }

        /** Event is a log message */
        virtual void    on_consume_log(int32_t log_level, const std::string& fac, const std::string& msg) override {
            printf("consumer_msg_hander::on_consume_log, log_level[%d][%s] %s.\n",
                log_level, fac.c_str(), msg.c_str());
        }

        /** Event is a throttle level signaling from the broker */
        virtual void    on_consume_throttle(int32_t throttle_time, const std::string& broker_name, int32_t broker_id) override {
            printf("consumer_msg_hander::on_consume_throttle, throttle_time[%d] broker_name[%s] broker_id[%d]\n",
                throttle_time, broker_name.c_str(), broker_id);
        }

        /** unknow event */
        virtual void    on_consume_uknow_event(int32_t event_type, const std::string& event_str, const std::string& error_desc) override {
            printf("consumer_msg_hander::on_consume_uknow_event, event_type[%d] event_str[%s] error_desc[%s].\n",
                event_type, event_str.c_str(), error_desc.c_str());
        }

        /** consume events */
        /** on consume time out */

        /** on consume msg */
        virtual void    on_consume_msg(RdKafka::Message* message) {
            printf("consumer_msg_hander::on_consume_msg, topic_name[%s] partition[%d] offset[%lld] key[%s] content[%s] len[%d].\n",
                message->topic_name().c_str(), message->partition(), message->offset(), message->key() ? message->key()->c_str() : "", (const char *)message->payload(), (int32_t)message->len());
        }

        /** on partition eof(last message) */
        virtual void    on_consume_partition_eof(int32_t parition, int32_t parition_count) override{
            printf("consumer_msg_hander::on_consume_partition_eof, partition[%d] partition_count[%d]\n",
                parition, parition_count);
        }

        /** on consume failed */
        virtual void    on_consume_failed(const std::string& error_desc) override {
            printf("consumer_msg_hander::on_consume_failed, error_desc[%s]\n",
                error_desc.c_str());
        }
    };

    void simple_consumer_test() {

        printf("simple_consumer_test\n");

        utility::kafka_simple_consumer_options options;
        options.broker_list = broker_list;
        options.use_sasl = true;
        options.sasl_username = username;
        options.sasl_password = password;
        options.start_offset = RdKafka::Topic::OFFSET_BEGINNING;
        
        printf("input topic name and partition:\n");
        std::cin >> options.topic_name >> options.partition;

        consumer_msg_hander event_handler;
        utility::kafka_simple_consumer  consumer(options, 1);
        consumer.set_event_handler(&event_handler);
        consumer.start();
        consumer.wait_for_stop();
    }

    void simple_consumer_test_consume_from_end() {
        printf("simple_consumer_test_consume_from_end\n");

        utility::kafka_simple_consumer_options options;
        options.broker_list = broker_list;
        options.use_sasl = true;
        options.sasl_username = username;
        options.sasl_password = password;
        options.start_offset = RdKafka::Topic::OFFSET_END;

        printf("input topic name and partition:\n");
        std::cin >> options.topic_name >> options.partition;

        consumer_msg_hander event_handler;
        utility::kafka_simple_consumer  consumer(options, 1);
        consumer.set_event_handler(&event_handler);
        consumer.start();
        consumer.wait_for_stop();
    }

    void simple_consumer_test_offset_stored() {
        printf("simple_consumer_test_offset_stored\n");

        utility::kafka_simple_consumer_options options;
        options.broker_list = broker_list;
        options.use_sasl = true;
        options.sasl_username = username;
        options.sasl_password = password;
        options.start_offset = RdKafka::Topic::OFFSET_STORED;
        options.group_id = "event_analysis_group";

        printf("input topic name and partition:\n");
        std::cin >> options.topic_name >> options.partition;

        consumer_msg_hander event_handler;
        utility::kafka_simple_consumer  consumer(options, 1);
        consumer.set_event_handler(&event_handler);
        consumer.start();
        consumer.wait_for_stop();
    }

    void custom_topic_consume_handler(const std::string& topic_name, int32_t partition, int64_t offset, const std::string* key, const char* msg, int32_t msg_len) {
        printf("custom_topic_consume_handler, topic_name[%s] partition[%d] offset[%lld] key[%s] content[%s] len[%d].\n",
            topic_name.c_str(), partition, offset, key ? key->c_str() : "", msg, msg_len);
    }

    void hp_consume_test() {
        printf("hp_consume_test\n");

        utility::kafka_consumer_options options;
        options.broker_list = broker_list;
        options.use_sasl = true;
        options.sasl_username = username;
        options.sasl_password = password;
        options.group_id = "event_analysis_group";
        //options.debug = "all";
        //options.debug = "topic,msg";
        //options.debug = "topic,msg,queue";

        consumer_msg_hander event_handler;
        utility::kafka_consumer consumer(options, 1);
        consumer.set_event_handler(&event_handler);

        std::string topic_name;
        printf("input topic name to subscribe:\n");
        std::cin >> topic_name;

        consumer.subscribe(topic_name, custom_topic_consume_handler);
        consumer.start();
        consumer.wait_for_stop();
    }

    void hp_consume_group_test() {
        printf("hp_consume_group_test\n");

        std::string group_name;
        printf("input group_name consume:\n");
        std::cin >> group_name;

        utility::kafka_consumer_options options;
        options.broker_list = broker_list;
        options.use_sasl = true;
        options.sasl_username = username;
        options.sasl_password = password;
        options.group_id = group_name;
        //options.debug = "all";
        //options.debug = "topic,msg";
        //options.debug = "topic,msg,queue";

        consumer_msg_hander event_handler;
        utility::kafka_consumer consumer(options, 1);
        consumer.set_event_handler(&event_handler);

        std::string topic_name;
        printf("input topic name to subscribe:\n");
        std::cin >> topic_name;

        consumer.subscribe(topic_name, custom_topic_consume_handler);
        consumer.start();
        consumer.wait_for_stop();
    }

    void printHex(const char* msg, int32_t msg_len) {
        printf("total len :%d\n", msg_len);
        printf("{\n");
        for (int32_t i = 0; i < msg_len; ++i) {
            printf("%02x", msg[i] & 0xFF);
            if (i % 2 == 1) {
                printf(" ");
            }
        }
        printf("\n}\n");
    }
}

struct test_env_t {
    int32_t     env_id;
    std::string broker_list;
    std::string username;
    std::string password;

    void print() {
        printf("env_id          : %d\n", env_id);
        printf("broker_list     : %s\n", broker_list.c_str());
        printf("username        : %s\n", username.c_str());
        printf("passwd          : %s\n", password.c_str());
        printf("\n");
    }
};

int main() {
    std::vector<test_env_t> env_list = {
        { 0, "192.168.94.92:9092,192.168.94.92:9093,192.168.94.92:9094" , "alice", "alice-secret", },
        { 1, "192.168.94.92:9092,192.168.94.92:9093,192.168.94.92:9094" , "admin", "admin-secret", },
        { 2, "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094" , "alice", "alice-secret", },
        { 3, "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094" , "admin", "admin-secret" },
    };


    for (int32_t i = 0; i < env_list.size(); ++i) {
        env_list[i].print();
    }

    test_env_t df = env_list[3];

    printf("please choose env id:\n");
    int i = 0;
    std::cin >> i;

    if (i >= 0 && i < (int32_t)env_list.size()) {
        df = env_list[i];
    }
    else {
        printf("Manual input env info[broker_list,username,password:\n");
        df.env_id = 4;
        std::cin >> df.broker_list >> df.username >> df.password;
    }
    printf("choose evn:\n");
    df.print();

    test1::broker_list = df.broker_list;
    test1::username = df.username;
    test1::password = df.password;

    printf("kafka_new_test\n");
    printf("0, metadata test\n");
    printf("1, producer test\n");
    printf("2, simple_consumer_test\n");
    printf("3, simple_consumer_test_consume_from_end\n");
    printf("4, simple_consumer_test_offset_stored\n");
    printf("5, hp_consume_test\n");
    printf("6, hp_consume_group_test\n");
    printf("7, game_statistics parser test\n");

    int32_t cmd_i = 0;
    std::cin >> cmd_i;

    if (cmd_i == 0) {
        test1::metadate_test();
    }
    else if (cmd_i == 1) {
        test1::produer_test();
    }
    else if (cmd_i == 2) {
        test1::simple_consumer_test();
    }
    else if (cmd_i == 3) {
        test1::simple_consumer_test_consume_from_end();
    }
    else if (cmd_i == 4) {
        test1::simple_consumer_test_offset_stored();
    }
    else if (cmd_i == 5) {
        test1::hp_consume_test();
    }
    else if (cmd_i == 6) {
        test1::hp_consume_group_test();
    }
    else {
        printf("unsupported cmd[%d].\n", cmd_i);
    }

#ifdef _WIN32
    system("pause");
#endif

    return 0;
}