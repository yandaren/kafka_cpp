/**
 * @brief kafka some default defines
 *
 *
 * @author  :   yandaren1220@126.com
 * @date    :   2019-04-26
 */

#ifndef __utility_common_kafka_default_define_hpp__
#define __utility_common_kafka_default_define_hpp__

#include "kafka_common.h"
#include <rdkafkacpp.h>
#include <time.h>
#include <random>
#include <functional>

namespace utility
{
    class munual_partitioner : public RdKafka::PartitionerCb 
    {
    protected:
        int32_t m_target_partition;

    public:
        munual_partitioner(int32_t partition) : m_target_partition(partition) {
        }

    public:
        int32_t partitioner_cb(const RdKafka::Topic *topic, const std::string *key,
            int32_t partition_cnt, void *msg_opaque) override {
            return m_target_partition;
        }
    };

    class random_partitioner : public RdKafka::PartitionerCb
    {
    protected:
        std::default_random_engine  m_rand_engine;
    public:
        random_partitioner(){
            m_rand_engine.seed((uint32_t)time(NULL));
        }

    public:
        int32_t partitioner_cb(const RdKafka::Topic *topic, const std::string *key,
            int32_t partition_cnt, void *msg_opaque) override {
            if (partition_cnt == 0) {
                return 0;
            }

            return rand_num(partition_cnt - 1);
        }

    protected:
        int32_t rand_num(int32_t up) {
            return std::uniform_int_distribution<int32_t>(0, up)(m_rand_engine);
        }
    };

    class round_robin_partitioner : public RdKafka::PartitionerCb
    {
    protected:
        int32_t m_cur_partition;
    public:
        round_robin_partitioner() : m_cur_partition(0) {
        }

    public:
        int32_t partitioner_cb(const RdKafka::Topic *topic, const std::string *key,
            int32_t partition_cnt, void *msg_opaque) override {
            if (m_cur_partition >= partition_cnt) {
                m_cur_partition = 0;
            }

            return m_cur_partition++;
        }
    };


    class custom_hash_partitioner : public RdKafka::PartitionerCb
    {
    public:
        typedef std::function<uint32_t(const char*, int32_t)> hash_func_type;

    protected:
        hash_func_type  m_hash_func;

    public:
        custom_hash_partitioner(){
            m_hash_func = custom_hash_partitioner::djb_hash;
        }

        custom_hash_partitioner(const hash_func_type& func) 
            :m_hash_func(func){
        }

    public:
        int32_t partitioner_cb(const RdKafka::Topic *topic, const std::string *key,
            int32_t partition_cnt, void *msg_opaque) override {
            if (!key) {
                return 0;
            }

            if (partition_cnt == 0) {
                return 0;
            }

            uint32_t hash_code = (m_hash_func)(key->c_str(), (int32_t)key->size());
            return int32_t(hash_code % partition_cnt);
        }

    protected:
        static inline uint32_t djb_hash(const char *str, size_t len) {
            uint32_t hash = 5381;
            for (size_t i = 0; i < len; i++)
                hash = ((hash << 5) + hash) + str[i];
            return hash;
        }
    };

} // end namespace utility

#endif