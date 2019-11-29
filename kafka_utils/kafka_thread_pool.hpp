/**
 * @brief kafka some default defines
 *
 *
 * @author  :   yandaren1220@126.com
 * @date    :   2019-04-28
 */

#ifndef __utility_common_kafka_thread_pool_hpp__
#define __utility_common_kafka_thread_pool_hpp__

#include <thread>
#include <atomic>
#include <functional>
#include <chrono>

namespace utility
{

class kafka_thread_pool
{
public:
    typedef std::function<bool()> tick_func_type;
protected:
    tick_func_type                  m_tick_func;
    int32_t                         m_work_thread_count;
    std::thread**                   m_work_thread_pool;
    std::atomic_bool                m_started;
    volatile bool                   m_stopped;

public:
    kafka_thread_pool(const tick_func_type& func, int32_t thread_count)
        : m_tick_func(func)
        , m_work_thread_pool(nullptr){
        if (thread_count < 0) {
            thread_count = 0;
        }

        m_work_thread_count = thread_count;
        m_started = false;
        m_stopped = false;
    }

    ~kafka_thread_pool() {
        stop();
        join_all();

        if (m_work_thread_pool) {

            for (int32_t i = 0; i < m_work_thread_count; ++i) {
                delete m_work_thread_pool[i];
            }
            delete[] m_work_thread_pool;
        }
    }

public:
    void    start() {
        if (!m_started.exchange(true)) {
            create_threads();
        }
    }

    void    stop() {
        m_stopped = true;
    }

    void    join_all() {
        if (m_work_thread_pool) {
            for (int32_t i = 0; i < m_work_thread_count; ++i) {
                if (m_work_thread_pool[i]->joinable()) {
                    m_work_thread_pool[i]->join();
                }
            }
        }
    }

protected:
    void    create_threads() {
        if (m_work_thread_count > 0) {
            m_work_thread_pool = new std::thread*[m_work_thread_count];
            for (int32_t i = 0; i < m_work_thread_count; ++i) {
                m_work_thread_pool[i] = new std::thread(std::bind(&kafka_thread_pool::event_loop, this, i));
            }
        }
    }

    void    event_loop(int32_t tid) {
        printf("kafka_thread_pool::event_loop tid[%d] start.\n", tid);

        while (!m_stopped) {
            bool ret = (m_tick_func)();
            if (!ret) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }

        printf("kafka_thread_pool::event_loop tid[%d] end.\n", tid);
    }
};


} // end namespace utility

#endif