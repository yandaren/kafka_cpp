/**
 * @brief kafka common define
 *
 * @author  :   yandaren1220@126.com
 * @date    :   2019-05-10
 */

#ifndef __utility_common_kafka_ip_utils_hpp__
#define __utility_common_kafka_ip_utils_hpp__

#include <utility/ip_getter.hpp>
#include <utility/str.hpp>
#include <string>
#include <vector>
#include <sstream>

namespace utility {

    // broker ip address list is lick  "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094";
    // broker ip host list is like "localhost:9092,localhost:9092,localhost:9093"
    static std::string broker_list_from_domain(const std::string& broker_list) {
        std::stringstream ss;
        bool is_first = true;
        std::vector<std::string> broker_list_split;
        utility::str::string_splits(broker_list.c_str(), ",", broker_list_split);
        for (auto broker : broker_list_split) {
            std::vector<std::string> addr_split;
            utility::str::string_splits(broker.c_str(), ":", addr_split);
            if (addr_split.size() == 2) {
                std::vector<std::string> ip_list = 
                    utility::ip_getter::v4::get_ip_list(addr_split[0].c_str());
                for (auto ip : ip_list) {
                    if (is_first) {
                        is_first = false;
                    }
                    else {
                        ss << ",";
                    }

                    ss << ip << ":" << addr_split[1];
                }
            }
        }

        return std::move(ss.str());
    }
}

#endif