/** 
 * @brief ip地址获取器
 *
 * @author  :   yandaren1220@126.com
 * @date    :   2018-05-18
 */

#ifndef __utility_ip_getter_h__
#define __utility_ip_getter_h__

#include <stdint.h>
#include "utility/asio_base/asio_standalone.hpp"
#include <asio.hpp>
#include <vector>

namespace utility
{
namespace ip_getter
{
namespace v4
{
    static std::vector<std::string> get_ip_list(const char* domain) {
        std::vector<std::string> ip_list;
        std::string hostname = domain ? domain : asio::ip::host_name();

        asio::io_service io_service;
        asio::ip::tcp::resolver resolver(io_service);
        asio::ip::tcp::resolver::query qr(hostname, "");
        asio::ip::tcp::resolver::iterator iter = resolver.resolve(qr);
        asio::ip::tcp::resolver::iterator end;
        while (iter != end) {
            asio::ip::tcp::endpoint ep = *iter++;
            if (ep.address().is_v4()) {
                ip_list.push_back(ep.address().to_string());
            }
        }
        return ip_list;
    }

    /** 
     * @brief check is local area network ip(private use)
     */
    static bool is_lan_ip(const unsigned long ip) {
        // 检查3类地址(私有地址)
        // A类：10.0.0.0 ~ 10.255.255.255         [10.0.0.0/8]
        // B类：172.16.0.0 ~ 172.31.255.255       [172.16.0.0/12]
        // C类：192.168.0.0 ~ 192.168.255.255     [192.168.0.0/16]
        return(
            (ip & 0xff000000) == 0xa000000  || // 10.0.0.0/8
            (ip & 0xfff00000) == 0xac100000 || // 172.16.0.0/12
            (ip & 0xffff0000) == 0xc0a80000    // 192.168.0.0/16
            );
    }

    /** 
     * @brief 是否是保留地址, 保留地址(包含private use 地址)
     */
    static bool is_reserved_ip(const unsigned long ip) {
        /** 
         * see: https://www.iana.org/assignments/iana-ipv4-special-registry/iana-ipv4-special-registry.xhtml
         * format, x.x.x.x/y, y是掩码位数, 从高位算起, 计算一个地址z是否属于该段地址
         * 只要 z & (y对应的掩码) == x.x.x.x 即可
         * 
         * IANA IPv4            Special-Purpose Address Registry
         * 0.0.0.0/8            "This host on this network"
         * 10.0.0.0/8           Private-Use
         * 100.64.0.0/10        Shared Address Space
         * 127.0.0.0/8          Loopback
         * 169.254.0.0/16       Link Local
         * 172.16.0.0/12        Private-Use
         * 192.0.0.0/24         IETF Protocol Assignments
         * 192.0.0.0/29         IPv4 Service Continuity Prefix
         * 192.0.0.8/32         IPv4 dummy address
         * 192.0.0.9/32         Port Control Protocol Anycast
         * 192.0.0.10/32        Traversal Using Relays around NAT Anycast
         * 192.0.0.170/32       NAT64/DNS64 Discovery
         * 192.0.0.171/32       NAT64/DNS64 Discovery
         * 192.0.2.0/24         Documentation (TEST-NET-1)
         * 192.31.196.0/24      AS112-v4
         * 192.52.193.0/24      AMT
         * 192.88.99.0/24       Deprecated (6to4 Relay Anycast)
         * 192.168.0.0/16       Private-Use
         * 192.175.48.0/24      Direct Delegation AS112 Service
         * 198.18.0.0/15        Benchmarking
         * 198.51.100.0/24      Documentation (TEST-NET-2)
         * 203.0.113.0/24       Documentation (TEST-NET-3)
         * 240.0.0.0/4          Reserved
         * 255.255.255.255/32   Limited Broadcast
         */

        return(
            (ip & 0xff000000) == 0x0        || // 0.0.0.0/8
            (ip & 0xff000000) == 0xa000000  || // 10.0.0.0/8
            (ip & 0xffc00000) == 0x64400000 || // 100.64.0.0/10
            (ip & 0xff000000) == 0x7f000000 || // 127.0.0.0/8
            (ip & 0xffff0000) == 0xa9fe0000 || // 169.254.0.0/16
            (ip & 0xfff00000) == 0xac100000 || // 172.16.0.0/12
            (ip & 0xffffff00) == 0xc0000000 || // 192.0.0.0/24
            (ip & 0xfffffff8) == 0xc0000000 || // 192.0.0.0/29
            (ip & 0xffffffff) == 0xc0000008 || // 192.0.0.8/32
            (ip & 0xffffffff) == 0xc0000009 || // 192.0.0.9/32
            (ip & 0xffffffff) == 0xc000000a || // 192.0.0.10/32
            (ip & 0xffffffff) == 0xc00000aa || // 192.0.0.170/32
            (ip & 0xffffffff) == 0xc00000ab || // 192.0.0.171/32
            (ip & 0xffffff00) == 0xc0000200 || // 192.0.2.0/24
            (ip & 0xffffff00) == 0xc01fc400 || // 192.31.196.0/24
            (ip & 0xffffff00) == 0xc034c100 || // 192.52.193.0/24
            (ip & 0xffffff00) == 0xc0586300 || // 192.88.99.0/24
            (ip & 0xffff0000) == 0xc0a80000 || // 192.168.0.0/16
            (ip & 0xffffff00) == 0xc0af3000 || // 192.175.48.0/24
            (ip & 0xfffe0000) == 0xc6120000 || // 198.18.0.0/15
            (ip & 0xffffff00) == 0xc6336400 || // 198.51.100.0/24
            (ip & 0xffffff00) == 0xcb007100 || // 203.0.113.0/24
            (ip & 0xf0000000) == 0xf0000000 || // 240.0.0.0/4
            (ip & 0xffffffff) == 0xffffffff    // 255.255.255.255/32
            );
    }

    static std::vector<unsigned long> get_lan_ip_list() {
        std::vector<unsigned long> ip_list;
        asio::io_service io_service;
        asio::ip::tcp::resolver resolver(io_service);
        asio::ip::tcp::resolver::query qr(asio::ip::host_name(), "");
        asio::ip::tcp::resolver::iterator iter = resolver.resolve(qr);
        asio::ip::tcp::resolver::iterator end;
        while (iter != end) {
            asio::ip::tcp::endpoint ep = *iter++;
            if (ep.address().is_v4()) {
                unsigned long ip_int = ep.address().to_v4().to_ulong();
                if (v4::is_lan_ip(ip_int)) {
                    ip_list.push_back(ip_int);
                }
            }
        }
        return ip_list;
    }

    static std::vector<std::string> get_lan_ip_string_list() {
        std::vector<std::string> ip_list;
        asio::io_service io_service;
        asio::ip::tcp::resolver resolver(io_service);
        asio::ip::tcp::resolver::query qr(asio::ip::host_name(), "");
        asio::ip::tcp::resolver::iterator iter = resolver.resolve(qr);
        asio::ip::tcp::resolver::iterator end;
        while (iter != end) {
            asio::ip::tcp::endpoint ep = *iter++;
            if (ep.address().is_v4()) {
                unsigned long ip_int = ep.address().to_v4().to_ulong();
                if (v4::is_lan_ip(ip_int)) {
                    ip_list.push_back(ep.address().to_string());
                }
            }
        }
        return ip_list;
    }

    static std::vector<unsigned long> get_wan_ip_list() {
        std::vector<unsigned long> ip_list;
        asio::io_service io_service;
        asio::ip::tcp::resolver resolver(io_service);
        asio::ip::tcp::resolver::query qr(asio::ip::host_name(), "");
        asio::ip::tcp::resolver::iterator iter = resolver.resolve(qr);
        asio::ip::tcp::resolver::iterator end;
        while (iter != end) {
            asio::ip::tcp::endpoint ep = *iter++;
            if (ep.address().is_v4()) {
                unsigned long ip_int = ep.address().to_v4().to_ulong();
                if (!v4::is_reserved_ip(ip_int)) {
                    ip_list.push_back(ip_int);
                }
            }
        }
        return ip_list;
    }

    static std::vector<std::string> get_wan_ip_string_list() {
        std::vector<std::string> ip_list;
        asio::io_service io_service;
        asio::ip::tcp::resolver resolver(io_service);
        asio::ip::tcp::resolver::query qr(asio::ip::host_name(), "");
        asio::ip::tcp::resolver::iterator iter = resolver.resolve(qr);
        asio::ip::tcp::resolver::iterator end;
        while (iter != end) {
            asio::ip::tcp::endpoint ep = *iter++;
            if (ep.address().is_v4()) {
                unsigned long ip_int = ep.address().to_v4().to_ulong();
                if (!v4::is_reserved_ip(ip_int)) {
                    ip_list.push_back(ep.address().to_string());
                }
            }
        }
        return ip_list;
    }
}
}
}

#endif