//
// Created by admin on 2019-01-15.
//

#ifndef CPPSERVER_REDISCONFIG_H
#define CPPSERVER_REDISCONFIG_H

#include <string>

class RedisConfig {
public:
    std::string address;
    std::string host;
    int port;
    int socketTimeout;
    int connTimeout;
    int expireSecond;
    int connPoolSize;
    std::string clientLogPath;
    int clientLogLevel;

    RedisConfig() = default;

    RedisConfig(const std::string &address, const std::string &host, int port, int socketTimeout, int connTimeout,
                int expireSecond, int connPoolSize, const std::string &clientLogPath, int clientLogLevel);
};


#endif //CPPSERVER_REDISCONFIG_H
