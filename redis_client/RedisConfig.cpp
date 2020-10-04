//
// Created by admin on 2019-01-15.
//

#include "RedisConfig.h"


RedisConfig::RedisConfig(const std::string &address, const std::string &host, int port, int socketTimeout,
                         int connTimeout, int expireSecond, int connPoolSize, const std::string &clientLogPath,
                         int clientLogLevel) : address(address), host(host), port(port), socketTimeout(socketTimeout),
                                               connTimeout(connTimeout), expireSecond(expireSecond),
                                               connPoolSize(connPoolSize), clientLogPath(clientLogPath),
                                               clientLogLevel(clientLogLevel) {}
