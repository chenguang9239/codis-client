//
// Created by admin on 2019-01-15.
//

#ifndef CPPSERVER_CODISCONFIG_H
#define CPPSERVER_CODISCONFIG_H

#include "redis_client/RedisConfig.h"
#include "zk_children_watcher/ZKConfig.h"

class CodisConfig {
public:
    RedisConfig redisConfig;
    ZKConfig zkConfig;

    CodisConfig() = default;
    CodisConfig(const RedisConfig &redisConfig, const ZKConfig &zkConfig);
};


#endif //CPPSERVER_CODISCONFIG_H
