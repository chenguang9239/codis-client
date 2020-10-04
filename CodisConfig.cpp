//
// Created by admin on 2019-01-15.
//

#include "CodisConfig.h"

CodisConfig::CodisConfig(const RedisConfig &redisConfig, const ZKConfig &zkConfig) :
        redisConfig(redisConfig),
        zkConfig(zkConfig) {}
