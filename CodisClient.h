//
// Created by admin on 2019-01-15.
//

#ifndef CPPSERVER_CODISCLIENT_H
#define CPPSERVER_CODISCLIENT_H

#include "commen.h"
#include "CodisConfig.h"
#include "redis_client/RedisClient.h"
#include "zk_children_watcher/ZKChildrenWatcher.h"
#include <unordered_map>
#include <atomic>

class CodisClient {
    CodisConfig codisConfig;
    REDIS_CONFIG innerRedisPoolConf;
    ZKChildrenWatcher childrenWatcher;

//    std::unordered_map<std::string, std::shared_ptr<RedisClient>> poolList;
    std::vector<std::shared_ptr<RedisClient> > poolList;
    boost::shared_mutex poolListSMtx;
    std::atomic<std::int64_t> roundRobinIndex;

    std::function<void()> reconnectNotifier;
    std::function<void()> resumeCustomWatcherNotifier;

public:
    CodisClient(const CodisConfig &config);

    void init();

    void initRoundRobinRedisPool();

    std::vector<std::shared_ptr<RedisClient> > *getRedisPool();

    std::shared_ptr<RedisClient> RoundRobinRedisPool();

    void proxyWatcher();

    std::shared_ptr<RedisClient> getRedisClient(const std::string &clusterAddr);

    long getTotalWaitConnNum();

    long getTotalIdleConnNum();

    bool isHealthy();

    void setZKReconnectNotifier(const std::function<void()> &func) { childrenWatcher.setReconnectNotifier(func); }

    void setZKResumeCustomWatcherNotifier(const std::function<void()> &func) {
        childrenWatcher.setResumeCustomWatcherNotifier(func);
    }
};


#endif //CPPSERVER_CODISCLIENT_H
