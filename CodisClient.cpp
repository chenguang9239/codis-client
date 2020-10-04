//
// Created by admin on 2019-01-15.
//

#include "CodisClient.h"
#include "Utils.h"
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

CodisClient::CodisClient(const CodisConfig &config) :
        childrenWatcher(config.zkConfig) {
    roundRobinIndex = -1;

    innerRedisPoolConf.connect_timeout = config.redisConfig.connTimeout;
    innerRedisPoolConf.net_readwrite_timeout = config.redisConfig.socketTimeout;
    innerRedisPoolConf.num_redis_socks = config.redisConfig.connPoolSize;
    innerRedisPoolConf.connect_failure_retry_delay = 1;
    innerRedisPoolConf.reader_buf_max_size = 1024 * 1024; // 1M
}

void CodisClient::init() {
    childrenWatcher.setChildrenWatcher(std::bind(&CodisClient::proxyWatcher, this));
    childrenWatcher.init();
}

void CodisClient::initRoundRobinRedisPool() {
    boost::property_tree::ptree pt;
    std::vector<std::shared_ptr<RedisClient>> additionalPoolList;
    for (auto &value : childrenWatcher.additionalValueList) {
        if (value.empty()) {
            LOG_ERROR << "codis proxy data empty! valueList size: " << childrenWatcher.valueList.size();
            continue;
        }
        while (value.back() != '}') value.pop_back();
        LOG_DEBUG << value;
        std::stringstream ss;
        ss << value;
        try {
            boost::property_tree::read_json(ss, pt);
            if (pt.get<std::string>("state") == "online") {
                auto addr = pt.get<std::string>("addr");
                additionalPoolList.emplace_back(getRedisClient(pt.get<std::string>("addr")));
            }
        } catch (std::exception &e) {
            LOG_ERROR << "parse codis proxy addr error: " << e.what();
        }
    }

    // 先删除， 后追加
    if (!childrenWatcher.additionalValueList.empty() || !childrenWatcher.deletedValueList.empty()) {
        // write lock
        boost::unique_lock<boost::shared_mutex> g(poolListSMtx);
        size_t tmpSize = childrenWatcher.valueList.size();

        if (!childrenWatcher.valueList.empty() && !childrenWatcher.deletedValueList.empty()) {
            auto finder = childrenWatcher.getFinder(childrenWatcher.deletedValueList);
            for (size_t i = 0; i < tmpSize;) {
                if (finder.count(childrenWatcher.valueList[i]) > 0)
                    poolList[i] = std::move(poolList[--tmpSize]);
                else ++i;
            }
            poolList.resize(tmpSize);
            LOG_SPCL << "delete " << childrenWatcher.deletedValueList.size() << " redis client";
        }

        if (!additionalPoolList.empty()) {
            size_t appendSize = additionalPoolList.size();
            std::move(additionalPoolList.begin(), additionalPoolList.end(),
                      std::inserter(poolList, poolList.end()));
            LOG_SPCL << "append " << appendSize << " new redis client";
        }

        if (poolList.empty()) {
            LOG_ERROR << "no valid codis proxy!";
        } else {
            LOG_SPCL << "init redis pool ok, redis pool number: " << poolList.size();
        }
    } else { LOG_SPCL << "no node to be added or deleted"; }
    LOG_SPCL << "codis redis client number: " << poolList.size();
}

std::shared_ptr<RedisClient> CodisClient::RoundRobinRedisPool() {
    boost::shared_lock<boost::shared_mutex> g(poolListSMtx);
    if (poolList.empty()) return std::shared_ptr<RedisClient>();
    // todo 优化？
    return poolList[++roundRobinIndex % poolList.size()];
//    roundRobinIndex = (roundRobinIndex + 1) % poolList.size();
//    return poolList[roundRobinIndex];
}

void CodisClient::proxyWatcher() {
//    childrenWatcher.updateValueList();
    initRoundRobinRedisPool();
}

std::shared_ptr<RedisClient> CodisClient::getRedisClient(const std::string &clusterAddr) {
    std::string host, port;
    std::vector<std::string> addrs = Utils::splitString(clusterAddr, ",");
    // boost::shared_array<REDIS_ENDPOINT> endpoints(new REDIS_ENDPOINT[addrs.size()]);
    std::vector<REDIS_ENDPOINT> endpoints(addrs.size());
    // addrs 是proxy地址，目前没有发现addrs有多个proxy地址的情况
    LOG_SPCL << "redis cluster size(proxy num): " << addrs.size();
    size_t i = 0;
    for (auto &addr : addrs) {
        host = Utils::getHost(addr);
        port = Utils::getPort(addr);
        memset(endpoints[i].host, 0, sizeof(endpoints[i].host));
        memcpy(endpoints[i].host, host.c_str(), host.size());
        endpoints[i].host[host.size()] = '\0';
        endpoints[i].port = std::stoi(port);
        LOG_SPCL << "codis proxy host: " << endpoints[i].host << ", codis proxy port: " << endpoints[i].port;
        ++i;
    }
    innerRedisPoolConf.num_endpoints = addrs.size();
    // conf.endpoints = endpoints.get();
    innerRedisPoolConf.endpoints = &(endpoints.at(0));

    std::shared_ptr<RedisClient> res = std::make_shared<RedisClient>(innerRedisPoolConf);
    if (!res->checkAllSocketConnected()) LOG_FATAL(std::string("cannot connect to codis proxy: ") + clusterAddr);
    return res;
}

std::vector<std::shared_ptr<RedisClient> > *CodisClient::getRedisPool() {
    return nullptr;
}

long CodisClient::getTotalWaitConnNum() {
    boost::shared_lock<boost::shared_mutex> g(poolListSMtx);
    long res = 0;
    for (auto &e : poolList) {
        res += e->getWaitingNum();
    }
    return res;
}

long CodisClient::getTotalIdleConnNum() {
    boost::shared_lock<boost::shared_mutex> g(poolListSMtx);
    long res = 0;
    for (auto &e : poolList) {
        res += e->getIdleNum();
    }
    return res;
}

bool CodisClient::isHealthy() {
    boost::shared_lock<boost::shared_mutex> g(poolListSMtx);
    bool res = false;
    for (auto &e : poolList) {
        res |= e->isHealthy();
    }
    return res;
}
