//
// Created by admin on 2019-01-15.
//

#include "ZKChildrenWatcher.h"
#include <boost/algorithm/string/trim.hpp>

bool
ZKChildrenWatcher::globalWatcherFunc(CppZooKeeper::ZookeeperManager &zkCli, int type, int state, const char *path) {
    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) { // 第一次连接成功与超时之后的重连成功，会触发ZOO_CONNECTED_STATE
            if (needToInitValueList) { // 不超时的重连成功也会触发会触发ZOO_CONNECTED_STATE
                LOG_SPCL << "first connection success, will call initValueList";
                needToInitValueList = false;
                initValueList();
            }
        } else if (state == ZOO_EXPIRED_SESSION_STATE) { // 超时会触发ZOO_EXPIRED_SESSION_STATE
//            needToInitValueList = true;
        }
    }
    return false;
}

bool ZKChildrenWatcher::watcherFunc(CppZooKeeper::ZookeeperManager &zkCli, int type, int state, const char *path) {
//    LOG_INFO << "子结点事件！！！";
    if (type == ZOO_CHILD_EVENT || (type == CppZooKeeper::RESUME_EVENT && state == CppZooKeeper::RESUME_SUCC)) {
        initValueList();
    } else {
        LOG_ERROR << "not expected event, type: " << type
                  << ", state: " << state << ", path: " << std::string(path ? path : "");
    }
    return false;
}

std::string ZKChildrenWatcher::getNodeValue(const std::string &path) {
    char *buffer = nullptr;
    Stat stat;
    int buf_len = 0;

    int ret = zkClient.Get(path, nullptr, &buf_len, &stat);
    if (ret != ZOK || stat.dataLength <= 0) {
//        LOG_ERROR << "ret: " << ret << ", dataLength: " << stat.dataLength;
        return "";
    }

    buffer = (char *) malloc(sizeof(char) * (stat.dataLength + 1));
    buf_len = stat.dataLength + 1;

    ret = zkClient.Get(path, buffer, &buf_len, &stat);
    if (ret != ZOK || buf_len <= 0) {
//        LOG_ERROR << "ret: " << ret << ", buf_len: " << buf_len;
        if (buffer != nullptr) {
            free(buffer);
            buffer = nullptr;
        }
        return "";
    }

    std::string return_str;
    if (buffer) {
        return_str = std::string(buffer, buf_len);
    } else { LOG_ERROR << "returned path buffer is nullptr"; }

    if (buffer != nullptr) {
        free(buffer);
        buffer = nullptr;
    }

    boost::algorithm::trim_if(return_str, boost::algorithm::is_any_of("/"));
    return return_str;
}

std::string ZKChildrenWatcher::RoundRobinValueList() {
    // read lock
    boost::shared_lock<boost::shared_mutex> g(valueListSMtx);
    if (valueList.empty()) return "";
    // todo more efficient
    return valueList[(++roundRobinIndex) % valueList.size()];
//    roundRobinIndex = (roundRobinIndex + 1) % valueList.size();
//    return valueList[roundRobinIndex];
}

void ZKChildrenWatcher::initValueList() {
    zkClient.GetChildren(config.path, stringVector,
                         std::make_shared<CppZooKeeper::WatcherFuncType>(std::bind(&ZKChildrenWatcher::watcherFunc,
                                                                                   this,
                                                                                   std::placeholders::_1,
                                                                                   std::placeholders::_2,
                                                                                   std::placeholders::_3,
                                                                                   std::placeholders::_4)));
    std::vector<std::string> newValueList;
    LOG_SPCL << "zkPath: " << config.path << ", get children number: " << stringVector.count;
    for (auto i = 0; i < stringVector.count; ++i) {
        newValueList.emplace_back(getNodeValue(config.path + '/' + stringVector.data[i]));
        LOG_SPCL << "node path: " << stringVector.data[i] << ", node data: " << newValueList.back();
    }

    additionalValueList = vectorMinus(newValueList, valueList);
    deletedValueList = vectorMinus(valueList, newValueList);
//    deletedValueList = std::move(newValueList);
    for (const auto &e : additionalValueList) {
        LOG_SPCL << "to be added value: " << e;
    }
    for (const auto &e : deletedValueList) {
        LOG_SPCL << "to be deleted value: " << e;
    }

    if (funcOnChildrenChange != nullptr) funcOnChildrenChange();

    // 先删除， 后追加
    if (!additionalValueList.empty() || !deletedValueList.empty()) {
        // write lock
        boost::unique_lock<boost::shared_mutex> g(valueListSMtx);
        size_t tmpSize = valueList.size();

        if (!valueList.empty() && !deletedValueList.empty()) {
            auto finder = getFinder(deletedValueList);
            for (size_t i = 0; i < tmpSize;) {
                if (finder.count(valueList[i]) > 0)
                    valueList[i] = std::move(valueList[--tmpSize]);
                else ++i;
            }
            valueList.resize(tmpSize);
            LOG_SPCL << "delete " << deletedValueList.size() << " node(s)";
        }

        if (!additionalValueList.empty()) {
            size_t appendSize = additionalValueList.size();
            std::move(additionalValueList.begin(), additionalValueList.end(),
                      std::inserter(valueList, valueList.end()));
            LOG_SPCL << "append " << appendSize << " new node(s)";
        }
//        valueList = std::move(newValueList);
    }
    LOG_SPCL << "zkPath: " << config.path << ", updated children node value list size: " << valueList.size();
}

ZKChildrenWatcher::ZKChildrenWatcher(const ZKConfig &zkConfig) : config(zkConfig) {
//    funcOnChildrenChange = std::bind(&ZKChildrenWatcher::updateValueList, this);
    funcOnChildrenChange = nullptr;
    needToInitValueList = true;
    roundRobinIndex = -1;
    globalWatherPtr = std::make_shared<CppZooKeeper::WatcherFuncType>(std::bind(&ZKChildrenWatcher::globalWatcherFunc,
                                                                                this,
                                                                                std::placeholders::_1,
                                                                                std::placeholders::_2,
                                                                                std::placeholders::_3,
                                                                                std::placeholders::_4));
    reconnectNotifier = std::bind(&ZKChildrenWatcher::innerReconnectNotifier, this);
    resumeGlobalWatcherNotifier = std::bind(&ZKChildrenWatcher::innerResumeGlobalWatcherNotifier, this);
    resumeCustomWatcherNotifier = std::bind(&ZKChildrenWatcher::innerResumeCustomWatcherNotifier, this);
    resumeEphemeralNodeNotifier = std::bind(&ZKChildrenWatcher::innerResumeEphemeralNodeNotifier, this);
}

void ZKChildrenWatcher::init() {
    zkClient.Init(config.address);
//    LOG_INFO << "recvTimeout: " << config.recvTimeout << ", connTimeout: " << config.connTimeout;
    zkClient.SetReconnectOptions(reconnectNotifier, config.userReconnectAlertCount);
    zkClient.SetResumeOptions(resumeEphemeralNodeNotifier, resumeCustomWatcherNotifier,
                              resumeGlobalWatcherNotifier, config.userResumeAlertCount);
    zkClient.SetCallWatcherFuncOnResume(true);
    int i = 0;
    while (++i <= 3) {
        if (ZOK == zkClient.Connect(globalWatherPtr, config.recvTimeout, config.connTimeout)) {
            break;
        } else {
            LOG_ERROR << "connect to zk server error, time(s): " << i << ", zk addr: " << config.address;
        }
    }
}

void ZKChildrenWatcher::setChildrenWatcher(std::function<void()> func) {
    funcOnChildrenChange = func;
}

std::vector<std::string>
ZKChildrenWatcher::vectorMinus(const std::vector<std::string> &a, const std::vector<std::string> &b) {
    std::vector<std::string> res;
    std::unordered_set<std::string> finder(b.begin(), b.end());

    // in a, not in b
    for (auto &e : a) {
        if (finder.count(e) <= 0) res.emplace_back(e);
    }

    return res;
}

std::unordered_set<std::string> ZKChildrenWatcher::getFinder(const std::vector<std::string> &a) {
    return std::unordered_set<std::string>(a.begin(), a.end());
}

void ZKChildrenWatcher::innerReconnectNotifier() {
    LOG_WARN << "reconnection reaches " << config.userReconnectAlertCount
             << " times, zk address: " << config.address << ", zk path: " << config.path;
}

void ZKChildrenWatcher::innerResumeGlobalWatcherNotifier() {
    LOG_WARN << "recovery of global watcher reaches " << config.userResumeAlertCount
             << " times, zk address: " << config.address << ", zk path: " << config.path;
}

void ZKChildrenWatcher::innerResumeCustomWatcherNotifier() {
    LOG_WARN << "recovery of custom watcher reaches " << config.userResumeAlertCount
             << " times, zk address: " << config.address << ", zk path: " << config.path;
}

void ZKChildrenWatcher::innerResumeEphemeralNodeNotifier() {
    LOG_WARN << "recovery of ephemeral node reaches " << config.userResumeAlertCount
             << " times, zk address: " << config.address << ", zk path: " << config.path;
}



