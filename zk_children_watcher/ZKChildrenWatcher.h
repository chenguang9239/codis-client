//
// Created by admin on 2019-01-15.
//

#ifndef CPPSERVER_ZKCHILDRENWATCHER_H
#define CPPSERVER_ZKCHILDRENWATCHER_H

#include "ZKConfig.h"
#include "commen.h"
#include <CppZooKeeper/CppZooKeeper.h>
#include <functional>
#include <memory>
#include <atomic>
#include <boost/thread/shared_mutex.hpp>

class ZKChildrenWatcher {
    ZKConfig config;
    std::atomic<std::int64_t> roundRobinIndex;
    boost::shared_mutex valueListSMtx;

    CppZooKeeper::ZookeeperManager zkClient;
    CppZooKeeper::ScopedStringVector stringVector;

    std::function<void()> funcOnChildrenChange;

    std::shared_ptr<CppZooKeeper::WatcherFuncType> globalWatherPtr;

    bool needToInitValueList;

    std::function<void()> reconnectNotifier;
    std::function<void()> resumeGlobalWatcherNotifier;
    std::function<void()> resumeCustomWatcherNotifier;
    std::function<void()> resumeEphemeralNodeNotifier;

public:
    std::vector<std::string> valueList;
    std::vector<std::string> additionalValueList;
    std::vector<std::string> deletedValueList;

    ZKChildrenWatcher(const ZKConfig &ZKConfig);

    void init();

    bool globalWatcherFunc(CppZooKeeper::ZookeeperManager &zkCli, int type, int state, const char *path);

    bool watcherFunc(CppZooKeeper::ZookeeperManager &zkCli, int type, int state, const char *path);

    void initValueList();

    std::string getNodeValue(const std::string &path);

    std::string RoundRobinValueList();

    void setChildrenWatcher(std::function<void()> func);

    std::vector<std::string> vectorMinus(const std::vector<std::string> &a, const std::vector<std::string> &b);

    std::unordered_set<std::string> getFinder(const std::vector<std::string> &a);

    void setReconnectNotifier(const std::function<void()> &func) { reconnectNotifier = func; }

    void setResumeGlobalWatcherNotifier(const std::function<void()> &func) { resumeGlobalWatcherNotifier = func; }

    void setResumeCustomWatcherNotifier(const std::function<void()> &func) { resumeCustomWatcherNotifier = func; }

    void setResumeEphemeralNodeNotifier(const std::function<void()> &func) { resumeEphemeralNodeNotifier = func; }

    void innerReconnectNotifier();

    void innerResumeGlobalWatcherNotifier();

    void innerResumeCustomWatcherNotifier();

    void innerResumeEphemeralNodeNotifier();
};


#endif //CPPSERVER_ZKCHILDRENWATCHER_H
