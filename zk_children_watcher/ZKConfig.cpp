//
// Created by admin on 2019-01-14.
//

#include "ZKConfig.h"
#include "limits.h"

ZKConfig::ZKConfig() {
    recvTimeout = 3000;
    connTimeout = 3000;
    userResumeAlertCount = 3;
    userResumeMaxCount = 5;
    userReconnectAlertCount = 3;
    userReconnectMaxCount = INT_MAX;
}

//ZKConfig::ZKConfig(const std::string &zkAddress,
//                   const std::string &zkPath,
//                   int recvTimeout,
//                   int connTimeout,
//                   const std::string &zkClientLogPath,
//                   int zkClientLogLevel,
//                   const std::string &zkLogPath,
//                   int zkLogLevel) :
//        clientLogPath(zkClientLogPath),
//        logPath(zkLogPath),
//        address(zkAddress),
//        path(zkPath),
//        recvTimeout(recvTimeout),
//        connTimeout(connTimeout),
//        clientLogLevel(zkClientLogLevel),
//        logLevel(zkLogLevel) {
//
//}