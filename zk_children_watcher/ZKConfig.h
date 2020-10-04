//
// Created by admin on 2019-01-14.
//

#ifndef CPPSERVER_ZKCONFIG_H
#define CPPSERVER_ZKCONFIG_H

#include <string>

class ZKConfig {
public:
    std::string address;
    std::string path;
    std::string value;
    int recvTimeout;
    int connTimeout;
    std::string clientLogPath;
    int clientLogLevel;
    std::string logPath;
    int logLevel;
    int userResumeAlertCount;
    int userResumeMaxCount;
    int userReconnectAlertCount;
    int userReconnectMaxCount;

    ZKConfig();

//    ZKConfig(const std::string &zkAddress,
//             const std::string &zkPath,
//             int recvTimeout,
//             int connTimeout,
//             const std::string &zkClientLogPath,
//             int zkClientLogLevel,
//             const std::string &zkLogPath,
//             int zkLogLevel);
};


#endif //CPPSERVER_ZKCONFIG_H
