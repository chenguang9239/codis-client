#include "RedisClient.h"
#include "clockUtil.h"
#include "reportUtil.h"
#include "Utils.h"
#include <stdarg.h>
#include <cstring>
#include <thread>
#include <functional>

using namespace std;

std::map<int, std::string> REDIS_ERROR = {
        {REDIS_ERR_IO,       "REDIS_ERR_IO"}, /* Error in read or write */
        {REDIS_ERR_EOF,      "REDIS_ERR_EOF"}, /* End of file */
        {REDIS_ERR_PROTOCOL, "REDIS_ERR_PROTOCOL"}, /* Protocol error */
        {REDIS_ERR_OOM,      "REDIS_ERR_OOM"}, /* Out of memory */
        {REDIS_ERR_OTHER,    "REDIS_ERR_OTHER"} /* Everything else... */
};

std::map<int, std::string> REPLY_ERROR = {
        {REDIS_REPLY_STRING,  "REDIS_REPLY_STRING"},
        {REDIS_REPLY_ARRAY,   "REDIS_REPLY_ARRAY"},
        {REDIS_REPLY_INTEGER, "REDIS_REPLY_INTEGER"},
        {REDIS_REPLY_NIL,     "REDIS_REPLY_NIL"},
        {REDIS_REPLY_STATUS,  "REDIS_REPLY_STATUS"},
        {REDIS_REPLY_ERROR,   "REDIS_REPLY_ERROR"}
};

//void RedisClient::checkError(REDIS_SOCKET *redisSocket, bool needToThrow) {
//    std::string exceptionMsg = "redisSocket is nullptr!";
//    if (redisSocket) {
//        auto c = (redisContext *) (redisSocket->conn);
//        exceptionMsg = "redisSocket->conn is nullptr!";
//        if (c) {
//            exceptionMsg = REDIS_ERROR[c->err] + ": " + c->errstr + ", " + std::string(strerror(errno));
//            log_(HPOOL_ERROR_LEVEL | HPOOL_CONS_LEVEL, "%s : exception!!! %s", __func__, exceptionMsg.c_str());
//            if (needToThrow) {
//                if (c->err == REDIS_ERR_IO && errno == EAGAIN)
//                    throw RWTIMEOUT_EXCEPTION(exceptionMsg);
//                else
//                    throw OTHER_EXCEPTION(exceptionMsg);
//            }
//        } else {
//            log_(HPOOL_ERROR_LEVEL | HPOOL_CONS_LEVEL, "%s : exception!!! %s", __func__, exceptionMsg.c_str());
//            if (needToThrow) {
//                throw OTHER_EXCEPTION(exceptionMsg);
//            }
//        }
//    } else {
//        log_(HPOOL_ERROR_LEVEL | HPOOL_CONS_LEVEL, "%s : exception!!! %s", __func__, exceptionMsg.c_str());
//        if (needToThrow) {
//            throw OTHER_EXCEPTION(exceptionMsg);
//        }
//    }
//}

int RedisClient::checkError(REDIS_SOCKET *redisSocket) {
    std::string exceptionMsg = "redisSocket is nullptr!";
    if (redisSocket) {
        auto c = (redisContext *) (redisSocket->conn);
        exceptionMsg = "redisSocket->conn is nullptr!";
        if (c) {
            exceptionMsg = REDIS_ERROR[c->err] + ": " + c->errstr + ", " + Utils::ptrToString(strerror(errno));
            log_(HPOOL_ERROR_LEVEL | HPOOL_CONS_LEVEL, "%s : exception!!! %s", __func__, exceptionMsg.c_str());
            if (c->err == REDIS_ERR_IO && errno == EAGAIN)
                return CLIENT_RWTIMEOUT;
            else
                return CLIENT_ERROR;
        } else {
            log_(HPOOL_ERROR_LEVEL | HPOOL_CONS_LEVEL, "%s : exception!!! %s", __func__, exceptionMsg.c_str());
            return CLIENT_ERROR;
        }
    } else {
        log_(HPOOL_ERROR_LEVEL | HPOOL_CONS_LEVEL, "%s : exception!!! %s", __func__, exceptionMsg.c_str());
        return CLIENT_ERROR;
    }
}

RedisReplyPtr RedisClient::redisCommand(const char *format, ...) {
    RedisReplyPtr reply;
    va_list ap;

    va_start(ap, format);
    reply = redisvCommand(format, ap);
    va_end(ap);

    return reply;
}

RedisReplyPtr RedisClient::redisvCommand(const char *format, va_list ap) {
    void *reply = nullptr;
    PooledSocket socket(inst);

    if (socket.notNull()) {
        reply = redis_vcommand(socket, inst, format, ap);
    } else {
        log_(HPOOL_ERROR_LEVEL | HPOOL_CONS_LEVEL,
             "%s : Can not get socket from redis connection pool, server down? or not enough connection?", __func__);
        checkError(socket);
    }

    if (reply == nullptr) checkError(socket);

    return RedisReplyPtr(reply);
}

void RedisClient::pingToServer() {
    try {
        int failedTimes = 0;
        int succTimes = 0;
        va_list ap;
        void *reply = nullptr;
        REDIS_INSTANCE *innerInst;
        REDIS_CONFIG innerConf;

        /* Assign config */
        innerConf.endpoints = (REDIS_ENDPOINT *) malloc(sizeof(REDIS_ENDPOINT) * inst->config->num_endpoints);
        memcpy(innerConf.endpoints, inst->config->endpoints, sizeof(REDIS_ENDPOINT) * inst->config->num_endpoints);
        innerConf.num_endpoints = inst->config->num_endpoints;
        innerConf.connect_timeout = inst->config->connect_timeout;
        innerConf.net_readwrite_timeout = inst->config->net_readwrite_timeout;
        innerConf.connect_failure_retry_delay = inst->config->connect_failure_retry_delay;
        innerConf.num_redis_socks = 1;
        innerConf.reader_buf_max_size = 512;

        log_(HPOOL_INFO_LEVEL | HPOOL_CONS_LEVEL,
             "%s : innerConf.num_endpoints %d, host %s, port %d, start ping to server", __func__,
             innerConf.num_endpoints,
             innerConf.endpoints[innerConf.num_endpoints - 1].host,
             innerConf.endpoints[innerConf.num_endpoints - 1].port);

        if (redis_pool_create(&innerConf, &innerInst) < 0)
            throw std::runtime_error("Can't create inner connection pool");

        PooledSocket socket(innerInst);
        if (!socket.notNull()) {
            log_(HPOOL_ERROR_LEVEL | HPOOL_CONS_LEVEL,
                 "%s : Can not get socket from inner connection pool, server down? or not enough connection?",
                 __func__);
            checkError(socket);
        }

        while (running) {
            reply = nullptr;
            reply = redis_vcommand(socket, innerInst, "PING", ap);
            RedisReplyPtr replyPtr(reply);

//            log_(HPOOL_INFO_LEVEL | HPOOL_CONS_LEVEL,
//                 "ping to server %s, ret code: %d, ret msg: %s",
//                 innerConf.endpoints[innerConf.num_endpoints - 1].host, replyPtr->type, replyPtr->str);

            if (replyPtr->type == REDIS_REPLY_STATUS && strcasecmp(replyPtr->str, "pong") == 0) {
                if (!isConnectedTo) { // 未连接状态
                    ++succTimes;
                    log_(HPOOL_ERROR_LEVEL | HPOOL_CONS_LEVEL,
                         "%s : ping to server success times: %d, ret code: %d, ret msg: %s", __func__,
                         succTimes, replyPtr->type, replyPtr->str);
                    if (succTimes == 5) { // 恢复
                        isConnectedTo = true;
                    }
                } else failedTimes = 0;
            } else {
                if (isConnectedTo) { // 已连接状态
                    ++failedTimes;
                    log_(HPOOL_ERROR_LEVEL | HPOOL_CONS_LEVEL,
                         "%s : ping to server error times: %d, ret code: %d, ret msg: %s", __func__,
                         failedTimes, replyPtr->type, replyPtr->str);
                    if (failedTimes == 5) { // 隔离
                        isConnectedTo = false;
                    }
                } else succTimes = 0;

            }
            sleep(1);
        }
    } catch (const exception &e) {
        log_(HPOOL_ERROR_LEVEL | HPOOL_CONS_LEVEL, "%s : ping to server exception: %s", __func__, e.what());
    }
}

RedisClient::RedisClient(const REDIS_CONFIG &conf) {
    if (redis_pool_create(&conf, &inst) < 0)
        throw std::runtime_error("Can't create connection pool");
//        else{
//            roundRobinIndex = -1;
//            pipelineList.reserve(conf.num_redis_socks);
//            for(int i = 0; i < conf.num_redis_socks; ++i){
//                pipelineList.emplace_back(inst);
//            }
//
//        }
    // new thread detecting connection to remote server
    running = true;
    isConnectedTo = true;
//    std::thread pingThread(std::bind(&RedisClient::pingToServer, this));
//    pingThread.detach();
}

// 自定义代码

//std::vector<RedisReplyPtr> RedisClient::doPipeline(std::vector<std::string> &pipelineCmds) {
//    std::vector<RedisReplyPtr> res;
//    PooledSocket socket(inst);
//
//    if (socket.notNull()) {
//        size_t n = pipelineCmds.size();
//        boost::shared_array<char *> cmdsPtr(new char *[n]);
//        char **cmds = cmdsPtr.get();
//
//        std::vector<boost::shared_array<char> > itemPtrVector;
//        itemPtrVector.reserve(n);
//
//        for (auto i = 0; i < n; ++i) {
//            itemPtrVector.emplace_back(new char[128]);
//            cmds[i] = itemPtrVector.back().get();
//            memcpy(cmds[i], pipelineCmds[i].c_str(), pipelineCmds[i].size());
//        }
//
//        if (REDIS_OK == pipeline_redisAppendCommand(socket, inst, cmds, n)) {
//            boost::shared_array<redisReply *> replyPtrVector(new redisReply *[n]);
//            redisReply **replyPtr = replyPtrVector.get();
//
//            // todo after free is NULL?
//            pipeline_redisGetReply(socket, (void **)replyPtr, n);
//
//            // todo test
//            res.reserve( n );
//            for (auto i = 0; i < n; ++i) {
//                res.emplace_back(replyPtr[i]);
//            }
//        } else
//            log_(HPOOL_ERROR_LEVEL | HPOOL_CONS_LEVEL, "pipeline add commands error!");
//
//    } else {
//        log_(HPOOL_ERROR_LEVEL | HPOOL_CONS_LEVEL, "Can not get socket from redis connection pool, server down? or not enough connection?");
//    }
//
//    return res;
//}

int pipeline::RedisAppendCommand(const char *format, ...) {
    int reply;
    va_list ap;

    va_start(ap, format);
    reply = RedisVAppendCommand(format, ap);
    va_end(ap);

    return reply;
}

int pipeline::RedisVAppendCommand(const char *format, va_list ap) {
    int reply = -1;
    if (socket->notNull()) {
        reply = redis_vappend_command(*socket, inst, format, ap);
    } else {
        log_(HPOOL_ERROR_LEVEL | HPOOL_CONS_LEVEL,
             "%s : Can not get pipeline socket from redis connection pool, server down? or not enough connection?",
             __func__);
        RedisClient::checkError(*socket);
    }
    if (reply == REDIS_OK) ++cmdNum;
    else
        log_(HPOOL_ERROR_LEVEL | HPOOL_CONS_LEVEL, "%s : redis appendCommand error!", __func__);
    return reply;
}

int pipeline::RedisGetReply(std::vector<RedisReplyPtr> &v) {
    static reportUtil *reportPtr = reportUtil::getInstance();
    static std::string getFirstReply("getFirstReply");
    static std::string getAllRemainReply("getAllRemainReply");

    if (cmdNum <= 0) {
        LOG_ERROR << "pipeline does not have commands!";
        return CLIENT_OTHER;
    }

    int code = CLIENT_OK;
    size_t tmpCmdNum = cmdNum;
    cmdNum = 0;

    try {
        redisReply *r = nullptr;
        clockUtil stepWatch;
        redis_get_reply(*socket, inst, (void **) &r);
        reportPtr->sendLatencyReport(getFirstReply, stepWatch.elapsedX());

        if (r == nullptr) {
            return RedisClient::checkError(*socket);
        }

        v.resize(tmpCmdNum);
        v[0].setReplyPtr(r);

        for (size_t i = 1; i < v.size(); ++i) {
            r = nullptr;
            redis_get_reply(*socket, inst, (void **) &r);
            if (r == nullptr) {
                code = RedisClient::checkError(*socket);
                break;
            }
            v[i].setReplyPtr(r);
        }
        reportPtr->sendLatencyReport(getAllRemainReply, stepWatch.elapsed());
    } catch (const std::exception &e) {
        LOG_ERROR << "RedisGetReply exception: " << e.what();
        code = CLIENT_OTHER;
    }
    return code;
}

pipeline::pipeline(REDIS_INSTANCE *inst) :
        cmdNum(0),
        inst(inst),
        socket(std::make_shared<PooledSocket>(inst)) {
    if (socket->isNull()) {
        RedisClient::checkError(*socket);
    }
}

pipeline RedisClient::pipelined() {
    return pipeline(inst);
//    boost::shared_lock<boost::shared_mutex> g(pipelineListSMtx);
//    return pipelineList[++roundRobinIndex % pipelineList.size()];
}

bool RedisClient::checkAllSocketConnected() {
    for (REDIS_SOCKET *p = inst->redis_pool; p != nullptr; p = p->next)
        if (p->state == redis_socket::sockunconnected) return false;
    return true;
}

void RedisClient::setRedisClientLog(const std::string &path, int level) {
    LOG_CONFIG logConf = {9, LOG_DEST_FILES, path.c_str(), "hiredisPool", level, 1};
    log_set_config(&logConf);
}
