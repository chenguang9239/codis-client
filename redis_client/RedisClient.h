/* Author:   Huanghao
 * Date:     2017-2
 * Revision: 0.1
 * Function: Thread-safe redis client
 * Usage:    see test_RedisClient.cpp
 */

#ifndef REDISCLIENT_H
#define REDISCLIENT_H


#include <pthread.h>

#include "hiredispool.h"
#include "hiredispool_log.h"
#include <hiredis/hiredis.h>

#include <cstring>
#include <string>
#include <vector>
#include <stdexcept>
#include <memory>
#include <vector>
#include <exception>
#include <map>
#include <unistd.h>
//#include <atomic>
//#include <boost/thread/shared_mutex.hpp>

// PooledSocket is a pooled socket wrapper that provides a convenient
// RAII-style mechanism for owning a socket for the duration of a
// scoped block.
class PooledSocket {
private:
    // non construct copyable and non copyable
    PooledSocket(const PooledSocket &);

    PooledSocket &operator=(const PooledSocket &);

public:
    // Get a pooled socket from a redis instance.
    // throw runtime_error if something wrong.
    PooledSocket(REDIS_INSTANCE *_inst) : inst(_inst) {
        sock = redis_get_socket(inst);
    }

    // Release the socket to pool
    ~PooledSocket() {
        redis_release_socket(inst, sock);
    }

    // Implicit convert to REDIS_SOCKET*
    operator REDIS_SOCKET *() const { return sock; }

    bool notNull() const { return (sock != NULL); }

    bool isNull() const { return (sock == NULL); }

private:
    REDIS_INSTANCE *inst;
    REDIS_SOCKET *sock;
};

// Helper
struct RedisReplyRef {
    redisReply *p;

    explicit RedisReplyRef(redisReply *_p) : p(_p) {}
};

// RedisReplyPtr is a smart pointer encapsulate redisReply*
class RedisReplyPtr {
public:
    explicit RedisReplyPtr(void *_p = 0) : p((redisReply *) _p) {}

    ~RedisReplyPtr() {
        //printf("freeReplyObject %p\n", (void*)p);
        freeReplyObject(p);
        p = nullptr;
    }

    // release ownership of the managed object
    redisReply *release() {
        redisReply *temp = p;
        p = nullptr;
        return temp;
    }

    // transfer ownership
    RedisReplyPtr(RedisReplyPtr &other) {
        p = other.release();
    }

    RedisReplyPtr &operator=(RedisReplyPtr &other) {
        if (this == &other)
            return *this;
        RedisReplyPtr temp(release());
        p = other.release();
        return *this;
    }

    // automatic conversions
    RedisReplyPtr(RedisReplyRef _ref) {
        p = _ref.p;
    }

    RedisReplyPtr &operator=(RedisReplyRef _ref) {
        if (p == _ref.p)
            return *this;
        RedisReplyPtr temp(release());
        p = _ref.p;
        return *this;
    }

    operator RedisReplyRef() { return RedisReplyRef(release()); }

    bool notNull() const { return (p != 0); }

    bool isNull() const { return (p == 0); }

    redisReply *get() const { return p; }

    redisReply *operator->() const { return p; }

    redisReply &operator*() const { return *p; }

    // 自定义
    void setReplyPtr(void *other) {
        if (p != 0 && p != other) {
            freeReplyObject(p);
            log_(HPOOL_ERROR_LEVEL | HPOOL_CONS_LEVEL, "p is %d!!!\n", p);
        }
        p = (redisReply *) other;
    }

private:
    redisReply *p;
};

// ---begin---
struct pipeline {
    REDIS_INSTANCE *inst;
    std::shared_ptr<PooledSocket> socket;
    size_t cmdNum;

    int RedisAppendCommand(const char *format, ...);

    int RedisVAppendCommand(const char *format, va_list ap);

    int RedisGetReply(std::vector<RedisReplyPtr> &v);

    pipeline(REDIS_INSTANCE *inst);
};

class RWTIMEOUT_EXCEPTION : public std::exception {
//    std::string errInfo;
//public:
//    explicit RWTIMEOUT_EXCEPTION(std::string &errInfo) : errInfo(std::move(errInfo)) {}
//    explicit RWTIMEOUT_EXCEPTION(std::string &&errInfo) : errInfo(errInfo) {}
//    const char* what() const noexcept override {
//        return errInfo.c_str();
//    }
public:
    RWTIMEOUT_EXCEPTION() : message_() {}

    RWTIMEOUT_EXCEPTION(const std::string &message) : message_(message) {}

    virtual ~RWTIMEOUT_EXCEPTION() throw() {}

    virtual const char *what() const throw() {
        if (message_.empty()) {
            return "Default RWTIMEOUT_EXCEPTION.";
        } else {
            return message_.c_str();
        }
    }

protected:
    std::string message_;
};

class OTHER_EXCEPTION : public std::exception {
//    std::string errInfo;
//public:
//    explicit OTHER_EXCEPTION(std::string &errInfo) : errInfo(std::move(errInfo)) {}
//    explicit OTHER_EXCEPTION(std::string &&errInfo) : errInfo(errInfo) {}
//    const char* what() const noexcept override {
//        return errInfo.c_str();
//    }
public:
    OTHER_EXCEPTION() : message_() {}

    OTHER_EXCEPTION(const std::string &message) : message_(message) {}

    virtual ~OTHER_EXCEPTION() throw() {}

    virtual const char *what() const throw() {
        if (message_.empty()) {
            return "Default OTHER_EXCEPTION.";
        } else {
            return message_.c_str();
        }
    }

protected:
    std::string message_;
};

// 使用到的enum值映射
extern std::map<int, std::string> REDIS_ERROR;
extern std::map<int, std::string> REPLY_ERROR;

enum CLIENT_CODE {
    CLIENT_OK = 0,
    CLIENT_RWTIMEOUT = -1,  // 读写超时
    CLIENT_ERROR = -2,      // RedisReplyPtr结构为空，命令执行失败
    CLIENT_OTHER = -3,      // catch到异常，传入的key为空，redisClientPtr为空
    CLIENT_INVALID_REPLY = -4 // RedisReplyPtr中的reply type为空类型，不是需要的类型，不是期望的值
};
// ---end---

// RedisClient provides a threadsafe redis client
class RedisClient {
private:
    bool running;
    bool isConnectedTo;

    // non construct copyable and non copyable
    RedisClient(const RedisClient &);

    RedisClient &operator=(const RedisClient &);

    void pingToServer();

public:
    RedisClient(const REDIS_CONFIG &conf);

//    RedisClient(const REDIS_CONFIG &conf, const LOG_CONFIG &logConf) {
//        log_set_config(&logConf);
//        if (redis_pool_create(&conf, &inst) < 0)
//            throw std::runtime_error("Can't create pool");
//    }

    static void setRedisClientLog(const std::string &path, int level);

    ~RedisClient() {
        running = false;
        sleep(3);
        redis_pool_destroy(inst);
    }

    // ----------------------------------------------------
    // Thread-safe command
    // ----------------------------------------------------

    // redisCommand is a thread-safe wrapper of that function in hiredis
    // It first get a connection from pool, execute the command on that
    // connection and then release the connection to pool.
    // the command's reply is returned as a smart pointer,
    // which can be used just like raw redisReply pointer.
    RedisReplyPtr redisCommand(const char *format, ...);

    RedisReplyPtr redisvCommand(const char *format, va_list ap);

//    std::vector<RedisReplyPtr> doPipeline(std::vector<std::string> &pipelineCmds);
    //自定义
    pipeline pipelined();

//    static void checkError(REDIS_SOCKET *redisSocket, bool needToThrow = true);

    static int checkError(REDIS_SOCKET *redisSocket);

    bool checkAllSocketConnected();

    long getWaitingNum() {
        return inst->wait_num;
    }

    long getIdleNum() {
        return inst->idle_num;
    }

    bool isHealthy() {
        return isConnectedTo;
    }

private:
    REDIS_INSTANCE *inst;
};

#endif // REDISCLIENT_H
