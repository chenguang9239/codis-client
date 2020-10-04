/* Author:   Huanghao
 * Date:     2017-2
 * Revision: 0.1
 * Function: Simple lightweight logging interface
 * Usage:
 */

#ifndef LOG_H
#define LOG_H

#ifdef __cplusplus
extern "C" {
#endif


/* Constants */
#define HPOOL_TRACE_LEVEL     512        /* 0x00000200 */
#define HPOOL_DEBUG_LEVEL     1024       /* 0x00000400 */
#define HPOOL_INFO_LEVEL      2048       /* 0x00000800 */
#define HPOOL_WARN_LEVEL      4096       /* 0x00001000 */
#define HPOOL_ERROR_LEVEL     8192       /* 0x00002000 */
#define HPOOL_FATAL_LEVEL     16384      /* 0x00004000 */
#define HPOOL_CONS_LEVEL      32768      /* 0x00008000 */

/* Types */
typedef enum _log_dest_t {
  LOG_DEST_FILES = 0,
  LOG_DEST_SYSLOG,
  LOG_DEST_STDOUT,
  LOG_DEST_STDERR,
  LOG_DEST_NULL
} log_dest_t;

typedef struct log_config {
    int verbose;
    log_dest_t dest;
    const char* file;
    const char* progname;
    int level_hold;
    int print_millisec;
} LOG_CONFIG;

typedef struct _name_number {
    const char *name;
    int         number;
} _NAME_NUMBER;

/* Functions */
int log_(int lvl, const char *msg, ...);
int log_debug(const char *msg, ...);
int log_trace(const char *msg, ...);

void log_set_config(const LOG_CONFIG* config);
int log_get_verbose();

#define HPOOL_DEBUG  (log_get_verbose() == 0) ?                           \
    0 : log_debug
#define DEBUG2 (log_get_verbose() >= 0 && log_get_verbose() <= 1) ? \
    0 : log_debug
#define DEBUG3 (log_get_verbose() >= 0 && log_get_verbose() <= 2) ? \
    0 : log_debug
#define DEBUG4 (log_get_verbose() >= 0 && log_get_verbose() <= 3) ? \
    0 : log_debug
#define DEBUG5 (log_get_verbose() >= 0 && log_get_verbose() <= 4) ? \
    0 : log_debug

#define HPOOL_TRACE  (log_get_verbose() >= -1) ? \
    0 : log_trace
#define TRACE2 (log_get_verbose() >= -2) ? \
    0 : log_trace
#define TRACE3 (log_get_verbose() >= -3) ? \
    0 : log_trace
#define TRACE4 (log_get_verbose() >= -4) ? \
    0 : log_trace
#define TRACE5 (log_get_verbose() >= -5) ? \
    0 : log_trace


#ifdef __cplusplus
}
#endif

#endif/*LOG_H*/
