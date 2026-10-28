/*
 * Copyright (c) 2016 The ZLToolKit project authors. All Rights Reserved.
 *
 * This file is part of ZLToolKit(https://github.com/ZLMediaKit/ZLToolKit).
 *
 * Use of this source code is governed by MIT license that can be found in the
 * LICENSE file in the root of the source tree. All contributing project authors
 * may be found in the AUTHORS file in the root of the source tree.
 */

#include <stdexcept>
#include "PipeWrap.h"
#include "Util/util.h"
#include "Util/logger.h"
#include "Util/uv_errno.h"
#include "Network/sockutil.h"

#if defined(_WIN32) && defined(PIPE_HAS_AF_UNIX)
#include <afunix.h>
#include <atomic>
#include <cstdio>
#include <cstring>
#endif

using namespace std;

#define checkFD(fd) \
    if (fd == -1) { \
        clearFD(); \
        throw runtime_error(StrPrinter << "Create windows pipe failed: " << get_uv_errmsg());\
    }

#define closeFD(fd) \
    if (fd != -1) { \
        close(fd);\
        fd = -1;\
    }

namespace toolkit {

PipeWrap::PipeWrap() {
    reOpen();
}

void PipeWrap::reOpen() {
    clearFD();
#if defined(_WIN32)
#if defined(PIPE_HAS_AF_UNIX)
    bool unix_mode = reOpenUnix();
    if (!unix_mode) {
        // AF_UNIX不可用(低于Win10 1803或运行时创建失败)，降级为回环TCP模拟
        reOpenTcp();
    }
    InfoL << "PipeWrap created, mode: " << (unix_mode ? "AF_UNIX" : "loopback TCP (fallback)");
#else
    reOpenTcp();
    InfoL << "PipeWrap created, mode: loopback TCP (AF_UNIX not compiled)";
#endif
#else
    if (pipe(_pipe_fd) == -1) {
        throw runtime_error(StrPrinter << "Create posix pipe failed: " << get_uv_errmsg());
    }
#endif // defined(_WIN32)
    SockUtil::setNoBlocked(_pipe_fd[0], true);
    SockUtil::setNoBlocked(_pipe_fd[1], false);
    SockUtil::setCloExec(_pipe_fd[0]);
    SockUtil::setCloExec(_pipe_fd[1]);
}

void PipeWrap::clearFD() {
    closeFD(_pipe_fd[0]);
    closeFD(_pipe_fd[1]);
#if defined(_WIN32)
    // 兜底删除socket文件，防止进程异常退出后残留
    if (!_unix_path.empty()) {
        DeleteFileA(_unix_path.c_str());
        _unix_path.clear();
    }
#endif // defined(_WIN32)
}

#if defined(_WIN32)
void PipeWrap::reOpenTcp() {
    // 回环TCP模拟管道，作为AF_UNIX不可用时的降级方案
    const char *localip = SockUtil::support_ipv6() ? "::1" : "127.0.0.1";
    auto listener_fd = SockUtil::listen(0, localip);
    checkFD(listener_fd)
    SockUtil::setNoBlocked(listener_fd,false);
    auto localPort = SockUtil::get_local_port(listener_fd);
    _pipe_fd[1] = SockUtil::connect(localip, localPort,false);
    checkFD(_pipe_fd[1])
    _pipe_fd[0] = (int)accept(listener_fd, nullptr, nullptr);
    checkFD(_pipe_fd[0])
    SockUtil::setNoDelay(_pipe_fd[0]);
    SockUtil::setNoDelay(_pipe_fd[1]);
    close(listener_fd);
}
#endif // defined(_WIN32)

#if defined(_WIN32) && defined(PIPE_HAS_AF_UNIX)
static bool makeUnixPipePath(char *path, size_t size) {
    static atomic<uint32_t> s_counter { 0 };
    char temp[MAX_PATH];
    auto len = GetTempPathA(sizeof(temp), temp);
    if (len == 0 || len >= sizeof(temp)) {
        return false;
    }
    // 去掉末尾反斜杠，确保路径总长度小于sun_path上限(108字符)
    if (len > 0 && temp[len - 1] == '\\') {
        temp[--len] = '\0';
    }
    auto n = snprintf(path, size, "%s\\zltk_pipe_%lu_%u.sock", temp, GetCurrentProcessId(), s_counter++);
    // 路径超长会导致bind失败，此时由调用方降级为回环TCP
    return n > 0 && n < (int)size && n < (int)sizeof(((sockaddr_un *)nullptr)->sun_path);
}

bool PipeWrap::reOpenUnix() {
    char path[128];
    if (!makeUnixPipePath(path, sizeof(path))) {
        return false;
    }
    // 清理上次异常退出残留的socket文件
    DeleteFileA(path);

    auto listener_fd = (int)socket(AF_UNIX, SOCK_STREAM, 0);
    if (listener_fd == -1) {
        // 系统不支持AF_UNIX(低于Win10 1803)，降级为回环TCP
        return false;
    }

    sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);

    bool success = ::bind(listener_fd, (sockaddr *)&addr, sizeof(addr)) == 0
        && listen(listener_fd, 1) == 0;
    if (success) {
        _pipe_fd[1] = (int)socket(AF_UNIX, SOCK_STREAM, 0);
        if (_pipe_fd[1] == -1) {
            success = false;
        } else if (connect(_pipe_fd[1], (sockaddr *)&addr, sizeof(addr)) == -1) {
            success = false;
        } else {
            _pipe_fd[0] = (int)accept(listener_fd, nullptr, nullptr);
            success = _pipe_fd[0] != -1;
        }
    }

    close(listener_fd);
    // listener已关闭，删除socket文件，已建立的连接不受影响
    DeleteFileA(path);
    if (!success) {
        clearFD();
        return false;
    }
    // 注意：不调用setNoDelay，TCP_NODELAY对AF_UNIX无意义
    _unix_path = path;
    return true;
}
#endif // defined(_WIN32) && defined(PIPE_HAS_AF_UNIX)

PipeWrap::~PipeWrap() {
    clearFD();
}

int PipeWrap::write(const void *buf, int n) {
    int ret;
    do {
#if defined(_WIN32)
        ret = send(_pipe_fd[1], (char *)buf, n, 0);
#else
        ret = ::write(_pipe_fd[1], buf, n);
#endif // defined(_WIN32)
    } while (-1 == ret && UV_EINTR == get_uv_error(true));
    return ret;
}

int PipeWrap::read(void *buf, int n) {
    int ret;
    do {
#if defined(_WIN32)
        ret = recv(_pipe_fd[0], (char *)buf, n, 0);
#else
        ret = ::read(_pipe_fd[0], buf, n);
#endif // defined(_WIN32)
    } while (-1 == ret && UV_EINTR == get_uv_error(true));
    return ret;
}

} /* namespace toolkit*/
