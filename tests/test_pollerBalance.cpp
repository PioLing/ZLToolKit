#include <algorithm>
#include <atomic>
#include <functional>
#include <iostream>
#include <vector>
#if defined(__linux) || defined(__linux__)
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#endif
#include "Util/logger.h"
#include "Network/TcpServer.h"
#include "Network/Session.h"

using namespace std;
using namespace toolkit;

class EchoSession : public Session {
public:
    EchoSession(const Socket::Ptr &sock) : Session(sock) {}
    void onRecv(const Buffer::Ptr &buf) override { send(buf); }
    void onError(const SockException &) override {}
    void onManager() override {}
};

static int connectMany(int count, vector<int> &fds, int port) {
    int ok = 0;
    for (int i = 0; i < count; ++i) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr {};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (connect(fd, (sockaddr *)&addr, sizeof(addr)) == 0) {
            fds.push_back(fd);
            ++ok;
        } else {
            close(fd);
        }
    }
    return ok;
}

static vector<EventPoller::Ptr> allPollers() {
    vector<EventPoller::Ptr> ret;
    EventPollerPool::Instance().for_each([&](const TaskExecutor::Ptr &ex) { ret.push_back(static_pointer_cast<EventPoller>(ex)); });
    return ret;
}

// 在poller上持续占用cpu(每次约8ms后让出), 保持高load但不阻塞事件循环
// Keep a poller cpu-busy (~8ms slices) without blocking its loop
static void keepBusy(const EventPoller::Ptr &p, const shared_ptr<atomic<bool>> &busy) {
    auto loop = make_shared<function<void()>>();
    *loop = [p, busy, loop]() {
        if (!*busy) {
            return;
        }
        auto until = chrono::steady_clock::now() + chrono::milliseconds(8);
        while (chrono::steady_clock::now() < until) {}
        p->async([loop]() { (*loop)(); }, false);
    };
    p->async([loop]() { (*loop)(); }, false);
}

int main(int argc, char *argv[]) {
    Logger::Instance().add(std::make_shared<ConsoleChannel>());
    Logger::Instance().setWriter(std::make_shared<AsyncLogWriter>());
    Logger::Instance().setLevel(LWarn);
    int threads = argc > 1 ? atoi(argv[1]) : 4;
    int conns = argc > 2 ? atoi(argv[2]) : 2000;
    EventPollerPool::setPoolSize(threads);

    TcpServer::Ptr server(new TcpServer());
    server->start<EchoSession>(0, "127.0.0.1");
    auto pollers = allPollers();

    // 阶段1: 全部空闲, 突发连接应按fd数均分
    // Stage 1: all idle, a burst should be split evenly by fd count
    vector<int> fds;
    connectMany(conns, fds, server->getPort());
    this_thread::sleep_for(chrono::seconds(1));

    size_t total = 0, maxv = 0, minv = SIZE_MAX;
    for (auto &p : pollers) {
        auto n = p->fdCount();
        total += n;
        maxv = max(maxv, n);
        minv = min(minv, n);
        cout << p->getThreadName() << ": fd=" << n << " pending=" << p->pendingFdCount() << " load=" << p->load() << endl;
    }
    cout << "stage1: connected=" << fds.size() << " total_fd=" << total << " max=" << maxv << " min=" << minv << endl;

    // 阶段2: 让偶数号poller忙碌; fd数相近时新连接应优先落到空闲poller
    // Stage 2: make even-indexed pollers busy; with close fd counts new connections should prefer idle ones
    auto busy = make_shared<atomic<bool>>(true);
    vector<bool> is_busy(pollers.size());
    for (size_t i = 0; i < pollers.size(); i += 2) {
        is_busy[i] = true;
        keepBusy(pollers[i], busy);
    }
    this_thread::sleep_for(chrono::seconds(1));

    vector<size_t> before;
    for (auto &p : pollers) {
        before.push_back(p->fdCount());
    }
    connectMany(conns / 4, fds, server->getPort());
    this_thread::sleep_for(chrono::seconds(1));
    *busy = false;

    size_t to_busy = 0, to_idle = 0;
    for (size_t i = 0; i < pollers.size(); ++i) {
        auto delta = pollers[i]->fdCount() - before[i];
        (is_busy[i] ? to_busy : to_idle) += delta;
        cout << pollers[i]->getThreadName() << (is_busy[i] ? " [busy]" : " [idle]") << ": +" << delta << " load=" << pollers[i]->load() << endl;
    }
    cout << "stage2: new conns to busy=" << to_busy << " to idle=" << to_idle << endl;

    // 先停服务器再断开客户端, 等待poller处理完关闭事件后退出, 避免退出时的析构竞争
    server.reset();
    for (auto fd : fds) close(fd);
    this_thread::sleep_for(chrono::seconds(1));
    return 0;
}
