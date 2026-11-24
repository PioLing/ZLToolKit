#include <algorithm>
#include <atomic>
#include <functional>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>
#if defined(__linux) || defined(__linux__)
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#endif
#include "Util/logger.h"
#include "Util/uv_errno.h"
#include "Network/TcpServer.h"
#include "Network/UdpServer.h"
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

// 多个客户端线程同时发起连接, 使多个poller线程并发accept并同时选择目标poller
// Several client threads connect at once so that multiple poller threads accept and pick a target poller concurrently
static int connectMany(int count, vector<int> &fds, int port, int client_threads = 4) {
    mutex mtx;
    vector<thread> workers;
    for (int t = 0; t < client_threads; ++t) {
        int n = count / client_threads + (t < count % client_threads ? 1 : 0);
        workers.emplace_back([&, n]() {
            vector<int> local;
            for (int i = 0; i < n; ++i) {
                int fd = socket(AF_INET, SOCK_STREAM, 0);
                sockaddr_in addr {};
                addr.sin_family = AF_INET;
                addr.sin_port = htons(port);
                addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
                if (connect(fd, (sockaddr *)&addr, sizeof(addr)) == 0) {
                    local.push_back(fd);
                } else {
                    close(fd);
                }
            }
            lock_guard<mutex> lck(mtx);
            fds.insert(fds.end(), local.begin(), local.end());
        });
    }
    for (auto &w : workers) {
        w.join();
    }
    return (int)fds.size();
}

// 多个客户端线程各自用独立的udp socket(不同源端口)向服务器各发一个数据包:
// 每个新peer都会触发UdpServer新建会话并在所选poller上创建一个peer fd;
// 内核按reuseport哈希把首包分散到各poller的server fd, 因此多个poller线程会并发选择目标poller
// Several client threads each send one datagram from their own udp sockets (distinct source ports):
// every new peer makes UdpServer create a session with one peer fd on the chosen poller; the kernel spreads
// first packets over the per-poller server fds (reuseport hash), so multiple poller threads select concurrently
static int sendManyUdp(int count, vector<int> &fds, int port, int client_threads = 4) {
    mutex mtx;
    vector<thread> workers;
    for (int t = 0; t < client_threads; ++t) {
        int n = count / client_threads + (t < count % client_threads ? 1 : 0);
        workers.emplace_back([&, n]() {
            vector<int> local;
            for (int i = 0; i < n; ++i) {
                int fd = socket(AF_INET, SOCK_DGRAM, 0);
                sockaddr_in addr {};
                addr.sin_family = AF_INET;
                addr.sin_port = htons(port);
                addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
                // connect后获得独立源端口, 且对端不可达时能收到错误而不是静默丢弃
                // connect assigns a distinct source port and surfaces unreachable errors instead of silent drops
                if (connect(fd, (sockaddr *)&addr, sizeof(addr)) == 0 && send(fd, "ping", 4, 0) == 4) {
                    local.push_back(fd);
                } else {
                    close(fd);
                }
                // 略微限速, 避免首包在server fd接收缓冲区溢出被内核丢弃(小核数机器上8个poller来不及消费)
                // Pace slightly so first packets are not dropped by the kernel when the server fds' receive
                // buffers overflow (on few-core machines the pollers cannot drain a burst fast enough)
                if ((i & 15) == 15) {
                    this_thread::sleep_for(chrono::milliseconds(1));
                }
            }
            lock_guard<mutex> lck(mtx);
            fds.insert(fds.end(), local.begin(), local.end());
        });
    }
    for (auto &w : workers) {
        w.join();
    }
    return (int)fds.size();
}

// 统计收到回显的udp客户端数, 即实际建立了会话(peer fd)的peer数
// Count udp clients that got an echo, i.e. peers for which a session (peer fd) was actually created
static bool countEchoed(const vector<int> &fds, size_t &echoed) {
    echoed = 0;
    bool success = true;
    char buf[16];
    for (auto fd : fds) {
        if (SockUtil::setNoBlocked(fd) != 0) {
            cerr << "failed to set UDP socket nonblocking: " << get_uv_errmsg(true) << endl;
            success = false;
            continue;
        }
        int received;
        int error = 0;
        do {
            received = recv(fd, buf, sizeof(buf), 0);
            error = received < 0 ? get_uv_error(true) : 0;
        } while (error == UV_EINTR);
        if (received > 0) {
            ++echoed;
        } else if (received < 0 && error != UV_EAGAIN) {
            cerr << "failed to receive UDP echo: " << uv_strerror(error) << endl;
            success = false;
        }
    }
    return success;
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

    // 所有fd都已注册, 预约应全部释放
    // Every fd is registered by now, so no reservation may remain
    auto pendingTotal = [&]() {
        size_t pending = 0;
        for (auto &p : pollers) {
            pending += p->pendingFdCount();
        }
        return pending;
    };
    auto pending = pendingTotal();
    cout << "pending after settle=" << pending << endl;
    int ret = pending == 0 ? 0 : 1;

    // 阶段3: udp; 每个新peer在所选poller上新建一个peer fd, 突发时同样应按fd数均分
    // Stage 3: udp; each new peer adds one peer fd on the chosen poller, a burst should likewise be split evenly
    this_thread::sleep_for(chrono::seconds(1));
    before.clear();
    for (auto &p : pollers) {
        before.push_back(p->fdCount());
    }
    UdpServer::Ptr udp_server(new UdpServer());
    udp_server->start<EchoSession>(0, "127.0.0.1");
    vector<int> udp_fds;
    int udp_peers = conns / 2;
    sendManyUdp(udp_peers, udp_fds, udp_server->getPort());
    this_thread::sleep_for(chrono::seconds(1));

    // 均衡的是poller的fd总数而非本阶段增量: 阶段2中忙碌poller分到的tcp连接较少, 这里会相应多分到一些udp peer
    // What gets balanced is each poller's total fd count, not this stage's delta: pollers kept busy in stage 2
    // received fewer tcp connections and are therefore handed somewhat more udp peers here
    size_t udp_total = 0;
    maxv = 0, minv = SIZE_MAX;
    for (size_t i = 0; i < pollers.size(); ++i) {
        auto n = pollers[i]->fdCount();
        // 每个poller额外持有一个udp server fd
        // Each poller also holds one udp server fd
        udp_total += n - before[i] - 1;
        maxv = max(maxv, n);
        minv = min(minv, n);
        cout << pollers[i]->getThreadName() << ": fd=" << n << " (udp peer +" << n - before[i] - 1 << ") pending=" << pollers[i]->pendingFdCount() << endl;
    }
    pending = pendingTotal();
    size_t echoed = 0;
    auto echo_count_ok = countEchoed(udp_fds, echoed);
    cout << "stage3(udp): sent=" << udp_fds.size() << " echoed=" << echoed << " peer_fd=" << udp_total
         << " total fd max=" << maxv << " min=" << minv << " pending=" << pending << endl;
    // 每个建立了会话的peer恰好对应一个已注册的peer fd, 且预约全部释放
    // Every peer with a session maps to exactly one registered peer fd, and no reservation remains
    if (!echo_count_ok || pending != 0 || udp_total != echoed) {
        ret = 1;
    }

    // 先停服务器再断开客户端, 等待poller处理完关闭事件后退出, 避免退出时的析构竞争
    server.reset();
    udp_server.reset();
    for (auto fd : fds) close(fd);
    for (auto fd : udp_fds) close(fd);
    this_thread::sleep_for(chrono::seconds(1));
    return ret;
}
