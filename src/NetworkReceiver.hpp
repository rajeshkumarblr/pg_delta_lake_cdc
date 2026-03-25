#pragma once

#include "BoundedBuffer.hpp"
#include <string>
#include <vector>
#include <libpq-fe.h>
#include <atomic>

struct WalMessage {
    std::vector<char> payload;
};

class NetworkReceiver {
public:
    NetworkReceiver(const std::string& conninfo, BoundedBuffer<WalMessage>& buffer);
    ~NetworkReceiver();

    void run();
    void stop();

private:
    std::string conninfo_;
    BoundedBuffer<WalMessage>& buffer_;
    PGconn* conn_;
    std::atomic<bool> keep_running_;

    void connect();
    void startLogicalReplication();
    void receiveLoop();
    void handleCopyData(char* msg, int length);
    void sendStandbyStatusUpdate();
};
