#pragma once

#include "BoundedBuffer.hpp"
#include "NetworkReceiver.hpp" // For WalMessage
#include "TableRegistry.hpp"
#include "TableWriter.hpp"
#include <string>
#include <atomic>
#include <memory>
#include <thread>
#include <unordered_map>

class ParquetWriter {
public:
    ParquetWriter(BoundedBuffer<WalMessage>& buffer, std::shared_ptr<TableRegistry> registry, size_t row_group_size = 100);
    ~ParquetWriter();

    void start();
    void stop();

private:
    BoundedBuffer<WalMessage>& buffer_;
    std::shared_ptr<TableRegistry> registry_;
    size_t row_group_size_;
    std::atomic<bool> keep_running_;
    std::thread worker_thread_;
    std::unordered_map<uint32_t, std::unique_ptr<TableWriter>> writers_;

    void run();
    void processMessage(const WalMessage& msg);
    void flushAll();
};
