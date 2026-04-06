#pragma once

#include "BoundedBuffer.hpp"
#include "WALReceiver.hpp" // For WalMessage
#include "TableRegistry.hpp"
#include "TableWriter.hpp"
#include <string>
#include <atomic>
#include <memory>
#include <thread>
#include <unordered_map>
#include <limits>
#include <chrono>

class ParquetWriter {
public:
    ParquetWriter(BoundedBuffer<WalMessage>& buffer, std::shared_ptr<TableRegistry> registry, 
                  const std::string& output_dir, std::shared_ptr<std::atomic<uint64_t>> committed_lsn,
                  size_t row_group_size = 100);
    ~ParquetWriter();

    void start();
    void stop();

private:
    BoundedBuffer<WalMessage>& buffer_;
    std::shared_ptr<TableRegistry> registry_;
    std::string output_dir_;
    size_t row_group_size_;
    std::atomic<bool> keep_running_;
    std::thread worker_thread_;
    std::shared_ptr<std::atomic<uint64_t>> committed_lsn_;
    std::unordered_map<uint32_t, std::unique_ptr<TableWriter>> writers_;
    uint64_t current_epoch_id_ = 1;
    uint64_t current_max_lsn_ = 0;

    void run();
    void processMessage(const WalMessage& msg);
    void broadcastFlushSignal(uint64_t epoch_id);
    void stopAllWriters();
};
