#include "ParquetWriter.hpp"
#include <iostream>
#include <chrono>

ParquetWriter::ParquetWriter(BoundedBuffer<WalMessage>& buffer, std::shared_ptr<TableRegistry> registry, 
                             const std::string& output_dir, std::shared_ptr<std::atomic<uint64_t>> committed_lsn, 
                             size_t row_group_size)
    : buffer_(buffer), registry_(std::move(registry)), output_dir_(output_dir), 
      row_group_size_(row_group_size), keep_running_(false), committed_lsn_(committed_lsn) {
}

ParquetWriter::~ParquetWriter() {
    stop();
}

void ParquetWriter::start() {
    keep_running_ = true;
    worker_thread_ = std::thread(&ParquetWriter::run, this);
}

void ParquetWriter::stop() {
    keep_running_ = false;
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
}

void ParquetWriter::run() {
    auto last_lsn_sync = std::chrono::steady_clock::now();
    uint64_t current_global_min = 0;
    
    while (keep_running_) {
        WalMessage msg;
        // Wait for up to 100ms for a message to arrive
        if (buffer_.pop_for(msg, std::chrono::milliseconds(100))) {
            processMessage(msg);
        }

        // Periodically aggregate LSNs (every 1 second)
        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now - last_lsn_sync).count() >= 1) {
            uint64_t min_lsn = std::numeric_limits<uint64_t>::max();
            bool any_active = false;

            for (auto& pair : writers_) {
                uint64_t writer_min = pair.second->getOldestPendingLSN();
                if (writer_min < min_lsn) {
                    min_lsn = writer_min;
                }
                any_active = true;
            }

            if (any_active && min_lsn != std::numeric_limits<uint64_t>::max()) {
                if (min_lsn > current_global_min) {
                    current_global_min = min_lsn;
                    committed_lsn_->store(current_global_min);
                }
            }
            last_lsn_sync = now;
        }
    }
    stopAllWriters();
}

void ParquetWriter::processMessage(const WalMessage& msg) {
    auto it = writers_.find(msg.relation_id);
    if (it == writers_.end()) {
        TableInfo info;
        if (registry_->getTableByRelationId(msg.relation_id, info)) {
            auto writer = std::make_unique<TableWriter>(info, output_dir_, committed_lsn_, row_group_size_);
            writer->start();
            writers_[msg.relation_id] = std::move(writer);
            it = writers_.find(msg.relation_id);
        } else {
            return;
        }
    }
    it->second->appendRow(msg.payload.data(), msg.payload.size(), msg.lsn);
}

void ParquetWriter::stopAllWriters() {
    std::cout << "ParquetWriter: Stopping all table workers..." << std::endl;
    for (auto& pair : writers_) {
        pair.second->stop();
    }
}
