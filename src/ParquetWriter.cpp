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
    auto last_epoch_tick = std::chrono::steady_clock::now();
    size_t rows_since_epoch = 0;
    
    while (keep_running_) {
        WalMessage msg;
        // Drain available messages from buffer
        while (keep_running_ && buffer_.pop_for(msg, std::chrono::milliseconds(0))) {
            if (msg.lsn > current_max_lsn_) current_max_lsn_ = msg.lsn;
            processMessage(msg);
            rows_since_epoch++;
            if (rows_since_epoch >= 50000) break; // Trigger check
        }

        auto now = std::chrono::steady_clock::now();
        // Global epoch tick: every 10 seconds or 50,000 rows
        if (rows_since_epoch >= 50000 || 
            (rows_since_epoch > 0 && std::chrono::duration_cast<std::chrono::seconds>(now - last_epoch_tick).count() >= 10)) {
            
            uint64_t epoch_lsn = current_max_lsn_;
            uint64_t epoch_id = current_epoch_id_++;
            
            std::cout << "ParquetWriter: Triggering Epoch " << epoch_id << " at LSN " << epoch_lsn << std::endl;
            broadcastFlushSignal(epoch_id);
            
            // Wait for all writers to commit this epoch's data
            bool all_ready = false;
            while (!all_ready && keep_running_) {
                all_ready = true;
                for (auto& pair : writers_) {
                    if (pair.second->getLastFlushedEpoch() < epoch_id) {
                        // std::cout << "ParquetWriter: Still waiting for Table [" << pair.first << "] (Last Flushed: " << pair.second->getLastFlushedEpoch() << " < Current: " << epoch_id << ")" << std::endl;
                        all_ready = false;
                        break;
                    }
                }
                if (!all_ready) std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            
            if (keep_running_) {
                committed_lsn_->store(epoch_lsn);
                std::cout << "ParquetWriter: Epoch " << epoch_id << " committed. Global LSN: " << epoch_lsn << std::endl;
            }
            
            rows_since_epoch = 0;
            last_epoch_tick = std::chrono::steady_clock::now();
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

void ParquetWriter::broadcastFlushSignal(uint64_t epoch_id) {
    for (auto& pair : writers_) {
        pair.second->sendFlushSignal(epoch_id);
    }
}

void ParquetWriter::stopAllWriters() {
    std::cout << "ParquetWriter: Stopping all table workers..." << std::endl;
    for (auto& pair : writers_) {
        pair.second->stop();
    }
}
