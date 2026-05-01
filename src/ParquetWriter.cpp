#include "ParquetWriter.hpp"
#include <iostream>
#include <chrono>

ParquetWriter::ParquetWriter(BoundedBuffer<WalMessage>& buffer, std::shared_ptr<TableRegistry> registry, 
                             const std::string& output_dir, std::shared_ptr<std::atomic<uint64_t>> committed_lsn, 
                             size_t row_group_size, uint64_t watermark_lsn)
    : buffer_(buffer), registry_(std::move(registry)), output_dir_(output_dir), 
      row_group_size_(row_group_size), keep_running_(false), committed_lsn_(committed_lsn),
      watermark_lsn_(watermark_lsn) {
    
    // Pre-initialize writers for all known tables to capture Snapshot ('S') rows
    auto all_tables = registry_->getAllTables();
    for (const auto& table : all_tables) {
        auto writer = std::make_unique<TableWriter>(table, output_dir_, committed_lsn_, row_group_size_, watermark_lsn_);
        writer->start();
        writers_[table.rel_id] = std::move(writer);
    }
}

ParquetWriter::~ParquetWriter() {
    stop();
}

void ParquetWriter::start() {
    if (keep_running_) return;
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
            std::cout << "[ParquetWriter] Popped LSN " << msg.lsn << " from global buffer." << std::endl;
            processMessage(msg);
            rows_since_epoch++;
            if (rows_since_epoch >= 50000) break; // Trigger check
        }

        auto now = std::chrono::steady_clock::now();
        // Global epoch tick: every 10 seconds or 50,000 rows
        if (rows_since_epoch >= 50000 || 
            (rows_since_epoch > 0 && std::chrono::duration_cast<std::chrono::seconds>(now - last_epoch_tick).count() >= 10)) {
            
            if (pending_epoch_id_ == 0) { // Only trigger if no epoch is currently waiting
                pending_epoch_id_ = current_epoch_id_++;
                pending_epoch_lsn_ = current_max_lsn_;
                
                std::cout << "ParquetWriter: Triggering Epoch " << pending_epoch_id_ << " at LSN " << pending_epoch_lsn_ << std::endl;
                broadcastFlushSignal(pending_epoch_id_);
            }
        }

        // Asynchronously check if the pending epoch is completed by all writers
        if (pending_epoch_id_ > 0) {
            bool all_ready = true;
            for (auto& pair : writers_) {
                if (pair.second->getLastFlushedEpoch() < pending_epoch_id_) {
                    all_ready = false;
                    break;
                }
            }

            if (all_ready) {
                committed_lsn_->store(pending_epoch_lsn_);
                std::cout << "ParquetWriter: Epoch " << pending_epoch_id_ << " committed. Global LSN: " << pending_epoch_lsn_ << std::endl;
                pending_epoch_id_ = 0;
                rows_since_epoch = 0;
                last_epoch_tick = std::chrono::steady_clock::now();
            }
        }
    }
    stopAllWriters();
}

void ParquetWriter::processMessage(const WalMessage& msg) {
    if (msg.pg_msg_type == 'B' || msg.pg_msg_type == 'C') {
        for (auto& pair : writers_) {
            pair.second->appendRow(nullptr, 0, msg.lsn, msg.pg_msg_type);
        }
        return;
    }

    if (msg.pg_msg_type == 'R') {
        auto it = writers_.find(msg.relation_id);
        if (it != writers_.end()) {
            std::cout << "ParquetWriter: Detected Schema Change for Relation " << msg.relation_id << ". Retiring old worker..." << std::endl;
            it->second->stop(); 
            writers_.erase(it);
        }
        return;
    }

    if (msg.pg_msg_type == 'F') {
        auto it = writers_.find(msg.relation_id);
        if (it != writers_.end()) {
            std::cout << "ParquetWriter: Received force-flush signal for " << msg.relation_id << std::endl;
            it->second->forceFlush();
        }
        return;
    }

    auto it = writers_.find(msg.relation_id);
    if (it == writers_.end()) {
        TableInfo info;
        if (registry_->getTableByRelationId(msg.relation_id, info)) {
            auto writer = std::make_unique<TableWriter>(info, output_dir_, committed_lsn_, row_group_size_, watermark_lsn_);
            writer->start();
            writers_[msg.relation_id] = std::move(writer);
            it = writers_.find(msg.relation_id);
        } else {
            return;
        }
    }
    
    if (it != writers_.end()) {
        std::cout << "[ParquetWriter] Dispatching LSN " << msg.lsn << " to TableWriter for relation " << msg.relation_id << std::endl;
        it->second->appendRow(msg.payload.data(), msg.payload.size(), msg.lsn, msg.pg_msg_type);
    }
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
