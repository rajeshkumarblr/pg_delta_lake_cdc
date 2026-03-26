#include "ParquetWriter.hpp"
#include <iostream>
#include <chrono>

ParquetWriter::ParquetWriter(BoundedBuffer<WalMessage>& buffer, std::shared_ptr<TableRegistry> registry, const std::string& output_dir, size_t row_group_size)
    : buffer_(buffer), registry_(std::move(registry)), output_dir_(output_dir), row_group_size_(row_group_size), keep_running_(false) {
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
    while (keep_running_) {
        WalMessage msg;
        if (buffer_.try_pop(msg)) {
            processMessage(msg);
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    flushAll();
}

void ParquetWriter::processMessage(const WalMessage& msg) {
    if (writers_.find(msg.relation_id) == writers_.end()) {
        TableInfo info;
        if (registry_->getTableByRelationId(msg.relation_id, info)) {
            writers_[msg.relation_id] = std::make_unique<TableWriter>(info, output_dir_, row_group_size_);
        } else {
            return;
        }
    }
    writers_[msg.relation_id]->appendRow(msg.payload.data(), msg.payload.size());
}

void ParquetWriter::flushAll() {
    for (auto& pair : writers_) {
        pair.second->flushPartition();
    }
}
