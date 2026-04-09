#pragma once

#include "TableRegistry.hpp"
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <arrow/filesystem/filesystem.h>
#include <memory>
#include <string>
#include <vector>
#include "BoundedBuffer.hpp"
#include "WALReceiver.hpp" // For WalMessage
#include <thread>

class TableWriter {
public:
    TableWriter(const TableInfo& info, const std::string& output_dir, 
                std::shared_ptr<std::atomic<uint64_t>> committed_lsn,
                size_t row_group_size = 100);
    ~TableWriter();

    void appendRow(const char* tuple_data, size_t length, uint64_t lsn, char pg_msg_type);
    void sendFlushSignal(uint64_t epoch_id);
    void flushPartition(uint64_t epoch_id);
    void start();
    void stop();

    // Get the oldest LSN currently in this writer's pipeline (queue or being processed)
    uint64_t getOldestPendingLSN() const;
    uint64_t getLastCommittedLSN() const { return committed_lsn_val_.load(); }
    uint64_t getLastFlushedEpoch() const { return last_flushed_epoch_.load(); }

private:
    TableInfo info_;
    std::string output_dir_;
    std::shared_ptr<arrow::fs::FileSystem> fs_;
    std::string base_path_;
    size_t file_counter_;
    size_t insert_count_;
    size_t update_count_;
    size_t delete_count_;
    size_t row_group_size_;
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders_;
    size_t current_rows_;
    int commit_version_;
    uint64_t latest_lsn_;
    std::shared_ptr<std::atomic<uint64_t>> global_committed_lsn_;
    
    // Threading and Queueing
    BoundedBuffer<WalMessage> queue_;
    std::thread worker_thread_;
    std::atomic<bool> keep_running_;
    std::atomic<uint64_t> committed_lsn_val_;
    std::atomic<uint64_t> last_flushed_epoch_;
    mutable std::mutex lsn_mtx_;
    uint64_t oldest_lsn_in_queue_;
    uint64_t pending_epoch_;

    void setupSchemaAndBuilders();
    void resetBuilders();
    std::string generateDeltaSchemaJSON();
    void run();
    void processInternal(const WalMessage& msg);
};
