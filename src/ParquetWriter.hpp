#pragma once

#include "BoundedBuffer.hpp"
#include "NetworkReceiver.hpp"
#include <string>
#include <atomic>
#include <memory>
#include <thread>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

class ParquetWriter {
public:
    ParquetWriter(BoundedBuffer<WalMessage>& buffer, size_t row_group_size = 10000);
    ~ParquetWriter();

    void start();
    void stop();

private:
    BoundedBuffer<WalMessage>& buffer_;
    size_t row_group_size_;
    std::atomic<bool> keep_running_;
    std::thread worker_thread_;

    std::shared_ptr<arrow::Schema> schema_;
    std::unique_ptr<arrow::Int32Builder> id_builder_;
    std::unique_ptr<arrow::StringBuilder> data_builder_;
    int current_rows_;

    void run();
    void processMessage(const WalMessage& msg);
    void flushPartition();
    void resetBuilders();
};
