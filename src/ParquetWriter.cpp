#include "ParquetWriter.hpp"
#include <iostream>
#include <arpa/inet.h>
#include <chrono>
#include <cstring>

ParquetWriter::ParquetWriter(BoundedBuffer<WalMessage>& buffer, size_t row_group_size)
    : buffer_(buffer), row_group_size_(row_group_size), keep_running_(false), current_rows_(0) {
    
    // Define schema: id (int32), data (utf8)
    schema_ = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("data", arrow::utf8())
    });
    
    resetBuilders();
}

ParquetWriter::~ParquetWriter() {
    stop();
}

void ParquetWriter::resetBuilders() {
    id_builder_ = std::make_unique<arrow::Int32Builder>(arrow::default_memory_pool());
    data_builder_ = std::make_unique<arrow::StringBuilder>(arrow::default_memory_pool());
    current_rows_ = 0;
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
        // Use try_pop so we don't block indefinitely on shutdown
        WalMessage msg;
        if (buffer_.try_pop(msg)) {
            processMessage(msg);
        } else {
            // Buffer was empty, wait a bit
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    // Flush any remaining when stopping
    if (current_rows_ > 0) {
        flushPartition();
    }
}

void ParquetWriter::processMessage(const WalMessage& msg) {
    const char* data = msg.payload.data();
    size_t len = msg.payload.size();
    
    if (len == 0) return;
    
    // We only care about Insert 'I' messages
    if (data[0] != 'I') return;
    
    size_t offset = 1; // Skip 'I'
    
    if (offset + 4 > len) return;
    // Skip relation id
    offset += 4;
    
    if (offset + 1 > len) return;
    char n_type = data[offset];
    offset += 1; // Skip 'N'
    if (n_type != 'N') return; // Expected 'N' for new tuple
    
    if (offset + 2 > len) return;
    uint16_t ncolumns_n;
    std::memcpy(&ncolumns_n, data + offset, 2);
    uint16_t ncolumns = ntohs(ncolumns_n);
    offset += 2;
    
    if (ncolumns < 2) return; // Need at least two columns
    
    int32_t parsed_id = 0;
    std::string parsed_data = "";
    
    for (int col = 0; col < ncolumns && col < 2 && offset < len; ++col) {
        char kind = data[offset++];
        
        if (kind == 'n') { // null
            continue; 
        } else if (kind == 'u') { // unchanged toast
            continue;
        } else if (kind == 't' || kind == 'b') { // text or binary
            if (offset + 4 > len) return;
            
            uint32_t col_len_n;
            std::memcpy(&col_len_n, data + offset, 4);
            uint32_t col_len = ntohl(col_len_n);
            offset += 4;
            
            if (offset + col_len > len) return;
            std::string col_val(data + offset, col_len);
            offset += col_len;
            
            if (col == 0) { // Id
                try {
                    parsed_id = std::stoi(col_val);
                } catch (...) { parsed_id = 0; }
            } else if (col == 1) { // Data
                parsed_data = col_val;
            }
        }
    }
    
    // Append to arrow builders
    auto status1 = id_builder_->Append(parsed_id);
    auto status2 = data_builder_->Append(parsed_data);
    
    if (status1.ok() && status2.ok()) {
        current_rows_++;
    }
    
    if (static_cast<size_t>(current_rows_) >= row_group_size_) {
        flushPartition();
    }
}

void ParquetWriter::flushPartition() {
    std::cout << "Flushing " << current_rows_ << " rows to Parquet..." << std::endl;
    
    std::shared_ptr<arrow::Array> id_array;
    auto status = id_builder_->Finish(&id_array);
    
    std::shared_ptr<arrow::Array> data_array;
    status = data_builder_->Finish(&data_array);
    
    std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema_, {id_array, data_array});
    
    // Generate a filename based on timestamp
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    std::string filename = "output_" + std::to_string(ms) + ".parquet";
    
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(
        outfile,
        arrow::io::FileOutputStream::Open(filename)
    );
    
    PARQUET_THROW_NOT_OK(
        parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, current_rows_)
    );
    
    std::cout << "Successfully wrote " << filename << std::endl;
    
    resetBuilders();
}
