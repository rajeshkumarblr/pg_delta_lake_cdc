#include "TableWriter.hpp"
#include <iostream>
#include <chrono>
#include <arpa/inet.h>
#include <cstring>
#include <filesystem>
#include "DeltaLogWriter.hpp"

TableWriter::TableWriter(const TableInfo& info, const std::string& output_dir, 
                         std::shared_ptr<std::atomic<uint64_t>> committed_lsn,
                         size_t row_group_size)
    : info_(info), output_dir_(output_dir), global_committed_lsn_(committed_lsn), 
      row_group_size_(row_group_size), file_counter_(1), current_rows_(0), 
      keep_running_(false), oldest_lsn_in_queue_(0), pending_epoch_(0), queue_(10000) {
    setupSchemaAndBuilders();
}

TableWriter::~TableWriter() {
    stop();
}

void TableWriter::start() {
    if (keep_running_) return;
    keep_running_ = true;
    worker_thread_ = std::thread(&TableWriter::run, this);
}

void TableWriter::stop() {
    if (!keep_running_) return;
    
    // Push a dummy message to wake up the worker if it's blocked on pop
    WalMessage dummy;
    dummy.relation_id = 0;
    queue_.push(dummy); 

    keep_running_ = false;

    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
}

void TableWriter::run() {
    while (keep_running_ || !queue_.empty()) {
        WalMessage msg;
        if (queue_.pop_for(msg, std::chrono::milliseconds(100))) {
            // Process ALL messages until queue is empty
            if (msg.relation_id == 0 && !keep_running_) continue; 
            
            // DEBUG LOGGING
            if (msg.is_flush_signal) {
                std::cout << "TableWriter [" << info_.table_name << "]: Received flush signal for Epoch " << msg.epoch_id << " (Pending until next COMMIT)" << std::endl;
                pending_epoch_ = msg.epoch_id;
            } else if (msg.pg_msg_type == 'C') { // Commit
                if (pending_epoch_ > 0) {
                    std::cout << "TableWriter [" << info_.table_name << "]: COMMIT reached. Flushing Epoch " << pending_epoch_ << std::endl;
                    flushPartition(pending_epoch_);
                    pending_epoch_ = 0;
                }
            }
            
            processInternal(msg);
        }
    }
    // Final flush on exit
    if (current_rows_ > 0) {
        flushPartition(0); // Use 0 for final exit flush if no epoch
    }
}

void TableWriter::setupSchemaAndBuilders() {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    
    auto pool = arrow::default_memory_pool();
    builders_.clear();

    for (const auto& col : info_.columns) {
        std::shared_ptr<arrow::DataType> arrow_type;
        std::shared_ptr<arrow::ArrayBuilder> builder;
        const std::string& dt = col.data_type;
        
        if (dt == "integer" || dt == "int4" || dt == "serial") {
            arrow_type = arrow::int32();
            builder = std::make_shared<arrow::Int32Builder>(pool);
        } else if (dt == "bigint" || dt == "int8" || dt == "bigserial") {
            arrow_type = arrow::int64();
            builder = std::make_shared<arrow::Int64Builder>(pool);
        } else if (dt == "real" || dt == "float4") {
            arrow_type = arrow::float32();
            builder = std::make_shared<arrow::FloatBuilder>(pool);
        } else if (dt == "double precision" || dt == "float8" || dt == "numeric") {
            arrow_type = arrow::float64();
            builder = std::make_shared<arrow::DoubleBuilder>(pool);
        } else if (dt == "boolean" || dt == "bool") {
            arrow_type = arrow::boolean();
            builder = std::make_shared<arrow::BooleanBuilder>(pool);
        } else {
            arrow_type = arrow::utf8(); 
            builder = std::make_shared<arrow::StringBuilder>(pool);
        }
        
        fields.push_back(arrow::field(col.name, arrow_type, col.is_nullable));
        builders_.push_back(builder);
    }
    
    // Add metadata columns
    fields.push_back(arrow::field("_cdc_op", arrow::utf8(), false));
    fields.push_back(arrow::field("_cdc_timestamp", arrow::int64(), false));
    fields.push_back(arrow::field("_cdc_lsn", arrow::uint64(), false));
    
    builders_.push_back(std::make_shared<arrow::StringBuilder>(pool)); // _cdc_op
    builders_.push_back(std::make_shared<arrow::Int64Builder>(pool));   // _cdc_timestamp
    builders_.push_back(std::make_shared<arrow::UInt64Builder>(pool)); // _cdc_lsn

    schema_ = arrow::schema(fields);
    current_rows_ = 0;
    insert_count_ = 0;
    update_count_ = 0;
}

void TableWriter::resetBuilders() {
    for (auto& builder : builders_) {
        builder->Reset();
    }
    current_rows_ = 0;
    insert_count_ = 0;
    update_count_ = 0;
}

void TableWriter::appendRow(const char* data, size_t length, uint64_t lsn, char pg_msg_type) {
    WalMessage msg;
    msg.relation_id = info_.rel_id;
    msg.lsn = lsn;
    msg.pg_msg_type = pg_msg_type;
    if (data && length > 0) {
        msg.payload.assign(data, data + length);
    }
    
    {
        std::lock_guard<std::mutex> lock(lsn_mtx_);
        if (oldest_lsn_in_queue_ == 0) {
            oldest_lsn_in_queue_ = lsn;
        }
    }
    
    queue_.push(msg);
}

void TableWriter::sendFlushSignal(uint64_t epoch_id) {
    WalMessage msg;
    msg.is_flush_signal = true;
    msg.epoch_id = epoch_id;
    msg.relation_id = info_.rel_id;
    queue_.push(msg);
}

uint64_t TableWriter::getOldestPendingLSN() const {
    std::lock_guard<std::mutex> lock(lsn_mtx_);
    if (queue_.empty()) {
        return latest_lsn_; 
    }
    return oldest_lsn_in_queue_;
}

std::string TableWriter::generateDeltaSchemaJSON() {
    std::string columns_json = "";
    for (size_t i = 0; i < info_.columns.size(); ++i) {
        const auto& col = info_.columns[i];
        if (col.name == "_cdc_op" || col.name == "_cdc_timestamp" || col.name == "_cdc_lsn") {
            continue; // Skip metadata columns from PG
        }
        std::string delta_type;
        const std::string& dt = col.data_type;

        if (dt == "integer" || dt == "int4" || dt == "serial") {
            delta_type = "integer";
        } else if (dt == "bigint" || dt == "int8" || dt == "bigserial") {
            delta_type = "long";
        } else if (dt == "boolean" || dt == "bool") {
            delta_type = "boolean";
        } else if (dt == "double precision" || dt == "float8" || dt == "numeric") {
            delta_type = "double";
        } else {
            delta_type = "string";
        }

        if (i > 0) columns_json += ",";
        columns_json += "{\"name\":\"" + col.name + "\",\"type\":\"" + delta_type + "\",\"nullable\":" + (col.is_nullable ? "true" : "false") + ",\"metadata\":{}}";
    }

    // Add CDC columns
    columns_json += ",{\"name\":\"_cdc_op\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}";
    columns_json += ",{\"name\":\"_cdc_timestamp\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}}";
    columns_json += ",{\"name\":\"_cdc_lsn\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}}";
    
    return "{\"type\":\"struct\",\"fields\":[" + columns_json + "]}";
}

void TableWriter::processInternal(const WalMessage& msg) {
    if (msg.is_flush_signal || msg.pg_msg_type == 'B' || msg.pg_msg_type == 'C') {
        return; 
    }
    
    // Original data processing logic...

    uint64_t lsn = msg.lsn;
    const char* data = msg.payload.data();
    size_t length = msg.payload.size();

    // Update latest processed LSN
    {
        std::lock_guard<std::mutex> lock(lsn_mtx_);
        latest_lsn_ = lsn;
    }

    if (length == 0) return;
    
    const char msg_type = data[0];
    switch (msg_type) {
        case 'I':
        case 'U':
        case 'D':
            break;
        default:
            return;
    }
    
    size_t offset = 1; // Skip message type 'I', 'U', 'D'
    offset += 4; // Skip relation_id
    
    switch (msg_type) {
        case 'D': return; // Deletes not yet fully supported in builders
        case 'U': {
            while (offset < length && data[offset] != 'N') {
                offset++;
            }
            break;
        }
        default:
            break;
    }

    if (offset + 1 > length) return;
    const char n_type = data[offset++];
    if (n_type != 'N') return;
    
    if (offset + 2 > length) return;
    uint16_t ncolumns_n;
    std::memcpy(&ncolumns_n, data + offset, 2);
    uint16_t ncolumns = ntohs(ncolumns_n);
    offset += 2;
    
    bool all_ok = true;
    for (uint16_t col = 0; col < ncolumns && col < info_.columns.size() && offset < length; ++col) {
        char kind = data[offset++];
        auto& builder = builders_[col];
        
        switch (kind) {
            case 'n': 
            case 'u':
                if (!builder->AppendNull().ok()) all_ok = false;
                break;
            case 't':
            case 'b': {
                if (offset + 4 > length) return;
                uint32_t col_len_n;
                std::memcpy(&col_len_n, data + offset, 4);
                uint32_t col_len = ntohl(col_len_n);
                offset += 4;

                if (offset + col_len > length) return;
                std::string col_val(data + offset, col_len);
                offset += col_len;

                arrow::Status status;
                const std::string& dt = info_.columns[col].data_type;

                if (dt == "integer" || dt == "int4" || dt == "serial") {
                    auto* b = static_cast<arrow::Int32Builder*>(builder.get());
                    try { status = b->Append(std::stoi(col_val)); } catch(...) { status = b->AppendNull(); }
                } else if (dt == "bigint" || dt == "int8" || dt == "bigserial") {
                    auto* b = static_cast<arrow::Int64Builder*>(builder.get());
                    try { status = b->Append(std::stoll(col_val)); } catch(...) { status = b->AppendNull(); }
                } else if (dt == "real" || dt == "float4") {
                    auto* b = static_cast<arrow::FloatBuilder*>(builder.get());
                    try { status = b->Append(std::stof(col_val)); } catch(...) { status = b->AppendNull(); }
                } else if (dt == "double precision" || dt == "float8" || dt == "numeric") {
                    auto* b = static_cast<arrow::DoubleBuilder*>(builder.get());
                    try { status = b->Append(std::stod(col_val)); } catch(...) { status = b->AppendNull(); }
                } else if (dt == "boolean" || dt == "bool") {
                    auto* b = static_cast<arrow::BooleanBuilder*>(builder.get());
                    bool val = (col_val == "t" || col_val == "true" || col_val == "1");
                    status = b->Append(val);
                } else {
                    auto* b = static_cast<arrow::StringBuilder*>(builder.get());
                    status = b->Append(col_val);
                }
                if (!status.ok()) all_ok = false;
                break;
            }
            default:
                break;
        }
    }
    
    // PAD: If the message had fewer columns than we expect, append NULL to the remaining ones
    for (size_t col = ncolumns; col < info_.columns.size(); ++col) {
        if (!builders_[col]->AppendNull().ok()) all_ok = false;
    }
    
    if (all_ok) {
        size_t col_idx = info_.columns.size();
        auto* op_builder = static_cast<arrow::StringBuilder*>(builders_[col_idx++].get());
        auto* ts_builder = static_cast<arrow::Int64Builder*>(builders_[col_idx++].get());
        auto* lsn_builder = static_cast<arrow::UInt64Builder*>(builders_[col_idx++].get());
        
        std::string op_str = (msg_type == 'I') ? "INSERT" : "UPDATE";
        uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        
        if (!op_builder->Append(op_str).ok()) all_ok = false;
        if (!ts_builder->Append(us).ok()) all_ok = false;
        if (!lsn_builder->Append(msg.lsn).ok()) all_ok = false;
    }
    
    if (all_ok) {
        current_rows_++;
        if (msg_type == 'I') insert_count_++;
        else if (msg_type == 'U') update_count_++;

        if (current_rows_ % 10 == 0) {
            std::cout << "TableWriter [" << info_.table_name << "]: Progress " << current_rows_ << "/" << row_group_size_ << std::endl;
        }
    }
    
    if (current_rows_ >= row_group_size_) {
        flushPartition(0); // Regular flush
    }
}

void TableWriter::flushPartition(uint64_t epoch_id) {
    if (current_rows_ == 0) {
        committed_lsn_val_.store(latest_lsn_);
        last_flushed_epoch_.store(epoch_id);
        return;
    }
    
    std::cout << "TableWriter [" << info_.table_name << "]: Writing Parquet batch (Epoch " << epoch_id << ") with " << current_rows_ << " rows..." << std::endl;
    
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (auto& builder : builders_) {
        std::shared_ptr<arrow::Array> array;
        PARQUET_THROW_NOT_OK(builder->Finish(&array));
        arrays.push_back(array);
    }
    
    std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema_, arrays);
    std::string table_dir = output_dir_.empty() ? info_.table_name : (output_dir_ + "/" + info_.table_name);
    std::filesystem::create_directories(table_dir);
    
    std::string filename;
    if (epoch_id > 0) {
        filename = info_.table_name + "_epoch_" + std::to_string(epoch_id) + ".parquet";
    } else {
        filename = info_.table_name + "_" + std::to_string(latest_lsn_) + ".parquet";
    }
    std::string full_path = table_dir + "/" + filename;
    
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(full_path));
    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, current_rows_));
    PARQUET_THROW_NOT_OK(outfile->Close());
    
    std::cout << "TableWriter [" << info_.table_name << "]: Wrote " << filename << std::endl;

    // Delta Protocol Generation
    size_t file_size = std::filesystem::file_size(full_path);
    std::string dynamic_schema = generateDeltaSchemaJSON();
    DeltaLogWriter::writeCommit(table_dir, commit_version_++, filename, file_size, dynamic_schema);
    
    committed_lsn_val_.store(latest_lsn_);
    last_flushed_epoch_.store(epoch_id);
    
    resetBuilders();
}
