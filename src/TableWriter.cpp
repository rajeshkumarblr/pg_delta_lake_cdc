#include "TableWriter.hpp"
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/result.h>
#include <parquet/exception.h>
#include <iostream>
#include <chrono>
#include <arpa/inet.h>
#include <cstring>
#include <filesystem>
#include "DeltaLogWriter.hpp"

TableWriter::TableWriter(const TableInfo& info, const std::string& output_dir, 
                         std::shared_ptr<std::atomic<uint64_t>> committed_lsn,
                         size_t row_group_size,
                         uint64_t watermark_lsn)
    : info_(info), output_dir_(output_dir), file_counter_(1),
      insert_count_(0), update_count_(0), delete_count_(0),
      row_group_size_(row_group_size), current_rows_(0),
      commit_version_(0),
      latest_lsn_(watermark_lsn), watermark_lsn_(watermark_lsn),
      global_committed_lsn_(committed_lsn), 
      keep_running_(false), oldest_lsn_in_queue_(0), pending_epoch_(0), queue_(10000) {
    // Initialize FileSystem from URI or local path
    if (!output_dir_.empty() && output_dir_.find("://") != std::string::npos) {
        std::string path;
        auto result = arrow::fs::FileSystemFromUri(output_dir_, &path);
        if (!result.ok()) throw std::runtime_error(result.status().ToString());
        fs_ = result.ValueOrDie();
        base_path_ = path;
    } else {
        fs_ = std::make_shared<arrow::fs::LocalFileSystem>();
        if (!output_dir_.empty() && output_dir_[0] == '/') {
            base_path_ = output_dir_;
        } else if (!output_dir_.empty()) {
            base_path_ = std::filesystem::current_path().string() + "/" + output_dir_;
        } else {
            base_path_ = std::filesystem::current_path().string();
        }
    }

    setupSchemaAndBuilders();

    // Initialize Delta Log version by scanning the _delta_log directory
    commit_version_ = 0;
    try {
        std::string table_dir = base_path_ + "/" + info_.table_name;
        std::string log_dir = table_dir + "/_delta_log";
        
        // Do NOT eagerly create dirs here - that would fool the snapshot
        // idempotency guard into thinking data already exists.
        // Dirs are created on-demand in flushPartition().
        
        std::cout << "TableWriter [" << info_.table_name << "]: Scanning for existing Delta logs in " << log_dir << std::endl;
        if (std::filesystem::exists(log_dir)) {
            for (const auto& entry : std::filesystem::directory_iterator(log_dir)) {
                std::string filename = entry.path().filename().string();
                if (filename.size() > 5 && filename.substr(filename.size() - 5) == ".json") {
                    try {
                        std::string version_str = filename.substr(0, 20);
                        int v = std::stoi(version_str);
                        if (v >= commit_version_) commit_version_ = v + 1;
                    } catch (...) {}
                }
            }
        }
    } catch (...) {}
    
    std::cout << "TableWriter [" << info_.table_name << "]: Bound to " << (base_path_ + "/" + info_.table_name) << ", Version " << commit_version_ << std::endl;
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

    keep_running_ = false;
    
    // Wake up worker
    WalMessage poison_pill;
    poison_pill.is_flush_signal = true; 
    // Wait up to 500ms to ensure the worker sees the stop signal
    queue_.push_for(poison_pill, std::chrono::milliseconds(500));

    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
}

void TableWriter::run() {
    auto last_flush = std::chrono::steady_clock::now();
    while (true) {
        WalMessage msg;
        bool has_msg = queue_.pop_for(msg, std::chrono::milliseconds(100));
        
        // Periodic flush check - ALWAYS check this
        if (current_rows_ > 0) {
            auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::seconds>(now - last_flush).count() >= 5) {
                std::cout << "TableWriter [" << info_.table_name << "]: Periodic flush trigger (" << current_rows_ << " rows)" << std::endl;
                flushPartition(0);
                last_flush = now;
            }
        }

        if (!has_msg) {
            if (!keep_running_) break;
            continue;
        }

        if (msg.is_flush_signal) {
            flushPartition(0);
            last_flush = std::chrono::steady_clock::now();
            if (!keep_running_) break;
            continue;
        }

        if (msg.pg_msg_type == 'S') {
            processSnapshotCopy(msg.payload.data(), msg.payload.size());
        } else {
            processInternal(msg);
        }

        if (current_rows_ >= row_group_size_) {
            flushPartition(0);
            last_flush = std::chrono::steady_clock::now();
        }
    }
    
    // Drain remaining queue before exiting
    WalMessage drain_msg;
    int drained = 0;
    while (queue_.pop_for(drain_msg, std::chrono::milliseconds(10))) {
        if (!drain_msg.is_flush_signal) {
            if (drain_msg.pg_msg_type == 'S') {
                processSnapshotCopy(drain_msg.payload.data(), drain_msg.payload.size());
            } else {
                processInternal(drain_msg);
            }
            drained++;
        }
    }
    
    if (drained > 0) {
        std::cout << "TableWriter [" << info_.table_name << "]: Drained " << drained << " rows from queue on shutdown." << std::endl;
    }

    // Final flush of any remaining data
    if (current_rows_ > 0) {
        std::cout << "TableWriter [" << info_.table_name << "]: Worker stopping. Final flush of " << current_rows_ << " rows." << std::endl;
        flushPartition(0);
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
        } else if (dt == "timestamp with time zone" || dt == "timestamp" || dt == "timestamptz") {
            // Store timestamps as strings - pgoutput sends them as text,
            // and binary COPY path will convert to string too.
            arrow_type = arrow::utf8();
            builder = std::make_shared<arrow::StringBuilder>(pool);
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
        if (oldest_lsn_in_queue_ == 0) oldest_lsn_in_queue_ = lsn;
    }
    
    // Non-blocking push to avoid stalling the dispatcher
    if (!queue_.push_for(msg, std::chrono::milliseconds(100))) {
        std::cerr << "[TableWriter:" << info_.table_name << "] Queue FULL. Dropping LSN " << lsn << std::endl;
    } else {
        std::cout << "[TableWriter:" << info_.table_name << "] Queued LSN " << lsn << " for processing." << std::endl;
    }
}

void TableWriter::sendFlushSignal(uint64_t epoch_id) {
    WalMessage msg;
    msg.is_flush_signal = true;
    msg.epoch_id = epoch_id;
    msg.relation_id = info_.rel_id;
    queue_.push(msg);
}

void TableWriter::forceFlush() {
    // Push a flush signal and wait for it to be processed
    WalMessage msg;
    msg.is_flush_signal = true;
    msg.epoch_id = 0;
    msg.relation_id = info_.rel_id;
    queue_.push(msg);
    
    // Simple wait: spin/sleep until queue is empty and current_rows_ is 0
    // In a prod system, use a promise/future or condition variable.
    int retries = 0;
    while ((!queue_.empty() || current_rows_ > 0) && retries++ < 100) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
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

    // Handover Guard: Ignore WAL messages that were already captured by the snapshot
    // Use strict < (not <=) because records at exactly the watermark LSN may include
    // new DML (DELETEs) that happened after the snapshot was taken.
    if (msg.pg_msg_type != 'S' && msg.lsn < watermark_lsn_) {
        return;
    }

    if (msg.pg_msg_type == 'S') {
        processSnapshotCopy(msg.payload.data(), msg.payload.size());
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
    
    if (msg_type == 'U') {
        while (offset < length && data[offset] != 'N') {
            offset++; 
        }
    }

    if (offset + 1 > length) return;
    const char tuple_type = data[offset++];
    if (tuple_type != 'N' && tuple_type != 'K' && tuple_type != 'O') return;
    
    if (offset + 2 > length) return;
    uint16_t ncolumns_n;
    std::memcpy(&ncolumns_n, data + offset, 2);
    uint16_t ncolumns = ntohs(ncolumns_n);
    offset += 2;
    
    uint16_t msg_columns = ncolumns;
    uint16_t schema_columns = info_.columns.size();
    
    std::vector<std::string> row_buffer(schema_columns);
    std::vector<bool> is_null(schema_columns, true);
    bool all_ok = true;

    for (size_t col = 0; col < std::max<size_t>(msg_columns, schema_columns); ++col) {
        if (offset >= length) break;
        
        char kind = data[offset++];
        bool in_schema = (col < schema_columns);

        if (kind == 't' || kind == 'b') {
            if (offset + 4 > length) { all_ok = false; break; }
            uint32_t col_len_n;
            std::memcpy(&col_len_n, data + offset, 4);
            uint32_t col_len = ntohl(col_len_n);
            offset += 4;

            if (offset + col_len > length) {
                std::cerr << "TableWriter [" << info_.table_name << "]: Buffer Overrun at col " << col << ". LSN " << msg.lsn << std::endl;
                all_ok = false;
                break;
            }

            if (in_schema) {
                row_buffer[col] = std::string(data + offset, col_len);
                is_null[col] = false;
            }
            offset += col_len;
        } else if (in_schema) {
            is_null[col] = true;
        }
    }
    
    if (all_ok) {
        // Phase 2: Atomic commit to Arrow
        // First, check if we can parse all types
        std::vector<arrow::Status> statuses;
        for (size_t i = 0; i < schema_columns; ++i) {
            auto& builder = builders_[i];
            if (is_null[i]) {
                statuses.push_back(builder->AppendNull());
            } else {
                const std::string& dt = info_.columns[i].data_type;
                const std::string& val = row_buffer[i];
                try {
                    if (dt == "integer" || dt == "int4" || dt == "serial") {
                        statuses.push_back(static_cast<arrow::Int32Builder*>(builder.get())->Append(std::stoi(val)));
                    } else if (dt == "bigint" || dt == "int8" || dt == "bigserial") {
                        statuses.push_back(static_cast<arrow::Int64Builder*>(builder.get())->Append(std::stoll(val)));
                    } else if (dt == "timestamp with time zone" || dt == "timestamp" || dt == "timestamptz") {
                        // Timestamps stored as strings; pgoutput sends text like '2026-04-12 15:30:00+00'
                        statuses.push_back(static_cast<arrow::StringBuilder*>(builder.get())->Append(val));
                    } else if (dt == "real" || dt == "float4") {
                        statuses.push_back(static_cast<arrow::FloatBuilder*>(builder.get())->Append(std::stof(val)));
                    } else if (dt == "double precision" || dt == "float8" || dt == "numeric") {
                        statuses.push_back(static_cast<arrow::DoubleBuilder*>(builder.get())->Append(std::stod(val)));
                    } else if (dt == "boolean" || dt == "bool") {
                        statuses.push_back(static_cast<arrow::BooleanBuilder*>(builder.get())->Append(val == "t" || val == "true" || val == "1"));
                    } else {
                        statuses.push_back(static_cast<arrow::StringBuilder*>(builder.get())->Append(val));
                    }
                } catch (...) {
                    statuses.push_back(builder->AppendNull());
                }
            }
        }

        // Append metadata
        std::string op_str = "UNKNOWN";
        char mtype = msg.pg_msg_type;
        if (mtype == 'I') op_str = "INSERT";
        else if (mtype == 'U') op_str = "UPDATE";
        else if (mtype == 'D') op_str = "DELETE";
        uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        statuses.push_back(static_cast<arrow::StringBuilder*>(builders_[schema_columns].get())->Append(op_str));
        statuses.push_back(static_cast<arrow::Int64Builder*>(builders_[schema_columns+1].get())->Append(us));
        statuses.push_back(static_cast<arrow::UInt64Builder*>(builders_[schema_columns+2].get())->Append(msg.lsn));

        for (const auto& s : statuses) {
            if (!s.ok()) {
                std::cerr << "TableWriter [" << info_.table_name << "]: CRITICAL APPEND FAILURE: " << s.ToString() << std::endl;
                all_ok = false;
                break;
            }
        }
    }
    
    if (all_ok) {
        current_rows_++;
        if (msg_type == 'I') insert_count_++;
        else if (msg_type == 'U') update_count_++;
        else if (msg_type == 'D') delete_count_++;

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
    
    std::cout << "[TableWriter:" << info_.table_name << "] Flushing " << current_rows_ << " rows (Batch size reached OR Periodic flush trigger)" << std::endl;
    
    std::cout << "TableWriter [" << info_.table_name << "]: Writing Parquet batch (Epoch " << epoch_id << ") with " << current_rows_ << " rows..." << std::endl;
    
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (auto& builder : builders_) {
        std::shared_ptr<arrow::Array> array;
        PARQUET_THROW_NOT_OK(builder->Finish(&array));
        arrays.push_back(array);
    }
    
    std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema_, arrays);
    std::string table_dir = base_path_ + "/" + info_.table_name;
    PARQUET_THROW_NOT_OK(fs_->CreateDir(table_dir));
    
    std::string filename;
    uint64_t now_us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    if (epoch_id > 0) {
        filename = info_.table_name + "_epoch_" + std::to_string(epoch_id) + "_" + std::to_string(now_us) + ".parquet";
    } else {
        filename = info_.table_name + "_" + std::to_string(latest_lsn_) + "_" + std::to_string(now_us) + ".parquet";
    }
    std::string full_path = table_dir + "/" + filename;
    
    auto outfile_res = fs_->OpenOutputStream(full_path);
    if (!outfile_res.ok()) {
        std::cerr << "TableWriter [" << info_.table_name << "]: FAILED to open output stream: " << outfile_res.status().ToString() << std::endl;
        return;
    }
    auto outfile = outfile_res.ValueOrDie();
    
    auto write_status = parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, current_rows_);
    if (!write_status.ok()) {
        std::cerr << "TableWriter [" << info_.table_name << "]: FAILED to write Parquet table: " << write_status.ToString() << std::endl;
        return;
    }
    auto close_status = outfile->Close();
    if (!close_status.ok()) {
        std::cerr << "TableWriter [" << info_.table_name << "]: Warning: Failed to close output stream: " << close_status.ToString() << std::endl;
    }
    
    std::cout << "TableWriter [" << info_.table_name << "]: Wrote " << filename << std::endl;

    // Delta Protocol Generation
    auto info_res = fs_->GetFileInfo(full_path);
    if (!info_res.ok()) {
        std::cerr << "TableWriter [" << info_.table_name << "]: FAILED to get file info: " << info_res.status().ToString() << std::endl;
        return;
    }
    size_t file_size = info_res.ValueOrDie().size();
    
    std::string dynamic_schema = generateDeltaSchemaJSON();
    DeltaLogWriter::writeCommit(fs_, table_dir, commit_version_++, filename, file_size, dynamic_schema);
    
    committed_lsn_val_.store(latest_lsn_);
    last_flushed_epoch_.store(epoch_id);
    
    resetBuilders();
}
void TableWriter::processSnapshotCopy(const char* data, size_t length) {
    size_t offset = 0;
    
    if (offset + 2 > length) return;
    int16_t ncolumns_n;
    std::memcpy(&ncolumns_n, data + offset, 2);
    int16_t ncolumns = ntohs(ncolumns_n);
    offset += 2;

    if (ncolumns == -1) return;

    bool all_ok = true;
    for (size_t col = 0; col < info_.columns.size(); ++col) {
        if (offset + 4 > length) { all_ok = false; break; }
        
        int32_t col_len_n;
        std::memcpy(&col_len_n, data + offset, 4);
        int32_t col_len = ntohl(col_len_n);
        offset += 4;

        auto& builder = builders_[col];
        if (col_len == -1) {
            if (!builder->AppendNull().ok()) all_ok = false;
            continue;
        }

        if (offset + col_len > length) { all_ok = false; break; }
        const char* col_data = data + offset;
        offset += col_len;

        arrow::Status status;
        const std::string& dt = info_.columns[col].data_type;

        if (dt == "integer" || dt == "int4" || dt == "serial") {
            auto* b = static_cast<arrow::Int32Builder*>(builder.get());
            if (col_len == 4 && offset - col_len + 4 <= length) {
                int32_t val_n;
                std::memcpy(&val_n, col_data, 4);
                status = b->Append(ntohl(val_n));
            } else { status = b->AppendNull(); }
        } else if (dt == "bigint" || dt == "int8" || dt == "bigserial") {
            auto* b = static_cast<arrow::Int64Builder*>(builder.get());
            if (col_len == 8 && offset - col_len + 8 <= length) {
                uint64_t val_n;
                std::memcpy(&val_n, col_data, 8);
                status = b->Append(be64toh(val_n));
            } else { status = b->AppendNull(); }
        } else if (dt == "timestamp with time zone" || dt == "timestamp" || dt == "timestamptz") {
            // Timestamps stored as strings; convert binary PG timestamp (int64 microseconds) to string
            auto* b = static_cast<arrow::StringBuilder*>(builder.get());
            if (col_len == 8 && offset - col_len + 8 <= length) {
                uint64_t val_n;
                std::memcpy(&val_n, col_data, 8);
                int64_t pg_us = static_cast<int64_t>(be64toh(val_n));
                // PG epoch is 2000-01-01, convert to string representation
                status = b->Append(std::to_string(pg_us));
            } else { status = b->AppendNull(); }
        } else if (dt == "real" || dt == "float4") {
            auto* b = static_cast<arrow::FloatBuilder*>(builder.get());
            if (col_len == 4 && offset - col_len + 4 <= length) {
                uint32_t val_n;
                std::memcpy(&val_n, col_data, 4);
                uint32_t val_h = ntohl(val_n);
                float fval;
                std::memcpy(&fval, &val_h, 4);
                status = b->Append(fval);
            } else { status = b->AppendNull(); }
        } else if (dt == "double precision" || dt == "float8" || dt == "numeric") {
            auto* b = static_cast<arrow::DoubleBuilder*>(builder.get());
            if (col_len == 8 && offset - col_len + 8 <= length) {
                uint64_t val_n;
                std::memcpy(&val_n, col_data, 8);
                uint64_t val_h = be64toh(val_n);
                double dval;
                std::memcpy(&dval, &val_h, 8);
                status = b->Append(dval);
            } else { status = b->AppendNull(); }
        } else if (dt == "boolean" || dt == "bool") {
            auto* b = static_cast<arrow::BooleanBuilder*>(builder.get());
            if (col_len == 1 && offset - col_len + 1 <= length) {
                status = b->Append(col_data[0] != 0);
            } else { status = b->AppendNull(); }
        } else {
            // Default to string for text, varchar, json, etc.
            auto* b = static_cast<arrow::StringBuilder*>(builder.get());
            status = b->Append(std::string(col_data, std::max<int32_t>(0, col_len)));
        }

        if (!status.ok()) {
            all_ok = false;
        }
    }

    if (all_ok) {
        // Meta columns
        size_t col_idx = info_.columns.size();
        auto* op_builder = static_cast<arrow::StringBuilder*>(builders_[col_idx++].get());
        auto* ts_builder = static_cast<arrow::Int64Builder*>(builders_[col_idx++].get());
        auto* lsn_builder = static_cast<arrow::UInt64Builder*>(builders_[col_idx++].get());

        if (!op_builder->Append("SNAPSHOT").ok()) all_ok = false;
        // Use a fixed timestamp in the past for snapshot records 
        // to ensure they are correctly superseded by any later WAL changes
        if (!ts_builder->Append(0).ok()) all_ok = false;
        if (!lsn_builder->Append(watermark_lsn_).ok()) all_ok = false;

        if (all_ok) {
            current_rows_++;
            if (current_rows_ >= row_group_size_) {
                flushPartition(0);
            }
        }
    }
}
