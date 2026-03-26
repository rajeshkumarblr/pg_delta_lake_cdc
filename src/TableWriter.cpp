#include "TableWriter.hpp"
#include <iostream>
#include <chrono>
#include <arpa/inet.h>
#include <cstring>

TableWriter::TableWriter(const TableInfo& info, const std::string& output_dir, size_t row_group_size)
    : info_(info), output_dir_(output_dir), file_counter_(0), insert_count_(0), update_count_(0), row_group_size_(row_group_size), current_rows_(0) {
    setupSchemaAndBuilders();
}

TableWriter::~TableWriter() {
    if (current_rows_ > 0) {
        flushPartition();
    }
}

void TableWriter::setupSchemaAndBuilders() {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    
    for (const auto& col : info_.columns) {
        std::shared_ptr<arrow::DataType> arrow_type;
        const std::string& dt = col.data_type;
        
        if (dt == "integer" || dt == "int4" || dt == "serial") {
            arrow_type = arrow::int32();
        } else if (dt == "bigint" || dt == "int8" || dt == "bigserial") {
            arrow_type = arrow::int64();
        } else if (dt == "real" || dt == "float4") {
            arrow_type = arrow::float32();
        } else if (dt == "double precision" || dt == "float8" || dt == "numeric") {
            arrow_type = arrow::float64();
        } else if (dt == "boolean" || dt == "bool") {
            arrow_type = arrow::boolean();
        } else {
            arrow_type = arrow::utf8(); // fallback to string
        }
        
        fields.push_back(arrow::field(col.name, arrow_type, col.is_nullable));
    }
    
    // Add metadata columns
    fields.push_back(arrow::field("_cdc_op", arrow::utf8(), false));
    fields.push_back(arrow::field("_cdc_timestamp", arrow::int64(), false));
    
    schema_ = arrow::schema(fields);
    resetBuilders();
}

void TableWriter::resetBuilders() {
    builders_.clear();
    auto pool = arrow::default_memory_pool();
    
    for (const auto& col : info_.columns) {
        std::shared_ptr<arrow::ArrayBuilder> builder;
        const std::string& dt = col.data_type;
        
        if (dt == "integer" || dt == "int4" || dt == "serial") {
            builder = std::make_shared<arrow::Int32Builder>(pool);
        } else if (dt == "bigint" || dt == "int8" || dt == "bigserial") {
            builder = std::make_shared<arrow::Int64Builder>(pool);
        } else if (dt == "real" || dt == "float4") {
            builder = std::make_shared<arrow::FloatBuilder>(pool);
        } else if (dt == "double precision" || dt == "float8" || dt == "numeric") {
            builder = std::make_shared<arrow::DoubleBuilder>(pool);
        } else if (dt == "boolean" || dt == "bool") {
            builder = std::make_shared<arrow::BooleanBuilder>(pool);
        } else {
            builder = std::make_shared<arrow::StringBuilder>(pool);
        }
        builders_.push_back(builder);
    }
    
    // Builders for metadata
    builders_.push_back(std::make_shared<arrow::StringBuilder>(pool)); // _cdc_op
    builders_.push_back(std::make_shared<arrow::Int64Builder>(pool));   // _cdc_timestamp
    
    current_rows_ = 0;
    insert_count_ = 0;
    update_count_ = 0;
}

void TableWriter::appendRow(const char* data, size_t length) {
    if (length == 0) return;
    
    char msg_type = data[0];
    if (msg_type != 'I' && msg_type != 'U' && msg_type != 'D') return;
    
    size_t offset = 1; // Skip message type 'I', 'U', 'D'
    offset += 4; // Skip relation_id
    
    if (msg_type == 'D') return; // For CDC, maybe ignore deletes for now or handle them 
    if (msg_type == 'U') {
        // Updates might have 'K' (key) or 'O' (old) tuple before 'N'.
        // Let's just scan for 'N' to keep it robust
        while (offset < length && data[offset] != 'N') {
            offset++;
        }
    }
    
    if (offset + 1 > length) return;
    char n_type = data[offset++];
    if (n_type != 'N') {
        std::cerr << "TableWriter: Failed to find 'N' tuple in message. Found '" << n_type << "' instead." << std::endl;
        return; 
    }
    
    if (offset + 2 > length) return;
    uint16_t ncolumns_n;
    std::memcpy(&ncolumns_n, data + offset, 2);
    uint16_t ncolumns = ntohs(ncolumns_n);
    offset += 2;
    
    bool all_ok = true;
    
    for (uint16_t col = 0; col < ncolumns && col < builders_.size() && offset < length; ++col) {
        char kind = data[offset++];
        auto& builder = builders_[col];
        
        if (kind == 'n') { // null
            if (!builder->AppendNull().ok()) all_ok = false;
        } else if (kind == 'u') { // unchanged toast
            if (!builder->AppendNull().ok()) all_ok = false; // we just append null for unchanged toast in this simple cdc
        } else if (kind == 't' || kind == 'b') { // text or binary
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
        }
    }
    
    if (all_ok) {
        // Append metadata columns
        auto* op_builder = static_cast<arrow::StringBuilder*>(builders_[builders_.size() - 2].get());
        auto* ts_builder = static_cast<arrow::Int64Builder*>(builders_[builders_.size() - 1].get());
        
        std::string op_str = (msg_type == 'I') ? "INSERT" : "UPDATE";
        uint64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        
        if (!op_builder->Append(op_str).ok()) all_ok = false;
        if (!ts_builder->Append(ms).ok()) all_ok = false;
    }
    
    if (all_ok) {
        current_rows_++;
        if (msg_type == 'I') insert_count_++;
        else if (msg_type == 'U') update_count_++;

        if (current_rows_ % 10 == 0) {
            std::cout << "TableWriter [" << info_.table_name << "]: Progress " << current_rows_ << "/100 (Inserts: " << insert_count_ << ", Updates: " << update_count_ << ")" << std::endl;
        }
    }
    
    if (current_rows_ >= row_group_size_) {
        flushPartition();
    }
}

void TableWriter::flushPartition() {
    if (current_rows_ == 0) return;
    
    std::cout << "TableWriter [" << info_.table_name << "]: Committing " << current_rows_ << " rows to Parquet..." << std::endl;
    
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (auto& builder : builders_) {
        std::shared_ptr<arrow::Array> array;
        PARQUET_THROW_NOT_OK(builder->Finish(&array));
        arrays.push_back(array);
    }
    
    std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema_, arrays);
    
    std::string filename = info_.table_name + "_" + std::to_string(++file_counter_) + ".parquet";
    std::string full_path = output_dir_.empty() ? filename : (output_dir_ + "/" + filename);
    
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(
        outfile,
        arrow::io::FileOutputStream::Open(full_path)
    );
    
    PARQUET_THROW_NOT_OK(
        parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, current_rows_)
    );
    
    std::cout << "Successfully wrote " << filename << std::endl;
    resetBuilders();
}
