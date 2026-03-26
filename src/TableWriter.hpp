#pragma once

#include "TableRegistry.hpp"
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <memory>
#include <string>
#include <vector>

class TableWriter {
public:
    TableWriter(const TableInfo& info, const std::string& output_dir, size_t row_group_size = 100);
    ~TableWriter();

    void appendRow(const char* tuple_data, size_t length);
    void flushPartition();

private:
    TableInfo info_;
    std::string output_dir_;
    size_t file_counter_;
    size_t insert_count_;
    size_t update_count_;
    size_t row_group_size_;
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders_;
    size_t current_rows_;

    void setupSchemaAndBuilders();
    void resetBuilders();
};
