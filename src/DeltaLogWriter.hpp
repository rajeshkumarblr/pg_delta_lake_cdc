#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <filesystem>
#include <chrono>
#include <iomanip>
#include <sstream>

#include <arrow/filesystem/filesystem.h>
#include <arrow/result.h>
#include <parquet/exception.h>
#include <iostream>

class DeltaLogWriter {
public:
    static void writeCommit(std::shared_ptr<arrow::fs::FileSystem> fs,
                           const std::string& table_path, 
                           int commit_version, 
                           const std::string& parquet_filename, 
                           size_t file_size, 
                           const std::string& schema_string) {
        
        std::string log_dir = table_path + "/_delta_log";
        auto status = fs->CreateDir(log_dir); // Recursive by default in many impls

        std::ostringstream oss;
        oss << std::setfill('0') << std::setw(20) << commit_version << ".json";
        std::string log_file_path = log_dir + "/" + oss.str();

        std::shared_ptr<arrow::io::OutputStream> log_stream;
        PARQUET_ASSIGN_OR_THROW(log_stream, fs->OpenOutputStream(log_file_path));

        uint64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();

        std::stringstream ss;
        if (commit_version == 0) {
            // 1. Protocol
            ss << "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}" << "\n";
            
            // 2. MetaData
            ss << "{\"metaData\":{"
                     << "\"id\":\"" << "d317a-8b1c-4e89-a3b4-f58c7" << "\"," 
                     << "\"format\":{\"provider\":\"parquet\",\"options\":{}},"
                     << "\"schemaString\":\"" << escapeJson(schema_string) << "\","
                     << "\"partitionColumns\":[],"
                     << "\"configuration\":{},"
                     << "\"createdTime\":" << now_ms
                     << "}}" << "\n";
        }

        // Add
        ss << "{\"add\":{"
                 << "\"path\":\"" << parquet_filename << "\","
                 << "\"partitionValues\":{},"
                 << "\"size\":" << file_size << ","
                 << "\"modificationTime\":" << now_ms << ","
                 << "\"dataChange\":true" 
                 << "}}" << "\n";

        // CommitInfo
        ss << "{\"commitInfo\":{"
                 << "\"timestamp\":" << now_ms << ","
                 << "\"operation\":\"WRITE\","
                 << "\"operationParameters\":{\"mode\":\"Append\"},"
                 << "\"isBlindAppend\":true"
                 << "}}" << "\n";

        PARQUET_THROW_NOT_OK(log_stream->Write(ss.str()));
        PARQUET_THROW_NOT_OK(log_stream->Close());
    }

private:
    static std::string escapeJson(const std::string& s) {
        std::ostringstream oss;
        for (auto c : s) {
            switch (c) {
                case '"': oss << "\\\""; break;
                case '\\': oss << "\\\\"; break;
                case '\b': oss << "\\b"; break;
                case '\f': oss << "\\f"; break;
                case '\n': oss << "\\n"; break;
                case '\r': oss << "\\r"; break;
                case '\t': oss << "\\t"; break;
                default: oss << c; break;
            }
        }
        return oss.str();
    }
};
