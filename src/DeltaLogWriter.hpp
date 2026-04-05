#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <filesystem>
#include <chrono>
#include <iomanip>
#include <sstream>

namespace fs = std::filesystem;

class DeltaLogWriter {
public:
    static void writeCommit(const std::string& output_dir, 
                           int commit_version, 
                           const std::string& parquet_filename, 
                           size_t file_size, 
                           const std::string& schema_string) {
        
        std::string log_dir = output_dir + "/_delta_log";
        fs::create_directories(log_dir);

        std::ostringstream oss;
        oss << std::setfill('0') << std::setw(20) << commit_version << ".json";
        std::string log_filename = log_dir + "/" + oss.str();

        std::ofstream log_file(log_filename);
        if (!log_file.is_open()) {
            throw std::runtime_error("Failed to open delta log file: " + log_filename);
        }

        uint64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();

        if (commit_version == 0) {
            // 1. Protocol
            log_file << "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}" << "\n";
            
            // 2. MetaData
            log_file << "{\"metaData\":{"
                     << "\"id\":\"" << "d317a-8b1c-4e89-a3b4-f58c7" << "\"," 
                     << "\"format\":{\"provider\":\"parquet\",\"options\":{}},"
                     << "\"schemaString\":\"" << escapeJson(schema_string) << "\","
                     << "\"partitionColumns\":[],"
                     << "\"configuration\":{},"
                     << "\"createdTime\":" << now_ms
                     << "}}" << "\n";
        }

        // Add
        log_file << "{\"add\":{"
                 << "\"path\":\"" << parquet_filename << "\","
                 << "\"partitionValues\":{},"
                 << "\"size\":" << file_size << ","
                 << "\"modificationTime\":" << now_ms << ","
                 << "\"dataChange\":true" 
                 << "}}" << "\n";

        // CommitInfo
        log_file << "{\"commitInfo\":{"
                 << "\"timestamp\":" << now_ms << ","
                 << "\"operation\":\"WRITE\","
                 << "\"operationParameters\":{\"mode\":\"Append\"},"
                 << "\"isBlindAppend\":true"
                 << "}}" << "\n";

        log_file.close();
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
