#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <mutex>

struct ColumnInfo {
    std::string name;
    std::string data_type;
    bool is_nullable;
};

struct TableInfo {
    uint32_t rel_id;
    std::string schema;
    std::string table_name;
    std::vector<ColumnInfo> columns;
};

class TableRegistry {
public:
    void addTable(const std::string& schema, const std::string& table_name, const TableInfo& info) {
        std::lock_guard<std::mutex> lock(mutex_);
        tables_[schema + "." + table_name] = info;
    }

    bool getTable(const std::string& schema, const std::string& table_name, TableInfo& out_info) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = tables_.find(schema + "." + table_name);
        if (it != tables_.end()) {
            out_info = it->second;
            return true;
        }
        return false;
    }

    void mapRelationId(uint32_t relation_id, const TableInfo& info) {
        std::lock_guard<std::mutex> lock(mutex_);
        active_relations_[relation_id] = info;
    }

    bool getTableByRelationId(uint32_t relation_id, TableInfo& out_info) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = active_relations_.find(relation_id);
        if (it != active_relations_.end()) {
            out_info = it->second;
            return true;
        }
        return false;
    }

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, TableInfo> tables_; 
    std::unordered_map<uint32_t, TableInfo> active_relations_; 
};
