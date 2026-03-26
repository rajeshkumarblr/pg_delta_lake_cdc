#include "NetworkReceiver.hpp"
#include <iostream>
#include <stdexcept>
#include <cstring>
#include <arpa/inet.h>

NetworkReceiver::NetworkReceiver(const std::string& conninfo, BoundedBuffer<WalMessage>& buffer, std::shared_ptr<TableRegistry> registry)
    : conninfo_(conninfo), buffer_(buffer), registry_(std::move(registry)), conn_(nullptr), keep_running_(true) {
}

NetworkReceiver::~NetworkReceiver() {
    stop();
    if (conn_) {
        PQfinish(conn_);
    }
}

void NetworkReceiver::stop() {
    keep_running_ = false;
}

void NetworkReceiver::fetchSchemas(PGconn* normal_conn) {
    std::cout << "Fetching schema definitions from PostgreSQL..." << std::endl;
    // We get columns for all tables in 'public' schema
    std::string query = 
        "SELECT table_schema, table_name, column_name, data_type, is_nullable "
        "FROM information_schema.columns "
        "WHERE table_schema = 'public' "
        "ORDER BY table_name, ordinal_position;";
        
    PGresult* res = PQexec(normal_conn, query.c_str());
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::string err = PQerrorMessage(normal_conn);
        PQclear(res);
        throw std::runtime_error("Failed to fetch schemas: " + err);
    }

    int rows = PQntuples(res);
    std::string current_table = "";
    TableInfo current_info;

    for (int i = 0; i < rows; ++i) {
        std::string schema = PQgetvalue(res, i, 0);
        std::string table = PQgetvalue(res, i, 1);
        std::string col_name = PQgetvalue(res, i, 2);
        std::string data_type = PQgetvalue(res, i, 3);
        std::string is_nullable_str = PQgetvalue(res, i, 4);

        if (table != current_table) {
            if (!current_table.empty()) {
                registry_->addTable(current_info.schema, current_table, current_info);
            }
            current_table = table;
            current_info.schema = schema;
            current_info.table_name = table;
            current_info.columns.clear();
        }

        ColumnInfo col;
        col.name = col_name;
        col.data_type = data_type;
        col.is_nullable = (is_nullable_str == "YES");
        current_info.columns.push_back(col);
    }
    
    if (!current_table.empty()) {
        registry_->addTable(current_info.schema, current_table, current_info);
    }

    PQclear(res);
    std::cout << "Schema definitions successfully populated in registry." << std::endl;
}

void NetworkReceiver::connect() {
    PGconn* normal_conn = PQconnectdb(conninfo_.c_str());
    if (PQstatus(normal_conn) != CONNECTION_OK) {
        std::string err = PQerrorMessage(normal_conn);
        PQfinish(normal_conn);
        throw std::runtime_error("Standard database connection failed: " + err);
    }
    fetchSchemas(normal_conn);
    PQfinish(normal_conn);

    std::string rep_conninfo = conninfo_;
    
    if (rep_conninfo.find("postgres://") == 0 || rep_conninfo.find("postgresql://") == 0) {
        if (rep_conninfo.find('?') == std::string::npos) {
            rep_conninfo += "?replication=database";
        } else {
            rep_conninfo += "&replication=database";
        }
    } else {
        rep_conninfo += " replication=database";
    }

    conn_ = PQconnectdb(rep_conninfo.c_str());

    if (PQstatus(conn_) != CONNECTION_OK) {
        std::string err = PQerrorMessage(conn_);
        PQfinish(conn_);
        conn_ = nullptr;
        throw std::runtime_error("Connection to database failed: " + err);
    }
    std::cout << "Connected to PostgreSQL for logical replication." << std::endl;
}

void NetworkReceiver::startLogicalReplication() {
    const char* env_slot = std::getenv("PG_SLOT_NAME");
    const char* env_pub = std::getenv("PG_PUBLICATION_NAME");
    
    std::string slot_name = env_slot ? env_slot : "hn_cdc_stream_slot";
    std::string pub_name = env_pub ? env_pub : "hn_cdc_stream";

    std::string query = "START_REPLICATION SLOT \"" + slot_name + "\" LOGICAL 0/0 (proto_version '1', publication_names '" + pub_name + "');";
    PGresult* res = PQexec(conn_, query.c_str());
    
    if (PQresultStatus(res) != PGRES_COPY_BOTH) {
        std::string err = PQerrorMessage(conn_);
        PQclear(res);
        throw std::runtime_error("Could not start logical replication: " + err);
    }
    PQclear(res);
    std::cout << "Started logical replication stream on slot '" << slot_name << "'." << std::endl;
}

void NetworkReceiver::receiveLoop() {
    while (keep_running_) {
        char* copy_data = nullptr;
        int ret = PQgetCopyData(conn_, &copy_data, 0); // 0 = block waiting for data
        
        if (ret > 0) {
            handleCopyData(copy_data, ret);
            PQfreemem(copy_data);
        } else if (ret == 0) {
            continue;
        } else if (ret == -1) {
            std::cout << "End of COPY data stream from PostgreSQL." << std::endl;
            break;
        } else if (ret == -2) {
            throw std::runtime_error(std::string("Error during PQgetCopyData: ") + PQerrorMessage(conn_));
        }
    }
}

void NetworkReceiver::handleCopyData(char* msg, int length) {
    if (length == 0) return;
    
    char msg_type = msg[0];
    if (msg_type == 'w') { // WAL data
        if (length < 25) return;
        
        static uint64_t wal_msg_count = 0;
        if (++wal_msg_count % 1000 == 0) {
            std::cout << "Received " << wal_msg_count << " WAL messages so far..." << std::endl;
        }

        int wal_payload_len = length - 25;
        if (wal_payload_len > 0) {
            char* payload_start = msg + 25;
            char pgoutput_msg_type = payload_start[0];
            
            if (pgoutput_msg_type == 'R') {
                size_t offset = 1;
                if (offset + 4 > wal_payload_len) return;
                
                uint32_t rel_id_n;
                std::memcpy(&rel_id_n, payload_start + offset, 4);
                uint32_t rel_id = ntohl(rel_id_n);
                offset += 4;
                
                std::string schema_name;
                while (offset < wal_payload_len && payload_start[offset] != '\0') {
                    schema_name += payload_start[offset++];
                }
                offset++;
                
                std::string table_name;
                while (offset < wal_payload_len && payload_start[offset] != '\0') {
                    table_name += payload_start[offset++];
                }
                offset++;
                
                // Read replica identity (1 byte)
                if (offset + 1 > wal_payload_len) return;
                offset++; 
                
                // Read num_columns (2 bytes)
                if (offset + 2 > wal_payload_len) return;
                uint16_t num_columns_n;
                std::memcpy(&num_columns_n, payload_start + offset, 2);
                uint16_t num_columns = ntohs(num_columns_n);
                offset += 2;
                
                TableInfo fetched_info;
                bool has_catalog = registry_->getTable(schema_name, table_name, fetched_info);
                
                TableInfo stream_info;
                stream_info.schema = schema_name;
                stream_info.table_name = table_name;
                
                for (uint16_t i = 0; i < num_columns; ++i) {
                    if (offset + 1 > wal_payload_len) break;
                    offset += 1; // flags
                    
                    std::string col_name;
                    while (offset < wal_payload_len && payload_start[offset] != '\0') {
                        col_name += payload_start[offset++];
                    }
                    offset++;
                    
                    if (offset + 8 > wal_payload_len) break;
                    offset += 8; // skip DataType OID (4 bytes) and atttypmod (4 bytes)
                    
                    std::string mapped_type = "text"; 
                    bool is_nullable = true;
                    
                    if (has_catalog) {
                        for (const auto& fc : fetched_info.columns) {
                            if (fc.name == col_name) {
                                mapped_type = fc.data_type;
                                is_nullable = fc.is_nullable;
                                break;
                            }
                        }
                    }
                    
                    ColumnInfo col;
                    col.name = col_name;
                    col.data_type = mapped_type;
                    col.is_nullable = is_nullable;
                    stream_info.columns.push_back(col);
                }
                
                std::cout << "Mapped Relation ID " << rel_id << " to " << schema_name << "." << table_name << " (" << stream_info.columns.size() << " streaming replica columns)" << std::endl;
                
                registry_->mapRelationId(rel_id, stream_info);
                
            } else if (pgoutput_msg_type == 'I' || pgoutput_msg_type == 'U' || pgoutput_msg_type == 'D') {
                if (wal_payload_len < 5) return;
                
                uint32_t rel_id_n;
                std::memcpy(&rel_id_n, payload_start + 1, 4);
                uint32_t relation_id = ntohl(rel_id_n);
                
                // Check if this relation_id belongs to the target schema (e.g. public, or mapped schema)
                TableInfo info;
                if (registry_->getTableByRelationId(relation_id, info)) {
                    WalMessage wal_msg;
                    wal_msg.relation_id = relation_id;
                    wal_msg.payload.assign(payload_start, payload_start + wal_payload_len);
                    buffer_.push(std::move(wal_msg)); 
                }
            }
        }
    } else if (msg_type == 'k') { // Keepalive
        // std::cout << "Received Keepalive message from PostgreSQL." << std::endl;
        if (length >= 18) {
            char reply_requested = msg[17];
            if (reply_requested) {
                // std::cout << "PostgreSQL requested an immediate reply to the Keepalive. Sending status update..." << std::endl;
                sendStandbyStatusUpdate();
            }
        }
    }
}

void NetworkReceiver::sendStandbyStatusUpdate() {
    char reply[34];
    reply[0] = 'r';
    // For this simple example, we just send all 0s for LSNs and timestamps.
    // In a production app, we would track and send the actual applied LSNs.
    std::memset(reply + 1, 0, 33);
    
    if (PQputCopyData(conn_, reply, 34) <= 0 || PQflush(conn_) != 0) {
        std::cerr << "Could not send standby status update: " << PQerrorMessage(conn_) << std::endl;
    }
}

void NetworkReceiver::run() {
    connect();
    startLogicalReplication();
    receiveLoop();
}
