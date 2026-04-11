#include "WALReceiver.hpp"
#include <arpa/inet.h>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <endian.h>
#include <thread>
#include <chrono>
#include <cstdio>

WALReceiver::WALReceiver(const std::string &conninfo,
                         BoundedBuffer<WalMessage> &buffer,
                         std::shared_ptr<TableRegistry> registry,
                         std::shared_ptr<std::atomic<uint64_t>> committed_lsn)
    : conninfo_(conninfo), buffer_(buffer), registry_(std::move(registry)),
      committed_lsn_(committed_lsn), conn_(nullptr), keep_running_(true) {}

WALReceiver::~WALReceiver() {
  stop();
  if (conn_) {
    PQfinish(conn_);
  }
}

void WALReceiver::stop() { keep_running_ = false; }

void WALReceiver::run() {
  connect();
  startLogicalReplication();
  receiveLoop();
}



void WALReceiver::connect() {
  PGconn *normal_conn = PQconnectdb(conninfo_.c_str());
  if (PQstatus(normal_conn) != CONNECTION_OK) {
    std::string err = PQerrorMessage(normal_conn);
    PQfinish(normal_conn);
    throw std::runtime_error("Standard database connection failed: " + err);
  }
  fetchSchemas(normal_conn);
  PQfinish(normal_conn);

  std::string rep_conninfo = conninfo_;

  if (rep_conninfo.find("postgres://") == 0 ||
      rep_conninfo.find("postgresql://") == 0) {
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

void WALReceiver::fetchSchemas(PGconn *normal_conn) {
  std::cout << "Fetching schema definitions from PostgreSQL..." << std::endl;
  // We get columns for all tables in 'public' schema, including OID for RelID mapping
  std::string query =
      "SELECT c.oid, n.nspname, c.relname, a.attname, format_type(a.atttypid, a.atttypmod), "
      "NOT a.attnotnull, COALESCE(i.indisprimary, false), c.relreplident "
      "FROM pg_class c "
      "JOIN pg_namespace n ON n.oid = c.relnamespace "
      "JOIN pg_attribute a ON a.attrelid = c.oid "
      "LEFT JOIN pg_index i ON i.indrelid = c.oid AND a.attnum = ANY(i.indkey) AND i.indisprimary "
      "WHERE c.relkind = 'r' AND n.nspname = 'public' AND a.attnum > 0 AND NOT a.attisdropped "
      "ORDER BY c.relname, a.attnum;";

  PGresult *res = PQexec(normal_conn, query.c_str());
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    std::string err = PQerrorMessage(normal_conn);
    PQclear(res);
    throw std::runtime_error("Failed to fetch schemas: " + err);
  }

  int rows = PQntuples(res);
  std::string current_table = "";
  TableInfo current_info;

  for (int i = 0; i < rows; ++i) {
    uint32_t oid = std::stoul(PQgetvalue(res, i, 0));
    std::string schema = PQgetvalue(res, i, 1);
    std::string table = PQgetvalue(res, i, 2);
    std::string col_name = PQgetvalue(res, i, 3);
    std::string data_type = PQgetvalue(res, i, 4);
    bool is_nullable = (std::string(PQgetvalue(res, i, 5)) == "t");
    bool is_pk = (std::string(PQgetvalue(res, i, 6)) == "t");
    char repl_ident = PQgetvalue(res, i, 7)[0];

    if (table != current_table) {
      if (!current_table.empty()) {
        registry_->mapRelationId(current_info.rel_id, current_info);
        registry_->addTable(current_info.schema, current_table, current_info);
      }
      current_table = table;
      current_info.schema = schema;
      current_info.table_name = table;
      current_info.rel_id = oid;
      current_info.repl_ident = repl_ident;
      current_info.columns.clear();
    }

    ColumnInfo col;
    col.name = col_name;
    col.data_type = data_type;
    col.is_nullable = is_nullable;
    col.pk_flag = is_pk;
    current_info.columns.push_back(col);
  }

  if (!current_table.empty()) {
    registry_->mapRelationId(current_info.rel_id, current_info);
    registry_->addTable(current_info.schema, current_table, current_info);
  }

  PQclear(res);
  std::cout << "Schema definitions successfully populated in registry." << std::endl;
}

void WALReceiver::startLogicalReplication() {
  const char *env_slot = std::getenv("PG_SLOT_NAME");
  const char *env_pub = std::getenv("PG_PUBLICATION_NAME");

  std::string slot_name = env_slot ? env_slot : "hn_stories_slot";
  std::string pub_name = env_pub ? env_pub : "hn_stories_pub";

  // Check if slot exists
  std::string check_sql = "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '" + slot_name + "';";
  PGconn *temp_conn = PQconnectdb(conninfo_.c_str());
  PGresult *check_res = PQexec(temp_conn, check_sql.c_str());
  bool exists = false;
  if (PQresultStatus(check_res) == PGRES_TUPLES_OK) {
      exists = (std::stoi(PQgetvalue(check_res, 0, 0)) > 0);
  }
  PQclear(check_res);

  if (!exists) {
      std::cout << "Creating replication slot '" << slot_name << "'..." << std::endl;
      std::string create_query = "CREATE_REPLICATION_SLOT \"" + slot_name + "\" LOGICAL pgoutput EXPORT_SNAPSHOT";
      PGresult *res = PQexec(conn_, create_query.c_str());
      
      if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
          std::string lsn_str = PQgetvalue(res, 0, 1);
          snapshot_id_ = PQgetvalue(res, 0, 2);
          
          uint32_t high, low;
          if (sscanf(lsn_str.c_str(), "%X/%X", &high, &low) == 2) {
              watermark_lsn_ = ((uint64_t)high << 32) | low;
          }
          std::cout << "Created slot. Snapshot: [" << snapshot_id_ << "], LSN: " << watermark_lsn_ << std::endl;
      } else {
          std::string err = PQerrorMessage(conn_);
          PQclear(res);
          PQfinish(temp_conn);
          throw std::runtime_error("Could not create replication slot: " + err);
      }
      PQclear(res);
  } else {
      std::cout << "Replication slot '" << slot_name << "' already exists. Continuing." << std::endl;
      std::string lsn_sql = "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '" + slot_name + "';";
      PGresult *lsn_res = PQexec(temp_conn, lsn_sql.c_str());
      if (PQresultStatus(lsn_res) == PGRES_TUPLES_OK) {
          std::string lsn_str = PQgetvalue(lsn_res, 0, 0);
          uint32_t high, low;
          if (sscanf(lsn_str.c_str(), "%X/%X", &high, &low) == 2) {
              watermark_lsn_ = ((uint64_t)high << 32) | low;
          }
      }
      PQclear(lsn_res);
  }
  PQfinish(temp_conn);
}

void WALReceiver::receiveLoop() {
  const char *env_slot = std::getenv("PG_SLOT_NAME");
  const char *env_pub = std::getenv("PG_PUBLICATION_NAME");
  std::string slot_name = env_slot ? env_slot : "hn_stories_slot";
  std::string pub_name = env_pub ? env_pub : "hn_stories_pub";

  std::cout << "Starting logical replication stream on slot '" << slot_name << "'..." << std::endl;
  std::string start_query = "START_REPLICATION SLOT \"" + slot_name +
                      "\" LOGICAL 0/0 (proto_version '1', publication_names '" +
                      pub_name + "');";
  PGresult *res = PQexec(conn_, start_query.c_str());

  if (PQresultStatus(res) != PGRES_COPY_BOTH) {
    std::string err = PQerrorMessage(conn_);
    PQclear(res);
    throw std::runtime_error("Could not start logical replication: " + err);
  }
  PQclear(res);

  char *msg = nullptr;
  uint64_t last_status_update_time = 0;
  
  while (keep_running_) {
    if (PQconsumeInput(conn_) == 0) {
        throw std::runtime_error(std::string("Error consuming input from Postgres: ") + PQerrorMessage(conn_));
    }

    int ret = PQgetCopyData(conn_, &msg, 1); // 1 = non-blocking

    uint64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();

    if (now - last_status_update_time > 5) {
        sendStandbyStatusUpdate(committed_lsn_->load());
        last_status_update_time = now;
    }

    if (ret == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
    } else if (ret == -1) {
      std::cout << "Replication stream closed." << std::endl;
      break;
    } else if (ret == -2) {
      std::cerr << "Error reading replication stream: " << PQerrorMessage(conn_) << std::endl;
      break;
    }

    handleCopyData(msg, ret);
    PQfreemem(msg);
  }
}


void WALReceiver::performSnapshot() {
    if (snapshot_id_.empty()) {
        std::cout << "No snapshot ID available or snapshot already performed. Skipping." << std::endl;
        return;
    }

    std::cout << "Starting coordinated snapshot from consistent point: " << watermark_lsn_ << std::endl;

    PGconn *snap_conn = PQconnectdb(conninfo_.c_str());
    if (PQstatus(snap_conn) != CONNECTION_OK) {
        std::string err = PQerrorMessage(snap_conn);
        PQfinish(snap_conn);
        throw std::runtime_error("Snapshot connection failed: " + err);
    }

    PQexec(snap_conn, "BEGIN ISOLATION LEVEL REPEATABLE READ;");
    std::string set_snap = "SET TRANSACTION SNAPSHOT '" + snapshot_id_ + "';";
    PGresult *res = PQexec(snap_conn, set_snap.c_str());
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::string err = PQerrorMessage(snap_conn);
        PQclear(res);
        PQfinish(snap_conn);
        throw std::runtime_error("Failed to set transaction snapshot: " + err);
    }
    PQclear(res);

    auto all_tables = registry_->getAllTables();
    for (const auto& table : all_tables) {
        std::cout << "Performing snapshot for table: " << table.schema << "." << table.table_name << std::endl;
        size_t table_rows = 0;
        
        std::string copy_query = "COPY " + table.schema + "." + table.table_name + " TO STDOUT WITH (FORMAT binary);";
        res = PQexec(snap_conn, copy_query.c_str());
        if (PQresultStatus(res) != PGRES_COPY_OUT) {
            std::cerr << "Failed to start binary COPY for " << table.table_name << ": " << PQerrorMessage(snap_conn) << std::endl;
            PQclear(res);
            continue;
        }
        PQclear(res);

        std::vector<char> copy_buffer;
        char *chunk_buffer = nullptr;
        int ret;
        bool header_skipped = false;

        while ((ret = PQgetCopyData(snap_conn, &chunk_buffer, 0)) > 0) {
            copy_buffer.insert(copy_buffer.end(), chunk_buffer, chunk_buffer + ret);
            PQfreemem(chunk_buffer);

            size_t offset = 0;
            // Handle header once
            if (!header_skipped && copy_buffer.size() >= 11 + 4 + 4) {
                if (std::memcmp(copy_buffer.data(), "PGCOPY\n\377\r\n\0", 11) == 0) {
                    offset = 11 + 4 + 4;
                    header_skipped = true;
                }
            } else if (!header_skipped) {
                continue; // Wait for more data
            }

            while (offset + 2 <= copy_buffer.size()) {
                int16_t ncolumns_n;
                std::memcpy(&ncolumns_n, copy_buffer.data() + offset, 2);
                int16_t ncolumns = ntohs(ncolumns_n);

                if (ncolumns == -1) { // Trailer
                    offset += 2;
                    break;
                }

                // Check if we have the whole row
                size_t row_start = offset;
                size_t temp_offset = offset + 2;
                bool row_complete = true;
                for (int i = 0; i < ncolumns; ++i) {
                    if (temp_offset + 4 > copy_buffer.size()) { row_complete = false; break; }
                    int32_t flen_n;
                    std::memcpy(&flen_n, copy_buffer.data() + temp_offset, 4);
                    int32_t flen = ntohl(flen_n);
                    temp_offset += 4;
                    if (flen != -1) {
                        if (temp_offset + flen > copy_buffer.size()) { row_complete = false; break; }
                        temp_offset += flen;
                    }
                }

                if (!row_complete) break; // Need more data

                // Emit row
                WalMessage msg;
                msg.relation_id = table.rel_id;
                msg.lsn = watermark_lsn_;
                msg.pg_msg_type = 'S';
                msg.payload.assign(copy_buffer.begin() + row_start, copy_buffer.begin() + temp_offset);
                buffer_.push(msg);

                table_rows++;
                offset = temp_offset;
            }
            // Remove processed data
            if (offset > 0) {
                copy_buffer.erase(copy_buffer.begin(), copy_buffer.begin() + offset);
            }
        }

        std::cout << "Table [" << table.table_name << "]: Snapshot emitted " << table_rows << " rows." << std::endl;

        if (ret == -2) {
            std::cerr << "Error reading COPY data for " << table.table_name << ": " << PQerrorMessage(snap_conn) << std::endl;
        }
    }

    PQexec(snap_conn, "COMMIT;");
    PQfinish(snap_conn);

    snapshot_id_ = ""; // Mark as done to prevent re-execution on reconnection
    std::cout << "Snapshot phase completed successfully." << std::endl;
}

void WALReceiver::handleCopyData(char *msg, int length) {
  if (length == 0)
    return;

  const char msg_type = msg[0];
  switch (msg_type) {
  case 'w': { // WAL data
    if (length < 25)
      return;

    static uint64_t wal_msg_count = 0;
    if (++wal_msg_count % 1000 == 0) {
      std::cout << "Received " << wal_msg_count << " WAL messages so far..."
                << std::endl;
    }

    int wal_payload_len = length - 25;
    if (wal_payload_len > 0) {
      uint64_t lsn_n;
      std::memcpy(&lsn_n, msg + 1, 8);
      uint64_t lsn = be64toh(lsn_n);

      char *payload_start = msg + 25;
      char pgoutput_msg_type = payload_start[0];

      switch (pgoutput_msg_type) {
      case 'R':
        handleRelationMessage(payload_start, wal_payload_len);
        break;
      case 'B': // Begin
      case 'C': // Commit
        {
          WalMessage tx_msg;
          tx_msg.relation_id = 0; // Global
          tx_msg.lsn = lsn;
          tx_msg.pg_msg_type = pgoutput_msg_type;
          tx_msg.payload.assign(payload_start, payload_start + wal_payload_len);
          while (keep_running_ && !buffer_.push_for(std::move(tx_msg), std::chrono::milliseconds(1000))) {
              sendStandbyStatusUpdate(committed_lsn_->load());
          }
        }
        break;
      case 'I':
      case 'U':
      case 'D':
      case 'T': // Truncate
        handleDataMessage(payload_start, wal_payload_len, lsn);
        break;
      default:
        break;
      }
    }
    break;
  }
  case 'k': // Keepalive
    handleKeepAliveMessage(msg, length);
    break;
  default:
    break;
  }
}

void WALReceiver::handleRelationMessage(char *payload, int length) {
  size_t offset = 1;
  if (offset + 4 > length)
    return;

  uint32_t rel_id_n;
  std::memcpy(&rel_id_n, payload + offset, 4);
  uint32_t rel_id = ntohl(rel_id_n);
  offset += 4;

  std::string schema_name;
  while (offset < length && payload[offset] != '\0') {
    schema_name += payload[offset++];
  }
  offset++;

  std::string table_name;
  while (offset < length && payload[offset] != '\0') {
    table_name += payload[offset++];
  }
  offset++;

  // Read replica identity (1 byte)
  if (offset + 1 > length)
    return;
  char stream_repl_ident = payload[offset++];

  // Read num_columns (2 bytes)
  if (offset + 2 > length)
    return;
  uint16_t num_columns_n;
  std::memcpy(&num_columns_n, payload + offset, 2);
  uint16_t num_columns = ntohs(num_columns_n);
  offset += 2;

  TableInfo fetched_info;
  bool has_catalog = registry_->getTable(schema_name, table_name, fetched_info);

  TableInfo stream_info;
  stream_info.rel_id = rel_id;
  stream_info.schema = schema_name;
  stream_info.table_name = table_name;
  stream_info.repl_ident = stream_repl_ident;

  parseRelationColumns(payload, length, offset, num_columns, stream_info,
                       fetched_info, has_catalog);

  std::cout << "Mapped Relation ID " << rel_id << " to " << schema_name << "."
            << table_name << " (" << stream_info.columns.size()
            << " streaming replica columns)" << std::endl;

  registry_->mapRelationId(rel_id, stream_info);

  // Schema Evolution Detection
  WalMessage schema_msg;
  schema_msg.relation_id = rel_id;
  schema_msg.pg_msg_type = 'R';
  schema_msg.lsn = 0;
  buffer_.push(schema_msg);
}

void WALReceiver::parseRelationColumns(const char *payload, int length,
                                       size_t &offset, uint16_t num_columns,
                                       TableInfo &info,
                                       const TableInfo &fetched_info,
                                       bool has_catalog) {
  for (uint16_t i = 0; i < num_columns; ++i) {
    if (offset + 1 > length)
      break;
    uint8_t flags = payload[offset++];
    bool is_key = (flags & 0x01);

    std::string col_name;
    while (offset < length && payload[offset] != '\0') {
      col_name += payload[offset++];
    }
    offset++;

    if (offset + 8 > length)
      break;
    offset += 8; // skip DataType OID (4 bytes) and atttypmod (4 bytes)

    std::string mapped_type = "text";
    bool is_nullable = true;
    bool pk_flag = is_key;

    if (has_catalog) {
      for (const auto &fc : fetched_info.columns) {
        if (fc.name == col_name) {
          mapped_type = fc.data_type;
          is_nullable = fc.is_nullable;
          if (is_key) pk_flag = true;
          break;
        }
      }
    }

    ColumnInfo col;
    col.name = col_name;
    col.data_type = mapped_type;
    col.is_nullable = is_nullable;
    col.pk_flag = pk_flag;
    info.columns.push_back(col);
  }
}

void WALReceiver::handleDataMessage(char *payload, int length, uint64_t lsn) {
  if (length < 5)
    return;

  uint32_t rel_id_n;
  std::memcpy(&rel_id_n, payload + 1, 4);
  uint32_t relation_id = ntohl(rel_id_n);

  TableInfo info;
  if (registry_->getTableByRelationId(relation_id, info)) {
    WalMessage msg;
    msg.relation_id = relation_id;
    msg.lsn = lsn;
    msg.pg_msg_type = payload[0]; // I, U, D
    msg.payload.assign(payload, payload + length);
    
    // Heartbeat-aware backpressure
    // HEARTBEAT-AWARE BACKPRESSURE: 
    // If the buffer is full, we must continue to send status updates to PG
    // to prevent replication timeouts while we wait for the ParquetWriter to catch up.
    while (keep_running_ && !buffer_.push_for(std::move(msg), std::chrono::milliseconds(1000))) {
        sendStandbyStatusUpdate(committed_lsn_->load());
        if (PQstatus(conn_) != CONNECTION_OK) return; 
    }
  }
}

void WALReceiver::handleKeepAliveMessage(char *msg, int length) {
  if (length >= 18) {
    char reply_requested = msg[17];
    if (reply_requested) {
      sendStandbyStatusUpdate(committed_lsn_->load());
    }
  }
}

void WALReceiver::sendStandbyStatusUpdate(uint64_t lsn) {
  if (lsn == 0)
    return;

  // std::cout << "Sending status update to PostgreSQL with LSN: " << lsn <<
  // std::endl;

  char reply[34];
  reply[0] = 'r';

  uint64_t lsn_n = htobe64(lsn);

  // Receive LSN
  std::memcpy(reply + 1, &lsn_n, 8);
  // Flush LSN
  std::memcpy(reply + 9, &lsn_n, 8);
  // Apply LSN
  std::memcpy(reply + 17, &lsn_n, 8);

  // Timestamp (8 bytes)
  uint64_t now_us = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
  uint64_t now_n = htobe64(now_us);
  std::memcpy(reply + 25, &now_n, 8);

  // Reply requested (1 byte)
  reply[33] = 0;

  if (PQputCopyData(conn_, reply, 34) <= 0 || PQflush(conn_) != 0) {
    std::cerr << "Could not send standby status update: "
              << PQerrorMessage(conn_) << std::endl;
  }
}
