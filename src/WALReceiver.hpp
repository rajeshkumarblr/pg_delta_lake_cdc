#pragma once

#include "BoundedBuffer.hpp"
#include "TableRegistry.hpp"
#include <atomic>
#include <memory>
#include <postgresql/libpq-fe.h>
#include <string>
#include <vector>

struct WalMessage {
  uint32_t relation_id;
  uint64_t lsn;
  std::vector<char> payload;
};

class WALReceiver {
public:
  WALReceiver(const std::string &conninfo, BoundedBuffer<WalMessage> &buffer,
              std::shared_ptr<TableRegistry> registry,
              std::shared_ptr<std::atomic<uint64_t>> committed_lsn);
  ~WALReceiver();

  void run();
  void stop();

private:
  std::string conninfo_;
  BoundedBuffer<WalMessage> &buffer_;
  std::shared_ptr<TableRegistry> registry_;
  std::shared_ptr<std::atomic<uint64_t>> committed_lsn_;
  PGconn *conn_;
  std::atomic<bool> keep_running_;

  void connect();
  void fetchSchemas(PGconn *normal_conn);
  void startLogicalReplication();
  void receiveLoop();
  void handleCopyData(char *msg, int length);
  void handleRelationMessage(char *payload, int length);
  void handleDataMessage(char *payload, int length, uint64_t lsn);
  void handleKeepAliveMessage(char *msg, int length);
  void parseRelationColumns(const char *payload, int length, size_t &offset,
                            uint16_t num_columns, TableInfo &info,
                            const TableInfo &fetched_info, bool has_catalog);
  void sendStandbyStatusUpdate(uint64_t lsn);
};
