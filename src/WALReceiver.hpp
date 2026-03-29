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
  std::vector<char> payload;
};

class WALReceiver {
public:
  WALReceiver(const std::string &conninfo, BoundedBuffer<WalMessage> &buffer,
              std::shared_ptr<TableRegistry> registry);
  ~WALReceiver();

  void run();
  void stop();

private:
  std::string conninfo_;
  BoundedBuffer<WalMessage> &buffer_;
  std::shared_ptr<TableRegistry> registry_;
  PGconn *conn_;
  std::atomic<bool> keep_running_;

  void connect();
  void fetchSchemas(PGconn *normal_conn);
  void startLogicalReplication();
  void receiveLoop();
  void handleCopyData(char *msg, int length);
  void sendStandbyStatusUpdate();
};
