#include "BoundedBuffer.hpp"
#include "ParquetWriter.hpp"
#include "TableRegistry.hpp"
#include "WALReceiver.hpp"
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>

// Global pointers for signal handler
WALReceiver *g_receiver = nullptr;
ParquetWriter *g_writer = nullptr;

void signalHandler(int signum) {
  std::cout << "\nInterrupt signal (" << signum
            << ") received. Shutting down...\n";
  if (g_receiver) {
    g_receiver->stop();
  }
  if (g_writer) {
    g_writer->stop();
  }
}

void loadEnv(const std::string &filename) {
  std::ifstream file(filename);
  if (!file.is_open())
    return;

  std::string line;
  while (std::getline(file, line)) {
    if (line.empty() || line[0] == '#')
      continue;
    auto pos = line.find('=');
    if (pos != std::string::npos) {
      std::string key = line.substr(0, pos);
      std::string value = line.substr(pos + 1);
      setenv(key.c_str(), value.c_str(), 1);
    }
  }
}

int main(int argc, char *argv[]) {
  // Register signal handlers for graceful shutdown
  std::signal(SIGINT, signalHandler);
  std::signal(SIGTERM, signalHandler);

  // Attempt to load .env file from current directory
  loadEnv(".env");

  std::string conninfo = "host=localhost dbname=postgres user=postgres";

  if (argc > 1) {
    conninfo = argv[1];
  } else {
    const char *env_conninfo = std::getenv("PG_CONNINFO");
    if (env_conninfo) {
      conninfo = env_conninfo;
    }
  }

  std::string output_dir = "data";
  const char *env_output_dir = std::getenv("OUTPUT_DIR");
  if (env_output_dir) {
    output_dir = env_output_dir;
  }

  if (!output_dir.empty()) {
    std::filesystem::create_directories(output_dir);
  }

  std::cout << "Starting CDC Daemon with connection string: " << conninfo
            << std::endl;

  try {
    auto registry = std::make_shared<TableRegistry>();
    auto committed_lsn = std::make_shared<std::atomic<uint64_t>>(0);
    BoundedBuffer<WalMessage> buffer(10000);

    WALReceiver receiver(conninfo, buffer, registry, committed_lsn);
    g_receiver = &receiver;
    
    receiver.connect();
    receiver.startLogicalReplication();
    uint64_t watermark_lsn = receiver.getWatermarkLsn();

    ParquetWriter writer(buffer, registry, output_dir, committed_lsn, 100, watermark_lsn);
    g_writer = &writer;
    writer.start();

    receiver.performSnapshot();

    bool first_run = true;
    while (true) {
        if (!first_run) {
            std::cout << "Re-connecting to PostgreSQL for CDC..." << std::endl;
            try {
                receiver.connect();
                receiver.startLogicalReplication();
            } catch (const std::exception& e) {
                std::cerr << "Reconnection failed: " << e.what() << ". Retrying in 5s..." << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(5));
                continue;
            }
        }
        first_run = false;
        
        receiver.receiveLoop();
        
        // If we get here, the loop finished (likely disconnect)
        // Check if we should actually stop
        const char* stopping = std::getenv("STOP_DAEMON");
        if (stopping || !g_receiver) break;
        
        std::cout << "Logical replication stream interrupted. Re-starting in 2s..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
  } catch (const std::exception &e) {
    std::cerr << "Fatal Error: " << e.what() << std::endl;
    return 1;
  }

  std::cout << "Daemon stopped cleanly.\n";
  return 0;
}
