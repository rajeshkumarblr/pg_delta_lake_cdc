#include <iostream>
#include <csignal>
#include <cstdlib>
#include <fstream>
#include "BoundedBuffer.hpp"
#include "NetworkReceiver.hpp"
#include "ParquetWriter.hpp"

// Global pointers for signal handler
NetworkReceiver* g_receiver = nullptr;
ParquetWriter* g_writer = nullptr;

void signalHandler(int signum) {
    std::cout << "\nInterrupt signal (" << signum << ") received. Shutting down...\n";
    if (g_receiver) {
        g_receiver->stop();
    }
    if (g_writer) {
        g_writer->stop();
    }
}

void loadEnv(const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) return;

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty() || line[0] == '#') continue;
        auto pos = line.find('=');
        if (pos != std::string::npos) {
            std::string key = line.substr(0, pos);
            std::string value = line.substr(pos + 1);
            setenv(key.c_str(), value.c_str(), 1);
        }
    }
}

int main(int argc, char* argv[]) {
    // Register signal handlers for graceful shutdown
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    // Attempt to load .env file from current directory
    loadEnv(".env");

    std::string conninfo = "host=localhost dbname=postgres user=postgres";
    
    if (argc > 1) {
        conninfo = argv[1];
    } else {
        const char* env_conninfo = std::getenv("PG_CONNINFO");
        if (env_conninfo) {
            conninfo = env_conninfo;
        }
    }

    std::cout << "Starting CDC Daemon with connection string: " << conninfo << std::endl;

    try {
        // Instantiate the bounded buffer with a capacity of 10,000 Unprocessed WAL messages
        BoundedBuffer<WalMessage> buffer(10000);

        // Instantiate the ParquetWriter, which flushes after 10,000 accumulated rows by default
        ParquetWriter writer(buffer, 10000);
        
        // Let the signal handler access the writer
        g_writer = &writer;
        
        // Start the background consumer thread
        writer.start();

        // Instantiate the NetworkReceiver to connect to Postgres
        NetworkReceiver receiver(conninfo, buffer);
        
        // Let the signal handler access the receiver
        g_receiver = &receiver;
        
        // Run the receiver loop (blocks the main thread)
        receiver.run(); 

        std::cout << "Receiver run-loop exited. Stopping writer..." << std::endl;
        writer.stop();

    } catch (const std::exception& e) {
        std::cerr << "Fatal Error: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "Daemon stopped cleanly.\n";
    return 0;
}
