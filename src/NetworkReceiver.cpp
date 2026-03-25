#include "NetworkReceiver.hpp"
#include <iostream>
#include <stdexcept>
#include <cstring>

NetworkReceiver::NetworkReceiver(const std::string& conninfo, BoundedBuffer<WalMessage>& buffer)
    : conninfo_(conninfo), buffer_(buffer), conn_(nullptr), keep_running_(true) {
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

void NetworkReceiver::connect() {
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
    std::string query = "START_REPLICATION SLOT \"hn_cdc_stream_slot\" LOGICAL 0/0 (proto_version '1', publication_names 'hn_cdc_stream');";
    PGresult* res = PQexec(conn_, query.c_str());
    
    if (PQresultStatus(res) != PGRES_COPY_BOTH) {
        std::string err = PQerrorMessage(conn_);
        PQclear(res);
        throw std::runtime_error("Could not start logical replication: " + err);
    }
    PQclear(res);
    std::cout << "Started logical replication stream on slot 'hn_cdc_stream_slot'." << std::endl;
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
        
        int wal_payload_len = length - 25;
        if (wal_payload_len > 0) {
            char* payload_start = msg + 25;
            WalMessage wal_msg;
            wal_msg.payload.assign(payload_start, payload_start + wal_payload_len);
            
            // Push to the bounded buffer. This is thread-safe and will block if full.
            buffer_.push(std::move(wal_msg)); 
        }
    } else if (msg_type == 'k') { // Keepalive
        if (length >= 18) {
            char reply_requested = msg[17];
            if (reply_requested) {
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
