// C++ header include
#include <iostream>
#include <memory>
#include <string>

// db connect instance include
#include "stdafx.h"
#include "db_connector_instance.h"
#include "query_planner.h"
#include "meta_data_manager.h"
#include "plan_executor.h"
#include "storage_engine_interface.h"
#include "parsed_query.h"
#include "keti_log.h"
#include "ip_config.h"
std::unique_ptr<DB_Connector_Instance> g_httpHandler;

void on_initialize(const string_t& address){
    web::uri_builder uri(address);  

    string storage_engine_address, storage_engine_ip, storage_engine_port;

    if (getenv("CLUSTER_MASTER_IP") != NULL){
        storage_engine_ip = getenv("CLUSTER_MASTER_IP");
        if (getenv("SE_INTERFACE_CONTAINER_POD_PORT") != NULL){
            storage_engine_port = getenv("SE_INTERFACE_CONTAINER_POD_PORT");
        }else{
            storage_engine_ip = STORAGE_ENGINE_LOCAL;
            storage_engine_port = to_string(SE_INTERFACE_CONTAINER_PORT);
        }
    }else{
        storage_engine_ip = STORAGE_ENGINE_LOCAL;
        storage_engine_port = to_string(SE_INTERFACE_CONTAINER_PORT);
    }

    storage_engine_address = storage_engine_ip + ":" + storage_engine_port;

    auto addr = uri.to_uri().to_string();
    g_httpHandler = std::unique_ptr<DB_Connector_Instance>(new DB_Connector_Instance(addr,storage_engine_address));
    g_httpHandler->open().wait();

    KETILOG::WARNLOG("DB Connector Instance","Listening for request at " + addr);

    return;
}

void on_shutdown(){
	g_httpHandler->close().wait();
    return;
}

int main(int argc, char** argv){
    if (argc >= 2) {
        KETILOG::SetLogLevel(stoi(argv[1]));
    }else if (getenv("LOG_LEVEL") != NULL){
        string env = getenv("LOG_LEVEL");
        int log_level;
        if (env == "TRACE"){
            log_level = DEBUGG_LEVEL::TRACE;
        }else if (env == "DEBUG"){
            log_level = DEBUGG_LEVEL::DEBUG;
        }else if (env == "INFO"){
            log_level = DEBUGG_LEVEL::INFO;
        }else if (env == "WARN"){
            log_level = DEBUGG_LEVEL::WARN;
        }else if (env == "ERROR"){
            log_level = DEBUGG_LEVEL::ERROR;
        }else{
            log_level = DEBUGG_LEVEL::FATAL;
        }
        KETILOG::SetLogLevel(log_level);
    }else{
        KETILOG::SetDefaultLogLevel();
    }

    utility::string_t port;
    if (getenv("QUERY_ENGINE_POD_PORT") != NULL){
        port = getenv("QUERY_ENGINE_POD_PORT"); //running on pod
    } else{
        port = U("40100"); //running on local
    }

    utility::string_t address = U("http://0.0.0.0:");

    address.append(port);

    on_initialize(address);

    // DB_Monitoring_Manager& manager = DB_Monitoring_Manager::GetInstance();

    while (true);

    on_shutdown();
    return 0;
}
