// C++ header include
#include <iostream>
#include <memory>
#include <string>

// db connect instance include
#include "stdafx.h"
#include "db_connector_instance.h"
#include "meta_data_manager.h"
#include "keti_log.h"
#include "ip_config.h"

std::unique_ptr<DBConnectorInstance> g_httpHandler;

void on_initialize(){
    string query_engine_address;

    query_engine_address = "http://" + (string)LOCALHOST + ":" + (string)QUERY_ENGINE_PORT;

    web::uri_builder query_engine_address_uri(query_engine_address);
    auto query_engine_address_ = query_engine_address_uri.to_uri().to_string();

    string storage_engine_address;

    storage_engine_address = (string)STORAGE_ENGINE_IP + ":" + (string)SE_INTERFACE_PORT;
    grpc::ChannelArguments channel_args;
    channel_args.SetMaxSendMessageSize(-1);
    channel_args.SetMaxReceiveMessageSize(-1);

    g_httpHandler = std::unique_ptr<DBConnectorInstance>(new DBConnectorInstance(query_engine_address_,storage_engine_address,channel_args));
    g_httpHandler->open().wait();

    KETILOG::WARNLOG("Query Engine","Listening for request at " + query_engine_address_);

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
        }else if (env == "FATAL"){
            log_level = DEBUGG_LEVEL::FATAL;
        }else{
            log_level = DEBUGG_LEVEL::INFO;
        }
        KETILOG::SetLogLevel(log_level);
    }else{
        KETILOG::SetDefaultLogLevel();
    }
    MetaDataManager::InitMetaDataManager();

    on_initialize();

    // DB_Monitoring_Manager& manager = DB_Monitoring_Manager::GetInstance();

    while (true);

    on_shutdown();
    return 0;
}
