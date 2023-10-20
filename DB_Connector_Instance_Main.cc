// C++ header include
#include <iostream>
#include <memory>
#include <string>

// db connect instance include
#include "stdafx.h"
#include "DB_Connector_Instance.h"
#include "Query_Planner.h"
#include "Meta_Data_Manager.h"
#include "Plan_Executer.h"
#include "Storage_Engine_Interface.h"
#include "Parsed_Query.h"
#include "keti_log.h"

std::unique_ptr<DB_Connector_Instance> g_httpHandler;

void on_initialize(const string_t& address){
    web::uri_builder uri(address);  

    auto addr = uri.to_uri().to_string();
     g_httpHandler = std::unique_ptr<DB_Connector_Instance>(new DB_Connector_Instance(addr));
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
    }else{
        KETILOG::SetDefaultLogLevel();
    }

    utility::string_t port = U("40100");
    utility::string_t address = U("http://10.0.4.80:");
    address.append(port);

    on_initialize(address);
    KETILOG::WARNLOG("DB Connector Instance", "Press ENTER to exit");

    std::string line;
    std::getline(std::cin, line);

    on_shutdown();
    return 0;
}
