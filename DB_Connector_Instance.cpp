#include "DB_Connector_Instance.h"

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 
#include "keti_log.h"

#include <ctime>

using namespace rapidjson;

DB_Connector_Instance::DB_Connector_Instance() : storageEngineInterface_(grpc::CreateChannel("10.0.4.82:40200", grpc::InsecureChannelCredentials())), meta_data_manager_("localhost:40200")
{
    //ctor
}
DB_Connector_Instance::DB_Connector_Instance(utility::string_t url):m_listener(url), storageEngineInterface_(grpc::CreateChannel("10.0.4.82:40200", grpc::InsecureChannelCredentials())), meta_data_manager_("localhost:40200")
{
    m_listener.support(methods::GET, std::bind(&DB_Connector_Instance::handle_get, this, std::placeholders::_1));
    m_listener.support(methods::PUT, std::bind(&DB_Connector_Instance::handle_put, this, std::placeholders::_1));
    m_listener.support(methods::POST, std::bind(&DB_Connector_Instance::handle_post, this, std::placeholders::_1));
    m_listener.support(methods::DEL, std::bind(&DB_Connector_Instance::handle_delete, this, std::placeholders::_1));
}
DB_Connector_Instance::~DB_Connector_Instance()
{
    //dtor
}

void DB_Connector_Instance::handle_error(pplx::task<void>& t)
{
    try
    {
        t.get();
    }
    catch(...)
    {
        // Ignore the error, Log it if a logger is available
    }
}


//
// Get Request 
//
void DB_Connector_Instance::handle_get(http_request message)
{
    clock_t start, end;
    double time;

    start = clock();

    auto body_json = message.extract_string();
    std::string json = utility::conversions::to_utf8string(body_json.get());
    
    Document document;
    document.Parse(json.c_str());
    
    Parsed_Query parsed_query(document["query"].GetString());
    
    KETILOG::DEBUGLOG("DB Connector Instance","\t------------:: STEP 1 ::------------");
    KETILOG::DEBUGLOG("DB Connector Instance","Recv Query");
    query_planner_.Parse(meta_data_manager_,parsed_query);
    std::string rep = plan_executer_.Execute_Query(storageEngineInterface_,parsed_query) + "\n";
    
    message.reply(status_codes::OK,rep);

    end = clock();
    time = (double)(end-start)/CLOCKS_PER_SEC;
    
    KETILOG::DEBUGLOG("DB Connector Instance","End Query time :" + std::to_string(time));

    return;

};

//
// A POST request
//
void DB_Connector_Instance::handle_post(http_request message)
{    
    struct timespec  begin, end;

    clock_gettime(CLOCK_MONOTONIC, &begin);
    
    auto body_json = message.extract_string();
    std::string json = utility::conversions::to_utf8string(body_json.get());
    KETILOG::DEBUGLOG("DB Connector Instance", json);
    
    Document document;
    document.Parse(json.c_str());
    
    Parsed_Query parsed_query(document["query"].GetString());
    
    KETILOG::DEBUGLOG("DB Connector Instance","Recv Query");
    query_planner_.Parse(meta_data_manager_,parsed_query);
    std::string rep = plan_executer_.Execute_Query(storageEngineInterface_,parsed_query) + "\n";
    
    clock_gettime(CLOCK_MONOTONIC, &end);
    double time = (double)(end.tv_sec - begin.tv_sec) + (double)(end.tv_nsec - begin.tv_nsec)/1000000000;

    message.reply(status_codes::OK,/*rep + */"End Query time : " + std::to_string(time) + "s");
    
    KETILOG::DEBUGLOG("DB Connector Instance","End Query time : " + std::to_string(time) + "s");
    return;
};

//
// A DELETE request
//
void DB_Connector_Instance::handle_delete(http_request message)
{
    ucout <<  message.to_string() << endl;
    message.reply(status_codes::NotFound,U("SUPPORT ONLY GET API"));
    return;
};


//
// A PUT request 
//
void DB_Connector_Instance::handle_put(http_request message)
{
    ucout <<  message.to_string() << endl;
    message.reply(status_codes::NotFound,U("SUPPORT ONLY GET API"));
    return;
};
