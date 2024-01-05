#include "db_connector_instance.h"

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 
#include "keti_log.h"

#include <ctime>
#include <iomanip>

using namespace rapidjson;

DB_Connector_Instance::DB_Connector_Instance() : storageEngineInterface_(grpc::CreateChannel("localhost:40200", grpc::InsecureChannelCredentials())), meta_data_manager_("localhost:40200")
{
    //ctor
}
DB_Connector_Instance::DB_Connector_Instance(utility::string_t url, string storage_engine_address):m_listener(url), storageEngineInterface_(grpc::CreateChannel(storage_engine_address, grpc::InsecureChannelCredentials())), meta_data_manager_("localhost:40200")
{
    m_listener.support(methods::GET, std::bind(&DB_Connector_Instance::handle_get, this, std::placeholders::_1));
    m_listener.support(methods::PUT, std::bind(&DB_Connector_Instance::handle_put, this, std::placeholders::_1));
    m_listener.support(methods::POST, std::bind(&DB_Connector_Instance::handle_post, this, std::placeholders::_1));
    m_listener.support(methods::DEL, std::bind(&DB_Connector_Instance::handle_delete, this, std::placeholders::_1));
}
DB_Connector_Instance::~DB_Connector_Instance()
{
    /*dtor*/
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
    utility::string_t url = message.relative_uri().path();

    if(url == "/query/run"){
        string begin, end;
        double execution;

        auto body_json = message.extract_string();
        std::string json = utility::conversions::to_utf8string(body_json.get());
        KETILOG::DEBUGLOG("DB Connector Instance", json);
        
        Document document;
        document.Parse(json.c_str());

        auto startTime = std::chrono::system_clock::now();
        std::time_t startTimeT = std::chrono::system_clock::to_time_t(startTime);
        std::tm *startLocalTime = std::localtime(&startTimeT);
        std::ostringstream startTimeStringStream;
        startTimeStringStream << std::put_time(startLocalTime, "%Y-%m-%d %H:%M:%S");
        begin = startTimeStringStream.str();
        
        Parsed_Query parsed_query(document["query"].GetString());
        string user_id = document["user_id"].GetString();

        if(document.HasMember("debug_mode")){
          bool is_debug_mode = document["debugMode"].GetBool();
        }
        
        query_planner_.Parse(meta_data_manager_,parsed_query);

        Query_Log query_log(user_id, parsed_query.GetParsedQuery(), begin, parsed_query.GetExecutionMode(), parsed_query.GetQueryType());

        std::string rep = plan_executer_.Execute_Query(storageEngineInterface_,parsed_query,query_log) + "\n";
        
        auto endTime = std::chrono::system_clock::now();
        std::time_t endTimeT = std::chrono::system_clock::to_time_t(endTime);
        std::tm *endLocalTime = std::localtime(&endTimeT);
        std::ostringstream endTimeStringStream;
        endTimeStringStream << std::put_time(endLocalTime, "%Y-%m-%d %H:%M:%S");
        end = endTimeStringStream.str();

        auto elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(endTime - startTime);
        execution = elapsed_seconds.count();

        query_log.AddTimeInfo(end,execution);
        query_log.InsertQueryLog();

        std::string response = query_log.QueryLog2Json();

        cout << response << endl;

        message.reply(status_codes::OK,response);
        
        KETILOG::INFOLOG("DB Connector Instance","End Query time : " + to_string(execution) + " sec");
        return;
    }else if(url == "/log-level"){
        try{
            utility::string_t query =  message.relative_uri().query();
            int equalPos = query.find("=");
            std::string logLevelStr = query.substr(equalPos + 1);
            int log_level = std::stoi(logLevelStr);

            KETILOG::SetLogLevel(log_level);

            KETILOG::FATALLOG("DB Connector Instance", "changed log level" + to_string(log_level));

            message.reply(status_codes::OK,"");
        }catch (std::exception &e) {
            KETILOG::INFOLOG("Handle Set Log Level", e.what());
            message.reply(status_codes::NotImplemented,"");
        }
    }else{
        struct timespec  begin, end;

        clock_gettime(CLOCK_MONOTONIC, &begin);
        
        auto body_json = message.extract_string();
        std::string json = utility::conversions::to_utf8string(body_json.get());
        KETILOG::DEBUGLOG("DB Connector Instance", json);
        
        Document document;
        document.Parse(json.c_str());
        
        Parsed_Query parsed_query(document["query"].GetString());

        Query_Log query_log;
        
        KETILOG::DEBUGLOG("DB Connector Instance","Recv Query");
        query_planner_.Parse(meta_data_manager_,parsed_query);
        std::string rep = plan_executer_.Execute_Query(storageEngineInterface_,parsed_query,query_log) + "\n";
        
        clock_gettime(CLOCK_MONOTONIC, &end);
        double time = (double)(end.tv_sec - begin.tv_sec) + (double)(end.tv_nsec - begin.tv_nsec)/1000000000;

        message.reply(status_codes::OK,rep /*+ "End Query time : " + std::to_string(time) + "s"*/);
        
        KETILOG::INFOLOG("DB Connector Instance","End Query time : " + std::to_string(time) + "s");
        return;
    }
};

//
// A POST request
//
void DB_Connector_Instance::handle_post(http_request message)
{    
    ucout <<  message.to_string() << endl;
    message.reply(status_codes::NotFound,U("SUPPORT ONLY GET API"));
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
