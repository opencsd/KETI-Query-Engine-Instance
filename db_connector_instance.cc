#include "db_connector_instance.h"

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 
#include "keti_log.h"

#include <ctime>
#include <iomanip>

using namespace rapidjson;

DBConnectorInstance::DBConnectorInstance() : storage_engine_connector_(grpc::CreateChannel("localhost:40200", grpc::InsecureChannelCredentials()))
{

}
DBConnectorInstance::DBConnectorInstance(utility::string_t url, string storage_engine_address, grpc::ChannelArguments channel_args):m_listener(url), storage_engine_connector_(grpc::CreateCustomChannel(storage_engine_address, grpc::InsecureChannelCredentials(), channel_args))
{
    m_listener.support(methods::GET, std::bind(&DBConnectorInstance::handle_get, this, std::placeholders::_1));
    m_listener.support(methods::PUT, std::bind(&DBConnectorInstance::handle_put, this, std::placeholders::_1));
    m_listener.support(methods::POST, std::bind(&DBConnectorInstance::handle_post, this, std::placeholders::_1));
    m_listener.support(methods::DEL, std::bind(&DBConnectorInstance::handle_delete, this, std::placeholders::_1));
}
DBConnectorInstance::~DBConnectorInstance()
{
    /*dtor*/
}

void DBConnectorInstance::handle_error(pplx::task<void>& t)
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
void DBConnectorInstance::handle_get(http_request message)
{
    utility::string_t url = message.relative_uri().path();

    if(url == "/query/run"){
        string begin, end;
        double execution;

        auto body_json = message.extract_string();
        std::string json = utility::conversions::to_utf8string(body_json.get());
        KETILOG::DEBUGLOG(LOGTAG, json);
        
        Document document;
        document.Parse(json.c_str());

        auto startTime = std::chrono::system_clock::now();
        std::time_t startTimeT = std::chrono::system_clock::to_time_t(startTime);
        std::tm *startLocalTime = std::localtime(&startTimeT);
        std::ostringstream startTimeStringStream;
        startTimeStringStream << std::put_time(startLocalTime, "%Y-%m-%d %H:%M:%S");
        begin = startTimeStringStream.str();
        
        ParsedQuery parsed_query(document["query"].GetString());
        string user_id = document["user_id"].GetString();

        if(document.HasMember("debug_mode")){
          bool is_debug_mode = document["debugMode"].GetBool();
        }

        // 쿼리 점수화 함수 실행
        cost_analyzer_.Query_Scoring(parsed_query);
        
        // Query Explain 기반 쿼리 수행 계획 함수 실행
        query_planner_.Planning_Query(parsed_query);
        
        query_planner_.Parse(parsed_query);

        QueryLog query_log(user_id, parsed_query.GetParsedQuery(), begin, parsed_query.GetExecutionMode(), parsed_query.GetQueryType());

        std::string rep = plan_executor_.ExecuteQuery(storage_engine_connector_,parsed_query,query_log) + "\n";

        cout << rep << endl;
        
        auto endTime = std::chrono::system_clock::now();
        std::time_t endTimeT = std::chrono::system_clock::to_time_t(endTime);
        std::tm *endLocalTime = std::localtime(&endTimeT);
        std::ostringstream endTimeStringStream;
        endTimeStringStream << std::put_time(endLocalTime, "%Y-%m-%d %H:%M:%S");
        end = endTimeStringStream.str();

        auto elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(endTime - startTime);
        execution = elapsed_seconds.count();

        query_log.AddTimeInfo(end,execution);
        // query_log.InsertQueryLog();

        std::string response = query_log.QueryLog2Json();

        message.reply(status_codes::OK,response);
        
        KETILOG::INFOLOG(LOGTAG,"End Query time : " + to_string(execution) + " sec");
        return;
    }else if(url == "/log-level"){
        try{
            utility::string_t query =  message.relative_uri().query();
            int equalPos = query.find("=");
            std::string logLevelStr = query.substr(equalPos + 1);
            int log_level = std::stoi(logLevelStr);

            KETILOG::SetLogLevel(log_level);

            KETILOG::FATALLOG(LOGTAG, "changed log level" + to_string(log_level));

            message.reply(status_codes::OK,"");
        }catch (std::exception &e) {
            KETILOG::INFOLOG("Handle Set Log Level", e.what());
            message.reply(status_codes::NotImplemented,"");
        }
    }else{
        KETILOG::FATALLOG(LOGTAG, "error:invalid url");
        return;
    }
};

//
// A POST request
//
void DBConnectorInstance::handle_post(http_request message)
{    
    ucout <<  message.to_string() << endl;
    message.reply(status_codes::NotFound,U("SUPPORT ONLY GET API"));
    return;
};

//
// A DELETE request
//
void DBConnectorInstance::handle_delete(http_request message)
{
    ucout <<  message.to_string() << endl;
    message.reply(status_codes::NotFound,U("SUPPORT ONLY GET API"));
    return;
};


//
// A PUT request 
//
void DBConnectorInstance::handle_put(http_request message)
{
    ucout <<  message.to_string() << endl;
    message.reply(status_codes::NotFound,U("SUPPORT ONLY GET API"));
    return;
};
