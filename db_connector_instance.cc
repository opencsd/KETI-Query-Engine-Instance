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

        string db_name = document["db_name"].GetString();
        if(document.HasMember("debug_mode")){
          bool is_debug_mode = document["debug_mode"].GetBool();
        }
        // string db_type = document["dbms_type"].GetString();
        
        // 쿼리 파싱 -> 쿼리 타입 , tpch 쿼리인지 
        query_planner_.Parse(parsed_query, db_name);
        // 쿼리 점수화 함수 실행->  쿼리 2개 이상이면 Generic
        cost_analyzer_.Query_Scoring(parsed_query); 
        cout << "쿼리 점수화 완료" << endl;

        // Query Explain 기반 쿼리 수행 계획 함수 실행  
        query_planner_.Planning_Query(parsed_query);
        

        QueryLog query_log(user_id, parsed_query.GetParsedQuery(), begin, parsed_query.GetExecutionMode(), parsed_query.GetQueryType());

        std::string rep = plan_executor_.ExecuteQuery(storage_engine_connector_,parsed_query, db_name, query_log) + "\n";

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
    }else if(url == "/db/status"){
        auto body_json = message.extract_string();
        std::string json = utility::conversions::to_utf8string(body_json.get());
        Document document;
        document.Parse(json.c_str());

        std::string response ="";
        message.reply(status_codes::OK,response);

    }else if(url == "/metadata/schema"){ //db 스키마 요청
        auto body_json = message.extract_string();
        std::string json = utility::conversions::to_utf8string(body_json.get());
        cout << json <<endl;

        Document document;
        document.Parse(json.c_str());
        string user_id = document["user_id"].GetString();
        cout << user_id <<endl;
        map<string, map<string, MetaDataManager::Table>> temp_db_map = MetaDataManager::GetInstance().GetMetaData();
        
        std::string response = MetaDataManager::GetInstance().convertToJson(temp_db_map);

        message.reply(status_codes::OK,response);

    }else if(url == "/metadata/sst-info"){ //sst-csd 정보
        auto body_json = message.extract_string();
        std::string json = utility::conversions::to_utf8string(body_json.get());
        Document document;
        document.Parse(json.c_str());

        map<string, vector<string>> temp_sst_info = MetaDataManager::GetInstance().GetSstInfo();
        //json으로 반환해주기
        std::string response = MetaDataManager::GetInstance().convertToJson(temp_sst_info);
        message.reply(status_codes::OK,response);
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
    utility::string_t url = message.relative_uri().path();
    if(url == "/query/snippet"){ // 스니펫 생성 요청
        auto body_json = message.extract_string();
        std::string json = utility::conversions::to_utf8string(body_json.get());
        Document document;
        document.Parse(json.c_str());
        
        cout << "json " << json <<endl;
        
        string query = document["query"].GetString();

        string user_id = document["user_id"].GetString();

        string db_name = document["db_name"].GetString();
        ParsedQuery parsed_query(query);
        parsed_query.SetCustomParsedQuery(query,db_name);
        vector <Snippet> snippets;
        ParsedCustomQuery parsed_custom_query = parsed_query.GetParsedCustomQuery();
        plan_executor_.create_snippet_init_info(db_name, parsed_custom_query, snippets);

        for(int i=0;i<snippets.size();i++){
            cout << "Setting MetaData of Query..." << endl;
            MetaDataManager::SetMetaData(snippets.at(i),db_name);
        }
        vector <string> empty_snippet;
        plan_executor_.generate_snippet_json(snippets, db_name, empty_snippet);
        
        std::string response = "";
        for(int i=0;i<snippets.size();i++){
            string snippet_name = "custom_snippet" + to_string(i) ;
            std::ifstream openFile("../snippets/"+ db_name + "/" + snippet_name + ".json");
            if(openFile.is_open() ){
                std::string line;
                while(getline(openFile, line)){
                    response += line;
                }
                openFile.close();
            }
        }
        
        message.reply(status_codes::OK,response);
    }else if(url == "/query/offload-cost"){ //비용 분석 요청 
        auto body_json = message.extract_string();
        std::string json = utility::conversions::to_utf8string(body_json.get());
        Document document;
        document.Parse(json.c_str());

        std::string response = "";
        message.reply(status_codes::OK,response);
    }else{
        message.reply(status_codes::NotFound,U("SUPPORT ONLY GET API"));

    }
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
    utility::string_t url = message.relative_uri().path();

    if(url == "/db/alter"){ //db type 변경
        auto body_json = message.extract_string();
        std::string json = utility::conversions::to_utf8string(body_json.get());
        Document document;
        document.Parse(json.c_str());

        std::string response = "";
        message.reply(status_codes::OK,response);
    }else{
        message.reply(status_codes::NotFound,U("SUPPORT ONLY GET API"));

    }
    return;
};
