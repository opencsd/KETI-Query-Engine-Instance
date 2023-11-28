#pragma once
#include <iostream>
#include <vector>

#include <mysql_driver.h>
#include <mysql_connection.h>

#include "snippet_sample.grpc.pb.h"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 

using namespace rapidjson;
using namespace std;
using StorageEngineInstance::SnippetRequest;

typedef enum EXECUTION_MODE {
    GENERIC = 0, // Default
    OFFLOADING = 1
}EXECUTION_MODE;

typedef enum QUERY_TYPE {
    SELECT = 0,
    UPDATE = 1,
    INSERT = 2,
    DELETE = 3,
    DCL = 4,
    DDL = 5,
    OTHER = 6
}QUERY_TYPE;

class Query_Log {
public:
    Query_Log(){}

    Query_Log(int user_id, string query_statement, timespec start_time, EXECUTION_MODE execution_mode, QUERY_TYPE query_type){
        user_id_ = user_id;
        query_statement_ = query_statement;
        start_time_ = start_time;
        execution_mode_ = execution_mode;
        query_type_ = query_type;
    }

    void AddSnippetInfo(int query_id,list<SnippetRequest> snippet_request){
        query_id_ = query_id;
        snippet_count_ = snippet_request.size();

        for (const auto& snippet : snippet_request) {
            Snippet_Log snippet_log;
            snippet_log.query_id_ = query_id;
            snippet_log.work_id_ = snippet.snippet().work_id();
            snippet_log.snippet_type_ = snippet.type();
            snippet_log.projection_count_ = snippet.snippet().column_projection_size();
            snippet_log.filter_count_ = snippet.snippet().table_filter_size();
            snippet_log.group_by_count_ = snippet.snippet().group_by_size();
            snippet_log.order_by_count_ = snippet.snippet().order_by().column_name_size();
            snippet_log.limit_exist_ = snippet.snippet().limit;
            snippet_log_.
        }
    }

    void AddResultInfo(int scanned_row_count, int filtered_row_count, string query_result){
        scanned_row_count_ = scanned_row_count;
        filtered_row_count_ = filtered_row_count;
        query_result_ = query_result;
    }

    void AddTimeInfo(timespec end_time, timespec execution_time){
         end_time_ = end_time;
        execution_time_ = execution_time;
    }

    bool InsertQueryLog(){
        // sql::mysql::MySQL_Driver *driver;
        // sql::Connection *con;

        // driver = sql::mysql::get_mysql_driver_instance();
        // con = driver->connect("tcp://127.0.0.1:3306", "username", "password");

        // con->setSchema("tpch_origin");

        // sql::Statement *stmt;
        // sql::ResultSet *res;

        // stmt = con->createStatement();
        // res = stmt->executeQuery("INSERT INTO query_log VALUES()");

        // delete res;
        // delete stmt;
        // delete con;

        return 0;
    }

    string QueryLog2Json(){
        StringBuffer block_buf;
        PrettyWriter<StringBuffer> writer(block_buf);

        writer.StartObject();

        writer.Key("query_id");
        writer.Int(query_id_);

        writer.Key("user_id");
        writer.Int(user_id_);

        writer.Key("query_statement");
        writer.String(query_statement_.c_str());

        writer.Key("query_result");
        writer.String(query_result_.c_str());

        writer.Key("execution_mode");
        writer.Int(execution_mode_);

        writer.Key("query_type");
        writer.Int(query_type_);

        writer.Key("scanned_row_count");
        writer.Int(scanned_row_count_);

        writer.Key("filtered_row_count");
        writer.Int(filtered_row_count_);

        writer.Key("snippet_count");
        writer.Int(snippet_count_);

        string result_json = block_buf.GetString();

        return result_json;
    }

    struct Snippet_Log{
        int query_id_;
        int work_id_;
        int snippet_type_;
        int projection_count_;
        int filter_count_;
        int group_by_count_;
        int order_by_count_;
        int having_count_;
        int limit_exist_;
    };

private:
    int query_id_;
    int user_id_;
    string query_statement_;
    string query_result_;
    EXECUTION_MODE execution_mode_;
    QUERY_TYPE query_type_;
    struct timespec start_time_;
    struct timespec end_time_;
    struct timespec execution_time_;
    int scanned_row_count_;
    int filtered_row_count_;
    int snippet_count_;
    vector<Query_Log::Snippet_Log> snippet_log_;
};