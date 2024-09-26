#pragma once
#include <iostream>
#include <vector>

#include <mysql_driver.h>
#include <mysql_connection.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/driver.h>
#include <cppconn/resultset.h>
#include <cppconn/exception.h>
#include <cppconn/statement.h>

#include "snippet_sample.grpc.pb.h"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 

#include "keti_log.h"

using namespace rapidjson;
using namespace std;
using StorageEngineInstance::SnippetRequest;

typedef enum EXECUTION_MODE {
    OFFLOADING = 1, 
    GENERIC = 2 // Default
}EXECUTION_MODE;

typedef enum QUERY_TYPE {
    SELECT = 1,
    UPDATE = 2,
    INSERT = 3,
    DELETE = 4,
    DCL = 5,
    DDL = 6,
    OTHER = 7
}QUERY_TYPE;

class QueryLog {
public:
    QueryLog(){}

    QueryLog(string user_id, string query_statement, string start_time, EXECUTION_MODE execution_mode, QUERY_TYPE query_type){
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
            snippet_log.work_id_ = snippet.work_id();
            snippet_log.snippet_type_ = snippet.type();
            snippet_log.projection_count_ = snippet.query_info().projection_size();
            snippet_log.filter_count_ = snippet.query_info().filtering_size();
            snippet_log.group_by_count_ = snippet.query_info().group_by_size();
            snippet_log.order_by_count_ = snippet.query_info().order_by().column_name_size();
            snippet_log.limit_exist_ = snippet.query_info().has_limit() ? true : false;
            snippet_log_.push_back(snippet_log);
        }
    }

    void AddResultInfo(int scanned_row_count, int filtered_row_count, string query_result){
        scanned_row_count_ = scanned_row_count;
        filtered_row_count_ = filtered_row_count;
        query_result_ = query_result;
    }

    void AddTimeInfo(string end_time, double execution_time){
        end_time_ = end_time;
        execution_time_ = execution_time;
    }

    bool InsertQueryLog(){
        try {
            sql::mysql::MySQL_Driver *driver = sql::mysql::get_mysql_driver_instance();
            sql::Connection *con = driver->connect("tcp://10.0.4.87:30702", "keti", "ketilinux");
            con->setSchema("keti_opencsd");

            string log_statement = "INSERT INTO query_log (query_id, user_id, query_statement, query_result, \
                                execution_mode, query_type, start_time, end_time, execution_time, \
                                scanned_row_count, filtered_row_count, snippet_count) \
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            
            sql::PreparedStatement *log_pstmt = con->prepareStatement(log_statement);

            log_pstmt->setInt(1, query_id_);
            log_pstmt->setString(2, user_id_);
            log_pstmt->setString(3, query_statement_);
            log_pstmt->setString(4, query_result_);
            log_pstmt->setInt(5, execution_mode_);
            log_pstmt->setInt(6, query_type_);
            log_pstmt->setDateTime(7, start_time_);
            log_pstmt->setDateTime(8, end_time_);
            log_pstmt->setDouble(9, execution_time_);
            log_pstmt->setInt(10, scanned_row_count_);
            log_pstmt->setInt(11, filtered_row_count_);
            log_pstmt->setInt(12, snippet_count_);
            log_pstmt->executeUpdate();

            for(int i=0; i<snippet_log_.size(); i++){
                string snippet_statement = "INSERT INTO query_snippet (query_id, work_id, snippet_type, \
                                projection_count, filter_count, group_by_count, order_by_count, limit_exist) \
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            
                sql::PreparedStatement *snippet_pstmt = con->prepareStatement(snippet_statement);

                snippet_pstmt->setInt(1, snippet_log_[i].query_id_);
                snippet_pstmt->setInt(2, snippet_log_[i].work_id_);
                snippet_pstmt->setInt(3, snippet_log_[i].snippet_type_);
                snippet_pstmt->setInt(4, snippet_log_[i].projection_count_);
                snippet_pstmt->setInt(5, snippet_log_[i].filter_count_);
                snippet_pstmt->setInt(6, snippet_log_[i].group_by_count_);
                snippet_pstmt->setInt(7, snippet_log_[i].order_by_count_);
                snippet_pstmt->setBoolean(8, snippet_log_[i].limit_exist_);
                snippet_pstmt->executeUpdate();

                delete snippet_pstmt;
            }
            
            delete log_pstmt;
            delete con;
        } catch (sql::SQLException &e) {
            KETILOG::ERRORLOG("DB Connector Instance::Query Log Assistant::SQL Error", e.what());
        }

        cout << "-------------------------" << endl;
        cout << "Insert Query Log" << endl;
        cout << "- query id: " << query_id_ << endl;
        cout << "- user id: " << user_id_ << endl;
        cout << "- query statement: " << query_statement_ << endl;
        cout << "- query result: " << query_result_ << endl;
        cout << "- execution mode: " << execution_mode_ << endl;
        cout << "- query type: " << query_type_ << endl;
        cout << "- start time: " << start_time_ << endl;
        cout << "- end time: " << end_time_ << endl;
        cout << "- execution time: " << execution_time_ << endl; 
        cout << "- scanned row count: " << scanned_row_count_ << endl;
        cout << "- filtered row count: " << filtered_row_count_ << endl;
        cout << "- snippet count: " << snippet_count_ << endl;
        cout << "-------------------------" << endl;

        return 0;
    }

    string QueryLog2Json(){
        StringBuffer block_buf;
        PrettyWriter<StringBuffer> writer(block_buf);

        writer.StartObject();

        writer.Key("query_id");
        writer.Int(query_id_);

        writer.Key("user_id");
        writer.String(user_id_.c_str());

        writer.Key("query_statement");
        writer.String(query_statement_.c_str());

        writer.Key("query_result");
        writer.String(query_result_.c_str());

        writer.Key("start_time");
        writer.String(start_time_.c_str());

        writer.Key("end_time");
        writer.String(end_time_.c_str());

        writer.Key("execution_mode");
        writer.Int(execution_mode_);

        writer.Key("execution_time");
        writer.Int(execution_time_);

        writer.Key("query_type");
        writer.Int(query_type_);

        writer.Key("scanned_row_count");
        writer.Int(scanned_row_count_);

        writer.Key("filtered_row_count");
        writer.Int(filtered_row_count_);

        writer.Key("snippet_count");
        writer.Int(snippet_count_);

        writer.EndObject();

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
        bool limit_exist_;
    };

private:
    int query_id_;
    string user_id_;
    string query_statement_;
    string query_result_;
    EXECUTION_MODE execution_mode_;
    QUERY_TYPE query_type_;
    string start_time_;
    string end_time_;
    double execution_time_;
    int scanned_row_count_;
    int filtered_row_count_;
    int snippet_count_;
    vector<QueryLog::Snippet_Log> snippet_log_;
};