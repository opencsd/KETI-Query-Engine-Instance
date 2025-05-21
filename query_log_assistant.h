#pragma once

#include <iostream>
#include <vector>
#include <experimental/filesystem>
#include <string.h>
#include <sstream>
#include <stdio.h>

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
#include "ip_config.h"
#include "keti_type.h"

namespace fs = std::experimental::filesystem;

using namespace rapidjson;
using namespace std;

using StorageEngineInstance::SnippetRequest;

inline long getCpuTimeForPid(const std::string &statPath, string uid) {
    long cpu_total = 0;
    try {
        for (const auto& entry : fs::directory_iterator(statPath)) {
            std::string path = entry.path().string();

            if (path.find(uid) != std::string::npos) {
                std::string cpu_usage_file = path + "/cpuacct.usage";
                
                std::ifstream file(cpu_usage_file);
                if (file.is_open()) {
                    std::string usage_str;
                    std::getline(file, usage_str);
                    file.close();
                    
                    long long usage = std::stoll(usage_str);
                    cpu_total += usage;
                } else {
                    std::cerr << "Failed to open: " << cpu_usage_file << std::endl;
                }
            }
        }
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Filesystem error: " << e.what() << std::endl;
    }

    return cpu_total / 1e7; // cpu(ns) -> 10ms(0.01sec) 환산 (100HZ=tick단위)
}

inline long long readRaplEnergy(const std::string &filePath) {
    std::ifstream ifs(filePath);
    if (!ifs.is_open()) {
        cout << "[alert] Failed to open RAPL file: " + filePath << endl;
        return 0;
    }
    long long energy = 0;
    ifs >> energy;
    return energy;
}

inline int countOccurrences(const std::string& text, const std::string& keyword) {
    int count = 0;
    size_t pos = text.find(keyword, 0);
    
    while (pos != std::string::npos) {
        count++;
        pos = text.find(keyword, pos + keyword.length());
    }
    
    return count;
}

inline std::string preprocessQuery(const std::string& query) {
    std::string cleaned = query;

    std::replace(cleaned.begin(), cleaned.end(), '\n', ' ');
    std::replace(cleaned.begin(), cleaned.end(), '\t', ' ');

    cleaned = std::regex_replace(cleaned, std::regex("\\s+"), " ");

    std::transform(cleaned.begin(), cleaned.end(), cleaned.begin(), ::tolower);

    return cleaned;
}

class QueryLog {
public:
    QueryLog(){
        string temp = INSTANCE_NAME;
        std::replace(temp.begin(), temp.end(), '-', '_');
        instance_name_ = temp;
    }

    QueryLog(string query_statement, string database_name, string uid){
        query_statement_ = query_statement;
        query_type_ = QUERY_TYPE::SELECT;
        data_size_ = 0;

        string temp = uid;
        std::replace(temp.begin(), temp.end(), '-', '_');
        pod_uid_ = temp;

        string processed_query = preprocessQuery(query_statement);

        for (const auto& keyword : keywords) {
            int count = countOccurrences(processed_query, keyword);
            float table_size = MetaDataManager::GetTableSize(database_name, keyword);
            if(count > 0){
                data_size_ += table_size * count;
            }
        }
        data_size_ = std::round(data_size_ * 100) / 100.0;
    }

    QueryLog(string user_id, string query_statement, EXECUTION_MODE execution_mode, QUERY_TYPE query_type, string database_name, string uid){
        user_id_ = user_id;
        query_statement_ = query_statement;
        execution_mode_ = execution_mode;
        query_type_ = query_type;
        database_name_ = database_name;

        string temp = uid;
        std::replace(temp.begin(), temp.end(), '-', '_');
        pod_uid_ = temp;

        temp = INSTANCE_NAME;
        std::replace(temp.begin(), temp.end(), '-', '_');
        instance_name_ = temp;
    }

    void AddSnippetInfo(int query_id,list<SnippetRequest> snippet_request){
        query_id_ = query_id;
        snippet_count_ = snippet_request.size();
        table_count_ = 0;
        data_size_ = 0;

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
            snippet_log.having_count_ = snippet.query_info().having_size();
            snippet_log_.push_back(snippet_log);

            if(snippet.type() == 0){
                table_count_++;
                string table_name = snippet.query_info().table_name(0);
                float table_size = MetaDataManager::GetTableSize(database_name_, table_name);
                data_size_ += table_size;
            }
        }
        data_size_ = std::round(data_size_ * 100) / 100.0;
    }

    void CheckMetricBefore(){
        metric_.cpu_before = getCpuTimeForPid(statPath, pod_uid_);
        metric_.power_before1 = readRaplEnergy(raplPath1);
        metric_.power_before2 = readRaplEnergy(raplPath2);
    }

    void AddMetricInfo(){
        metric_.cpu_after = getCpuTimeForPid(statPath, pod_uid_);
        metric_.power_after1 = readRaplEnergy(raplPath1);
        metric_.power_after2 = readRaplEnergy(raplPath2);

        cpu_usage_ = metric_.cpu_after - metric_.cpu_before;
        cpu_utilization_ = std::round((cpu_usage_ * 100.0) / ((execution_time_ / 0.01) * 40) * 100.0) / 100.0;

        long long power_usage1 = metric_.power_after1 - metric_.power_before1;
        if(power_usage1 < 0){
            power_usage1 = 262143328850 - metric_.power_before1 + metric_.power_after1;
            cout << "%% " << power_usage1 << "=" << "262143328850-" << metric_.power_before1 << "+" << metric_.power_after1 << endl;
        }

        long long power_usage2 = metric_.power_after2 - metric_.power_before2;
        if(power_usage2 < 0){
            power_usage2 = 262143328850 - metric_.power_before2 + metric_.power_after2;
            cout << "%% " << power_usage2 << "=" << "262143328850-" << metric_.power_before2 << "+" << metric_.power_after2 << endl;
        }

        power_usage_ = (power_usage1 + power_usage2) / 1e6;
    }

    void AddResultInfo(string query_result){
        query_result_ = query_result;
    }

    void AddResultInfo(int scanned_row_count, int filtered_row_count, string query_result){
        scanned_row_count_ = scanned_row_count;
        filtered_row_count_ = filtered_row_count;
        query_result_ = query_result;
    }

    void AddTimeInfo(string start_time, string end_time, double execution_time){
        start_time_ = start_time;
        end_time_ = end_time;
        execution_time_ = execution_time;
    }

    bool InsertQueryLog(){
        try {
            sql::mysql::MySQL_Driver *driver = sql::mysql::get_mysql_driver_instance();
            sql::Connection *con = driver->connect("tcp://10.0.4.80:40806", "root", "ketilinux");
            con->setSchema(instance_name_);

            string log_statement = "INSERT INTO query_log (query_id, user_name, query_statement, query_result, \
                                execution_mode, query_type, start_time, end_time, execution_time, \
                                scanned_row_count, filtered_row_count, snippet_count, database_name, table_count, \
                                cpu_usage, power_usage, data_size) \
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            
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
            log_pstmt->setString(13, database_name_);
            log_pstmt->setInt(14, table_count_);
            log_pstmt->setInt(15, cpu_usage_);
            log_pstmt->setInt(16, power_usage_);
            log_pstmt->setDouble(17, data_size_);
            log_pstmt->executeUpdate();

            for(int i=0; i<snippet_log_.size(); i++){
                string snippet_statement = "INSERT INTO snippet (query_id, work_id, snippet_type, \
                                projection, filter, group_by, order_by, limit_exist, `having`) \
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            
                sql::PreparedStatement *snippet_pstmt = con->prepareStatement(snippet_statement);

                snippet_pstmt->setInt(1, snippet_log_[i].query_id_);
                snippet_pstmt->setInt(2, snippet_log_[i].work_id_);
                snippet_pstmt->setInt(3, snippet_log_[i].snippet_type_);
                snippet_pstmt->setInt(4, snippet_log_[i].projection_count_);
                snippet_pstmt->setInt(5, snippet_log_[i].filter_count_);
                snippet_pstmt->setInt(6, snippet_log_[i].group_by_count_);
                snippet_pstmt->setInt(7, snippet_log_[i].order_by_count_);
                snippet_pstmt->setBoolean(8, snippet_log_[i].limit_exist_);
                snippet_pstmt->setInt(9, snippet_log_[i].having_count_);
                snippet_pstmt->executeUpdate();

                delete snippet_pstmt;
            }
            
            delete log_pstmt;
            delete con;
        } catch (sql::SQLException &e) {
            KETILOG::ERRORLOG("DB Connector Instance::Query Log Assistant::SQL Error", e.what());
        }

        // cout << "-------------------------" << endl;
        // cout << "Insert Query Log" << endl;
        // cout << "- query id: " << query_id_ << endl;
        // cout << "- user id: " << user_id_ << endl;
        // cout << "- query statement: " << query_statement_ << endl;
        // cout << "- query result: " << query_result_ << endl;
        // cout << "- execution mode: " << execution_mode_ << endl;
        // cout << "- query type: " << query_type_ << endl;
        // cout << "- start time: " << start_time_ << endl;
        // cout << "- end time: " << end_time_ << endl;
        // cout << "- execution time: " << execution_time_ << endl; 
        // cout << "- scanned row count: " << scanned_row_count_ << endl;
        // cout << "- filtered row count: " << filtered_row_count_ << endl;
        // cout << "- snippet count: " << snippet_count_ << endl;
        // cout << "-------------------------" << endl;

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
        writer.Double(execution_time_);

        writer.Key("query_type");
        writer.Int(query_type_);

        writer.Key("scanned_row_count");
        writer.Int(scanned_row_count_);

        writer.Key("filtered_row_count");
        writer.Int(filtered_row_count_);

        writer.Key("snippet_count");
        writer.Int(snippet_count_);

        writer.Key("cpu_usage");
        writer.Int(cpu_usage_);

        writer.Key("cpu_utilization");
        writer.Double(cpu_utilization_);

        writer.Key("power_usage");
        writer.Int(power_usage_);

        std::stringstream stream;
        stream << std::fixed << std::setprecision(2) << data_size_;
        std::string _data_size = stream.str();

        writer.Key("data_size");
        writer.String(_data_size.c_str());

        writer.EndObject();

        string result_json = block_buf.GetString();

        return result_json;
    }

    string QueryLog2JsonSsd(){
        StringBuffer block_buf;
        PrettyWriter<StringBuffer> writer(block_buf);

        writer.StartObject();

        writer.Key("query_statement");
        writer.String(query_statement_.c_str());

        writer.Key("query_result");
        writer.String(query_result_.c_str());

        writer.Key("start_time");
        writer.String(start_time_.c_str());

        writer.Key("end_time");
        writer.String(end_time_.c_str());

        writer.Key("execution_time");
        writer.Double(execution_time_);

        writer.Key("query_type");
        writer.Int(query_type_);

        writer.Key("cpu_usage");
        writer.Int(cpu_usage_);

        writer.Key("cpu_utilization");
        writer.Double(cpu_utilization_);

        writer.Key("power_usage");
        writer.Int(power_usage_);

        std::stringstream stream;
        stream << std::fixed << std::setprecision(2) << data_size_;
        std::string _data_size = stream.str();

        writer.Key("data_size");
        writer.String(_data_size.c_str());

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
        int having_count_;
    };

    struct Metric{
        long cpu_before = 0;
        long cpu_after = 0;
        long long power_before1 = 0;
        long long power_before2 = 0;
        long long power_after1 = 0;
        long long power_after2 = 0;
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
    string database_name_;
    int table_count_;
    vector<QueryLog::Snippet_Log> snippet_log_;
    string instance_name_;
    QueryLog::Metric metric_;
    long cpu_usage_;
    double cpu_utilization_;
    long power_usage_;
    float data_size_;
    string pod_uid_;

    std::string statPath = "/host/sys/fs/cgroup/cpu,cpuacct/system.slice/containerd.service";
    std::string raplPath1 = "/host/sys/class/powercap/intel-rapl:0/energy_uj";
    std::string raplPath2 = "/host/sys/class/powercap/intel-rapl:1/energy_uj";
    std::vector<string> keywords = {"lineitem", "nation", "supplier", "region", "customer", "partsupp", "orders", "part"};
};