#pragma once

#include <vector>
#include <unordered_map>
#include <mutex>
#include <fcntl.h>
#include <unistd.h>
#include <iomanip>
#include <algorithm>
#include <condition_variable>
#include <iostream>
#include <fstream>
#include <string>
#include <map>

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 
#include <experimental/filesystem>
#include <rapidjson/istreamwrapper.h>
namespace fs = std::experimental::filesystem;

#include "keti_log.h"
#include "snippet.h"
using namespace std;
using namespace rapidjson;

class MetaDataManager { /* modify as singleton class */

/* Methods */
public:
    const string LOGTAG = "Query Engine::Meta Data Manager";

    struct EnvironmentInfo {
        string scheduling_algorithm;
        int block_count;
        bool using_index;
        map<string, float> db_size_map; // key - db_name, value db_size
    };
    
	struct Table {
		string table_name;
        int table_index_number;
        int index_type;
		vector<Column> column_list;
        map<string,int> sst_block_map ; //key - sst_name, value - sst_block_count
        int total_block_count = 0;
	};

    static map<string, map<string, Table>> GetMetaDataAll(){
        return GetInstance().getMetaDataAll();
    }

    static map<string, Table> GetMetaData(string db_name){
        return GetInstance().getMetaData(db_name);
    }

    static void SetMetaData(Snippet &snippet, const string &db_name){
        GetInstance().setMetaData(snippet, db_name);
        
    }
    
    static MetaDataManager& GetInstance() {
        static MetaDataManager _instance;
        return _instance;
    }

    static vector<string> GetTableColumns(const string& db_name, const string& table_name){
        return GetInstance().getTableColumns(db_name, table_name);
    }

    static vector<string> GetTablePriority(const string& db_name){
        return GetInstance().getTablePriority(db_name);
        
    }
	static void InitMetaDataManager(){
		GetInstance().initMetaDataManager();
	}

    static map<string, vector<string>> GetSstInfo(){
        return GetInstance().getSstInfo(); 
    }

    static string GetEnvInfoJson(string db_name){
        return GetInstance().getEnvInfoJson(db_name);
    }

    static void UpdateEnvInfoJson(){
        return GetInstance().updateEnvInfoJson();
    }

    // static void InsertDB(string db_name, DB db){
	// 	GetInstance().insertDB(db_name, db);
	// }
    void print_databases();

    string convertToJson(map<string, map<string, Table>> &db_map);
    string convertToJson(map<string, Table> &table_map);
    string convertToJson(map<string, vector<string>> &sst_csd_map);
private:
    
	MetaDataManager() {
        environment_info.scheduling_algorithm = "DCS";
        environment_info.block_count = 100;
        environment_info.using_index = true;
    };

    MetaDataManager(const MetaDataManager&);
    MetaDataManager& operator=(const MetaDataManager&){
        return *this;
    }
    
    //Snippet set schema_info, sst_info, result_info - total_block_count
    void setMetaData(Snippet &snippet, const std::string &db_name);

    map<string, vector<string>> getSstInfo(){
        return sst_csd_map;
    }
    map<string, map<string, Table>> getMetaDataAll(){
        return db_map;
    }
    map<string, Table> getMetaData(string db_name){
        return db_map[db_name];
    }

    vector<string> getTableColumns(const string& db_name, const string& table_name);

    std::vector<std::string> getTablePriority(const std::string& db_name);

    // void insertDB(string db_name, DB db){
    //     GetInstance().MetaDataManager_[db_name] = db;
    // }

    void load_schema_info(const string &json, const string &db_name);
    void db_info_generate(const string &db_name);
    void generate_sst_csd_map(const string &sst_csd_info_path);
    void load_sst_block_count(const std::string& jsonFilePath, const std::string& db_name);
	void initMetaDataManager();
    string getEnvInfoJson(string db_name);
    void updateEnvInfoJson();
    
private:
    std::vector<std::string> scan_priority; 
    map<string, map<string, Table>> db_map; //key : db name, value Table map 
    map<string, vector<string>> sst_csd_map;
    mutex mutex_;
    struct Snippet snippet;
    struct EnvironmentInfo environment_info;
	// unordered_map<string,struct DB> MetaDataManager_;
};