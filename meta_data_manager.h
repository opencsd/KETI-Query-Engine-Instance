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

#include "keti_log.h"

using namespace std;
using namespace rapidjson;

class MetaDataManager { /* modify as singleton class */

/* Methods */
public:
    const string LOGTAG = "Monitoring Container::Table Manager";

	struct Column {
		string column_name;
		int datatype;
		int length;
		int offset;
        bool is_pk;
        bool nullable;
	};

    struct Index {
        vector<string> index_column_name;
    };

	struct SSTFile{
		string sst_name;
		int sst_rows;
        int data_size;
		// map<string,vector<string>> IndexTable; // 오픈시스넷 03.08 논의 결정
	};
	
	struct Table {
		string table_name;
        int table_index_number;
		float table_size;
        int table_rows;
		map<string, struct Column> table_schema;
		map<string, struct SSTFile> sst_list;
        vector<string> column_name_list;
        vector<int> column_datatype_list;
        vector<int> column_offset_list;
        vector<int> column_length_list;
		vector<struct Index> index_column_info;
		vector<string> pk_column_name; 
        // map<string, string> lbapba_map; //오픈시스넷 03.08 논의 결정, lbapba map 스니펫에 오프로딩
	};

    struct DB {
        string db_name;
        float db_size;
        map<string, struct Table> table_list;
    };

    static unordered_map<string, struct DB> GetMetaData(){
        return GetInstance().getMetaData();
    }

	static void InitMetaDataManager(){
		GetInstance().initMetaDataManager();
	}

    static void InsertDB(string db_name, DB db){
		GetInstance().insertDB(db_name, db);
	}

private:
	MetaDataManager() {};
    MetaDataManager(const MetaDataManager&);
    MetaDataManager& operator=(const MetaDataManager&){
        return *this;
    };

    static MetaDataManager& GetInstance() {
        static MetaDataManager _instance;
        return _instance;
    }

    unordered_map<string, struct DB> getMetaData(){
        return GetInstance().MetaDataManager_;
    }

    void insertDB(string db_name, DB db){
        GetInstance().MetaDataManager_[db_name] = db;
    }

	void initMetaDataManager(){
        string json = "";
        std::ifstream openFile("../metadata/metadata.json");
        if(openFile.is_open() ){
            std::string line;
            while(getline(openFile, line)){
                json += line;
            }
            openFile.close();
        }else{
            KETILOG::DEBUGLOG("MetaData","file open error");
        }
        
        //parse json	
        Document document;
        document.Parse(json.c_str());

        Value &dbList = document["dbList"];
        for(int i=0; i<dbList.Size(); i++){
            MetaDataManager::DB db;
            string db_name = dbList[i]["dbName"].GetString();
            db.db_name = db_name;
            db.db_size = dbList[i]["dbSize"].GetFloat();
            
            Value &tableList = dbList[i]["tableList"];
            for(int j=0; j<tableList.Size(); j++){
                MetaDataManager::Table table;
                string table_name = tableList[j]["tableName"].GetString();
                table.table_name = table_name;
                table.table_size = tableList[j]["tableSize"].GetFloat();
                table.table_index_number = tableList[j]["tableIndexNumber"].GetInt();
                table.table_rows = tableList[j]["tableRows"].GetInt();
                
                Value &tableSchema = tableList[j]["tableSchema"];
                for(int l=0; l<tableSchema.Size(); l++){
                    MetaDataManager::Column column;
                    string column_name = tableSchema[l]["columnName"].GetString();
                    column.column_name = column_name;
                    column.datatype = tableSchema[l]["datatype"].GetInt();
                    column.length = tableSchema[l]["length"].GetInt();
                    column.offset = tableSchema[l]["offset"].GetInt();
                    column.is_pk = tableSchema[l]["isPK"].GetBool();
                    column.nullable = tableSchema[l]["nullable"].GetBool();

                    table.column_name_list.push_back(column_name);
                    table.column_datatype_list.push_back(column.datatype);
                    table.column_offset_list.push_back(column.offset);
                    table.column_length_list.push_back(column.length);
                    // table.index_column_info
                    if(column.nullable){
                        table.pk_column_name.push_back(column_name);
                    }

                    table.table_schema[column_name] = column;
                }

                Value &sstList = tableList[j]["sstList"];
                for(int l=0; l<sstList.Size(); l++){
                    MetaDataManager::SSTFile sst;
                    string sst_name = sstList[l].GetString();
                    sst.sst_name = sst_name;
                    sst.sst_rows = 0;//temp
                    sst.data_size = 0;//temp
                    table.sst_list[sst_name] = sst;
                }

                db.table_list[table_name] = table;
            }

            MetaDataManager_[db_name] = db;
        }

        // Index Info **
        
        return;
    }

private:
    mutex mutex_;
	unordered_map<string,struct DB> MetaDataManager_;
};