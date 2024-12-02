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

    const string LOGTAG = "Monitoring Container::Table Manager";

	struct Table {
		string table_name;
        int table_index_number;
        int index_type;
		vector<Column> column_list;
        map<string,int> sst_block_map ; //key - sst_name, value - sst_block_count
        int total_block_count = 0;

	};

    static map<string, map<string, Table>> GetMetaData(){
        return GetInstance().getMetaData();
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
    // static void InsertDB(string db_name, DB db){
	// 	GetInstance().insertDB(db_name, db);
	// }
    void print_databases(){
        for (const auto& db_entry : db_map) {
            cout << "Database: " << db_entry.first << endl;  

            for (const auto& table_entry : db_entry.second) {
                cout << "  Table Name: " << table_entry.first << endl;
                cout << "    Table Index Number: " << table_entry.second.table_index_number << endl;
                
                for (const auto& column_entry : table_entry.second.column_list) {
                    cout << "      Column Name: " << column_entry.name << endl;
                    cout << "      Type: " << column_entry.type << endl;
                    cout << "      Length: " << column_entry.length << endl;
                    cout << "      Primary: " << column_entry.primary << endl;
                    cout << "      Index: " << column_entry.index << endl;
                    cout << "      Nullable: " << column_entry.nullable << endl;
                    cout << "-----------------------------------" << endl;
                }
                cout << "   Sst Info: ";
                for(auto& sst : table_entry.second.sst_block_map){
                    cout << sst.first << ", " << sst.second;
                }
                cout << endl << "-----------------------------------" << endl;
            }
        }
    }

    static string convertToJson(map<string, map<string, Table>> &db_map){
        cout << "convertToJson" <<endl;
        Document json_document;
        json_document.SetObject();
        auto& allocator = json_document.GetAllocator();

        for (const auto& db_entry : db_map) {
            const string& db_name = db_entry.first;                     // Database 이름
            const map<string, Table>& table_map = db_entry.second;      // 테이블 맵

            Value db_value(kObjectType);

            for (const auto& table_entry : table_map) {
                cout << table_entry.first;
                const string& table_name = table_entry.first;           // 테이블 이름
                const Table& table = table_entry.second;                // Table 객체

                Value table_value(kObjectType);

                // 테이블 메타데이터 추가
                table_value.AddMember("table_name", Value(table.table_name.c_str(), allocator), allocator);
                table_value.AddMember("table_index_number", table.table_index_number, allocator);
                table_value.AddMember("index_type", table.index_type, allocator);
                table_value.AddMember("total_block_count", table.total_block_count, allocator);

                // 컬럼 리스트 추가
                Value column_array(kArrayType);
                for (const auto& column : table.column_list) {
                    Value column_value(kObjectType);
                    column_value.AddMember("column_name", Value(column.name.c_str(), allocator), allocator);
                    column_value.AddMember("column_type", column.type, allocator);
                    column_value.AddMember("primary", column.primary, allocator);
                    column_value.AddMember("index", column.index, allocator);
                    column_value.AddMember("nullable", column.nullable, allocator);

                    column_array.PushBack(column_value, allocator);
                }
                table_value.AddMember("column_list", column_array, allocator);

                // // SST 블록 맵 추가
                // Value sst_block_map_value(kObjectType);
                // for (const auto& sst_entry : table.sst_block_map) {
                //     sst_block_map_value.AddMember(Value(sst_entry.first.c_str(), allocator), sst_entry.second, allocator);
                // }
                // table_value.AddMember("sst_block_map", sst_block_map_value, allocator);

                // 테이블 추가
                db_value.AddMember(Value(table_name.c_str(), allocator), table_value, allocator);
            }

            // 데이터베이스 추가
            json_document.AddMember(Value(db_name.c_str(), allocator), db_value, allocator);
        }

        // JSON 직렬화
        StringBuffer buffer;
        Writer<StringBuffer> writer(buffer);
        json_document.Accept(writer);

        return buffer.GetString();

    }
    static string convertToJson(map<string, vector<string>> &sst_csd_map) {
        Document document;
        document.SetObject();
        auto& allocator = document.GetAllocator();

        // 맵 데이터를 JSON으로 변환
        for (const auto& entry : sst_csd_map) {
            const string& key = entry.first;                // 맵의 키
            const vector<string>& values = entry.second;    // 맵의 값 (벡터)

            // JSON 배열 생성
            Value json_array(kArrayType);
            for (const auto& value : values) {
                json_array.PushBack(Value(value.c_str(), allocator), allocator);
            }

            // JSON 객체에 키와 배열 추가
            document.AddMember(Value(key.c_str(), allocator), json_array, allocator);
        }

        // JSON 직렬화
        StringBuffer buffer;
        Writer<StringBuffer> writer(buffer);
        document.Accept(writer);

        return buffer.GetString(); // JSON 문자열 반환
    }
private:
    
	MetaDataManager() {};
    MetaDataManager(const MetaDataManager&);
    MetaDataManager& operator=(const MetaDataManager&){
        return *this;
    }
    
    //Snippet set schema_info, sst_info, result_info - total_block_count
    void setMetaData(Snippet &snippet, const std::string &db_name) {
        cout << "hj :: snippet 타입 : " << snippet.type << endl;
        if (snippet.type != QueryType::FULL_SCAN && 
            snippet.type != QueryType::INDEX_SCAN && 
            snippet.type != QueryType::INDEX_TABLE_SCAN) {
            snippet.result_info.total_block_count = 0;
            return;
        }
        std::string table_name = snippet.query_info.table_name1;
        // db_map에서 해당 db_name과 table_name이 존재하는지 확인
        if (db_map.find(db_name) == db_map.end()) {
            std::cerr << "setMetaData()::Database not found: " << db_name << std::endl;
            return;
        }
        
        auto &table_map = db_map[db_name];
        if (table_map.find(table_name) == table_map.end()) {
            std::cerr << "Table not found in database " << db_name << ": " << table_name << std::endl;
            return;
        }

        Table &table = table_map[table_name];
        cout << table_name << "'s index number : " << table.table_index_number << endl;
        snippet.schema_info.table_index_number = table.table_index_number;
        std::cout << "Table Index Number: " << snippet.schema_info.table_index_number << std::endl;
        snippet.result_info.total_block_count = table.total_block_count;
        std::cout << "Table 전체블록: " << snippet.result_info.total_block_count << std::endl;
        for (const auto &col_entry : table.column_list) {
     
            std::cout << "  Column Name: " << col_entry.name 
                    << ", Type: " << col_entry.type 
                    << ", Length: " << col_entry.length << std::endl;
            snippet.schema_info.column_list.push_back(col_entry);
        }

        //스니펫 sst_info 채우기 
        for(const auto &sst_entry : table.sst_block_map){
            string sst_name = sst_entry.first;
            int block_count = sst_entry.second;
            cout << sst_name << " "<< block_count  << endl << endl;
            SstInfo sst_info;
            sst_info.sst_name = sst_name;
            sst_info.sst_block_count = block_count;

            if(sst_csd_map.find(sst_name) != sst_csd_map.end()){
                vector<string> temp_csd_list = sst_csd_map[sst_name];
                for(auto &csd_i : temp_csd_list){
                    CSD csd;
                    csd.csd_id = csd_i;
                    csd.partition = "/dev/ngd-blk3";
                    sst_info.csd.push_back(csd);
                }
                
            }
            snippet.sst_info.push_back(sst_info);
            

        }  
        
        cout<< "hj :: setMetaData end " << endl;
        
         
    }
    map<string, vector<string>> getSstInfo(){
        return sst_csd_map;
    }
    map<string, map<string, Table>> getMetaData(){
        return db_map;
    }

    vector<string> getTableColumns(const string& db_name, const string& table_name){
        vector<string> column_names;
        
        // 데이터베이스 존재 확인
        if (db_map.find(db_name) == db_map.end()) {
            std::cerr << "getTableColumns()::Database not found: " << db_name << std::endl;
            return column_names;
        }

        // 테이블 존재 확인
        auto& table_map = db_map[db_name];
        if (table_map.find(table_name) == table_map.end()) {
            std::cerr << "Table not found in database " << db_name << ": " << table_name << std::endl;
            return column_names;
        }

        // 테이블의 컬럼 리스트에서 컬럼 이름을 추출
        Table& table = table_map[table_name];
        for (const auto& column : table.column_list) {
            column_names.push_back(column.name);
        }

        return column_names;
    }

    static std::vector<std::string> getTablePriority(const std::string& db_name) {
        std::vector<std::string> table_names;

        std::cout << "hj :: db_name : " << db_name << std::endl;
    
        // SQL 명령 생성
        std::string command =
            "mysql -e \""
            "SELECT table_name "
            "FROM information_schema.tables "
            "WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') "
            "  AND table_schema = '" + db_name + "' "
            "ORDER BY data_length ASC;\"";

        // popen을 사용하여 명령 실행 및 결과 읽기
        FILE* pipe = popen(command.c_str(), "r");
        if (!pipe) {
            std::cerr << "Failed to execute command." << std::endl;
            return table_names;
        }

        char buffer[128];
        std::ostringstream result;
        while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
            result << buffer;
        }

        // 명령 결과 닫기
        pclose(pipe);

        // 결과 파싱 (첫 번째 줄은 헤더이므로 제외)
        std::istringstream iss(result.str());
        std::string line;
        bool is_first_line = true;

        while (std::getline(iss, line)) {
            if (is_first_line) { // 헤더 스킵
                is_first_line = false;
                continue;
            }
            // 테이블 이름만 추출
            std::istringstream line_stream(line);
            std::string table_name;
            if (line_stream >> table_name) {
                table_names.push_back(table_name);
            }
        }
        

        return table_names;
    }
    // void insertDB(string db_name, DB db){
    //     GetInstance().MetaDataManager_[db_name] = db;
    // }

    /**
     * @brief schema_info.json 파싱
     * 
     * @param json 
     * @param db_name 
     */
    void load_schema_info(const string &json, const string &db_name){
        Document document;
        document.Parse(json.c_str());
        Value &tableList = document["tableList"];

        for (SizeType i = 0; i < tableList.Size(); ++i) {
            Table table;
            const Value& tableData = tableList[i];
            
            table.table_name = tableData["tableName"].GetString();
            table.table_index_number = tableData["table_index_number"].GetInt();

            const Value& columnList = tableData["columnList"];
            for (SizeType j = 0; j < columnList.Size(); ++j) {
                Column column;
                const Value& columnData = columnList[j];

                column.name = columnData["name"].GetString();
                column.type = columnData["type"].GetInt();
                column.length = columnData["length"].GetInt();
                column.primary = columnData["primary"].GetBool();
                column.index = columnData["index"].GetBool();
                column.nullable = columnData["nullable"].GetBool();

                // table.table_schema[column.name] = column;
                table.column_list.push_back(column);
            }

            db_map[db_name][table.table_name] = table;
        }
        cout << "hj :: 1. schema_info load" << endl;


    }

    /**
     * @brief table_sst_info.json 생성
     * 
     * @param db_name 
     */
    void db_info_generate(const string &db_name){
        
        cout << "hj :: db_name : " << db_name << endl;
        if(db_name == "tpch_origin"){
            //파일 8개로 다 나눈 버전이라 일단 이렇게 해놨음
        }else{
            
            string command = "mysql -D INFORMATION_SCHEMA"  
            " -e \"select d.TABLE_NAME, i.SST_NAME, d.INDEX_NUMBER, d.INDEX_TYPE from ROCKSDB_DDL d "
            "INNER JOIN ROCKSDB_INDEX_FILE_MAP i ON d.INDEX_NUMBER = i.INDEX_NUMBER "
            "WHERE d.TABLE_SCHEMA = '" + db_name + "'\" > ../metadata/" + db_name + "/table_sst_info.txt";

            int result = system(command.c_str());
        }

        string table_sst_info_path = "../metadata/" + db_name + "/table_sst_info.txt";
        ifstream inputFile(table_sst_info_path);
        if (!inputFile.is_open()) {
            cerr << "Failed to open table_sst_info.txt" << endl;
            return ;
        }

        string line;

        getline(inputFile, line);

        while (getline(inputFile, line)) {
            std::istringstream iss(line);
            std::string table_name, sst_name;
            int index_number, index_type;

            // 파싱 수행 (탭을 구분자로 사용)
            getline(iss, table_name, '\t');
            getline(iss, sst_name, '\t');

            iss >> index_number >> index_type;
            // `db_map`에 해당 테이블 정보 업데이트
            auto &table_map = db_map[db_name];
            if (table_map.find(table_name) != table_map.end()) {
                table_map[table_name].sst_block_map[sst_name] = -1; 
                table_map[table_name].index_type = index_type;
            } else {
                std::cerr << "Table not found in db_map: " << table_name << std::endl;
            }
        }

        inputFile.close();
        
        rapidjson::StringBuffer s;
        rapidjson::Writer<rapidjson::StringBuffer> writer(s);

        writer.StartObject(); 
        writer.Key("tableList");
        writer.StartArray(); 

        for (const auto& entry : db_map) {

            for (const auto& info : db_map[db_name]) {
                writer.StartObject(); 
                writer.Key("tableName");
                writer.String(info.second.table_name.c_str());

                writer.Key("tableIndexNumber");
                writer.Int(info.second.table_index_number);

                writer.Key("indexType");
                writer.Int(info.second.index_type);

                writer.Key("sstList");
                writer.StartArray();
                for (const auto& sst : info.second.sst_block_map) {
                    
                    writer.String(sst.first.c_str());
                }
                writer.EndArray(); // "sstList" array end

                writer.EndObject(); 
            }
        }

        writer.EndArray(); // "tableList" array end
        writer.EndObject(); 

        ofstream outputFile("../metadata/" + db_name + "/table_sst_info.json");
        if (outputFile.is_open()) {
            outputFile << s.GetString();
            outputFile.close();
            cout << "Table SST Info JSON file created successfully." << endl;
        } else {
            cerr << "Failed to create JSON file." << endl;
        }        
    }

    void generate_sst_csd_map(const string &sst_csd_info_path){
        ifstream inputFile(sst_csd_info_path);
        if (!inputFile.is_open()) {
            cerr << "Failed to open map file: " << sst_csd_info_path << endl;
            return ;
        }

        string line;
        while (getline(inputFile, line)) {
            istringstream iss(line);
            string sst_file, csd_num;
            // Parse the line
            size_t sstPos = line.find("sst file : ");
            size_t numPos = line.find(", num : [");
            if (sstPos != string::npos && numPos != string::npos) {
                sst_file = line.substr(sstPos + 11, numPos - (sstPos + 11));  // Extract SST name
                csd_num = line.substr(numPos + 9, line.find("]", numPos + 9) - (numPos + 9));
                sst_csd_map[sst_file].push_back(csd_num) ;  // Map SST file to CSD number
            }
        }
        inputFile.close();
    }

    void load_sst_block_count(const std::string& jsonFilePath, const std::string& db_name) {
        ifstream ifs(jsonFilePath);
        assert(ifs.is_open());
        IStreamWrapper isw(ifs);

        Document doc;
        doc.ParseStream(isw);
        assert(doc.HasMember("sst_info"));
        
        for (const auto& sst_entry : doc["sst_info"].GetArray()) {
            string sst_name = sst_entry["sst_name"].GetString();
            int sst_block_count = sst_entry["sst_block_count"].GetInt();

            // db_map에서 해당 데이터베이스의 테이블 맵을 순회
            for (auto& table_map_entry : db_map[db_name]) {
                string table_name = table_map_entry.first; // 테이블 이름
                Table& table_data = table_map_entry.second; // Table 객체

                // sst_block_map에서 sst_name이 존재하는지 확인
                if (table_data.sst_block_map.find(sst_name) != table_data.sst_block_map.end()) {                    
                    table_data.sst_block_map[sst_name] = sst_block_count; // sst_block_count 업데이트
                    db_map[db_name][table_name].sst_block_map[sst_name] = sst_block_count;
                    db_map[db_name][table_name].total_block_count += sst_block_count;
                }
            }
        }
    }


	void initMetaDataManager(){
        cout << "Meta Data Manager initialization" << endl;

        string metaDirPath = "../metadata/";
        string json = "";
        string schema_info_json = "";
        string table_sst_info_json = "";
        for (const auto& entry : fs::directory_iterator(metaDirPath)) { // 디렉토리 순회
            if (fs::is_directory(entry.path())) {  
                std::string db_name = entry.path().filename().string();
                
                schema_info_json = "";
                table_sst_info_json = "";

                std::ifstream openFile1("../metadata/" + db_name + "/schema_info.json");
                if (openFile1.is_open()) {
                    std::string line;
                    while (std::getline(openFile1, line)) {
                        schema_info_json += line;
                    }
                    openFile1.close();
                } else {
                    std::cerr << "MetaData: file open error for "  << endl;
                }     
                //1. schema mapping
                load_schema_info(schema_info_json,db_name);
                                
                //2. 테이블이 어디 sst에 저장되어있는지 -> information_schema.ROCKSDB_DDL, information_schema.ROCKSDB_INDEX_FILE_MAP
                db_info_generate(db_name);

                //3. sst 파일이 어디 csd에 저장되어있는지
                generate_sst_csd_map("../metadata/" + db_name + "/sst_csd_info.txt");
                
                //4. total_block_count
                load_sst_block_count("../metadata/" + db_name + "/sst_block_info.json", db_name);
            }
            // for (const auto& iter : sst_csd_map) {
            //     cout << "SST File: " << iter.first << " -> CSD Numbers: ";
            //     for (const auto& csd_num : iter.second) {
            //         cout << csd_num << ", ";
            //     }
            //     cout << endl; 
            // }
        }
        // print_databases();
    }
    
    
private:
    std::vector<std::string> scan_priority; 
    map<string, map<string, Table>> db_map; //key : db name, value Table map 
    map<string, vector<string>> sst_csd_map;
    mutex mutex_;
    struct Snippet snippet;
	// unordered_map<string,struct DB> MetaDataManager_;
};