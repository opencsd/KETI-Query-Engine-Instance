#include "plan_executor.h"
#include "kodbc.h"

void load_snippet(std::list<SnippetRequest> &list,std::string snippet_name,const string &db_name);

std::string PlanExecutor::ExecuteQuery(StorageEngineConnector &storageEngineInterface, ParsedQuery &parsed_query, const string &db_name, QueryLog &query_log){
    string res = "";
    KETILOG::DEBUGLOG(LOGTAG,"Analyzing Query ...");

    // opencsd offloading O -> Offloading Query
    // opencsd offloading X -> Generic Query
    // use other dbms -> K-ODBC

    if(parsed_query.isGenericQuery()){ //Generic Query
        KETILOG::DEBUGLOG(LOGTAG," => Generic Query");

        // DB_Monitoring_Manager::UpdateSelectCount();//구분필요!!!!
        //Generic Query 및 K-ODBC Query 구분 필요!!
        char *szDSN  = (char*)db_name.c_str();;

        char *szUID ;
        char *szPWD ; 
        char *szSQL;
        SQLHENV hEnv = SQL_NULL_HENV;
        SQLHDBC hDbc = SQL_NULL_HDBC;

        //Open database
        if(!OpenDatabase(&hEnv, &hDbc, szDSN, szUID, szPWD)){
        return "DB Open Error";
        }

        printf("[K-ODBC] K-OpenSource DB Connected!\n[K-ODBC] Using DBMS : \n");
        system("mysql --version");
        printf("[K-ODBC] DSN : %s\n", szDSN);
        printf("[K-ODBC] User ID : root\n");
        printf("[K-ODBC] Using DataBase : %s\n\n", db_name.c_str());
        printf("[K-ODBC] Input Query : \n");
        cout << parsed_query.GetOriginalQuery() << endl;
        //Execute SQL
        szSQL = strcpy(new char[parsed_query.GetOriginalQuery().length() + 1], parsed_query.GetOriginalQuery().c_str());
        printf("%s\n", szSQL);
        ExecuteSQL( hDbc, szSQL, res);
        delete[] szSQL;

        query_log.AddResultInfo(/*table total row count*/0,0,/*query result*/"");

        //Database Close
        CloseDatabase(hEnv, hDbc);
    } else { //Offloading Query

        QueryStringResult result;
        KETILOG::DEBUGLOG(LOGTAG," => Pushdown Query");

        DB_Monitoring_Manager::UpdateSelectCount();
        DB_Monitoring_Manager::UpdateOffloadingCount();
        
        int query_id = GetQueryID();

        auto snippet_list = genSnippet(parsed_query, db_name);
        std::size_t length = snippet_list->size();

        cout << "[PlanExecutor] generated snippet {ID:" << to_string(query_id) << "} count: " << to_string(length) << endl;

        query_log.AddSnippetInfo(query_id, *snippet_list);

        result = queryOffload(storageEngineInterface,*snippet_list,query_id);

        query_log.AddResultInfo(result.scanned_row_count(),result.filtered_row_count(), result.query_result());

        DB_Monitoring_Manager::UpdateScanRowCount(result.scanned_row_count());
        DB_Monitoring_Manager::UpdateFilterRowCount(result.filtered_row_count());

        res = result.query_result();
    }
    return res;
}

QueryStringResult PlanExecutor::queryOffload(StorageEngineConnector &storageEngineInterface,std::list<SnippetRequest> &snippet_list, int query_id){
    std::lock_guard<std::mutex> lock(gRPC_mutex);
	cout << "[PlanExecutor] send snippet to storage engine interface {ID:" << to_string(query_id) << "}" << endl;
    return storageEngineInterface.OffloadingQuery(snippet_list, query_id);
}

void PlanExecutor::setQueryID(){
    std::lock_guard<std::mutex> lock(mutex);
    try {
        sql::mysql::MySQL_Driver *driver = sql::mysql::get_mysql_driver_instance();
        sql::Connection *con = driver->connect("tcp://10.0.4.80:40806", "root", "ketilinux");
        con->setSchema(instance_name_);

        sql::Statement *stmt = con->createStatement();
        sql::ResultSet  *res = stmt->executeQuery("SELECT MAX(query_id) FROM query_log");

        if (res->next()) {
            int max_query_id = res->getInt(1); 
            queryID_ = max_query_id + 1;
        } else {
            queryID_ = 0;
        }

        delete res;
        delete stmt;
        delete con;
    } catch (sql::SQLException &e) {
        KETILOG::INFOLOG(LOGTAG," failed to get max query_id in log database");
        std::cerr << "SQL Error: " << e.what() << std::endl;          // 오류 메시지
        std::cerr << "Error Code: " << e.getErrorCode() << std::endl; // MySQL 오류 코드
        std::cerr << "SQL State: " << e.getSQLState() << std::endl;   // SQL 상태 코드

        queryID_ = 0;
    }

    cout << "[QueryEngine] start query id from " << queryID_ << endl;
}

std::unique_ptr<std::list<SnippetRequest>> PlanExecutor::genSnippet(ParsedQuery &parsed_query, const string &db_name){ // test code
    std::unique_ptr<std::list<SnippetRequest>> ret(new std::list<SnippetRequest>());
    std::string query_str = parsed_query.GetOriginalQuery();
    
    KETILOG::DEBUGLOG(LOGTAG,"Creating Snippet ...");
    Snippet tpch_snippet;
    vector<Snippet> tpch_snippets;
    vector<string> scan_snippet_name_list;

    if (query_str == "TPC-H_01"){ //TPC-H Query 1
        for(int i = 0; i < 1;i++){ // scan snippet 수
            string scan_snippet_name = "tpch01-" + to_string(i);
            tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
            tpch_snippets.push_back(tpch_snippet);
            scan_snippet_name_list.push_back(scan_snippet_name);
        }

        for(int i=0;i<tpch_snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(tpch_snippets.at(i),db_name);
        }
        generate_snippet_json(tpch_snippets, db_name, scan_snippet_name_list);
        
        for(int i = 0; i < 2;i++){ //전체 스니펫 수
            string snippet_name = "tpch01-" + to_string(i);
            load_snippet(*ret,snippet_name,db_name);

        }
        
    } else if (query_str == "TPC-H_02"){ //TPC-H Query 2
        for(int i = 0; i < 5 ;i++){ // scan snippet 수
            string scan_snippet_name = "tpch02-" + to_string(i);
            tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
            tpch_snippets.push_back(tpch_snippet);
            scan_snippet_name_list.push_back(scan_snippet_name);

        }
        for(int i=0;i<tpch_snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(tpch_snippets.at(i),db_name);
        }
        generate_snippet_json(tpch_snippets, db_name, scan_snippet_name_list);

        for(int i = 0; i < 13;i++){ //전체 스니펫 수
            string snippet_name = "tpch02-" + to_string(i);
            load_snippet(*ret,snippet_name,db_name);

        }
    } else if (query_str == "TPC-H_03"){ //TPC-H Query 3
        for(int i = 0; i < 3 ;i++){ // scan snippet 수
            string scan_snippet_name = "tpch03-" + to_string(i);
            tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
            tpch_snippets.push_back(tpch_snippet);
            scan_snippet_name_list.push_back(scan_snippet_name);

        }
        for(int i=0;i<tpch_snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(tpch_snippets.at(i),db_name);
        }
        generate_snippet_json(tpch_snippets, db_name, scan_snippet_name_list);

        for(int i = 0; i < 5;i++){ //전체 스니펫 수
            string snippet_name = "tpch03-" + to_string(i);
            load_snippet(*ret,snippet_name,db_name);

        }

    } else if (query_str == "TPC-H_04"){ //TPC-H Query 4
        for(int i = 0; i < 2 ;i++){ // scan snippet 수
            string scan_snippet_name = "tpch04-" + to_string(i);
            tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
            tpch_snippets.push_back(tpch_snippet);
            scan_snippet_name_list.push_back(scan_snippet_name);

        }
        for(int i=0;i<tpch_snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(tpch_snippets.at(i),db_name);
        }
        generate_snippet_json(tpch_snippets, db_name, scan_snippet_name_list);
        
        for(int i = 0; i < 3;i++){ //전체 스니펫 수
            string snippet_name = "tpch04-" + to_string(i);
            load_snippet(*ret,snippet_name,db_name);

        }
    } else if(query_str == "TPC-H_05"){ //TPC-H Query 5
        for(int i = 0; i < 6 ;i++){ // scan snippet 수
            string scan_snippet_name = "tpch05-" + to_string(i);
            tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
            tpch_snippets.push_back(tpch_snippet);
            scan_snippet_name_list.push_back(scan_snippet_name);

        }
        for(int i=0;i<tpch_snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(tpch_snippets.at(i),db_name);
        }
        generate_snippet_json(tpch_snippets, db_name, scan_snippet_name_list);
        for(int i = 0; i < 11;i++){ //전체 스니펫 수
            string snippet_name = "tpch05-" + to_string(i);
            load_snippet(*ret,snippet_name,db_name);

        }
        
    } else if (query_str == "TPC-H_06"){ //TPC-H Query 6
        for(int i = 0; i < 1 ;i++){ // scan snippet 수
            string scan_snippet_name = "tpch06-" + to_string(i);
            tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
            tpch_snippets.push_back(tpch_snippet);
            scan_snippet_name_list.push_back(scan_snippet_name);

        }
        for(int i=0;i<tpch_snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(tpch_snippets.at(i),db_name);
        }
        generate_snippet_json(tpch_snippets, db_name, scan_snippet_name_list);
        for(int i = 0; i < 2;i++){ //전체 스니펫 수
            string snippet_name = "tpch06-" + to_string(i);
            load_snippet(*ret,snippet_name,db_name);

        }
    } else if(query_str == "TPC-H_07"){ //TPC-H Query 7
        for(int i = 0; i < 6 ;i++){ // scan snippet 수
            string scan_snippet_name = "tpch07-" + to_string(i);
            tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
            tpch_snippets.push_back(tpch_snippet);
            scan_snippet_name_list.push_back(scan_snippet_name);

        }
        for(int i=0;i<tpch_snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(tpch_snippets.at(i),db_name);
        }
        generate_snippet_json(tpch_snippets, db_name, scan_snippet_name_list);
        for(int i = 0; i < 11;i++){ //전체 스니펫 수
            string snippet_name = "tpch07-" + to_string(i);
            load_snippet(*ret,snippet_name,db_name);

        }
    } else if(query_str == "TPC-H_08"){ //TPC-H Query 8
        for(int i = 0; i < 8 ;i++){ // scan snippet 수
            string scan_snippet_name = "tpch08-" + to_string(i);
            tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
            tpch_snippets.push_back(tpch_snippet);
            scan_snippet_name_list.push_back(scan_snippet_name);

        }
        for(int i=0;i<tpch_snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(tpch_snippets.at(i),db_name);
        }
        generate_snippet_json(tpch_snippets, db_name, scan_snippet_name_list);
        for(int i = 0; i < 17;i++){ //전체 스니펫 수
            string snippet_name = "tpch08-" + to_string(i);
            load_snippet(*ret,snippet_name,db_name);

        }
    } else if(query_str == "TPC-H_09"){ //TPC-H Query 9
        for(int i = 0; i < 6 ;i++){ // scan snippet 수
            string scan_snippet_name = "tpch09-" + to_string(i);
            tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
            tpch_snippets.push_back(tpch_snippet);
            scan_snippet_name_list.push_back(scan_snippet_name);

        }
        for(int i=0;i<tpch_snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(tpch_snippets.at(i),db_name);
        }
        generate_snippet_json(tpch_snippets, db_name, scan_snippet_name_list);
        for(int i = 0; i < 12;i++){ //전체 스니펫 수
            string snippet_name = "tpch09-" + to_string(i);
            load_snippet(*ret,snippet_name,db_name);

        }
    } else if(query_str == "TPC-H_10"){ //TPC-H Query 10
        for(int i = 0; i < 4 ;i++){ // scan snippet 수
            string scan_snippet_name = "tpch10-" + to_string(i);
            tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
            tpch_snippets.push_back(tpch_snippet);
            scan_snippet_name_list.push_back(scan_snippet_name);

        }
        for(int i=0;i<tpch_snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(tpch_snippets.at(i),db_name);
        }
        generate_snippet_json(tpch_snippets, db_name, scan_snippet_name_list);
        for(int i = 0; i < 7;i++){ //전체 스니펫 수
            string snippet_name = "tpch10-" + to_string(i);
            load_snippet(*ret,snippet_name,db_name);

        }
    } else if(query_str == "TPC-H_11"){ //TPC-H Query 11
        for(int i = 0; i < 3 ;i++){ // scan snippet 수
            string scan_snippet_name = "tpch11-" + to_string(i);
            tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
            tpch_snippets.push_back(tpch_snippet);
            scan_snippet_name_list.push_back(scan_snippet_name);

        }
        for(int i=0;i<tpch_snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(tpch_snippets.at(i),db_name);
        }
        generate_snippet_json(tpch_snippets, db_name, scan_snippet_name_list);
        for(int i = 0; i < 8;i++){ //전체 스니펫 수
            string snippet_name = "tpch11-" + to_string(i);
            load_snippet(*ret,snippet_name,db_name);

        }
    } else if(query_str == "TPC-H_12"){ //TPC-H Query 12
        for(int i = 0; i < 2 ;i++){ // scan snippet 수
            string scan_snippet_name = "tpch12-" + to_string(i);
            tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
            tpch_snippets.push_back(tpch_snippet);
            scan_snippet_name_list.push_back(scan_snippet_name);

        }
        for(int i=0;i<tpch_snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(tpch_snippets.at(i),db_name);
        }
        generate_snippet_json(tpch_snippets, db_name, scan_snippet_name_list);
        for(int i = 0; i < 3;i++){ //전체 스니펫 수
            string snippet_name = "tpch12-" + to_string(i);
            load_snippet(*ret,snippet_name,db_name);

        }
    } else if(query_str == "TPC-H_13"){ //TPC-H Query 13
        for(int i = 0; i < 2 ;i++){ // scan snippet 수
            string scan_snippet_name = "tpch13-" + to_string(i);
            tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
            tpch_snippets.push_back(tpch_snippet);
            scan_snippet_name_list.push_back(scan_snippet_name);

        }
        for(int i=0;i<tpch_snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(tpch_snippets.at(i),db_name);
        }
        generate_snippet_json(tpch_snippets, db_name, scan_snippet_name_list);
        for(int i = 0; i < 4;i++){ //전체 스니펫 수
            string snippet_name = "tpch13-" + to_string(i);
            load_snippet(*ret,snippet_name,db_name);

        }
    } else if(query_str == "TPC-H_14"){ //TPC-H Query 14
        for(int i = 0; i < 2;i++ ){ // scan snippet 수
            string scan_snippet_name = "tpch14-" + to_string(i);
            tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
            tpch_snippets.push_back(tpch_snippet);
            scan_snippet_name_list.push_back(scan_snippet_name);

        }
        for(int i=0;i<tpch_snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(tpch_snippets.at(i),db_name);
        }
        generate_snippet_json(tpch_snippets, db_name, scan_snippet_name_list);
        for(int i = 0; i < 4;i++){ //전체 스니펫 수
            string snippet_name = "tpch14-" + to_string(i);
            load_snippet(*ret,snippet_name,db_name);

        }
    } else if(query_str == "TPC-H_15"){ //TPC-H Query 15
        string scan_snippet_name = "tpch15-" + to_string(0);
        tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
        tpch_snippets.push_back(tpch_snippet);
        scan_snippet_name_list.push_back(scan_snippet_name);

        scan_snippet_name = "tpch15-" + to_string(2);
        tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
        tpch_snippets.push_back(tpch_snippet);
        scan_snippet_name_list.push_back(scan_snippet_name);

        for(int i=0;i<tpch_snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(tpch_snippets.at(i),db_name);
        }
        cout << "[PlanExecutor] generate snippet..." << endl;
        generate_snippet_json(tpch_snippets, db_name, scan_snippet_name_list);
        for(int i = 0; i < 6;i++){ //전체 스니펫 수
            string snippet_name = "tpch15-" + to_string(i);
            load_snippet(*ret,snippet_name,db_name);

        }
    } else if(query_str == "TPC-H_16"){ //TPC-H Query 16
        for(int i = 0; i < 3 ;i++){ // scan snippet 수
            string scan_snippet_name = "tpch16-" + to_string(i);
            tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
            tpch_snippets.push_back(tpch_snippet);
            scan_snippet_name_list.push_back(scan_snippet_name);

        }
        cout << "[PlanExecutor] generate snippet..." << endl;
        for(int i=0;i<tpch_snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(tpch_snippets.at(i),db_name);
        }
        generate_snippet_json(tpch_snippets, db_name, scan_snippet_name_list);
        for(int i = 0; i < 5;i++){ //전체 스니펫 수
            string snippet_name = "tpch16-" + to_string(i);
            load_snippet(*ret,snippet_name,db_name);

        }
    } else if(query_str == "TPC-H_17"){ //TPC-H Query 17
        for(int i = 0; i < 2;i++ ){ // scan snippet 수
            string scan_snippet_name = "tpch17-" + to_string(i);
            tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
            tpch_snippets.push_back(tpch_snippet);
            scan_snippet_name_list.push_back(scan_snippet_name);

        }
        for(int i=0;i<tpch_snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(tpch_snippets.at(i),db_name);
        }
        generate_snippet_json(tpch_snippets, db_name, scan_snippet_name_list);
        for(int i = 0; i < 4;i++){ //전체 스니펫 수
            string snippet_name = "tpch17-" + to_string(i);
            load_snippet(*ret,snippet_name,db_name);

        }
    } else if(query_str == "TPC-H_18"){ //TPC-H Query 18
        for(int i = 0; i < 3 ;i++){ // scan snippet 수
            string scan_snippet_name = "tpch18-" + to_string(i);
            tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
            tpch_snippets.push_back(tpch_snippet);
            scan_snippet_name_list.push_back(scan_snippet_name);

        }
        for(int i=0;i<tpch_snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(tpch_snippets.at(i),db_name);
        }
        generate_snippet_json(tpch_snippets, db_name, scan_snippet_name_list);
        for(int i = 0; i < 7;i++){ //전체 스니펫 수
            string snippet_name = "tpch18-" + to_string(i);
            load_snippet(*ret,snippet_name,db_name);

        }
    } else if(query_str == "TPC-H_19"){ //TPC-H Query 19
        load_snippet(*ret,"tpch19-0",db_name);
        load_snippet(*ret,"tpch19-1",db_name);
        load_snippet(*ret,"tpch19-2",db_name);
        load_snippet(*ret,"tpch19-3",db_name);
        load_snippet(*ret,"tpch19-4",db_name);
        load_snippet(*ret,"tpch19-5",db_name);
        load_snippet(*ret,"tpch19-6",db_name);
        load_snippet(*ret,"tpch19-7",db_name);
        load_snippet(*ret,"tpch19-8",db_name);
        load_snippet(*ret,"tpch19-9",db_name);
        load_snippet(*ret,"tpch19-10",db_name);
    } else if(query_str == "TPC-H_20"){ //TPC-H Query 20
        for(int i = 0; i < 5 ;i++){ // scan snippet 수
            string scan_snippet_name = "tpch20-" + to_string(i);
            tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
            tpch_snippets.push_back(tpch_snippet);
            scan_snippet_name_list.push_back(scan_snippet_name);

        }
        for(int i=0;i<tpch_snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(tpch_snippets.at(i),db_name);
        }
        generate_snippet_json(tpch_snippets, db_name, scan_snippet_name_list);
        for(int i = 0; i < 9;i++){ //전체 스니펫 수
            string snippet_name = "tpch20-" + to_string(i);
            load_snippet(*ret,snippet_name,db_name);

        }
    } else if(query_str == "TPC-H_21"){ //TPC-H Query 21
        for(int i = 0; i < 6 ;i++){ // scan snippet 수
            string scan_snippet_name = "tpch21-" + to_string(i);
            tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
            tpch_snippets.push_back(tpch_snippet);
            scan_snippet_name_list.push_back(scan_snippet_name);

        }
        for(int i=0;i<tpch_snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(tpch_snippets.at(i),db_name);
        }
        generate_snippet_json(tpch_snippets, db_name, scan_snippet_name_list);
        for(int i = 0; i < 11;i++){ //전체 스니펫 수
            string snippet_name = "tpch21-" + to_string(i);
            load_snippet(*ret,snippet_name,db_name);

        }
    } else if(query_str == "TPC-H_22"){ //TPC-H Query 22
        string scan_snippet_name = "tpch22-" + to_string(0);
        tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
        tpch_snippets.push_back(tpch_snippet);
        scan_snippet_name_list.push_back(scan_snippet_name);

        scan_snippet_name = "tpch22-" + to_string(2);
        tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
        tpch_snippets.push_back(tpch_snippet);
        scan_snippet_name_list.push_back(scan_snippet_name);

        scan_snippet_name = "tpch22-" + to_string(3);
        tpch_snippet = parsing_tpch_snippet(scan_snippet_name, db_name); 
        tpch_snippets.push_back(tpch_snippet);
        scan_snippet_name_list.push_back(scan_snippet_name);
        for(int i=0;i<tpch_snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(tpch_snippets.at(i),db_name);
        }
        generate_snippet_json(tpch_snippets, db_name, scan_snippet_name_list);
        for(int i = 0; i < 6 ;i++){ //전체 스니펫 수
            string snippet_name = "tpch22-" + to_string(i);
            load_snippet(*ret,snippet_name,db_name);
        }
    }
    else if (query_str == "test_lineitem"){ 
        load_snippet(*ret,"test_lineitem",db_name);
    } else if (query_str == "test_customer"){ 
        load_snippet(*ret,"test_customer",db_name);
    } else if (query_str == "test_orders"){ 
        load_snippet(*ret,"test_orders",db_name);
    } else if (query_str == "test_part"){ 
        load_snippet(*ret,"test_part",db_name);
    } else if (query_str == "test_partsupp"){ 
        load_snippet(*ret,"test_partsupp",db_name);
    } else if (query_str == "test_nation"){ 
        load_snippet(*ret,"test_nation",db_name);
    } else if (query_str == "test_region"){ 
        load_snippet(*ret,"test_region",db_name);
    } else if (query_str == "test_supplier"){ 
        load_snippet(*ret,"test_supplier",db_name);
    } else if (query_str == "test"){ 
        load_snippet(*ret,"test1",db_name);
        load_snippet(*ret,"test2",db_name);
    } else{
        cout << "Custom Query: " << query_str << ", DB Name : " << db_name << endl;
        //***Create Snippet (type, query_info, result_info)***
        vector <Snippet> snippets;
        ParsedCustomQuery parsed_custom_query = parsed_query.GetParsedCustomQuery();
        create_snippet_init_info(db_name, parsed_custom_query, snippets);
        if(parsed_custom_query.is_parsing_custom_query ==false){
            cout << "파싱 실패, Generic으로 바꿈" << endl;
            KETILOG::DEBUGLOG(LOGTAG," => Generic Query");

            // DB_Monitoring_Manager::UpdateSelectCount();//구분필요!!!!
            //Generic Query 및 K-ODBC Query 구분 필요!!
            char *szDSN  = (char*)db_name.c_str();;

            char *szUID ;
            char *szPWD ; 
            char *szSQL;
            SQLHENV hEnv = SQL_NULL_HENV;
            SQLHDBC hDbc = SQL_NULL_HDBC;

            //Open database
            if(!OpenDatabase(&hEnv, &hDbc, szDSN, szUID, szPWD)){
             cout << "DB Open Error";
            }

            printf("[K-ODBC] K-OpenSource DB Connected!\n[K-ODBC] Using DBMS : \n");
            system("mysql --version");
            printf("[K-ODBC] DSN : %s\n", szDSN);
            printf("[K-ODBC] User ID : root\n");
            printf("[K-ODBC] Using DataBase : %s\n\n", db_name.c_str());
            printf("[K-ODBC] Input Query : \n");
            cout << parsed_query.GetOriginalQuery() << endl;
            //Execute SQL
            szSQL = strcpy(new char[parsed_query.GetOriginalQuery().length() + 1], parsed_query.GetOriginalQuery().c_str());
            printf("%s\n", szSQL);
            std::string res;
            ExecuteSQL( hDbc, szSQL, res);
            delete[] szSQL;

            // query_log.AddResultInfo(/*table totla row count*/0,0,/*query result*/"");

            //Database Close
            CloseDatabase(hEnv, hDbc);
        } 
        KETILOG::DEBUGLOG(LOGTAG,"Create Snippet Init Info...");
        
        //***Set MetaData of Snippet (query_id, work_id, schema_info, sst_info, result_info - total_block_count)***

        for(int i=0;i<snippets.size();i++){
            KETILOG::DEBUGLOG(LOGTAG,"Setting MetaData of Query...");
            MetaDataManager::SetMetaData(snippets.at(i),db_name);
        }
        vector <string> empty_snippet;
        generate_snippet_json(snippets, db_name, empty_snippet);
        string snippet_name;
        for(int i = 0; i < snippets.size();i++){
            snippet_name = "custom_snippet" + to_string(i) ;
            KETILOG::DEBUGLOG(LOGTAG,"Custom Snippet Name : " +  snippet_name);
            load_snippet(*ret, snippet_name, db_name);

        }
    }

	return ret;
    
}

void read_json(std::string& request,std::string snippet_name, const string &db_name){
    request = "";
     	std::ifstream openFile("../snippets/"+ db_name + "/" + snippet_name + ".json");
	if(openFile.is_open() ){
		std::string line;
		while(getline(openFile, line)){
			request += line;
		}
		openFile.close();
	}
}

void load_snippet(std::list<SnippetRequest> &list,std::string snippet_name, const string &db_name){
    SnippetRequest request;

    std::string json_str;
    read_json(json_str, snippet_name, db_name);
    google::protobuf::util::JsonParseOptions options;
    options.ignore_unknown_fields = true;

    auto status = google::protobuf::util::JsonStringToMessage(json_str, &request, options);
    if (!status.ok()) {
        std::cerr << "Error parsing JSON: " << status.ToString() << std::endl;
        return;
    }
    list.push_back(request);
}
ValueType determineType(const std::string& value) {
    // 정수 
    if (std::regex_match(value, std::regex(R"(\d+)"))) {
        if (value.length() <= 3) return ValueType::INT8;
        else if (value.length() <= 5) return ValueType::INT16;
        else if (value.length() <= 10) return ValueType::INT32;
        else return ValueType::INT64;
    }
    // 실수 
    else if (std::regex_match(value, std::regex(R"(\d+\.\d+)"))) {
        if (value.length() <= 7) return ValueType::FLOAT32;
        else return ValueType::FLOAT64;
    }
    // 날짜 
    else if (std::regex_match(value, std::regex(R"(\d{4}-\d{2}-\d{2})"))) {
        return ValueType::DATE;
    }
    // 타임스탬프 
    else if (std::regex_match(value, std::regex(R"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"))) {
        return ValueType::TIMESTAMP;
    }
    // 문자열 판별
    else if (value.front() == '\'' && value.back() == '\'') {
        return ValueType::STRING;
    }
    // 기본적으로 STRING으로 처리
    return ValueType::STRING;
}

//1. custom-create_snippet_init_info type, query_info, result_info 생성
//type, query_info, result_info
void PlanExecutor::create_snippet_init_info(const string &db_name,ParsedCustomQuery &parsed_custom_query, vector<Snippet> &snippets){
    
    int snippet_i = 0;
    for(auto &query_table_entry : parsed_custom_query.query_tables){
        Snippet snippet;
        std::string snippet_name = query_table_entry.first;
        QueryTable &query_table = query_table_entry.second;
 
        /*query_type*/
        snippet.type = query_table.query_type;

        /*work_id*/
        snippet.work_id = snippet_i;

        /*query_info - table_name*/
        snippet.query_info.table_name1 = query_table.table_name1 ;
        snippet.query_info.table_name2 = query_table.table_name2 ;
        snippet.query_info.table_alias1 = query_table.table_alias1 ;
        snippet.query_info.table_alias2 = query_table.table_alias2 ;

        /*query_info - filtering*/
        setFilterToSnippet(parsed_custom_query, query_table, snippet);
        
        /*query_info - projection*/
        setProjectionToSnippet(db_name, parsed_custom_query, query_table, snippet);
        
        if(snippet_i == parsed_custom_query.query_tables.size()-1){
            /*query_info - order by*/
            setOrderByToSnippet(parsed_custom_query, snippet);

            /*query_info - limit*/
            setLimitToSnippet(parsed_custom_query, snippet);
        }  

        /*result_info - table_alias, column_alias */
        if(snippet.type == QueryType::AGGREGATION){
            for(auto &column_alias_entry : parsed_custom_query.result_columns_columns_alias_map){
                snippet.result_info.columns_alias.push_back(column_alias_entry.second);
            
            }
        }else{
            for(auto &column_alias_entry : query_table.select_columns){
                snippet.result_info.columns_alias.push_back(column_alias_entry);
            
            }
        }
        
        snippet.result_info.table_alias = query_table.result_table_alias;

        snippets.push_back(snippet);

        snippet_i++;

    }


}

void PlanExecutor::generate_snippet_json(vector<Snippet> &snippets, const string &db_name, vector<string> &tpch_scan_snippet_name) {
    for (int i = 0; i < snippets.size(); i++) {
        Document snippet_obj;
        snippet_obj.SetObject();  // 매 반복마다 새로운 JSON 문서 시작
        Document::AllocatorType& allocator = snippet_obj.GetAllocator();
        // "type", "query_id", "work_id" fields
        snippet_obj.AddMember("type", snippets[i].type, allocator);
        snippet_obj.AddMember("query_id", snippets[i].query_id, allocator);
        snippet_obj.AddMember("work_id", snippets[i].work_id, allocator);

        // "query_info" object
        Value query_info(kObjectType);
        // "table_name" array
        Value table_name(kArrayType);
        
        Value t1, t2;
        // cout  << snippets[i].query_info.table_name1.c_str() << endl;
        t1.SetString(snippets[i].query_info.table_name1.c_str(), allocator);
        table_name.PushBack(t1, allocator);
        if(snippets[i].query_info.table_name2 != ""){
            t2.SetString(snippets[i].query_info.table_name2.c_str(), allocator);
            table_name.PushBack(t2, allocator);

        }
        // Step 3: Add "table_name" array to query_info
        query_info.AddMember("table_name", table_name, allocator);

        // "filtering" array
        Value filtering_array(kArrayType);
        int and_flag = 0;

        for (const auto& filter : snippets[i].query_info.filtering) {
            Value filter_obj(kObjectType);

            // 빈 값이 없으면 바로 처리
            if (filter.lv.values.size() == 0 && filter.rv.values.size() == 0) {
                Value operator_value;
                operator_value.SetInt(filter.operator_);
                filter_obj.AddMember("operator", operator_value, allocator);
                filtering_array.PushBack(filter_obj, allocator);
                continue;
            }
            
            // lv 처리
            Value lv_obj(kObjectType);
            Value lv_type_array(kArrayType);
            for(const auto& lv_type : filter.lv.types){
                Value lv_type_val;
                lv_type_val.SetInt(lv_type);
                lv_type_array.PushBack(lv_type_val, allocator);
            }
            Value lv_value_array(kArrayType);
            for(const auto& lv_value : filter.lv.values){
                Value lv_value_val;
                lv_value_val.SetString(lv_value.c_str(), allocator);
                lv_value_array.PushBack(lv_value_val, allocator);
            }
            lv_obj.AddMember("type",lv_type_array,allocator);
            lv_obj.AddMember("value",lv_value_array,allocator);
            filter_obj.AddMember("lv", lv_obj, allocator);

            // operator 추가
            filter_obj.AddMember("operator", filter.operator_, allocator);

            // rv 처리
            Value rv_obj(kObjectType);
            Value rv_type_array(kArrayType);
            for(const auto& rv_type : filter.rv.types){
                Value rv_type_val;
                rv_type_val.SetInt(rv_type);
                rv_type_array.PushBack(rv_type_val, allocator);
            }
            Value rv_value_array(kArrayType);
            for(const auto& rv_value : filter.rv.values){
                Value rv_value_val;
                rv_value_val.SetString(rv_value.c_str(), allocator);
                rv_value_array.PushBack(rv_value_val, allocator);
            }
            rv_obj.AddMember("type",rv_type_array,allocator);
            rv_obj.AddMember("value",rv_value_array,allocator);
            filter_obj.AddMember("rv", rv_obj, allocator);

            // filtering array에 필터 추가
            filtering_array.PushBack(filter_obj, allocator);

            // 마지막이 아니면 "AND" 추가
            if (and_flag < snippets[i].query_info.filtering.size() - 1) {
                Value filetring_and_value;
                filetring_and_value.SetInt(13);  // "AND"를 나타내는 값
                Value and_operator(kObjectType);
                and_operator.AddMember("operator", filetring_and_value, allocator);
                filtering_array.PushBack(and_operator, allocator);
            }
            and_flag++;
        }

        // filtering array를 query_info에 추가
        query_info.AddMember("filtering", filtering_array, allocator);

        // "projection" array
        Value projection_array(kArrayType);
        for (const auto& proj : snippets[i].query_info.projection) {
            Value proj_obj(kObjectType);
            proj_obj.AddMember("select_type", proj.select_type, allocator);

            Value value_array(kArrayType);
            for (const auto& val : proj.expression.values) {
                Value value;
                value.SetString(val.c_str(), allocator);
                value_array.PushBack(value, allocator);
            }
            proj_obj.AddMember("value", value_array, allocator);

            Value type_array(kArrayType);
            for (const auto& type : proj.expression.types) {
                type_array.PushBack(type, allocator);
            }
            proj_obj.AddMember("value_type", type_array, allocator);

            projection_array.PushBack(proj_obj, allocator);
        }
        query_info.AddMember("projection", projection_array, allocator);

        // "group by" array
        if(snippets[i].query_info.group_by.size() > 0){
            Value group_array(kArrayType);

            for(const auto& group_by_column : snippets[i].query_info.group_by){
                Value group_by_column_value;
                group_by_column_value.SetString(group_by_column.c_str(), allocator);
                group_array.PushBack(group_by_column_value, allocator);
            }
            query_info.AddMember("group_by", group_array, allocator);
        }

        // "order by" array
        if(i == snippets.size()-1){
            Value ascending_array(kArrayType);
            Value column_name_array(kArrayType);

            for (const auto& order_by_field : snippets[i].query_info.order_by) {
                // order_by_field는 {column_name, is_asc} 형태의 std::pair
                const std::string& column_name = order_by_field.first;
                bool is_asc = order_by_field.second;

                Value column_name_value;
                column_name_value.SetString(column_name.c_str(), allocator);
                column_name_array.PushBack(column_name_value, allocator);

                ascending_array.PushBack(is_asc ? 0 : 1, allocator);
            }

            Value order_by_obj(kObjectType);
            order_by_obj.AddMember("ascending", ascending_array, allocator);
            order_by_obj.AddMember("column_name", column_name_array, allocator);

            query_info.AddMember("order_by", order_by_obj, allocator);
        }
            

        // "limit" object
        if(snippets[i].query_info.limit.offset != -1 && snippets[i].query_info.limit.length != -1){
            Value limit_object(kObjectType);
            Value offset_value;
            Value length_value;

            offset_value.SetInt(snippets[i].query_info.limit.offset);
            length_value.SetInt(snippets[i].query_info.limit.length);
            limit_object.AddMember("offset", offset_value, allocator);
            limit_object.AddMember("length", length_value, allocator);
            query_info.AddMember("limit", limit_object, allocator);
            
        }

        snippet_obj.AddMember("query_info", query_info, allocator);

        // "schema_info" object
        Value schema_obj(kObjectType);
        Value column_list(kArrayType);
        for (const auto& column : snippets[i].schema_info.column_list) {
            Value column_obj(kObjectType);
            column_obj.AddMember("name", Value().SetString(column.name.c_str(), allocator), allocator);
            column_obj.AddMember("type", column.type, allocator);
            column_obj.AddMember("length", column.length, allocator);
            column_obj.AddMember("primary", column.primary, allocator);
            column_obj.AddMember("index", column.index, allocator);
            column_obj.AddMember("nullable", column.nullable, allocator);
            column_list.PushBack(column_obj, allocator);
        }
        if(column_list.Size() != 0){
            schema_obj.AddMember("column_list", column_list, allocator);
            schema_obj.AddMember("table_index_number", snippets[i].schema_info.table_index_number, allocator);
        }

        snippet_obj.AddMember("schema_info", schema_obj, allocator);

        // "wal_info" object
        Value wal_info(kObjectType);
        Value deleted_key_array(kArrayType), inserted_key_array(kArrayType), inserted_value_array(kArrayType);
        for (const auto& key : snippets[i].wal_info.deleted_key) deleted_key_array.PushBack(Value().SetString(key.c_str(), allocator), allocator);
        for (const auto& key : snippets[i].wal_info.inserted_key) inserted_key_array.PushBack(Value().SetString(key.c_str(), allocator), allocator);
        for (const auto& val : snippets[i].wal_info.inserted_value) inserted_value_array.PushBack(Value().SetString(val.c_str(), allocator), allocator);
        wal_info.AddMember("deleted_key", deleted_key_array, allocator);
        wal_info.AddMember("inserted_key", inserted_key_array, allocator);
        wal_info.AddMember("inserted_value", inserted_value_array, allocator);
        snippet_obj.AddMember("wal_info", wal_info, allocator);

        // "result_info" object
        Value result_info(kObjectType);
        result_info.AddMember("table_alias", Value().SetString(snippets[i].result_info.table_alias.c_str(), allocator), allocator);

        Value column_alias_array(kArrayType);
        for (const auto& alias : snippets[i].result_info.columns_alias) {
            Value column_alias_value;
            column_alias_value.SetString(alias.c_str(), allocator);
            column_alias_array.PushBack(column_alias_value, allocator);
        }
        result_info.AddMember("column_alias", column_alias_array, allocator);
        if(snippets[i].result_info.total_block_count !=0){
            result_info.AddMember("total_block_count", snippets[i].result_info.total_block_count, allocator);

        }
        snippet_obj.AddMember("result_info", result_info, allocator);
    
        // "sst_info" array
        Value sst_info_array(kArrayType);
        for (const auto& sst : snippets[i].sst_info) {
            Value sst_obj(kObjectType);
            sst_obj.AddMember("sst_name", Value().SetString(sst.sst_name.c_str(), allocator), allocator);

            Value csd_array(kArrayType);
            for (const auto& csd : sst.csd) {
                Value csd_obj(kObjectType);
                csd_obj.AddMember("csd_id", Value().SetString(csd.csd_id.c_str(), allocator), allocator);
                csd_obj.AddMember("partition", Value().SetString(csd.partition.c_str(), allocator), allocator);
                csd_array.PushBack(csd_obj, allocator);
            }
            sst_obj.AddMember("csd", csd_array, allocator);
            sst_obj.AddMember("sst_block_count", Value().SetInt(sst.sst_block_count), allocator);
            sst_info_array.PushBack(sst_obj, allocator);
        }
        snippet_obj.AddMember("sst_info", sst_info_array, allocator);
        
        // Convert document to JSON string
        StringBuffer buffer;
        Writer<StringBuffer> writer(buffer);
        snippet_obj.Accept(writer);

        // 결과를 파일로 저장
        std::string outputFilePath;
        if(tpch_scan_snippet_name.size() > 0){
            outputFilePath = "../snippets/" + db_name + "/" + tpch_scan_snippet_name.at(i) + ".json";

        }else{
            outputFilePath = "../snippets/" + db_name + "/custom_snippet" + std::to_string(i) + ".json";
            
        }
        std::ofstream outputFile(outputFilePath);
        if (outputFile.is_open()) {
            outputFile << buffer.GetString();
            outputFile.close();
        } else {
            std::cerr << "파일을 열 수 없습니다: " << outputFilePath << std::endl;
        }
    }
}

void PlanExecutor::setFilterToSnippet(const ParsedCustomQuery& parsed_custom_query, const QueryTable &query_table, Snippet& snippet) {    
    if(query_table.where_conditions.size() == 0){
        KETILOG::DEBUGLOG(LOGTAG,"where 없음" );
        
    }else{
        for (const auto& condition_unit : query_table.where_conditions) { //condition 수만큼 Filtering 만들어야함
            WhereCondition where_condition = condition_unit;

            //snippet
            Filtering filter;
            Operand left_operand, right_operand;


            // 왼쪽 피연산자 설정 (컬럼 이름을 기준으로 ValueType을 COLUMN으로 설정)
            left_operand.values.push_back(where_condition.left_values.at(0).value);
            left_operand.types.push_back(static_cast<int>(ValueType::COLUMN));
            
            filter.lv = left_operand;
            // 연산자 설정 (연산자 문자열을 OperType으로 매핑)
            if (where_condition.op == ">=") {
                filter.operator_ = static_cast<int>(OperType::GE);
            } else if (where_condition.op == "<=") {
                filter.operator_ = static_cast<int>(OperType::LE);
            } else if (where_condition.op == ">") {
                filter.operator_ = static_cast<int>(OperType::GT);
            } else if (where_condition.op == "<") {
                filter.operator_ = static_cast<int>(OperType::LT);
            } else if (where_condition.op == "=") {
                filter.operator_ = static_cast<int>(OperType::EQ);
            } else if (where_condition.op == "<>") {
                filter.operator_ = static_cast<int>(OperType::NE);
            } else if (where_condition.op == "LIKE" || where_condition.op == "like") {
                filter.operator_ = static_cast<int>(OperType::LIKE);
            } else if (where_condition.op == "NOT LIKE" || where_condition.op == "not like") {
                filter.operator_ = static_cast<int>(OperType::NOTLIKE);
            } else if (where_condition.op == "BETWEEN"|| where_condition.op == "between" ) {
                filter.operator_ = static_cast<int>(OperType::BETWEEN);
            } else if (where_condition.op == "IN" || where_condition.op == "in") {
                filter.operator_ = static_cast<int>(OperType::IN);
            } else if (where_condition.op == "NOT IN" || where_condition.op == "not in") {
                filter.operator_ = static_cast<int>(OperType::NOTIN);
            } else if (where_condition.op == "IS" || where_condition.op == "is") {
                filter.operator_ = static_cast<int>(OperType::IS);
            } else if (where_condition.op == "IS NOT"|| where_condition.op == "is not" ) {
                filter.operator_ = static_cast<int>(OperType::ISNOT);
            }
            //오른쪽 처리 
            for(auto &value : where_condition.right_values){
                right_operand.values.push_back(value.value);
                right_operand.types.push_back(value.value_type);
            }
    
            // Filtering 구조체 설정
            filter.lv = left_operand;
            filter.rv = right_operand;

            // Snippet의 query_info.filtering에 추가
            snippet.query_info.filtering.push_back(filter);
        }
    }

    if(query_table.join_condition.left_table_name != "" && query_table.join_condition.right_table_name != ""){
        Filtering filter;
        Operand left_operand, right_operand;

        left_operand.values.push_back(query_table.join_condition.left_column);
        left_operand.types.push_back(static_cast<int>(ValueType::COLUMN));

        right_operand.values.push_back(query_table.join_condition.right_column);
        right_operand.types.push_back(static_cast<int>(ValueType::COLUMN));

        filter.lv = left_operand;
        filter.rv = right_operand;
        filter.operator_ = static_cast<int>(OperType::EQ);
        snippet.query_info.filtering.push_back(filter);
    }
}


void PlanExecutor::setProjectionToSnippet(const string &db_name, const ParsedCustomQuery& parsed_custom_query, const QueryTable &query_table, Snippet& snippet) {
    
    if(snippet.type == QueryType::AGGREGATION){
        for(auto &col_entry : parsed_custom_query.result_columns_columns_alias_map){
            
            //1. 그냥 컬럼일때
            if(parsed_custom_query.column_table_map.find(col_entry.first) != parsed_custom_query.column_table_map.end()) {
                Projection projection;
                projection.select_type = static_cast<int>(SelectType::COLUMNNAME);
                projection.expression.types.push_back(static_cast<int>(ValueType::COLUMN));
                projection.expression.values.push_back(col_entry.first);
                snippet.query_info.projection.push_back(projection); // Snippet에 projection 추가

            }

        }
 
        for(auto &agg_col_entry : parsed_custom_query.aggregation_column_map){
            Projection projection;
            string func = agg_col_entry.first;
            string col = agg_col_entry.second;
            
            if (func == "SUM" || func == "sum") {
                projection.select_type = static_cast<int>(SelectType::SUM);
                // Projectioning 구조체에 표현식 추가
                projection.expression.types.push_back(static_cast<int>(ValueType::COLUMN));
                projection.expression.values.push_back(col);
            } else if (func == "AVG" || func == "avg") {
                projection.select_type = static_cast<int>(SelectType::AVG);
                // Projectioning 구조체에 표현식 추가
                projection.expression.types.push_back(static_cast<int>(ValueType::COLUMN));
                projection.expression.values.push_back(col);
            } else if (func == "COUNT(*)" ||func == "count(*)") {
                projection.select_type = static_cast<int>(SelectType::COUNTSTAR);  // COUNT(*)
            } else if (func == "COUNT(DISTINCT)" || func == "count(distinct)") {
                projection.select_type = static_cast<int>(SelectType::COUNTDISTINCT);  // COUNT(DISTINCT)
                // Projectioning 구조체에 표현식 추가
                projection.expression.types.push_back(static_cast<int>(ValueType::COLUMN));
                projection.expression.values.push_back(col);
            }  else if (func == "COUNT" || func == "count") {
                projection.select_type = static_cast<int>(SelectType::COUNT);  // COUNT()
                // Projectioning 구조체에 표현식 추가
                projection.expression.types.push_back(static_cast<int>(ValueType::COLUMN));
                projection.expression.values.push_back(col);
            } else if (func == "MIN" || func == "min") {
                projection.select_type = static_cast<int>(SelectType::MIN);
                // Projectioning 구조체에 표현식 추가
                projection.expression.types.push_back(static_cast<int>(ValueType::COLUMN));
                projection.expression.values.push_back(col);
            } else if (func == "MAX" || func == "max") {
                projection.select_type = static_cast<int>(SelectType::MAX);
                // Projectioning 구조체에 표현식 추가
                projection.expression.types.push_back(static_cast<int>(ValueType::COLUMN));
                projection.expression.values.push_back(col);
            } else if (func == "TOP" || func == "top") {
                projection.select_type = static_cast<int>(SelectType::TOP);
                // Projectioning 구조체에 표현식 추가
                projection.expression.types.push_back(static_cast<int>(ValueType::COLUMN));
                projection.expression.values.push_back(col);
            }
            
            snippet.query_info.projection.push_back(projection); // Snippet에 projection 추가
        }
            
    }else{
        // cout << "hj :: select_field 확인 : " ;
        for(auto &col : query_table.select_columns){
            // cout << col << " " ;
            
            Projection projection;
            projection.select_type = static_cast<int>(SelectType::COLUMNNAME);
            projection.expression.types.push_back(static_cast<int>(ValueType::COLUMN));
            projection.expression.values.push_back(col);
            // Projectioning 구조체에 표현식 추가
            snippet.query_info.projection.push_back(projection); // Snippet에 projection 추가
        }
    }
    
        
    
}
void PlanExecutor::setGroupByToSnippet(const ParsedCustomQuery& parsed_custom_query, Snippet& snippet){
    if(parsed_custom_query.group_by_fields.size()==0){
        KETILOG::DEBUGLOG(LOGTAG,"group by 없음" );
    }else{
        for(auto &item : parsed_custom_query.group_by_fields){
            snippet.query_info.group_by.push_back(item);

        }

    }
    for(auto &column : parsed_custom_query.group_by_fields){
        snippet.query_info.group_by.push_back(column);
    }
}
void PlanExecutor::setOrderByToSnippet(const ParsedCustomQuery& parsed_custom_query, Snippet& snippet){
    if(parsed_custom_query.order_by_fields.size()==0){
        KETILOG::DEBUGLOG(LOGTAG,"order 없음" );
    }else{
        snippet.query_info.order_by = parsed_custom_query.order_by_fields;

    }
    
}
void PlanExecutor::setLimitToSnippet(const ParsedCustomQuery& parsed_custom_query, Snippet& snippet){
    if( parsed_custom_query.limit.offset == -1){
        KETILOG::DEBUGLOG(LOGTAG,"limit 없음" );
    }else{
            snippet.query_info.limit = parsed_custom_query.limit;

    }
}
void PlanExecutor::trim(std::string& s) {
        s.erase(0, s.find_first_not_of(" \t\n\r\f\v"));
        s.erase(s.find_last_not_of(" \t\n\r\f\v") + 1);
    }

Snippet PlanExecutor::parsing_tpch_snippet(const string &file_name, const std::string &db_name){
    string file_path = "../snippets/"+ db_name + "/" + file_name + ".json";
    ifstream ifs(file_path);
    
    if (!ifs.is_open()) {
        cerr << "Failed to open file: " << file_path << endl;
    }

    Snippet snippet;
    IStreamWrapper isw(ifs);
    Document doc;
    doc.ParseStream(isw);

    if (doc.HasParseError()) {
        cerr << "JSON parse error: " << GetParseError_En(doc.GetParseError())
            << " at offset " << doc.GetErrorOffset() << endl;
    }

    // Snippet의 기본 정보 파싱
    if (doc.HasMember("type") && doc["type"].IsInt())
        snippet.type = doc["type"].GetInt();

    if (doc.HasMember("query_id") && doc["query_id"].IsInt())
        snippet.query_id = doc["query_id"].GetInt();

    if (doc.HasMember("work_id") && doc["work_id"].IsInt())
        snippet.work_id = doc["work_id"].GetInt();

    // result_info 파싱 (total_block_count 제외)
    if (doc.HasMember("result_info") && doc["result_info"].IsObject()) {
        const Value& result_info = doc["result_info"];

        if (result_info.HasMember("table_alias") && result_info["table_alias"].IsString())
            snippet.result_info.table_alias = result_info["table_alias"].GetString();

        if (result_info.HasMember("column_alias") && result_info["column_alias"].IsArray()) {
            for (const auto& alias : result_info["column_alias"].GetArray())
                snippet.result_info.columns_alias.push_back(alias.GetString());
        }

        // total_block_count는 제외
    }

    // query_info 파싱
    if (doc.HasMember("query_info") && doc["query_info"].IsObject()) {
        const Value& query_info = doc["query_info"];

        if (query_info.HasMember("table_name") && query_info["table_name"].IsArray()) {
            for (const auto& table : query_info["table_name"].GetArray())
                snippet.query_info.table_name1 = table.GetString(); 
        }

        if (query_info.HasMember("filtering") && query_info["filtering"].IsArray()) {
            for (const auto& filtering_entry : query_info["filtering"].GetArray()) {
                Filtering filtering;

                if (filtering_entry.HasMember("lv") && filtering_entry["lv"].IsObject()) {
                    const Value& lv = filtering_entry["lv"];
                    if (lv.HasMember("type") && lv["type"].IsArray()) {
                        for (const auto& type : lv["type"].GetArray())
                            filtering.lv.types.push_back(type.GetInt());
                    }
                    if (lv.HasMember("value") && lv["value"].IsArray()) {
                        for (const auto& value : lv["value"].GetArray())
                            filtering.lv.values.push_back(value.GetString());
                    }
                }

                if (filtering_entry.HasMember("operator") && filtering_entry["operator"].IsInt())
                    filtering.operator_ = filtering_entry["operator"].GetInt();

                if (filtering_entry.HasMember("rv") && filtering_entry["rv"].IsObject()) {
                    const Value& rv = filtering_entry["rv"];
                    if (rv.HasMember("type") && rv["type"].IsArray()) {
                        for (const auto& type : rv["type"].GetArray())
                            filtering.rv.types.push_back(type.GetInt());
                    }
                    if (rv.HasMember("value") && rv["value"].IsArray()) {
                        for (const auto& value : rv["value"].GetArray())
                            filtering.rv.values.push_back(value.GetString());
                    }
                }

                snippet.query_info.filtering.push_back(filtering);
            }
        }

        cout << "[QueryPlanner] snippet {WorkID:" << to_string(snippet.work_id) << "}" << endl;
        cout << "[QueryPlanner] L work_type: " << to_string(snippet.type) << endl;
        cout << "[QueryPlanner] L table_name: " << snippet.query_info.table_name1 << " " << snippet.query_info.table_name2 << endl;
        cout << "[QueryPlanner] L filter_count: " << snippet.query_info.filtering.size() << endl;
        cout << "[QueryPlanner] L projection: ";

        if (query_info.HasMember("projection") && query_info["projection"].IsArray()) {
            for (const auto& projection_entry : query_info["projection"].GetArray()) {
                Projection projection;

                if (projection_entry.HasMember("select_type") && projection_entry["select_type"].IsInt())
                    projection.select_type = projection_entry["select_type"].GetInt();

                if (projection_entry.HasMember("value") && projection_entry["value"].IsArray()) {
                    for (const auto& value : projection_entry["value"].GetArray()){
                        projection.expression.values.push_back(value.GetString());
                        cout << value.GetString() << " ";
                    }
                }

                if (projection_entry.HasMember("value_type") && projection_entry["value_type"].IsArray()) {
                    for (const auto& type : projection_entry["value_type"].GetArray())
                        projection.expression.types.push_back(type.GetInt());
                }

                snippet.query_info.projection.push_back(projection);
            }
        }
    }
    
    cout << endl;
    
    return snippet;
}