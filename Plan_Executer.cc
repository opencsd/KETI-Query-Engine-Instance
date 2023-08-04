#include "Plan_Executer.h"
#include <iostream>
#include <fstream>

#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <sql.h>
#include <sqlext.h>
#include "kodbc.h"
#include "keti_log.h"

void load_snippet(std::list<SnippetRequest> &list,std::string snippet_name);

std::string Plan_Executer::Execute_Query(Storage_Engine_Interface &storageEngineInterface,Parsed_Query &parsed_query){
    std::string res="";
    KETILOG::DEBUGLOG("Plan Executer","Analyzing Query ...");

    if(parsed_query.isGenericQuery()){
        KETILOG::DEBUGLOG("Plan Executer"," => Generic Query");
        char *szDSN = (char*)"myodbc5w";
        char *szUID;
        char *szPWD;
        char *szSQL;
        SQLHENV hEnv = SQL_NULL_HENV;
        SQLHDBC hDbc = SQL_NULL_HDBC;

        //Open database
        if(!OpenDatabase(&hEnv, &hDbc, szDSN, szUID, szPWD)){
        return "DB Open Error";
        }
        printf("[K-ODBC] K-OpenSource DB Connected!\n[K-ODBC] Using DBMS : \n");
        system("mysql --version");
        // printf("\n");
        printf("[K-ODBC] DSN : %s\n", szDSN);
        printf("[K-ODBC] User ID : root\n");
        printf("[K-ODBC] Using DataBase : tpch_small\n\n");
        printf("[K-ODBC] Input Query : \n");

        //Execute SQL
        szSQL = strcpy(new char[parsed_query.Get_Parsed_Query().length() + 1], parsed_query.Get_Parsed_Query().c_str());
        printf("%s\n", szSQL);
        ExecuteSQL( hDbc, szSQL, res);
        delete[] szSQL;

        //Database Close
        CloseDatabase(hEnv, hDbc);
    } else {
        KETILOG::DEBUGLOG("Plan Executer"," => Pushdown Query");
        int query_id = Set_Query_ID();
        auto snippet_list = Gen_Snippet(parsed_query);
        
        Send_Snippets(storageEngineInterface,*snippet_list,query_id);
        res = Get_Query_Result(storageEngineInterface,query_id);
        // res = Send_Snippets_And_Get_Query_Result(storageEngineInterface, *snippet_list, query_id);
    }
    return res;
}

std::string Plan_Executer::Send_Snippets_And_Get_Query_Result(Storage_Engine_Interface &storageEngineInterface, std::list<SnippetRequest> &snippet_list, int query_id){
    std::lock_guard<std::mutex> lock(gRPC_mutex);

    std::string query_result = storageEngineInterface.SendSnippetAndRun(snippet_list, query_id);
    
    return query_result;
}

void Plan_Executer::Send_Snippets(Storage_Engine_Interface &storageEngineInterface,std::list<SnippetRequest> &snippet_list, int query_id){
    std::lock_guard<std::mutex> lock(gRPC_mutex);

    std::list<SnippetRequest>::iterator iter = snippet_list.begin();
    storageEngineInterface.OpenStream();
    for(;iter != snippet_list.end();iter++){
        //set query id
        iter->mutable_snippet()->set_query_id(query_id);
        storageEngineInterface.SendSnippet(*iter);
    }
    storageEngineInterface.CloseStream();
}

std::string Plan_Executer::Get_Query_Result(Storage_Engine_Interface &storageEngineInterface,int query_id){
    // std::lock_guard<std::mutex> lock(mutex);
    return storageEngineInterface.Run(query_id);
}

int Plan_Executer::Set_Query_ID(){
    std::lock_guard<std::mutex> lock(mutex);
    return Query_ID++;
}

std::unique_ptr<std::list<SnippetRequest>> Plan_Executer::Gen_Snippet(Parsed_Query &parsed_query){ // test code
    std::unique_ptr<std::list<SnippetRequest>> ret(new std::list<SnippetRequest>());
    std::string query_str = parsed_query.Get_Ori_Query();
    
    KETILOG::DEBUGLOG("Plan Executer","Creating Snippet ...");

    if (query_str == "TPC-H_01"){ //TPC-H Query 1
        load_snippet(*ret,"tpch01-0");
        load_snippet(*ret,"tpch01-1");
    } else if (query_str == "TPC-H_02"){ //TPC-H Query 2
        load_snippet(*ret,"tpch02-0");
        load_snippet(*ret,"tpch02-1");
        load_snippet(*ret,"tpch02-2");
        load_snippet(*ret,"tpch02-3");
        load_snippet(*ret,"tpch02-4");
        load_snippet(*ret,"tpch02-5");
        load_snippet(*ret,"tpch02-6");
        load_snippet(*ret,"tpch02-7");
        load_snippet(*ret,"tpch02-8");
        load_snippet(*ret,"tpch02-9");
        load_snippet(*ret,"tpch02-10");
        load_snippet(*ret,"tpch02-11");
        load_snippet(*ret,"tpch02-12");
    } else if (query_str == "TPC-H_03"){ //TPC-H Query 3
        load_snippet(*ret,"tpch03-0");
        load_snippet(*ret,"tpch03-1");
        load_snippet(*ret,"tpch03-2");
        load_snippet(*ret,"tpch03-3");
        load_snippet(*ret,"tpch03-4");
        load_snippet(*ret,"tpch03-5");
    } else if (query_str == "TPC-H_04"){ //TPC-H Query 4
        load_snippet(*ret,"tpch04-0");
        load_snippet(*ret,"tpch04-1");
        load_snippet(*ret,"tpch04-2");
    } else if(query_str == "TPC-H_05"){ //TPC-H Query 5
        load_snippet(*ret,"tpch05-0");
        load_snippet(*ret,"tpch05-1");
        load_snippet(*ret,"tpch05-2");
        load_snippet(*ret,"tpch05-3");
        load_snippet(*ret,"tpch05-4");
        load_snippet(*ret,"tpch05-5");
        load_snippet(*ret,"tpch05-6");
        load_snippet(*ret,"tpch05-7");
        load_snippet(*ret,"tpch05-8");
        load_snippet(*ret,"tpch05-9");
        load_snippet(*ret,"tpch05-10");
        load_snippet(*ret,"tpch05-11");
    } else if (query_str == "TPC-H_06"){ //TPC-H Query 6
        load_snippet(*ret,"tpch06-0");
        load_snippet(*ret,"tpch06-1");
    } else if(query_str == "TPC-H_07"){ //TPC-H Query 7
        load_snippet(*ret,"tpch07-0");
        load_snippet(*ret,"tpch07-1");
        load_snippet(*ret,"tpch07-2");
        load_snippet(*ret,"tpch07-3");
        load_snippet(*ret,"tpch07-4");
        load_snippet(*ret,"tpch07-5");
        load_snippet(*ret,"tpch07-6");
        load_snippet(*ret,"tpch07-7");
        load_snippet(*ret,"tpch07-8");
        load_snippet(*ret,"tpch07-9");
        load_snippet(*ret,"tpch07-10");
        load_snippet(*ret,"tpch07-11");
        load_snippet(*ret,"tpch07-12");
        load_snippet(*ret,"tpch07-13");
        load_snippet(*ret,"tpch07-14");
    } else if(query_str == "TPC-H_08"){ //TPC-H Query 8
        load_snippet(*ret,"tpch08-0");
        load_snippet(*ret,"tpch08-1");
        load_snippet(*ret,"tpch08-2");
        load_snippet(*ret,"tpch08-3");
        load_snippet(*ret,"tpch08-4");
        load_snippet(*ret,"tpch08-5");
        load_snippet(*ret,"tpch08-6");
        load_snippet(*ret,"tpch08-7");
        load_snippet(*ret,"tpch08-8");
        load_snippet(*ret,"tpch08-9");
        load_snippet(*ret,"tpch08-10");
        load_snippet(*ret,"tpch08-11");
        load_snippet(*ret,"tpch08-12");
        load_snippet(*ret,"tpch08-13");
        load_snippet(*ret,"tpch08-14");
        load_snippet(*ret,"tpch08-15");
        load_snippet(*ret,"tpch08-16");
    } else if(query_str == "TPC-H_09"){ //TPC-H Query 9
        load_snippet(*ret,"tpch09-0");
        load_snippet(*ret,"tpch09-1");
        load_snippet(*ret,"tpch09-2");
        load_snippet(*ret,"tpch09-3");
        load_snippet(*ret,"tpch09-4");
        load_snippet(*ret,"tpch09-5");
        load_snippet(*ret,"tpch09-6");
        load_snippet(*ret,"tpch09-7");
        load_snippet(*ret,"tpch09-8");
        load_snippet(*ret,"tpch09-9");
        load_snippet(*ret,"tpch09-10");
        load_snippet(*ret,"tpch09-11");
    } else if(query_str == "TPC-H_10"){ //TPC-H Query 10
        load_snippet(*ret,"tpch10-0");
        load_snippet(*ret,"tpch10-1");
        load_snippet(*ret,"tpch10-2");
        load_snippet(*ret,"tpch10-3");
        load_snippet(*ret,"tpch10-4");
        load_snippet(*ret,"tpch10-5");
        load_snippet(*ret,"tpch10-6");
        load_snippet(*ret,"tpch10-7");
    } else if(query_str == "TPC-H_11"){ //TPC-H Query 11
        load_snippet(*ret,"tpch11-0");
        load_snippet(*ret,"tpch11-1");
        load_snippet(*ret,"tpch11-2");
        load_snippet(*ret,"tpch11-3");
        load_snippet(*ret,"tpch11-4");
        load_snippet(*ret,"tpch11-5");
        load_snippet(*ret,"tpch11-6");
        load_snippet(*ret,"tpch11-7");
    } else if(query_str == "TPC-H_12"){ //TPC-H Query 12
        load_snippet(*ret,"tpch12-0");
        load_snippet(*ret,"tpch12-1");
        load_snippet(*ret,"tpch12-2");
    } else if(query_str == "TPC-H_13"){ //TPC-H Query 13
        load_snippet(*ret,"tpch13-0");
        load_snippet(*ret,"tpch13-1");
        load_snippet(*ret,"tpch13-2");
        load_snippet(*ret,"tpch13-3");
    } else if(query_str == "TPC-H_14"){ //TPC-H Query 14
        load_snippet(*ret,"tpch14-0");
        load_snippet(*ret,"tpch14-1");
        load_snippet(*ret,"tpch14-2");
        load_snippet(*ret,"tpch14-3");
    } else if(query_str == "TPC-H_15"){ //TPC-H Query 15
        load_snippet(*ret,"tpch15-0");
        load_snippet(*ret,"tpch15-1");
        load_snippet(*ret,"tpch15-2");
        load_snippet(*ret,"tpch15-3");
        load_snippet(*ret,"tpch15-4");
        load_snippet(*ret,"tpch15-5");
    } else if(query_str == "TPC-H_16"){ //TPC-H Query 16
        load_snippet(*ret,"tpch16-0");
        load_snippet(*ret,"tpch16-1");
        load_snippet(*ret,"tpch16-2");
        load_snippet(*ret,"tpch16-3");
        load_snippet(*ret,"tpch16-4");
    } else if(query_str == "TPC-H_17"){ //TPC-H Query 17
        load_snippet(*ret,"tpch17-0");
        load_snippet(*ret,"tpch17-1");
        load_snippet(*ret,"tpch17-2");
        load_snippet(*ret,"tpch17-3");
    } else if(query_str == "TPC-H_18"){ //TPC-H Query 18
        load_snippet(*ret,"tpch18-0");
        load_snippet(*ret,"tpch18-1");
        load_snippet(*ret,"tpch18-2");
        load_snippet(*ret,"tpch18-3");
        load_snippet(*ret,"tpch18-4");
        load_snippet(*ret,"tpch18-5");
        load_snippet(*ret,"tpch18-6");
        load_snippet(*ret,"tpch18-7");
        load_snippet(*ret,"tpch18-8");
    } else if(query_str == "TPC-H_19"){ //TPC-H Query 19
        load_snippet(*ret,"tpch19-0");
        load_snippet(*ret,"tpch19-1");
        load_snippet(*ret,"tpch19-2");
        load_snippet(*ret,"tpch19-3");
        load_snippet(*ret,"tpch19-4");
        load_snippet(*ret,"tpch19-5");
        load_snippet(*ret,"tpch19-6");
        load_snippet(*ret,"tpch19-7");
        load_snippet(*ret,"tpch19-8");
        load_snippet(*ret,"tpch19-9");
        load_snippet(*ret,"tpch19-10");
    } else if(query_str == "TPC-H_20"){ //TPC-H Query 20
        load_snippet(*ret,"tpch20-0");
        load_snippet(*ret,"tpch20-1");
        load_snippet(*ret,"tpch20-2");
        load_snippet(*ret,"tpch20-3");
        load_snippet(*ret,"tpch20-4");
        load_snippet(*ret,"tpch20-5");
        load_snippet(*ret,"tpch20-6");
        load_snippet(*ret,"tpch20-7");
        load_snippet(*ret,"tpch20-8");
    } else if(query_str == "TPC-H_21"){ //TPC-H Query 21
        load_snippet(*ret,"tpch21-0");
        load_snippet(*ret,"tpch21-1");
        load_snippet(*ret,"tpch21-2");
        load_snippet(*ret,"tpch21-3");
        load_snippet(*ret,"tpch21-4");
        load_snippet(*ret,"tpch21-5");
        load_snippet(*ret,"tpch21-6");
        load_snippet(*ret,"tpch21-7");
        load_snippet(*ret,"tpch21-8");
        load_snippet(*ret,"tpch21-9");
    } else if(query_str == "TPC-H_22"){ //TPC-H Query 22
        load_snippet(*ret,"tpch22-0");
        load_snippet(*ret,"tpch22-1");
        load_snippet(*ret,"tpch22-2");
        load_snippet(*ret,"tpch22-3");
        load_snippet(*ret,"tpch22-4");
        load_snippet(*ret,"tpch22-5");
        load_snippet(*ret,"tpch22-6");
        load_snippet(*ret,"tpch22-7");
        load_snippet(*ret,"tpch22-8");
        load_snippet(*ret,"tpch22-9");
        load_snippet(*ret,"tpch22-10");
        load_snippet(*ret,"tpch22-11");
    } else if (query_str == "test_lineitem"){ 
        load_snippet(*ret,"test_lineitem");
    } else if (query_str == "test_customer"){ 
        load_snippet(*ret,"test_customer");
    } else if (query_str == "test_orders"){ 
        load_snippet(*ret,"test_orders");
    } else if (query_str == "test_part"){ 
        load_snippet(*ret,"test_part");
    } else if (query_str == "test_partsupp"){ 
        load_snippet(*ret,"test_partsupp");
    } else if (query_str == "test_nation"){ 
        load_snippet(*ret,"test_nation");
    } else if (query_str == "test_region"){ 
        load_snippet(*ret,"test_region");
    } else if (query_str == "test_supplier"){ 
        load_snippet(*ret,"test_supplier");
    } else if (query_str == "test_tpch08-0"){ 
        load_snippet(*ret,"test_tpch08-0");
    }
	
    return ret;
}

void read_json(std::string& request,std::string snippet_name){
    request = "";
	std::ifstream openFile("../../../snippets/" + snippet_name + ".json");
	if(openFile.is_open() ){
		std::string line;
		while(getline(openFile, line)){
			request += line;
		}
		openFile.close();
	}
}

void load_snippet(std::list<SnippetRequest> &list,std::string snippet_name){
    SnippetRequest request;

    std::string json_str;
    read_json(json_str, snippet_name);
    google::protobuf::util::JsonParseOptions options;
    options.ignore_unknown_fields = true;
    google::protobuf::util::JsonStringToMessage(json_str, &request, options);


  list.push_back(request);
}