#pragma once
#include "storage_engine_interface.h"
#include "parsed_query.h"
#include <list>

class Plan_Executer {
public:
	Plan_Executer(){
        Set_Query_ID();
    }
    int Get_Query_ID(){
        return ++Query_ID;
    }
    std::string Execute_Query(Storage_Engine_Interface &storageEngineInterface,Parsed_Query &parsed_query, Query_Log &query_log);
private:
    std::mutex gRPC_mutex;
    std::mutex mutex;
    int Query_ID;
    void Set_Query_ID();
    std::unique_ptr<std::list<SnippetRequest>> Gen_Snippet(Parsed_Query &parsed_query);
    void Send_Snippets(Storage_Engine_Interface &storageEngineInterface,std::list<SnippetRequest> &snippet_list, int query_id);
    Result Get_Query_Result(Storage_Engine_Interface &storageEngineInterface,int query_id);
};

