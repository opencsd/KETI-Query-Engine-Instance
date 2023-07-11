#pragma once
#include "Storage_Engine_Interface.h"
#include "Parsed_Query.h"
#include <list>

class Plan_Executer {
public:
	Plan_Executer( ){
        Query_ID = 0;
    }
    std::string Execute_Query(Storage_Engine_Interface &storageEngineInterface,Parsed_Query &parsed_query);
private:
    std::mutex gRPC_mutex;
    std::mutex mutex;
    int Query_ID;
    int Set_Query_ID();
    std::unique_ptr<std::list<SnippetRequest>> Gen_Snippet(Parsed_Query &parsed_query);
    void Send_Snippets(Storage_Engine_Interface &storageEngineInterface,std::list<SnippetRequest> &snippet_list, int query_id);
    std::string Get_Query_Result(Storage_Engine_Interface &storageEngineInterface,int query_id);
};