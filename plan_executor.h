#pragma once
#include "storage_engine_connector.h"
#include "parsed_query.h"
#include <list>

class PlanExecutor {
public:
	PlanExecutor(){
        setQueryID();
    }
    int GetQueryID(){
        std::lock_guard<std::mutex> lock(mutex);
        return ++queryID_;
    }
    std::string ExecuteQuery(StorageEngineConnector &storageEngineInterface, ParsedQuery &parsed_query, QueryLog &query_log);
private:
    std::mutex gRPC_mutex;
    std::mutex mutex;
    int queryID_;
    void setQueryID();
    std::unique_ptr<std::list<SnippetRequest>> genSnippet(ParsedQuery &parsed_query);
    QueryStringResult queryOffload(StorageEngineConnector &storageEngineInterface,std::list<SnippetRequest> &snippet_list, int query_id);
};

