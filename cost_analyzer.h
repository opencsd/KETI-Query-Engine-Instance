#pragma once
#include "storage_engine_connector.h"
#include "parsed_query.h"
#include <list>

class CostAnalyzer {
public:
	CostAnalyzer(){
        // setQueryID();
    }
    int GetQueryID(){
        // std::lock_guard<std::mutex> lock(mutex);
        // return ++queryID_;
    }
    int Query_Scoring(const std::string& query) {
        // 쿼리 점수화 함수 추가

        return query.length();
    }
private:
    // std::mutex gRPC_mutex;
    // std::mutex mutex;
    // int queryID_;
    // void setQueryID();
    // std::unique_ptr<std::list<SnippetRequest>> genSnippet(ParsedQuery &parsed_query);
    // QueryStringResult queryOffload(StorageEngineConnector &storageEngineInterface,std::list<SnippetRequest> &snippet_list, int query_id);
    // Response querySend(StorageEngineConnector &storageEngineInterface, std::string query, std::string dbName);
    const std::string LOGTAG = "Query Engine::Cost Analyzer";
};

