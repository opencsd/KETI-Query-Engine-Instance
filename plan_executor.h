#pragma once
#include <list>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <sql.h>
#include <sqlext.h>
#include <regex>
#include <rapidjson/error/en.h>

#include "storage_engine_connector.h"
#include "parsed_query.h"
#include "snippet.h"
#include "keti_log.h"
#include "query_log_assistant.h"

class PlanExecutor {
public:
	PlanExecutor(){
        string temp = INSTANCE_NAME;
        std::replace(temp.begin(), temp.end(), '-', '_');
        instance_name_ = temp;
        setQueryID();
    }
    int GetQueryID(){
        std::lock_guard<std::mutex> lock(mutex);
        return queryID_++;
        // return 52;
    }

    std::string ExecuteQuery(StorageEngineConnector &storageEngineInterface, ParsedQuery &parsed_query, const string &db_name, QueryLog &query_log);
    void create_snippet_init_info(const string &db_name, ParsedCustomQuery &parsed_custom_query, vector<Snippet> &snippets); 
    void generate_snippet_json(vector<Snippet> &snippets, const string &db_name, vector<string> &tpch_scan_snippet_name);
    void setFilterToSnippet(const ParsedCustomQuery& parsed_custom_query, const QueryTable &query_table, Snippet& snippet);
    void setProjectionToSnippet(const string &db_name, const ParsedCustomQuery& parsed_custom_query, const QueryTable &query_table, Snippet& snippet);
    void setGroupByToSnippet(const ParsedCustomQuery& parsed_custom_query, Snippet& snippet);
    void setOrderByToSnippet(const ParsedCustomQuery& parsed_custom_query, Snippet& snippet);
    void setLimitToSnippet(const ParsedCustomQuery& parsed_custom_query, Snippet& snippet);
    Snippet parsing_tpch_snippet(const string &file_name, const std::string &db_name);

private:
    string instance_name_;
    std::mutex gRPC_mutex;
    std::mutex mutex;
    vector<Snippet> snippets;
    int queryID_ = 0;
    std::string db_name_;
    void setQueryID();
    std::unique_ptr<std::list<SnippetRequest>> genSnippet(ParsedQuery &parsed_query, const string &db_name);
    QueryStringResult queryOffload(StorageEngineConnector &storageEngineInterface,std::list<SnippetRequest> &snippet_list, int query_id);
    const std::string LOGTAG = "Query Engine::Plan Executor";
    void trim(std::string& s);
};

