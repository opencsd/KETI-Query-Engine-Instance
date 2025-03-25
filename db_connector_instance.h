#pragma once

#include <iostream>
#include "stdafx.h"
#include "query_planner.h"
#include "plan_executor.h"
#include "cost_analyzer.h"

using namespace std;
using namespace web;
using namespace http;
using namespace utility;
using namespace http::experimental::listener;

class DBConnectorInstance
{
    public:
        DBConnectorInstance();
        DBConnectorInstance(utility::string_t url, string storage_engine_address,grpc::ChannelArguments channel_args);
        virtual ~DBConnectorInstance();

        pplx::task<void>open(){return m_listener.open();}
        pplx::task<void>close(){return m_listener.close();}

    protected:

    private:
        void handle_get(http_request message);
        void handle_put(http_request message);
        void handle_post(http_request message);
        void handle_delete(http_request message);
        void handle_error(pplx::task<void>& t);
        void handle_options(http_request message);
        void add_cors_headers(http_response response);
        http_listener m_listener;
        
	CostAnalyzer cost_analyzer_; // 쿼리 점수화 모듈
    QueryPlanner query_planner_;
	PlanExecutor plan_executor_;
	StorageEngineConnector storage_engine_connector_;
    const std::string LOGTAG = "Query Engine";
};

inline string formatTable(sql::ResultSet* res) {
    if (!res) return "No data.\n";

    map<string, size_t> max_col_width;
    vector<string> column_names;

    sql::ResultSetMetaData* meta = res->getMetaData();
    int column_count = meta->getColumnCount();

    for (int i = 1; i <= column_count; i++) {
        string col_name = meta->getColumnLabel(i);
        column_names.push_back(col_name);
        max_col_width[col_name] = col_name.length();
    }

    vector<vector<string>> table_data;
    while (res->next()) {
        vector<string> row_data;
        for (int i = 1; i <= column_count; i++) {
            string cell_value = res->getString(i);
            row_data.push_back(cell_value);
            max_col_width[column_names[i - 1]] = max(max_col_width[column_names[i - 1]], cell_value.length());
        }
        table_data.push_back(row_data);
    }

    ostringstream table;
    
    table << "+";
    for (const auto& col_name : column_names) {
        table << string(max_col_width[col_name] + 2, '-') << "+";
    }
    table << "\n|";

    for (const auto& col_name : column_names) {
        table << " " << setw(max_col_width[col_name]) << left << col_name << " |";
    }
    table << "\n+";
    for (const auto& col_name : column_names) {
        table << string(max_col_width[col_name] + 2, '-') << "+";
    }
    table << "\n";

    for (const auto& row : table_data) {
        table << "|";
        for (size_t i = 0; i < row.size(); i++) {
            table << " " << setw(max_col_width[column_names[i]]) << left << row[i] << " |";
        }
        table << "\n";
    }

    table << "+";
    for (const auto& col_name : column_names) {
        table << string(max_col_width[col_name] + 2, '-') << "+";
    }
    table << "\n";

    return table.str();
}

