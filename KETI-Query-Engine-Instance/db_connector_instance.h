#pragma once

#include <iostream>
#include "stdafx.h"

// db connect instance include
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
        http_listener m_listener;
        
	CostAnalyzer cost_analyzer_; // 쿼리 점수화 모듈
    QueryPlanner query_planner_;
	PlanExecutor plan_executor_;
	StorageEngineConnector storage_engine_connector_;
    const std::string LOGTAG = "Query Engine";
};


