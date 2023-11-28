#pragma once

#include <iostream>
#include "stdafx.h"

// db connect instance include
#include "query_planner.h"
#include "meta_data_manager.h"
#include "plan_executor.h"
#include "storage_engine_interface.h"
#include "parsed_query.h"

using namespace std;
using namespace web;
using namespace http;
using namespace utility;
using namespace http::experimental::listener;


class DB_Connector_Instance
{
    public:
        DB_Connector_Instance();
        DB_Connector_Instance(utility::string_t url);
        virtual ~DB_Connector_Instance();

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
        
	Query_Planner query_planner_;
	Meta_Data_Manager meta_data_manager_;
	Plan_Executer plan_executer_;
	Storage_Engine_Interface storageEngineInterface_;
};


