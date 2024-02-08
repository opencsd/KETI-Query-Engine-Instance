#pragma once

#include "query_log_assistant.h"
#include "db_monitoring_manager.h"

class ParsedQuery {
public:
    ParsedQuery(std::string query){
        ori_query_ = query;
        execution_mode_ = EXECUTION_MODE::GENERIC;
        query_type_ = QUERY_TYPE::SELECT;//구분필요!!!!
    }
    
    bool isGenericQuery(){
        return (execution_mode_ == EXECUTION_MODE::GENERIC) ? true : false;
    }

    void SetQueryTypeAsOffloading(){
        execution_mode_ = EXECUTION_MODE::OFFLOADING;
    }

    std::string GetParsedQuery(){
        return parsed_query_;
    }
    void SetParsedQuery(std::string parsed_query){
        parsed_query_ = parsed_query;
    }
    std::string GetOriginalQuery(){
        return ori_query_;
    }
    EXECUTION_MODE GetExecutionMode(){
        return execution_mode_;
    }
    QUERY_TYPE GetQueryType(){
        return query_type_;
    }

private:
    std::string ori_query_;
    std::string parsed_query_;
    EXECUTION_MODE execution_mode_;
    QUERY_TYPE query_type_;
};