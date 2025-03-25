#pragma once

#include <regex>
#include <unordered_map>
#include <algorithm>
#include <iostream>
#include <string.h>
#include <sstream>
#include <stdio.h>
#include <vector>

#include "snippet_sample.grpc.pb.h"
#include "keti_type.h"
#include "db_monitoring_manager.h"
#include "snippet.h"

using namespace std;

struct JoinCondition {
    std::string left_table_name = "";     
    std::string left_table_alias = "";     
    std::string left_column = "";    
    std::string right_table_name = "";
    std::string right_table_alias = "";     
    std::string right_column = "";  

    // setJoinCondition 함수 정의
    void setJoinCondition(const std::string& l_table_name = "",
                          const std::string& l_table_alias = "",
                          const std::string& l_column = "",
                          const std::string& r_table_name = "",
                          const std::string& r_table_alias = "",
                          const std::string& r_column = "") {
        left_table_name = l_table_name;
        left_table_alias = l_table_alias;
        left_column = l_column;
        right_table_name = r_table_name;
        right_table_alias = r_table_alias;
        right_column = r_column;
    }
    
};

struct OperValue{
    int value_type;
    std::string table_name = "";
    std::string table_alias = "";
    std::string value = "";

    void setOperValue(int value_type = static_cast<int>(ValueType::COLUMN),
                    const std::string& table_name = "",
                    const std::string& table_alias = "",
                    const std::string& value = "") {
    this->value_type = value_type;
    this->table_name = table_name;
    this->table_alias = table_alias;
    this->value = value;
    }
};
struct WhereCondition{
    std::vector<OperValue> left_values;
    std::string op;
    std::vector<OperValue> right_values;
};
struct QueryTable{ //쿼리 분석해서 나온 테이블 스니펫 당 하나씩생성
    int query_type;
    std::string table_name1 ="";
    std::string table_name2 ="";
    std::string table_alias1 ="";
    std::string table_alias2 ="";
    std::unordered_set<std::string> select_columns; //사용 projection
    std::vector<WhereCondition> where_conditions;
    JoinCondition join_condition;
    std::vector<std::string> having_conditions;
    std::string result_table_alias = ""; // result_info, table 
    

};
struct ParsedCustomQuery {
    std::string db_name;
    std::map<std::string, QueryTable> query_tables; //key : snippet n, value :  query table
    std::map<std::string, std::string> from_table_map ; //key : table alias, value : table name
    std::map<std::string, std::string> column_table_map; // key : column, value: table name //컬럼이 어디 테이블에 있는지 
    std::vector<std::string> total_where_clause;
    std::unordered_set<std::string> result_columns; //select 마지막에 사용하는 컬럼명 저장
    std::map<std::string, std::string> result_columns_columns_alias_map; // key : colum, value: column_alias; 결과 컬럼 
    std::map<std::string, std::string> aggregation_column_map; //key : 집계함수 , value : column
    std::unordered_set<std::string> group_by_fields; 
    std::map<string,bool> order_by_fields; // key : column, value : asc, desc 
    int join_count = 0; 
    Limit limit; //limit
    bool is_parsing_custom_query = true;
    int logical = -1;
    
};

class ParsedQuery {
public:
    ParsedQuery(std::string query){
        ori_query_ = query;
        execution_mode_ = EXECUTION_MODE::GENERIC;
        query_type_ = QUERY_TYPE::SELECT;//구분필요!!!!
    }

    bool isGenericQuery() const {
        return execution_mode_ == EXECUTION_MODE::GENERIC;
    }

    void SetQueryTypeAsOffloading() {
        execution_mode_ = EXECUTION_MODE::OFFLOADING;
    }

    std::string GetParsedQuery() const {
        return parsed_query_;
    }

    std::string GetDBName() const {
        return parsed_custom_query_.db_name;
    }

    void SetParsedQuery(const std::string& parsed_query) {
        parsed_query_ = parsed_query;
    }

    std::string GetOriginalQuery() const {
        return ori_query_;
    }

    EXECUTION_MODE GetExecutionMode() const {
        return execution_mode_;
    }

    QUERY_TYPE GetQueryType() const {
        return query_type_;
    }

    void SetColumnAlias(const std::vector<std::string>& column_alias) {
        column_alias_ = column_alias;
    }

    const std::vector<std::string>& GetColumnAlias() const {
        return column_alias_;
    }

    void SetCustomParsedQuery(const std::string& parsed_query,const std::string &db_name) {
        join_conditions.clear();
        join_columns.clear();
        parsed_custom_query_.db_name = db_name;
        parseFromClause(parsed_query); 
        parseWhereConditions(parsed_query);
        parseSelectFields(parsed_query);
        parseGroupByFields(parsed_query);        
        parseOrderByFields(parsed_query);
        parseLimitClause(parsed_query);
             
    }

    
    ParsedCustomQuery GetParsedCustomQuery() const {
        KETILOG::DEBUGLOG(LOGTAG, "Parse Custom Query");
        KETILOG::DEBUGLOG(LOGTAG, "쿼리 테이블 수: " + std::to_string(parsed_custom_query_.query_tables.size()));
        KETILOG::DEBUGLOG(LOGTAG, "사용한 논리 연산자: " + parsed_custom_query_.logical);

        for (const auto& table_entry : parsed_custom_query_.query_tables) {
            KETILOG::DEBUGLOG(LOGTAG, "-----------Snippet Name---------- " + table_entry.first);
            const QueryTable& query_table = table_entry.second;

            KETILOG::DEBUGLOG(LOGTAG, "Query Type: " + std::to_string(query_table.query_type));
            KETILOG::DEBUGLOG(LOGTAG, "Table Names: " + query_table.table_name1 + " " + query_table.table_name2);
            KETILOG::DEBUGLOG(LOGTAG, "Table Aliases: " + query_table.table_alias1 + " " + query_table.table_alias2);

            std::string select_columns_log = "Select Columns: ";
            for (const auto& entry : query_table.select_columns) {
                select_columns_log += entry + " ";
            }
            KETILOG::DEBUGLOG(LOGTAG, select_columns_log);

            std::string aggregation_log = "Aggregation: ";
            for (const auto& entry : parsed_custom_query_.aggregation_column_map) {
                aggregation_log += entry.first + " " + entry.second + " ";
            }
            KETILOG::DEBUGLOG(LOGTAG, aggregation_log);

            KETILOG::DEBUGLOG(LOGTAG, "Where Conditions:");
            for (const auto& condition : query_table.where_conditions) {
                KETILOG::DEBUGLOG(LOGTAG,
                    "  Left Value: " +
                    std::to_string(condition.left_values.at(0).value_type) +
                    ", Table: " + condition.left_values.at(0).table_name +
                    ", Alias: " + condition.left_values.at(0).table_alias +
                    ", Value: " + condition.left_values.at(0).value);

                KETILOG::DEBUGLOG(LOGTAG, "  Operator: " + condition.op);

                std::string right_values_log = "  Right Values: ";
                for (const auto& iter : condition.right_values) {
                    right_values_log += iter.value + " ";
                }
                KETILOG::DEBUGLOG(LOGTAG, right_values_log);
            }

            KETILOG::DEBUGLOG(LOGTAG, "Join Condition:");
            KETILOG::DEBUGLOG(LOGTAG,
                "  <Left> Table: " + query_table.join_condition.left_table_name +
                ", Alias: " + query_table.join_condition.left_table_alias +
                ", Column: " + query_table.join_condition.left_column);
            KETILOG::DEBUGLOG(LOGTAG,
                "  <Right> Table: " + query_table.join_condition.right_table_name +
                ", Alias: " + query_table.join_condition.right_table_alias +
                ", Column: " + query_table.join_condition.right_column);

            std::string group_by_log = "Group By Fields: ";
            for (const auto& entry : parsed_custom_query_.group_by_fields) {
                group_by_log += entry + " ";
            }
            KETILOG::DEBUGLOG(LOGTAG, group_by_log);

            KETILOG::DEBUGLOG(LOGTAG, "Order By Fields:");
            for (const auto& entry : parsed_custom_query_.order_by_fields) {
                KETILOG::DEBUGLOG(LOGTAG,
                    "  Column: " + entry.first + ", Order: " + (entry.second ? "ASC" : "DESC"));
            }

            KETILOG::DEBUGLOG(LOGTAG, "Limit Offset: " + std::to_string(parsed_custom_query_.limit.offset));
            KETILOG::DEBUGLOG(LOGTAG, "Limit Length: " + std::to_string(parsed_custom_query_.limit.length));

            KETILOG::DEBUGLOG(LOGTAG, "Result Table Alias: " + query_table.result_table_alias);

            std::string result_columns_log = "Result Columns: ";
            for (const auto& entry : parsed_custom_query_.result_columns) {
                result_columns_log += entry + " ";
            }
            KETILOG::DEBUGLOG(LOGTAG, result_columns_log);

            std::string result_columns_alias_log = "Result Columns Aliases: ";
            for (const auto& entry : parsed_custom_query_.result_columns_columns_alias_map) {
                result_columns_alias_log += entry.second + " ";
            }
            KETILOG::DEBUGLOG(LOGTAG, result_columns_alias_log);

            KETILOG::DEBUGLOG(LOGTAG, "");
        }

        return parsed_custom_query_;
    }

  
private:
    const std::string LOGTAG = "Query Engine::Parsed Query";
    std::vector <JoinCondition> join_conditions;
    std::unordered_set<std::string> join_columns;
    std::map<std::string, std::vector<std::string>> meta_table_columns_map; // key - table_name, value - columns 
    bool field_found = false;
    int scan_snippet_i = 0;
    int join_snippet_i = 0;
    int last_order_snippet_i = 0;
    int join_type;

    bool is_aggregation = false;
    int pivot1 = 0, pivot2 = 1;
    std::vector<std::pair<std::string, std::string>> table_priority;
    std::vector <std::string> filtered_columns;                                                                     

    std::string ori_query_;
    std::string parsed_query_;
    std::vector<std::string> column_alias_;
    EXECUTION_MODE execution_mode_;
    QUERY_TYPE query_type_;
    ParsedCustomQuery parsed_custom_query_;
    
    
    void parseSelectFields(const std::string& query) { // COUNT(c.c_custkey) AS c_custkey_count, COUNT(*) AS total_count, COUNT(DISTINCT c_name), MIN(c_acctbal)
        std::regex select_regex(R"(SELECT\s+(.*?)\s+(FROM|WHERE|GROUP BY|ORDER BY|LIMIT))", std::regex::icase);
        std::smatch match;
        is_aggregation = false;
        field_found = false;
        std::string table_alias;
        std::string table_name ;
        std::string column_name;
        if (std::regex_search(query, match, select_regex)) {
            std::istringstream fields(match[1].str());
            std::string field;

            while (std::getline(fields, field, ',')) {
                trim(field);

                if (field == "*") { // 선택한 테이블 전체 컬럼 저장
                    if(parsed_custom_query_.from_table_map.size()!=1){
                        KETILOG::ERRORLOG(LOGTAG, "COUNT(*) 여러개 테이블 지원 안함!");
                        parsed_custom_query_.is_parsing_custom_query = false;
                        return;
                    }
                    for (auto& table_entry : parsed_custom_query_.from_table_map) {
                        std::string table_name = table_entry.second;
                        int snippet_i = 0;
                        std::string snippet_name = "snippet-" + to_string(snippet_i);
                        const auto& columns = meta_table_columns_map[table_name];

                        
                        for (const auto& column : columns) {
                            parsed_custom_query_.query_tables[snippet_name].select_columns.insert(column);
                        }
                    }
                } else {
                    // AS 기준으로 파싱
                    std::regex alias_regex(R"((.*?)\s+AS\s+(\w+))", std::regex::icase);
                    std::smatch alias_match;
                    std::string column_alias;

                    if (std::regex_search(field, alias_match, alias_regex)) {
                        field = alias_match[1].str();
                        column_alias = alias_match[2].str();
                        trim(field); 
                        trim(column_alias);
                        
                    } else {
                        column_alias = field;
                    }
                    parsed_custom_query_.result_columns_columns_alias_map[field] = column_alias;
                    // 집계 함수 및 CASE WHEN 구문 탐지
                    std::regex case_when_regex(R"(CASE\s+WHEN\s+.*?\s+THEN\s+.*?\s+END)", std::regex::icase);
                    std::regex agg_func_regex(R"((SUM|AVG|COUNT|MIN|MAX)\s*\(\s*(.*?)\s*\))", std::regex::icase);

                    std::smatch agg_match;
                    
                    // CASE WHEN 구문을 먼저 검사
                    if (std::regex_search(field, agg_match, case_when_regex)) {
                        KETILOG::ERRORLOG(LOGTAG, "CASE 지원 안함!");
                        parsed_custom_query_.is_parsing_custom_query = false;
                        return;
                    } 
                    
                    if (std::regex_search(field, agg_match, agg_func_regex)) {
                        is_aggregation = true;
                        std::string func_name = agg_match[1].str();  // 집계 함수 이름
                        std::string inner_value = agg_match[2].str(); // 괄호 안의 값
                        trim(inner_value);
                        
                        if (func_name == "COUNT" || func_name == "count") {
                            if (inner_value == "*") {
                                parsed_custom_query_.aggregation_column_map[field] = inner_value;
                                for(auto &entry : parsed_custom_query_.column_table_map ){
                                    column_name = entry.first;
                                    parsed_custom_query_.result_columns.insert(column_name);
                                    parsed_custom_query_.aggregation_column_map[field] = column_name;
                                    break; 
                                }

                            } else if (inner_value.find("DISTINCT") == 0 || inner_value.find("distinct") == 0) {  // DISTINCT가 포함된 경우
                                inner_value = inner_value.substr(9);  // "DISTINCT " 뒤의 문자열 추출
                                trim(inner_value);
                                std::string temp_column = inner_value;
                                
                                size_t dot_pos = temp_column.find('.');
                                if (dot_pos != std::string::npos) {
                                    table_alias = temp_column.substr(0, dot_pos);
                                    table_name = parsed_custom_query_.from_table_map[table_alias];                      
                                    column_name = temp_column.substr(dot_pos + 1);
                                } else {
                                    column_name = temp_column;
                                    table_name = parsed_custom_query_.column_table_map[column_name];
                                    table_alias = parsed_custom_query_.column_table_map[column_name];
                                }

                                if(parsed_custom_query_.column_table_map.find(column_name) != parsed_custom_query_.column_table_map.end()){
                                    KETILOG::DEBUGLOG(LOGTAG, column_name + " 은 컬럼");
                                    
                                }else{
                                    KETILOG::ERRORLOG(LOGTAG, "SELECT 컬럼이 아니라 지원 안함!");
                                    parsed_custom_query_.is_parsing_custom_query = false;
                                }
                                KETILOG::DEBUGLOG(LOGTAG, "DISTINCT " + column_name);
                                
                                parsed_custom_query_.result_columns.insert(column_name);  
                                field = "COUNT(DISTINCT)";
                                parsed_custom_query_.aggregation_column_map[field] = column_name;
                                
                            } else { // COUNT 외 집계 함수
                                std::string temp_column = inner_value;
                                
                                size_t dot_pos = temp_column.find('.');
                                if (dot_pos != std::string::npos) {
                                    table_alias = temp_column.substr(0, dot_pos);
                                    table_name = parsed_custom_query_.from_table_map[table_alias];
                                    
                                    column_name = temp_column.substr(dot_pos + 1);
                                } else {
                                    column_name = temp_column;
                                    table_name = parsed_custom_query_.column_table_map[column_name];
                                    table_alias = parsed_custom_query_.column_table_map[column_name];
                                }
                                if(parsed_custom_query_.column_table_map.find(column_name) != parsed_custom_query_.column_table_map.end()){
                                    KETILOG::DEBUGLOG(LOGTAG, column_name + " 은 컬럼");
                                    
                                }else{
                                    KETILOG::ERRORLOG(LOGTAG, "SELECT 컬럼이 아니라 지원 안함!");
                                    parsed_custom_query_.is_parsing_custom_query = false;
                                }
                                KETILOG::DEBUGLOG(LOGTAG, "COUNT column_name " +  column_name);
                                field = "COUNT";
                                parsed_custom_query_.aggregation_column_map[field] = column_name;
                                parsed_custom_query_.result_columns.insert(column_name);
                                
                            }
                            
                        } else { // 카운트가 아닌 다른 집계함수 처리
                        
                            std::string temp_column = inner_value;
                            
                            size_t dot_pos = temp_column.find('.');
                            if (dot_pos != std::string::npos) {
                                table_alias = temp_column.substr(0, dot_pos);
                                table_name = parsed_custom_query_.from_table_map[table_alias];
                                
                                column_name = temp_column.substr(dot_pos + 1);
                            } else {
                                column_name = temp_column;
                                table_name = parsed_custom_query_.column_table_map[column_name];
                                table_alias = parsed_custom_query_.column_table_map[column_name];
                            }
                            if(parsed_custom_query_.column_table_map.find(column_name) != parsed_custom_query_.column_table_map.end()){
                                KETILOG::DEBUGLOG(LOGTAG, column_name + " 은 컬럼");
                                
                            }else{
                                KETILOG::ERRORLOG(LOGTAG, "SELECT 컬럼이 아니라 지원 안함!");
                                parsed_custom_query_.is_parsing_custom_query = false;
                            }
                            KETILOG::DEBUGLOG(LOGTAG, "집계함수 column_name " +  column_name);
                            parsed_custom_query_.aggregation_column_map[func_name] = column_name;
                            parsed_custom_query_.result_columns.insert(column_name);
                            
                        }
                        
                    } else { // 일반 SELECT 필드 처리-> aggregation 스니펫이 없는경우
                        
                        std::string temp_column = field;
                        size_t dot_pos = temp_column.find('.');
                        if (dot_pos != std::string::npos) {
                            table_alias = temp_column.substr(0, dot_pos);
                            table_name = parsed_custom_query_.from_table_map[table_alias];
                            
                            column_name = temp_column.substr(dot_pos + 1);
                        } else {
                            column_name = temp_column;
                            table_name = parsed_custom_query_.column_table_map[column_name];
                            table_alias = parsed_custom_query_.column_table_map[column_name];
                        }

                        
                        parsed_custom_query_.result_columns.insert(column_name);
                    }
                }
                    
            }
            //scan이랑 join채우기
            for(auto &query_table_entry : parsed_custom_query_.query_tables){ 
                std::string snippet_name = query_table_entry.first;
                
                if(query_table_entry.second.table_name2 == ""){
                    std::string scan_table_name = parsed_custom_query_.query_tables[snippet_name].table_name1;
                    KETILOG::DEBUGLOG(LOGTAG, "scan 스니펫" + snippet_name +" 테이블 이름 " + scan_table_name);

                    for(auto &col :  parsed_custom_query_.result_columns){ //result columns alias 에서
                        string temp_col_table_name = parsed_custom_query_.column_table_map[col]; // col이 어디 테이블에 있는지  
                        if(scan_table_name == temp_col_table_name){
                            // cout << col << " ";
                            parsed_custom_query_.query_tables[snippet_name].select_columns.insert(col);
                        }
                        
                    }
                }else{
                    std::string snippet_name1 = parsed_custom_query_.query_tables[snippet_name].table_name1; 
                    std::string snippet_name2 = parsed_custom_query_.query_tables[snippet_name].table_name2; 

                    //set에 추가하고
                    std::string table_name1 = parsed_custom_query_.query_tables[snippet_name1].table_name1;  
                    std::string table_name2 = parsed_custom_query_.query_tables[snippet_name2].table_name1;

                    std::string table_alias1 = parsed_custom_query_.query_tables[snippet_name1].table_alias1;  
                    std::string table_alias2 = parsed_custom_query_.query_tables[snippet_name2].table_alias1; 
                    
                    
                    auto is_table1 = parsed_custom_query_.from_table_map.find(table_alias1);
                    auto is_table2 = parsed_custom_query_.from_table_map.find(table_alias2);

                    if(is_table1 != parsed_custom_query_.from_table_map.end() && 
                            is_table2 != parsed_custom_query_.from_table_map.end()){ //양쪽 다 테이블 스캔일때 (조인 처음 시작) 
                        KETILOG::DEBUGLOG(LOGTAG, "양쪽 Scan 테이블 스니펫" + table_name1 +" , " + table_name2);

                        for(auto &result_col_iter: parsed_custom_query_.result_columns){ //select 결과에 추가 
                            if(table_name1 == parsed_custom_query_.column_table_map[result_col_iter]){ 
                                parsed_custom_query_.query_tables[snippet_name].select_columns.insert(result_col_iter);
                                
                            }
                        }
                        for(auto &result_col_iter: parsed_custom_query_.result_columns){
                            if(table_name2 == parsed_custom_query_.column_table_map[result_col_iter]){
                                parsed_custom_query_.query_tables[snippet_name].select_columns.insert(result_col_iter);
                                
                            }
                        }
                    }else if(is_table1 == parsed_custom_query_.from_table_map.end() && 
                            is_table2 != parsed_custom_query_.from_table_map.end()){  //왼쪽이 조인 스니펫, 오른쪽이 스캔 스니펫일 경우
                        KETILOG::DEBUGLOG(LOGTAG, "왼쪽이 Join 오른쪽이 Scan 테이블 스니펫, 오른쪽 스캔 스니펫 테이블 이름 " + table_name2);

                        for(auto &col : parsed_custom_query_.query_tables[snippet_name1].select_columns){ //snippet 결과 컬럼 가져오고 
                            parsed_custom_query_.query_tables[snippet_name].select_columns.insert(col);
                           
                        }

                        //조인할때 쓴 컬럼 지우기 
                        JoinCondition join_condition = parsed_custom_query_.query_tables[snippet_name].join_condition;
                        auto join_col = parsed_custom_query_.query_tables[snippet_name].select_columns.find(join_condition.left_column);
                        if(join_col != parsed_custom_query_.query_tables[snippet_name].select_columns.end()){
                            
                            parsed_custom_query_.query_tables[snippet_name].select_columns.erase(join_condition.left_column);
                        }
                        join_col = parsed_custom_query_.query_tables[snippet_name].select_columns.find(join_condition.right_column);
                        if(join_col != parsed_custom_query_.query_tables[snippet_name].select_columns.end()){
                            
                            parsed_custom_query_.query_tables[snippet_name].select_columns.erase(join_condition.right_column);
                        }

                        for(auto &result_col_iter: parsed_custom_query_.result_columns){
                            if(table_name2 == parsed_custom_query_.column_table_map[result_col_iter]){
                                parsed_custom_query_.query_tables[snippet_name].select_columns.insert(result_col_iter);
                                

                            }

                        }

                        //join할때도 쓰고 select 할때도 씀 
                        for(auto &result_col_iter: parsed_custom_query_.result_columns){ 
                            if(join_condition.left_table_name == parsed_custom_query_.column_table_map[result_col_iter]){ 
                                
                                parsed_custom_query_.query_tables[snippet_name].select_columns.insert(result_col_iter);
                                
                            }
                            if(join_condition.right_table_name == parsed_custom_query_.column_table_map[result_col_iter]){ 
                                
                                parsed_custom_query_.query_tables[snippet_name].select_columns.insert(result_col_iter);
                                
                            }
                        }
                    }else if(is_table1 != parsed_custom_query_.from_table_map.end() && 
                                is_table2 == parsed_custom_query_.from_table_map.end()){// 왼쪽이 스캔 스니펫, 오른쪽이 조인 스니펫
                        KETILOG::DEBUGLOG(LOGTAG, "왼쪽이 Scan 오른쪽이 Join 테이블 스니펫, 왼쪽 스캔 스니펫 테이블 이름 " + table_name1);

                        for(auto &result_col_iter: parsed_custom_query_.result_columns){
                            if(table_name1 == parsed_custom_query_.column_table_map[result_col_iter]){
                                parsed_custom_query_.query_tables[snippet_name].select_columns.insert(result_col_iter);
                                
                            }
                        }
                        for(auto &col : parsed_custom_query_.query_tables[snippet_name2].select_columns){
                            parsed_custom_query_.query_tables[snippet_name].select_columns.insert(col);

                        }

                        //조인할때 쓴 컬럼 지우기 
                        JoinCondition join_condition = parsed_custom_query_.query_tables[snippet_name].join_condition;
                        auto join_col = parsed_custom_query_.query_tables[snippet_name].select_columns.find(join_condition.left_column);
                        if(join_col != parsed_custom_query_.query_tables[snippet_name].select_columns.end()){
                            
                            parsed_custom_query_.query_tables[snippet_name].select_columns.erase(join_condition.left_column);
                        }
                        join_col = parsed_custom_query_.query_tables[snippet_name].select_columns.find(join_condition.right_column);
                        if(join_col != parsed_custom_query_.query_tables[snippet_name].select_columns.end()){
                            
                            parsed_custom_query_.query_tables[snippet_name].select_columns.erase(join_condition.right_column);
                        }

                        //join할때도 쓰고 select 할때도 씀 
                        for(auto &result_col_iter: parsed_custom_query_.result_columns){ 
                            if(join_condition.left_table_name == parsed_custom_query_.column_table_map[result_col_iter]){ 
                                
                                parsed_custom_query_.query_tables[snippet_name].select_columns.insert(result_col_iter);
                                
                            }
                            if(join_condition.right_table_name == parsed_custom_query_.column_table_map[result_col_iter]){ 
                                
                                parsed_custom_query_.query_tables[snippet_name].select_columns.insert(result_col_iter);
                                
                            }
                        }
                    }else{  
                        KETILOG::ERRORLOG(LOGTAG, "양쪽 다 스니펫 이름, 지원 안함! " + parsed_custom_query_.query_tables[snippet_name].table_name1 + " " + parsed_custom_query_.query_tables[snippet_name].table_name2);

                        parsed_custom_query_.is_parsing_custom_query = false;
                    }
                    
                }
                
                
            }
                 
            if(is_aggregation == true){ 
                KETILOG::DEBUGLOG(LOGTAG, "집계함수 스니펫 생성");
                QueryTable query_table;
                query_table.query_type = QueryType::AGGREGATION;
                int aggregation_snippet_i = parsed_custom_query_.query_tables.size();
                std::string snippet_name = "snippet-" + to_string(aggregation_snippet_i);
                query_table.table_name1 = "snippet-" + to_string(aggregation_snippet_i-1);
                
                query_table.result_table_alias = snippet_name;
                for(auto&i : parsed_custom_query_.result_columns){
                    // cout<< i << " ";
                    query_table.select_columns.insert(i);
                        
                }
                
                parsed_custom_query_.query_tables[snippet_name] = query_table;
            }
        } else {
            KETILOG::ERRORLOG(LOGTAG, "Query Parsing Failed in Select");
            parsed_custom_query_.is_parsing_custom_query = false;
        }
    }


    void parseFromClause(const std::string& query) {
        std::regex from_regex(R"(FROM\s+(.+?)(?=\s+JOIN|\s+LEFT|\s+RIGHT|\s+CROSS|\s+WHERE|\s+GROUP BY|\s+ORDER BY|\s+LIMIT|;|$))", std::regex::icase);
        std::smatch match;
        std::vector<std::string>meta_table_priority = MetaDataManager::GetInstance().GetTablePriority(parsed_custom_query_.db_name);
        std::vector<std::string>tables ;
        std::map<std::string, std::string>table_map;
        std::string table_name, table_alias;
        if (std::regex_search(query, match, from_regex)) {
            std::string from_clause = match[1].str();
            std::istringstream tables(from_clause);
            std::string table;

            std::regex table_alias_regex(R"(\s*([a-zA-Z_][\w]*)\s*(?:AS\s+)?([a-zA-Z_][\w]*)?\s*)", std::regex::icase);
        
            while (std::getline(tables, table, ',')) {
                trim(table);
                std::smatch table_match;
                
                if (std::regex_match(table, table_match, table_alias_regex)) {
                    table_name = table_match[1].str();
                    table_alias = table_match[2].matched ? table_match[2].str() : table_name;
                    
                    table_map[table_name] = table_alias; 
                    
                    parsed_custom_query_.from_table_map[table_alias] = table_name;
                    
                }
            }
            


                                /* ***Print From*** */
            /*{
                cout << "parseFromClause: " << endl;
                for (size_t i = 0; i < parsed_custom_query_.tables.size(); ++i) {
                    cout << "Table: " << parsed_custom_query_.tables[i] << ", Alias: " << parsed_custom_query_.tables_alias[i] << endl;
                }
            }*/
        } else {
            KETILOG::ERRORLOG(LOGTAG, "From Clause does not have a Table " );
            parsed_custom_query_.is_parsing_custom_query = false;
        }


        // JOIN 유형 및 테이블, 조건을 포함한 정규식
        std::regex join_regex(
            R"((\b(LEFT OUTER|RIGHT OUTER|FULL OUTER|INNER|CROSS)?\s*JOIN)\s+([a-zA-Z_][\w]*\s*[a-zA-Z_]*)\s+ON\s+([a-zA-Z_][\w]*\.[a-zA-Z_][\w]*)\s*=\s*([a-zA-Z_][\w]*\.[a-zA-Z_][\w]*))",
            std::regex::icase
        );
        std::sregex_iterator iter(query.begin(), query.end(), join_regex);
        std::sregex_iterator end;
        JoinCondition join_condition;

        while (iter != end) {
            // 추출된 값을 처리
            std::string join_type_string = (*iter)[2].str();  // JOIN 유형 (e.g., LEFT, RIGHT, INNER, FULL)
            std::string table = (*iter)[3].str();             // JOIN된 테이블과 별칭
            std::string left_column = (*iter)[4].str();       // JOIN 조건의 왼쪽 컬럼
            std::string right_column = (*iter)[5].str();      // JOIN 조건의 오른쪽 컬럼

            // 디버깅 출력
            // std::cout << "join_type_string: " << join_type_string << std::endl;
            // std::cout << "table: " << table << std::endl;
            // std::cout << "left_column: " << left_column << std::endl;
            // std::cout << "right_column: " << right_column << std::endl;
            std::istringstream iss(table);
            std::string table_name, table_alias;

            iss >> table_name;

            if (iss >> table_alias) {
                parsed_custom_query_.from_table_map[table_alias] = table_name;
            } else {
                parsed_custom_query_.from_table_map[table_name] = table_name; 
            }
            table_map[table_name] = table_alias; 

            // JOIN 유형이 비어 있을 경우 기본적으로 INNER JOIN으로 설정
            if (join_type_string.empty()) {
                join_type_string = "INNER";
            }

            if (join_type_string == "INNER") {
                join_type = QueryType::INNER_JOIN;
            } else if (join_type_string == "LEFT OUTER") {
                join_type = QueryType::LEFT_OUTER_JOIN;
            } else if (join_type_string == "RIGHT OUTER") {
                join_type = QueryType::RIGHT_OUTER_JOIN;
            } else if (join_type_string == "FULL OUTER") {
                KETILOG::ERRORLOG(LOGTAG, "FULL OUTER 지원 안함!" );
            } else if (join_type_string == "CROSS") {
                join_type = QueryType::CROSS_JOIN;
            } else {
                join_type = QueryType::INNER_JOIN;
            }
            string l_table_alias, l_value_type, l_table_name, l_column_name;
            string r_table_alias, r_value_type, r_table_name, r_column_name;
            size_t dot_pos = left_column.find('.');
            if (dot_pos != std::string::npos) {
                l_table_alias = left_column.substr(0, dot_pos);
                if(parsed_custom_query_.from_table_map.find(l_table_alias) != parsed_custom_query_.from_table_map.end()){
                    l_value_type = static_cast<int>(ValueType::COLUMN);                            
                    l_table_name = parsed_custom_query_.from_table_map[l_table_alias];
                    l_column_name = left_column.substr(dot_pos + 1);
                    

                }
            }
            
            dot_pos = right_column.find('.');
            if (dot_pos != std::string::npos) {
                r_table_alias = right_column.substr(0, dot_pos);
                if(parsed_custom_query_.from_table_map.find(r_table_alias) != parsed_custom_query_.from_table_map.end()){
                    r_value_type = static_cast<int>(ValueType::COLUMN);                            
                    r_table_name = parsed_custom_query_.from_table_map[r_table_alias];
                    r_column_name = right_column.substr(dot_pos + 1);
                    

                }
            }
            join_condition.setJoinCondition(l_table_name, l_table_alias, l_column_name, r_table_name, r_table_alias, r_column_name);
            join_conditions.push_back(join_condition);
            join_columns.insert(join_condition.left_column);
            join_columns.insert(join_condition.right_column);
            ++iter;
        }

        
        int i=0;
        for(auto &table_iter : meta_table_priority){
            auto item = table_map.find(table_iter);
            if(item != table_map.end()){
                KETILOG::DEBUGLOG(LOGTAG, "<스캔 테이블 생성>");
                QueryTable query_table;
                query_table.query_type = QueryType::FULL_SCAN;
                query_table.table_name1 = table_iter;
                query_table.table_alias1 = table_map[table_iter];
                std::string snippet_name = "snippet-" + to_string(i);
                query_table.result_table_alias = snippet_name;

                parsed_custom_query_.query_tables[snippet_name] = query_table;
                table_priority.emplace_back(snippet_name, table_iter);
                i++;
            }
            
        }
        for (const auto& table_entry : parsed_custom_query_.query_tables) {
            const QueryTable& query_table = table_entry.second;  
            meta_table_columns_map[query_table.table_name1] = MetaDataManager::GetInstance().GetTableColumns(parsed_custom_query_.db_name, query_table.table_name1);
            
        }
        for (const auto& table_entry : meta_table_columns_map) {
            const std::string& table_name = table_entry.first;
            const std::vector<std::string>& columns = table_entry.second;

            for (const auto& column : columns) {
                parsed_custom_query_.column_table_map[column] = table_name;
            }
        }

    }

    

    std::string toLowerCase(const std::string& str) {
        std::string lower_str = str;
        std::transform(lower_str.begin(), lower_str.end(), lower_str.begin(),
                    [](unsigned char c) { return std::tolower(c); });
        return lower_str;
    }

    void mergeBetweenConditions(std::vector<std::string>& where_conditions) {
        for (size_t i = 0; i < where_conditions.size(); ++i) {
            if (toLowerCase(where_conditions[i]).find("between") != std::string::npos) {
                if (i + 2 < where_conditions.size() && toLowerCase(where_conditions[i + 1]) == "and") {
                    where_conditions[i] += " AND " + where_conditions[i + 2]; // 병합
                    where_conditions.erase(where_conditions.begin() + i + 1, where_conditions.begin() + i + 3); // 병합된 요소 제거
                }
            }
        }
    }
    //Where 논리연산자 - AND OR NOT 으로 나눔 
    void removeQuotesAndParentheses(std::string& str) {
        // 작은따옴표, 큰따옴표, 괄호 제거
        str.erase(std::remove(str.begin(), str.end(), '\''), str.end());
        str.erase(std::remove(str.begin(), str.end(), '\"'), str.end());
        str.erase(std::remove(str.begin(), str.end(), '('), str.end());
        str.erase(std::remove(str.begin(), str.end(), ')'), str.end());
    }
    
    std::vector<OperValue> parseValues(const std::string& input) {
        std::vector<OperValue> tokens;
        std::regex token_regex(
            R"((\d+\.\d+)|(\d+)|('.*?')|(\b\d{4}-\d{2}-\d{2}\b)|(\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\b)|([A-Za-z0-9_!_@_#_$_%_&][A-Za-z0-9_!_@_#_$_%_&]*)|([\+\-\*/])|(\bIN\b)|(\([^)]*\)))"
        );

        std::smatch match;
        std::string expr = input;
        bool matched = false;

        while (std::regex_search(expr, match, token_regex)) {
            matched = true;
            std::string token = match.str();
            OperValue parsed_token;
            std::cout << "Token: " << token << std::endl;
             
            // 정수 (INT8 ~ INT64)
            if (std::regex_match(token, std::regex(R"(\d+)"))) {
                long long value = std::stoll(token);
                if (value <= 127) {
                    parsed_token.value_type = static_cast<int>(ValueType::INT8);
                } else if (value <= 32767) {
                    parsed_token.value_type = static_cast<int>(ValueType::INT16);
                } else if (value <= 2147483647) {
                    parsed_token.value_type = static_cast<int>(ValueType::INT32);
                } else {
                    parsed_token.value_type = static_cast<int>(ValueType::INT64);
                }
                parsed_token.value = token;
            }
            // 실수 (FLOAT32 or FLOAT64)
            else if (std::regex_match(token, std::regex(R"(\d+\.\d+)"))) {
                if (token.size() <= 7) {
                    parsed_token.value_type = static_cast<int>(ValueType::FLOAT32);
                } else {
                    parsed_token.value_type = static_cast<int>(ValueType::FLOAT64);
                }
                parsed_token.value = token;
            }
            // 날짜 (DATE)
            else if (std::regex_match(token, std::regex(R"(\b\d{4}-\d{2}-\d{2}\b)"))) {
                parsed_token.value_type = static_cast<int>(ValueType::DATE);
                parsed_token.value = token;
            }
            // TIMESTAMP
            else if (std::regex_match(token, std::regex(R"(\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\b)"))) {
                parsed_token.value_type = static_cast<int>(ValueType::TIMESTAMP);
                parsed_token.value = token;
            }
            // 문자열 (STRING)
            else if (std::regex_match(token, std::regex(R"('.*?')"))) {
                parsed_token.value_type = static_cast<int>(ValueType::STRING);
                parsed_token.value = token;
            }
            // DECIMAL
            else if (std::regex_match(token, std::regex(R"(\d+\.\d+)"))) {
                parsed_token.value_type = static_cast<int>(ValueType::DECIMAL);
                parsed_token.value = token;
            }
            // 연산자 (OPERATOR)
            else if (std::regex_match(token, std::regex(R"([\+\-\*/])"))) {
                parsed_token.value_type = static_cast<int>(ValueType::OPERATOR);
                parsed_token.value = token;
            }
            // 리스트 형태 (예: `(3, 4, 5)`)
            else if (std::regex_match(token, std::regex(R"(\((.*?)\))"))) {
                std::smatch match;

                if (std::regex_search(token, match, std::regex(R"(\((.*?)\))"))) {
                    std::string content = match[1].str(); // 괄호 내부 내용
                    // 내부 값을 쉼표로 분리
                    std::regex value_regex(R"('[^']*'|\d+\.\d+|\d+)");
                    std::smatch value_match;

                    while (std::regex_search(content, value_match, value_regex)) {
                        std::string inner_token = value_match.str();
                        OperValue parsed_value;

                        // 타입 판별
                        if (std::regex_match(inner_token, std::regex(R"(\d+)"))) {
                            long long value = std::stoll(inner_token);
                            if (value <= 127) {
                                parsed_token.value_type = static_cast<int>(ValueType::INT8);
                            } else if (value <= 32767) {
                                parsed_token.value_type = static_cast<int>(ValueType::INT16);
                            } else if (value <= 2147483647) {
                                parsed_token.value_type = static_cast<int>(ValueType::INT32);
                            } else {
                                parsed_token.value_type = static_cast<int>(ValueType::INT64);
                            }
                        } else if (std::regex_match(inner_token, std::regex(R"(\d+\.\d+)"))) {
                            if (inner_token.size() <= 7) {
                                parsed_token.value_type = static_cast<int>(ValueType::FLOAT32);
                            } else {
                                parsed_token.value_type = static_cast<int>(ValueType::FLOAT64);
                            }
                        } else if (std::regex_match(inner_token, std::regex(R"(\b\d{4}-\d{2}-\d{2}\b)"))) {
                            parsed_token.value_type = static_cast<int>(ValueType::DATE);
                        } else if (std::regex_match(inner_token, std::regex(R"('.*?')"))) {
                            parsed_token.value_type = static_cast<int>(ValueType::STRING);
                        } else if (std::regex_match(inner_token, std::regex(R"(\b\d{4}-\d{2}-\d{2}\b)"))) {
                            parsed_token.value_type = static_cast<int>(ValueType::DATE);
                        } else {
                            KETILOG::ERRORLOG(LOGTAG, "Value 지원 안함!" + inner_token);
                            parsed_custom_query_.is_parsing_custom_query = false;
                        }

                        parsed_value.value = inner_token;
                        tokens.push_back(parsed_value);

                        content = value_match.suffix().str(); // 나머지 문자열로 이동
                    }
                }
            }else{
                parsed_token.value_type = static_cast<int>(ValueType::STRING);
                parsed_token.value = token;
            }

            tokens.push_back(parsed_token);
            expr = match.suffix().str(); // 남은 문자열로 이동
        }

        // 수식 처리 (예: 3 * 4 + 5)
        if (!matched) {
            KETILOG::ERRORLOG(LOGTAG, "Value 지원 안함!" );
            parsed_custom_query_.is_parsing_custom_query = false;
            std::regex expr_regex(R"((\d+)|([\+\-\*/]))");
            while (std::regex_search(expr, match, expr_regex)) {
                std::string token = match.str();
                OperValue parsed_token;

                if (std::regex_match(token, std::regex(R"(\d+)"))) {
                    long long value = std::stoll(token);
                    parsed_token.value_type = static_cast<int>(ValueType::INT32);
                    parsed_token.value = token;
                } else if (std::regex_match(token, std::regex(R"([\+\-\*/])"))) {
                    parsed_token.value_type = static_cast<int>(ValueType::OPERATOR);
                    parsed_token.value = token;
                }

                tokens.push_back(parsed_token);
                expr = match.suffix().str();
            }
        }
        // cout << "parseValues 확인 : " << " "; 
        // for(auto &token_iter : tokens){
        //     cout << token_iter.value << ", type " << token_iter.value_type  << endl;
        // }
        // cout << endl;
        return tokens;
    }
    void parseWhereConditions(const std::string& query) { 
        // 정규식으로 WHERE 절 추출
        std::regex where_regex(R"(WHERE\s+(.+?)(?=\s+GROUP BY|\s+ORDER BY|\s+LIMIT|$))", std::regex::icase);
        std::smatch match;
        field_found = false;
        if (std::regex_search(query, match, where_regex)) {
            std::string where_clause = match[1].str();

            // 마지막 문자가 ';'이면 제거
            if (!where_clause.empty() && where_clause.back() == ';') {
                where_clause.pop_back();
            }

            // 논리 연산자와 조건을 분리
            std::regex oper_split(R"(\s*(AND|OR|NOT)\s+)", std::regex::icase);
            std::sregex_token_iterator iter(where_clause.begin(), where_clause.end(), oper_split, -1);
            std::sregex_token_iterator logic_iter(where_clause.begin(), where_clause.end(), oper_split, 1);
            std::sregex_token_iterator end;

            // 조건과 논리 연산자를 저장
            while (iter != end) {
                std::string condition = *iter++;
                trim(condition);
                if (!condition.empty()) {
                    parsed_custom_query_.total_where_clause.push_back(condition);
                }

                if (logic_iter != end) {
                    std::string logic_operator = logic_iter->str();
                    trim(logic_operator);
                    if (!logic_operator.empty()) {
                        parsed_custom_query_.total_where_clause.push_back(logic_operator); // 논리 연산자 추가
                    }
                    ++logic_iter;
                }

            }
        }else{
            KETILOG::DEBUGLOG(LOGTAG, "Where 없음");
            return;
        }
        
        mergeBetweenConditions(parsed_custom_query_.total_where_clause);

        //디버깅용 출력
        // for (const auto& condition : parsed_custom_query_.total_where_clause) {
        //     std::cout << "WHERE Condition: " << condition << std::endl;

        // }
        //whereCondition, joinCondition 구별 및 생성
        std::string table_name;
        int and_count = 0, or_count = 0;
        vector<std::string> temp_condition;
        bool is_join_and = false;
        int value_type ;
        std::string table_alias;
        std::string talbe_name; 
        std::string column_name;

        vector<OperValue> temp_right_values;

        for (auto& condition : parsed_custom_query_.total_where_clause) {
            temp_right_values.clear();
            //Between먼저 처리
            std::regex between_regex(R"((\w+\.\w+)\s+(BETWEEN|between)\s+'([^']*)'\s+AND\s+'([^']*)')", std::regex::icase);
            std::smatch between_match;
            if (std::regex_search(condition, between_match, between_regex)) {
                OperValue l_value, inner_right_value, inner_left_value;
                std::string column_name = between_match[1].str();  // 컬럼 이름
                std::string start_value = between_match[3].str();  // 시작값
                std::string end_value = between_match[4].str();    // 종료값
                size_t dot_pos = column_name.find('.');
                if (dot_pos != std::string::npos) {
                    // `.`가 존재하면 별칭과 컬럼명으로 나눔
                    value_type = static_cast<int>(ValueType::COLUMN);
                    table_alias = column_name.substr(0, dot_pos);
                    table_name = parsed_custom_query_.from_table_map[table_alias];                    
                    column_name = column_name.substr(dot_pos + 1);
                    
                    if(parsed_custom_query_.column_table_map.find(column_name) != parsed_custom_query_.column_table_map.end()){
                        KETILOG::DEBUGLOG(LOGTAG, column_name + " 은 컬럼");
                        l_value.setOperValue(value_type,table_name, table_alias, column_name);
                    }else{
                        KETILOG::ERRORLOG(LOGTAG, "왼쪽 연산자가 컬럼이 아니라 지원 안함!");
                        parsed_custom_query_.is_parsing_custom_query = false;

                    }
                    
                }else{
                    if(parsed_custom_query_.column_table_map.find(column_name) != parsed_custom_query_.column_table_map.end()){
                        KETILOG::DEBUGLOG(LOGTAG, column_name + " 은 컬럼");
                        table_name = parsed_custom_query_.column_table_map[column_name];
                        table_alias = table_name;
                        l_value.setOperValue(value_type,table_name, table_alias, column_name);
                    }else{
                        KETILOG::ERRORLOG(LOGTAG, "왼쪽 연산자가 컬럼이 아니라 지원 안함!");
                        parsed_custom_query_.is_parsing_custom_query = false;
                    }
                }
                //오른쪽 set
                
                if (std::regex_match(start_value, std::regex(R"(\d+)"))) {
                    long long value = std::stoll(start_value);
                    if (value <= 127) {
                        inner_left_value.value_type = static_cast<int>(ValueType::INT8);
                    } else if (value <= 32767) {
                        inner_left_value.value_type = static_cast<int>(ValueType::INT16);
                    } else if (value <= 2147483647) {
                        inner_left_value.value_type = static_cast<int>(ValueType::INT32);
                    } else {
                        inner_left_value.value_type = static_cast<int>(ValueType::INT64);
                    }
                    inner_left_value.value = start_value;
                }
                // 실수 (FLOAT32 or FLOAT64)
                else if (std::regex_match(start_value, std::regex(R"(\d+\.\d+)"))) {
                    if (start_value.size() <= 7) {
                        inner_left_value.value_type = static_cast<int>(ValueType::FLOAT32);
                    } else {
                        inner_left_value.value_type = static_cast<int>(ValueType::FLOAT64);
                    }
                    inner_left_value.value = start_value;
                }
                // 날짜 (DATE)
                else if (std::regex_match(start_value, std::regex(R"(\b\d{4}-\d{2}-\d{2}\b)"))) {
                    inner_left_value.value_type = static_cast<int>(ValueType::DATE);
                    inner_left_value.value = start_value;
                }
                // TIMESTAMP
                else if (std::regex_match(start_value, std::regex(R"(\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\b)"))) {
                    inner_left_value.value_type = static_cast<int>(ValueType::TIMESTAMP);
                    inner_left_value.value = start_value;
                }
                // 문자열 (STRING)
                else if (std::regex_match(start_value, std::regex(R"('.*?')"))) {
                    inner_left_value.value_type = static_cast<int>(ValueType::STRING);
                    inner_left_value.value = start_value;
                }
                // DECIMAL
                else if (std::regex_match(start_value, std::regex(R"(\d+\.\d+)"))) {
                    inner_left_value.value_type = static_cast<int>(ValueType::DECIMAL);
                    inner_left_value.value = start_value;
                }
                inner_right_value.value = end_value;
                inner_right_value.value_type= inner_left_value.value_type;
                WhereCondition where_condition;
                where_condition.left_values.push_back(l_value); 
                where_condition.op = static_cast<int>(OperType::BETWEEN); 
                where_condition.right_values.push_back(inner_left_value);
                where_condition.right_values.push_back(inner_right_value);

                if(l_value.value_type == static_cast<int>(ValueType::COLUMN)){ // 왼쪽 컬럼만 추가 테이블 1개
                                            
                    //왼쪽 컬럼에 해당하는 스캔 스니펫 추가ㅣ
                    for(auto &query_table_entry : parsed_custom_query_.query_tables){
                        string snippet_name1 = query_table_entry.first;

                        if(snippet_name1 == query_table_entry.second.result_table_alias &&
                        where_condition.left_values.at(0).table_name ==  query_table_entry.second.table_name1){
                            parsed_custom_query_.query_tables[snippet_name1].select_columns.insert(where_condition.left_values.at(0).value);
                            parsed_custom_query_.query_tables[snippet_name1].where_conditions.push_back(where_condition);
                        }
                    }
                }
                
            } 
            // 그외 처리
            std::regex condition_regex(
                R"(\(?([\w.]+)\s*(=|<>|>=|<=|>|<|NOT LIKE|LIKE|NOT IN|IN|IS NOT|IS)\s*('[^']*'|\([^\)]+\)|[^\(\);]+)\)?)",
                std::regex::icase
            );

            std::smatch match;
            // 반복적으로 조건 파싱
            while (std::regex_search(condition, match, condition_regex)) {
                cout << condition <<endl ;
                std::string operand1 = match[1].str();
                std::string operator_str = match[2].str();
                std::string operand2 = match[3].str();
                
                OperValue l_value, r_value;
                size_t dot_pos = operand1.find('.');
                //왼쪽 연산자 처리
                if (dot_pos != std::string::npos) {
                    // `.`가 존재하면 별칭과 컬럼명으로 나눔
                    value_type = static_cast<int>(ValueType::COLUMN);
                    table_alias = operand1.substr(0, dot_pos);
                    table_name = parsed_custom_query_.from_table_map[table_alias];                    
                    column_name = operand1.substr(dot_pos + 1);
                    if(parsed_custom_query_.column_table_map.find(column_name) != parsed_custom_query_.column_table_map.end()){
                        KETILOG::DEBUGLOG(LOGTAG, column_name + " 은 컬럼");
                    }
                    
                } else { //아니면 컬럼명이 있는지 없는지 확인
                    if(parsed_custom_query_.column_table_map.find(operand1) != parsed_custom_query_.column_table_map.end()){
                        KETILOG::DEBUGLOG(LOGTAG, column_name + " 은 컬럼");
                        value_type = static_cast<int>(ValueType::COLUMN);
                        table_name = parsed_custom_query_.column_table_map[operand1];
                        table_alias = table_name;
                        column_name = operand1;

                    }else{
                        KETILOG::ERRORLOG(LOGTAG, "왼쪽 연산자가 컬럼이 아니라 지원 안함!");
                        parsed_custom_query_.is_parsing_custom_query = false;
                        return;

                    }
                }
                
                l_value.setOperValue(value_type,table_name, table_alias, column_name);

                //오른쪽 연산자 처리
                if(operator_str == "="){ //암묵적 조인이 있을경우
                    join_type = QueryType::INNER_JOIN;
                    size_t dot_pos = operand2.find('.');
                    if (dot_pos != std::string::npos) {
                        
                        std::string table_alias = operand2.substr(0, dot_pos);
                        //오른쪽이 별칭있고 컬럼일때
                        if(parsed_custom_query_.from_table_map.find(table_alias) != parsed_custom_query_.from_table_map.end()){
                            value_type = static_cast<int>(ValueType::COLUMN);                            
                            table_name = parsed_custom_query_.from_table_map[table_alias];
                            std::string column_name = operand2.substr(dot_pos + 1);
                            r_value.setOperValue(value_type, table_name, table_alias, column_name);

                        }else{//오른쪽이 별칭있고 값일때
                            // cout << "<c1 = vn인 경우>" << endl;
                            temp_right_values = parseValues(operand2);
                            value_type = static_cast<int>(ValueType::STRING); 
                            
                            
                        }
                        cout << endl;
                    } else {
                        //오른쪽이 컬럼일때
                        if(parsed_custom_query_.column_table_map.find(operand2) != parsed_custom_query_.column_table_map.end()){
                            value_type = static_cast<int>(ValueType::COLUMN); 
                            table_name = parsed_custom_query_.column_table_map[operand2];
                            table_alias = table_name;
                            column_name = operand2; 
                            r_value.setOperValue(value_type,table_name, table_alias, column_name);
                            
                        }else{//오른쪽이 값일때
                            // cout << "<c1 = vn인 경우>" << endl;
                            temp_right_values = parseValues(operand2);
                            value_type = static_cast<int>(ValueType::STRING); 
                            
                        }
                    }

                    if(value_type == static_cast<int>(ValueType::COLUMN)){ //join condition에 저장 
                        std::string snippet_name, snippet_name1 ,snippet_name2, join_snippet_name;
                        int snippet_i, pre_snippet_i;
                        
                        JoinCondition join_condition;                
                        //join snippet set
                        if(table_priority.size() < 2){
                            KETILOG::ERRORLOG(LOGTAG, "Join Condition에 r_value를 채울수 없음!");
                            parsed_custom_query_.is_parsing_custom_query = false;
                            return;
                        }else {
                            if (table_priority.front().second == l_value.table_name){
                                join_condition.setJoinCondition(l_value.table_name,
                                                            l_value.table_alias,
                                                            l_value.value,
                                                            r_value.table_name,
                                                            r_value.table_alias,
                                                            r_value.value);
                            }else{
                                join_condition.setJoinCondition(r_value.table_name,
                                                            r_value.table_alias,
                                                            r_value.value,
                                                            l_value.table_name,
                                                            l_value.table_alias,
                                                            l_value.value);
                            }
                            if(table_priority.size() > 2){
                                table_priority.erase(table_priority.begin());

                            }
                            
                        }
                        // for (const auto& pair : table_priority) {
                        //     std::cout << "Table: " << pair.first << ", Priority: " << pair.second << std::endl;
                            
                        // }
                        
                        join_conditions.push_back(join_condition);
                        
                        join_columns.insert(join_condition.left_column);
                        join_columns.insert(join_condition.right_column);
                        is_join_and = true;

                    }else{  //join 아닐때 where condition에 저장 
                        
                        WhereCondition where_condition;
                        where_condition.left_values.push_back(l_value); 
                        where_condition.op = operator_str;
                        where_condition.right_values = temp_right_values;
                        std::string snippet_name;
                                                  
                        if(l_value.value_type == static_cast<int>(ValueType::COLUMN)){ // 왼쪽 컬럼 스캔 테이블에 컬럼 추가

                            for(auto &query_table_entry : parsed_custom_query_.query_tables){
                                snippet_name = query_table_entry.first;
                                if(snippet_name == query_table_entry.second.result_table_alias &&
                                where_condition.left_values.at(0).table_name ==  query_table_entry.second.table_name1){
                                    parsed_custom_query_.query_tables[snippet_name].select_columns.insert(where_condition.left_values.at(0).value);
                                    parsed_custom_query_.query_tables[snippet_name].where_conditions.push_back(where_condition);
                                }
                            }
                            

                        }
                    }
                    temp_condition.push_back(condition);
                }
                else{ // = 가 아닐때 오른쪽 연산자 처리
                    size_t dot_pos = operand2.find('.');
                    if (dot_pos != std::string::npos) {
                        // `.`가 존재하면 별칭과 컬럼명으로 나눔
                        std::string table_alias = operand2.substr(0, dot_pos);
                        //.이 별칭인가 실수인가 확인
                        if(parsed_custom_query_.from_table_map.find(table_alias) != parsed_custom_query_.from_table_map.end()){
                            value_type = static_cast<int>(ValueType::COLUMN);                            
                            table_name = parsed_custom_query_.from_table_map[table_alias];
                            std::string column_name = operand2.substr(dot_pos + 1);
                            r_value.setOperValue(value_type, table_name, table_alias, column_name);

                        }else{
                            // cout << "<c1 비교 vn인 경우>" << endl;
                        
                            temp_right_values = parseValues(operand2);
                                                                                 
                        }

                    } else {
                        // `.`가 없으면 컬럼명인지 아닌지 확인
                        if(parsed_custom_query_.column_table_map.find(operand2) != parsed_custom_query_.column_table_map.end()){
                            value_type = static_cast<int>(ValueType::COLUMN); 
                            table_name = parsed_custom_query_.column_table_map[operand2];
                            table_alias = table_name;
                            column_name = operand2; 
                            r_value.setOperValue(value_type,table_name, table_alias, column_name);
                            
                        }else{
                            // cout << "<c1 비교 vn인 경우>" << endl;                           
                            temp_right_values = parseValues(operand2);
                                                            
                        }
                        WhereCondition where_condition;
                        where_condition.left_values.push_back(l_value);
                        where_condition.op = operator_str;
                        where_condition.right_values = temp_right_values;
                        std::string snippet_name;
                        if(l_value.value_type == static_cast<int>(ValueType::COLUMN)){ // 왼쪽 컬럼만 추가 테이블 1개
                                            
                            //왼쪽 컬럼에 해당하는 스캔 스니펫 추가ㅣ
                            for(auto &query_table_entry : parsed_custom_query_.query_tables){
                                snippet_name = query_table_entry.first;

                                if(snippet_name == query_table_entry.second.result_table_alias &&
                                where_condition.left_values.at(0).table_name ==  query_table_entry.second.table_name1){
                                    parsed_custom_query_.query_tables[snippet_name].select_columns.insert(where_condition.left_values.at(0).value);
                                    parsed_custom_query_.query_tables[snippet_name].where_conditions.push_back(where_condition);
                                }
                            }
                            for(auto &value:  where_condition.right_values){
                                if(value.value_type == static_cast<int>(ValueType::COLUMN)){
                                    KETILOG::ERRORLOG(LOGTAG, "조건절 오른쪽에 컬럼 존재, 지원 안함!");
                                    parsed_custom_query_.is_parsing_custom_query=false;
                                    return;
                                }
                            }
                        }
                    }
                } 
                //논리연산자 And랑 or랑 섞여있으면 안됨, not 있을때도 안되게 
                if(is_join_and == true){
                    is_join_and = false;
                    
                }else{
                    if(condition == "NOT" || condition == "not"){
                        KETILOG::ERRORLOG(LOGTAG, "NOT 조건절 지원 안함!");
                        parsed_custom_query_.is_parsing_custom_query = false;
                    }else if(condition == "AND" || condition == "and"){

                        and_count++;
                        
                    }else if(condition == "OR" || condition == "or"){
                        or_count++;
                    }
                    
                    temp_condition.push_back(condition);
                }
                condition = match.suffix().str();
                
            }
        }
        // cout << "hj :: temp_condition 확인 " << endl;
        // for(auto &iter : temp_condition){
        //     cout << iter << endl; 
        // }
        // cout << endl;
        if(and_count == 0 && or_count == 0) {
            parsed_custom_query_.logical = -1;
        }else if(and_count > 0 && or_count == 0){
            parsed_custom_query_.logical =  static_cast<int>(OperType::AND);
        }else if(and_count == 0 && or_count > 0){
            parsed_custom_query_.logical =  static_cast<int>(OperType::OR);
        }else{
            
            KETILOG::ERRORLOG(LOGTAG, "and랑 or랑 섞여있음! 지원 안함!");
            parsed_custom_query_.is_parsing_custom_query = false;
        }

        // 정렬된 결과 출력
        // std::cout << "Join Conditions:" << std::endl;
        // for (const auto& join : join_conditions) {
        //     std::cout << "Left Table: " << join.left_table_name << ", Column: " << join.left_column
        //             << " | Right Table: " << join.right_table_name << ", Column: " << join.right_column
        //             << std::endl;
        // }
        // cout << endl;
        //join에 사용된 column들을 각 scan 테이블에다가 추가
        for(auto &join_condition : join_conditions){
            for(auto &query_table_entry : parsed_custom_query_.query_tables){
                std::string snippet_name = query_table_entry.first; 
                if(snippet_name == query_table_entry.second.result_table_alias &&
                    join_condition.left_table_name ==  query_table_entry.second.table_name1){
                    parsed_custom_query_.query_tables[snippet_name].select_columns.insert(join_condition.left_column);
                    
                }
                if(snippet_name == query_table_entry.second.result_table_alias &&
                    join_condition.right_table_name ==  query_table_entry.second.table_name1){
                    parsed_custom_query_.query_tables[snippet_name].select_columns.insert(join_condition.right_column);
                }

            }
        }
        int i=0;
        std::unordered_set<std::string> use_table;
        for (const auto& join : join_conditions) {
            join_snippet_i = parsed_custom_query_.query_tables.size();
            QueryTable query_table;
            query_table.query_type = join_type;
            std::string table_name1 ,table_name2;
            if(i == 0){     //1. 양쪽다 scan일때
                for(auto query_table_entry : parsed_custom_query_.query_tables){ //정렬한 조인 스니펫으로 snippet 이름 정의
                    std::string snippet_name = query_table_entry.first;  
                    if(query_table_entry.second.table_name1 == join.left_table_name){ //nation0, region1, suppler2, part3, partsuppt4순일때
                        
                        table_name1 = snippet_name;
                        use_table.insert(query_table_entry.second.table_name1);
                    }
                    
                }

                query_table.table_name1 = table_name1;
                query_table.table_alias1 = table_name1;

                for(auto query_table_entry : parsed_custom_query_.query_tables){ //정렬한 조인 스니펫으로 snippet 이름 정의
                    std::string snippet_name = query_table_entry.first;  
                    if(query_table_entry.second.table_name1 == join.right_table_name){ //nation0, region1, suppler2, part3, partsuppt4순일때
                        
                        table_name2 = snippet_name;
                        use_table.insert(query_table_entry.second.table_name1);
                    }
                    
                }
                query_table.table_name2 = table_name2;
                query_table.table_alias2 = table_name2;
                
            }else{            //2. 한쪽만 scan일때
                table_name1 = "snippet-" +to_string(join_snippet_i-1); //왼쪽은 전 조인스니펫 결과
                //오른쪽은 처음 쓰는 스캔 테이블 
                query_table.table_name1 = table_name1;
                query_table.table_alias1 = table_name1;
                
                auto it = use_table.find(join.left_table_name);
                if(it == use_table.end()){ // 왼쪽 테이블이 처음쓰는 스캔 테이블일때
                    for(auto query_table_entry : parsed_custom_query_.query_tables){
                        std::string snippet_name = query_table_entry.first;
                        if(join.left_table_name == query_table_entry.second.table_name1)
                        table_name2 = snippet_name; 
                        
                    }
                    use_table.insert(join.left_table_name);
                    
                }

                it = use_table.find(join.right_table_name);
                if(it == use_table.end()){ // 오른쪽 테이블이 처음쓰는 스캔 테이블일때
                    for(auto query_table_entry : parsed_custom_query_.query_tables){
                        std::string snippet_name = query_table_entry.first;
                        if(join.right_table_name == query_table_entry.second.table_name1)
                        table_name2 = snippet_name; 
                    }
                    use_table.insert(join.right_table_name);
                    
                }
                query_table.table_name2 = table_name2;
                query_table.table_alias2 = table_name2;
                
            }

            query_table.join_condition = join;
            query_table.result_table_alias = "snippet-" + to_string(join_snippet_i);
            //set 에서 해당하는 컬럼 지우고 남은 컬럼 추가 
            join_columns.erase(join.left_column);
            join_columns.erase(join.right_column);
            for(auto &join_col : join_columns){
                if(join.left_table_name == parsed_custom_query_.column_table_map[join_col]){
                    query_table.select_columns.insert(join_col);
                     
                }
            }
            for(auto &join_col : join_columns){
                if(join.right_table_name == parsed_custom_query_.column_table_map[join_col]){
                    query_table.select_columns.insert(join_col);
                }
            }
            
            parsed_custom_query_.query_tables[query_table.result_table_alias] = query_table;
            i++;
        }
        // ParsedCustomQuery pa = GetParsedCustomQuery();
        
    }
    void parseGroupByFields(const std::string& query) {
        std::regex agg_regex(R"(\b(SUM|AVG|COUNT\(\*\)|COUNT\(DISTINCT\s+\w+\)|COUNT|TOP|MIN|MAX)\b)", std::regex::icase);
        std::smatch agg_match;
        if ((std::regex_search(query, agg_match, agg_regex))) {
            is_aggregation = true;
        }
        std::regex group_by_regex(R"(GROUP BY\s+(.+?)(?=\s+ORDER BY|\s+LIMIT|$))", std::regex::icase);
        std::smatch match;
        if (std::regex_search(query, match, group_by_regex)) {
            if(is_aggregation == false){
                KETILOG::ERRORLOG(LOGTAG, "집계합수 없이 group by 사용, 지원 안함! ");
                parsed_custom_query_.is_parsing_custom_query = false;
                return;
            }
            std::string group_by_fields(match[1].str());

            if (!group_by_fields.empty() && group_by_fields.back() == ';') {
                group_by_fields.pop_back();  // 마지막 문자가 ';'이면 제거
            }

            std::istringstream fields_stream(group_by_fields);
            std::string field;
            int aggregation_snippet_i = parsed_custom_query_.query_tables.size()-1;
            std::string snippet_name = "snippet-" + to_string(aggregation_snippet_i);
            std::string scan_snippet_name ;
            // cout << snippet_name << endl;
            // group_by_fields 처리, projection , result_info - column_alias set, column으로 
            while (std::getline(fields_stream, field, ',')) {
                trim(field); 
                size_t dot_pos = field.find('.');
                std::string column_name;
                if (dot_pos != std::string::npos) {
                    // `.`가 존재하면 별칭과 컬럼명으로 나눔
                    column_name = field.substr(dot_pos + 1);

                }else{
                    
                    column_name = field;
                    
                }

                std::string table_name = parsed_custom_query_.column_table_map[column_name];

                //scan 스니펫에 컬럼 추가 
                auto item = find(parsed_custom_query_.result_columns.begin(),parsed_custom_query_.result_columns.end(),column_name);
                if(item == parsed_custom_query_.result_columns.end()){
                    KETILOG::ERRORLOG(LOGTAG, "Group By::Select에 없는 컬럼 사용 불가 ");
                    parsed_custom_query_.is_parsing_custom_query = false;
                    return;
                }
                
                parsed_custom_query_.group_by_fields.insert(column_name);
            }
            
        }
        // `HAVING` 절 필드 처리
        std::regex having_regex(R"(HAVING\s+(.+?)(?=\s+ORDER BY|\s+LIMIT|$))", std::regex::icase);
        if (std::regex_search(query, match, having_regex)) {
            // std::string having_conditions(match[1].str());

            // if (!having_conditions.empty() && having_conditions.back() == ';') {
            //     having_conditions.pop_back();  // 마지막 문자가 ';'이면 제거
            // }

            // std::istringstream having_stream(having_conditions);
            // std::string condition;
            
            // while (std::getline(having_stream, condition, ',')) {
            //     trim(condition);
            //     cout << "hj :: Having : " << condition <<  " ";
            // }
            KETILOG::ERRORLOG(LOGTAG, "having 절 지원 안 함!");
            parsed_custom_query_.is_parsing_custom_query = false;
        }
    
    }


    void parseOrderByFields(const std::string& query) {
        std::regex order_by_regex(R"(ORDER BY\s+(.+?)(?=\s+LIMIT|$))", std::regex::icase);
        std::smatch match;
        if (std::regex_search(query, match, order_by_regex)) {
            
            std::string order_by_fields(match[1].str());

            if (!order_by_fields.empty() && order_by_fields.back() == ';') {
                order_by_fields.pop_back();  // 마지막 문자가 ';'이면 제거
            }

            std::istringstream fields_stream(order_by_fields);
            std::string field;
            int snippet_i = parsed_custom_query_.query_tables.size();
            string snippet_name = "snippet-" + to_string(snippet_i);

            while (std::getline(fields_stream, field, ',')) {
                trim(field);
                // cout << field << " ";
                size_t dot_pos = field.find('.');
                std::string column_name;
                if (dot_pos != std::string::npos) {
                    // `.`가 존재하면 테이블 별칭과 컬럼명으로 나눔
                    column_name = field.substr(dot_pos + 1);

                }else{
                    column_name = field;
                }
                bool is_asc = field.find("DESC") == std::string::npos;  // DESC가 없으면 오름차순(ASC)으로 간주
                column_name = std::regex_replace(field, std::regex(R"(\s+(ASC|DESC)\b)", std::regex::icase), "");
                
                std::string table_name = parsed_custom_query_.column_table_map[column_name];
                // cout << "hj :: Order_by에서 사용하는 table : " << table_name << " column_name : " << column_name <<endl;
                //scan 스니펫에 컬럼 추가 
                for(auto &query_table_entry : parsed_custom_query_.query_tables){
                    std::string temp_snippet_name = query_table_entry.first;
                    std::string temp_table_name = query_table_entry.second.table_name1;
                    if(table_name == temp_table_name){
                        parsed_custom_query_.query_tables[temp_snippet_name].select_columns.insert(column_name);
                    }
                }
                // order_by_fields에 필드와 정렬 순서 추가
                parsed_custom_query_.order_by_fields.insert({column_name, is_asc});
            }
        }
    }
    void parseLimitClause(const std::string& query) {
        //LIMIT 절 및 OFFSET 절을 처리: "LIMIT length" 또는 "LIMIT offset, length" 및 "LIMIT length OFFSET offset"
        std::regex limit_offset_regex(R"(LIMIT\s+(\d+)(?:\s*,\s*(\d+))?(?:\s+OFFSET\s+(\d+))?)", std::regex::icase);
        std::smatch match;
        Limit limit;
        if (std::regex_search(query, match, limit_offset_regex)) {
        
            if (match[2].matched) {
                // LIMIT offset, length 형태인 경우
                limit.offset = std::stoi(match[1].str());
                limit.length = std::stoi(match[2].str());
            } else if (match[3].matched) {
                // LIMIT length OFFSET offset 형태인 경우
                limit.offset = std::stoi(match[3].str());
                limit.length = std::stoi(match[1].str());
            } else {
                // LIMIT length 형태인 경우 (offset은 0으로 간주)
                limit.offset = 0;
                limit.length = std::stoi(match[1].str());
            }
            
        } else {
            // LIMIT 절이 없을 경우 기본값 설정
            limit.offset = -1;
            limit.length = -1;
        }
        parsed_custom_query_.limit = limit;
    }


    void trim(std::string& s) {
        s.erase(0, s.find_first_not_of(" \t\n\r\f\v"));
        s.erase(s.find_last_not_of(" \t\n\r\f\v") + 1);
    }
    
};
