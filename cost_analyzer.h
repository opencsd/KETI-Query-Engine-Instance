#pragma once
#include "storage_engine_connector.h"
#include "parsed_query.h"
#include "regex"
#include <list>

class CostAnalyzer {
public:
	CostAnalyzer(){
        // setQueryID();
    }
    void extractQueries(const std::string& query, std::vector<std::string>& queries) {
        queries.clear();
        std::regex subquery_regex(R"(\b(?:SELECT|INSERT|UPDATE|DELETE)\b\s*.*\b(FROM|WHERE)\b.*\(\s*\b(?:SELECT|INSERT|UPDATE|DELETE)\b)", std::regex_constants::icase);
        std::smatch match;

        std::string remaining_query = query;
        while (std::regex_search(remaining_query, match, subquery_regex)) {
            std::string subquery = match.str();
            size_t subquery_start = remaining_query.find(subquery);

            if (subquery_start != std::string::npos) {
                queries.push_back(subquery);
                remaining_query = remaining_query.substr(0, subquery_start) + remaining_query.substr(subquery_start + subquery.length());
            }
        }

        if (!remaining_query.empty()) {
            queries.push_back(remaining_query);
        }
    }
    void Query_Scoring(ParsedQuery &parsed_query) {
        // 쿼리 점수화 함수 추가
        KETILOG::DEBUGLOG(LOGTAG,"Start Query Scoring");
        KETILOG::DEBUGLOG(LOGTAG,"Scoring Query ...");
        std::string query = parsed_query.GetOriginalQuery();

        extractQueries(query, queries);

        

        if (queries.size() > 1) { 
            execution_mode_ = EXECUTION_MODE::GENERIC;  // Set GENERIC if there are multiple subqueries
            KETILOG::DEBUGLOG(LOGTAG, "Execution mode set to GENERIC due to multiple subqueries.");
        } else {
            execution_mode_ = EXECUTION_MODE::OFFLOADING;  // Set OFFLOADING if only one query
            parsed_query.SetQueryTypeAsOffloading();
            KETILOG::DEBUGLOG(LOGTAG, "Execution mode set to OFFLOADING due to single query.");
        }
           
    }
private:
    const std::string LOGTAG = "Query Engine::Cost Analyzer";
    EXECUTION_MODE execution_mode_;
    vector<string> queries;
};

