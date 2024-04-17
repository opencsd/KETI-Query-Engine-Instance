#pragma once
#include "storage_engine_connector.h"
#include "parsed_query.h"
#include <list>

class CostAnalyzer {
public:
	CostAnalyzer(){
        // setQueryID();
    }
    int Query_Scoring(const std::string& query) {
        // 쿼리 점수화 함수 추가

        return query.length();
    }
private:
    const std::string LOGTAG = "Query Engine::Cost Analyzer";
};

