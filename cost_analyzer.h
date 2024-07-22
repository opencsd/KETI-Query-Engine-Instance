#pragma once
#include "storage_engine_connector.h"
#include "parsed_query.h"
#include <list>

class CostAnalyzer {
public:
	CostAnalyzer(){
        // setQueryID();
    }
    void Query_Scoring(ParsedQuery &parsed_query) {
        // 쿼리 점수화 함수 추가
        KETILOG::DEBUGLOG(LOGTAG,"Start Query Scoring");
        KETILOG::DEBUGLOG(LOGTAG,"Scoring Query ...");
        
    }
private:
    const std::string LOGTAG = "Query Engine::Cost Analyzer";
};

