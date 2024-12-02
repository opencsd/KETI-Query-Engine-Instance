#pragma once
#include <iostream>

using namespace std;
// struct Snippet {
//   int type; // query planner 
//   int work_id; 
//   QueryInfo query_info; // query planner
//   string schema_info; 
//   string sst_info; 
//   string wal_info; 
//   vector<string> result_info; // query planner
// };
    struct Limit{
        int offset = -1;
        int length;
    };
    struct Operand {
        vector<int> types;
        vector<string> values;
    };

    struct Projection {
        int select_type; // KETI_SELECT_TYPE
        Operand expression;
    };

    struct Filtering {
        Operand lv;
        int operator_;
        Operand rv;
    };
    struct QueryInfo {
        string table_name1;
        string table_name2;
        string table_alias1;
        string table_alias2;
        vector<Filtering> filtering;
        vector<Projection> projection;
        vector<string> seek_key;
        vector<string> group_by;
        std::map<string,bool> order_by;
        Filtering condition;
        Limit limit;
    };

    struct Column {
		string name;
		int type;
		int length;
        bool primary;
        bool index;
        bool nullable;
	};
    struct SchemaInfo {
        vector<Column> column_list;
        int table_index_number;
    };
    struct CSD{
        string csd_id;
        string partition;
    };
    struct SstInfo {
        string sst_name;
        int sst_block_count;
        vector<CSD> csd; 
    };

    struct WalInfo {
        vector<string> deleted_key;
        vector<string> inserted_key;
        vector<string> inserted_value;

        void clear(){
            deleted_key.clear();
            inserted_key.clear();
            inserted_value.clear();
        }
    };

    struct ResultInfo {
        string table_alias;
        vector<string> columns_alias;
        vector<int> return_column_type;
        vector<int> return_column_length;
        int total_block_count;
        int csd_block_count;
        int sst_block_count;
    };

    struct Snippet {
        int type; // query planner 
        int query_id = 0; 
        int work_id; 
        QueryInfo query_info; // query planner
        SchemaInfo schema_info; 
        vector<SstInfo> sst_info; 
        WalInfo wal_info; 
        ResultInfo result_info; // query planner
    };
    
enum QueryType {
    FULL_SCAN = 0,
    INDEX_SCAN,
    INDEX_TABLE_SCAN,
    AGGREGATION,
    FILTER,
    INNER_JOIN,
    LEFT_OUTER_JOIN,
    RIGHT_OUTER_JOIN,
    CROSS_JOIN,
    UNION,
    IN,
    EXIST,
    DEPENDENCY_INNER_JOIN,
    DEPENDENCY_EXIST,
    DEPENDENCY_IN,
    TMAX_SNIPPET
};
enum class OperType {
    GE = 0,       // >=
    LE = 1,       // <=
    GT = 2,       // >
    LT = 3,       // <
    EQ = 4,       // =
    NE = 5,       // <>
    LIKE = 6,     // like
    NOTLIKE = 7,  // not like
    BETWEEN = 8,  // between
    IN = 9,       // in
    NOTIN = 10,   // not in
    IS = 11,      // is
    ISNOT = 12,   // is not
    AND = 13,     // and
    OR = 14,      // or
    NOT = 15      //not (+hj update)
};

// ValueType: postfix 식에서 사용하는 값의 타입
enum class ValueType {
    INT8 = 0,       // 1바이트 INT
    INT16 = 1,      // 2바이트 INT
    INT32 = 2,      // 4바이트 INT
    INT64 = 3,      // 8바이트 INT
    FLOAT32 = 4,    // 4바이트 FLOAT
    FLOAT64 = 5,    // 8바이트 FLOAT
    DECIMAL = 6,    // decimal
    DATE = 7,       // date
    TIMESTAMP = 8,  // timestamp
    STRING = 9,     // string
    COLUMN = 10,    // 테이블 컬럼 명
    OPERATOR = 11   // 연산자, string으로 확인 ("substring" 등)
};

// SelectType: 컬럼 프로젝션 함수 타입
enum class SelectType {
    COLUMNNAME = 0,      // postfix 식 그대로 사용
    SUM = 1,             // sum()
    AVG = 2,             // avg()
    COUNT = 3,           // count()
    COUNTSTAR = 4,       // count(*)
    COUNTDISTINCT = 5,   // count(distinct)
    TOP = 6,             // top
    MIN = 7,             // min()
    MAX = 8              // max()
};