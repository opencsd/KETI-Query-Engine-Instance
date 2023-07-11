#pragma once

class Parsed_Query {
public:
    Parsed_Query(std::string query){
        ori_query_ = query;
        query_type_ = Parsed_Query_Type::GenericQuery;
    }
    
    bool isGenericQuery(){
        return (query_type_ == Parsed_Query_Type::GenericQuery) ? true : false;
    }

    void Set_Query_Type_As_PushdownQuery(){
        query_type_ = Parsed_Query_Type::PushdownQuery;
    }

    enum Parsed_Query_Type{
        GenericQuery, // Default
        PushdownQuery
    };

    std::string Get_Parsed_Query(){
        return parsed_query_;
    }
    void Set_Parsed_Query(std::string parsed_query){
        parsed_query_ = parsed_query;
    }
    std::string Get_Ori_Query(){
        return ori_query_;
    }

private:
    std::string ori_query_;
    std::string parsed_query_;
    Parsed_Query_Type query_type_;
};