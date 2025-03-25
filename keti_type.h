#ifndef _KETI_TYPE_H_
#define _KETI_TYPE_H_

typedef enum EXECUTION_MODE {
    OFFLOADING = 1, 
    GENERIC = 2 // Default
}EXECUTION_MODE;

typedef enum QUERY_TYPE {
    SELECT = 1,
    UPDATE = 2,
    INSERT = 3,
    DELETE = 4,
    DCL = 5,
    DDL = 6,
    OTHER = 7
}QUERY_TYPE;

#endif