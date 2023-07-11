#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sql.h>
#include <sqlext.h>
#include <string>

#define MAX_DATA_WIDTH 300

#ifndef max
#define max( a, b ) (((a) > (b)) ? (a) : (b))
#endif

#ifndef min
#define min( a, b ) (((a) < (b)) ? (a) : (b))
#endif


int     version3                    = 0;
SQLUSMALLINT    has_moreresults     = 1;
int     max_col_size                = MAX_DATA_WIDTH;
int nUserWidth = 0;
int bQuote = 0;

static int OpenDatabase( SQLHENV *phEnv, SQLHDBC *phDbc, char *szDSN, char *szUID, char *szPWD );
static int ExecuteSQL( SQLHDBC hDbc, char *szSQL );
static int	CloseDatabase( SQLHENV hEnv, SQLHDBC hDbc );

static void WriteHeaderNormal( SQLHSTMT hStmt, SQLCHAR	**szSepLine ,std::string &res);
static SQLLEN WriteBodyNormal( SQLHSTMT hStmt ,std::string &res);
static void WriteFooterNormal( SQLHSTMT hStmt, SQLCHAR	*szSepLine, SQLLEN nRows ,std::string &res);

static void mem_error( int line );
static SQLUINTEGER OptimalDisplayWidth( SQLHSTMT hStmt, SQLINTEGER nCol, int nUserWidth );

//Open Database
static int
OpenDatabase( SQLHENV *phEnv, SQLHDBC *phDbc, char *szDSN, char *szUID, char *szPWD )
{

    if ( SQLAllocEnv( phEnv ) != SQL_SUCCESS )
    {
        fprintf( stderr, "[K-ODBC] ERROR: Could not SQLAllocEnv\n" );
        return 0;
    }

    if ( SQLAllocConnect( *phEnv, phDbc ) != SQL_SUCCESS )
    {
        fprintf( stderr, "[K-ODBC] ERROR: Could not SQLAllocConnect\n" );
        SQLFreeEnv( *phEnv );
        return 0;
    }

    if ( !SQL_SUCCEEDED( SQLConnect( *phDbc, (SQLCHAR*)szDSN, SQL_NTS, (SQLCHAR*)szUID, 0, (SQLCHAR*)szPWD, 0)))
    {
        fprintf( stderr, "[K-ODBC] ERROR: Could not SQLConnect\n" );
        SQLFreeConnect( *phDbc );
        SQLFreeEnv( *phEnv );
        return 0;
    }

    /*
     * does the driver support SQLMoreResults
     */

    if ( !SQL_SUCCEEDED( SQLGetFunctions( *phDbc, SQL_API_SQLMORERESULTS, &has_moreresults )))
    {
        has_moreresults = 0;
    }

    return 1;
}


//ExecuteSQL
static int
ExecuteSQL( SQLHDBC hDbc, char *szSQL, std::string &res)
{
    SQLHSTMT        hStmt;
    SQLSMALLINT     cols;
    SQLLEN          nRows                   = 0;
    SQLINTEGER      ret;
    SQLCHAR         *szSepLine;
    int             mr;

    szSepLine = (SQLCHAR*)calloc(1, 32001);

    if ( version3 )
    {
        if ( SQLAllocHandle( SQL_HANDLE_STMT, hDbc, &hStmt ) != SQL_SUCCESS )
        {
            fprintf( stderr, "[K-ODBC] ERROR: Could not SQLAllocHandle( SQL_HANDLE_STMT )\n" );
            free(szSepLine);
            return 0;
        }
    }
    else
    {
        if ( SQLAllocStmt( hDbc, &hStmt ) != SQL_SUCCESS )
        {
            fprintf( stderr, "[K-ODBC] ERROR: Could not SQLAllocStmt\n" );
            free(szSepLine);
            return 0;
        }
    }

    

    if ( SQLPrepare( hStmt, (SQLCHAR*)szSQL, strlen( szSQL )) != SQL_SUCCESS )
    {
        fprintf( stderr, "[K-ODBC] ERROR: Could not SQLPrepare\n" );
        SQLFreeStmt( hStmt, SQL_DROP );
        free(szSepLine);
        return 0;
    }

    ret =  SQLExecute( hStmt );

    if ( ret == SQL_NO_DATA )
    {
        fprintf( stderr, "[K-ODBC] INFO: SQLExecute returned SQL_NO_DATA\n" );
    }
    else if ( ret == SQL_SUCCESS_WITH_INFO )
    {
        fprintf( stderr, "[K-ODBC] INFO: SQLExecute returned SQL_SUCCESS_WITH_INFO\n" );
    }
    else if ( ret != SQL_SUCCESS )
    {
        fprintf( stderr, "[K-ODBC] ERROR: Could not SQLExecute\n" );
        SQLFreeStmt( hStmt, SQL_DROP );
        free(szSepLine);
        return 0;
    }


    /*
     * Loop while SQLMoreResults returns success
     */

    mr = 0;

    do
    {
        if ( mr )
        {
            if ( ret == SQL_SUCCESS_WITH_INFO )
            {
                fprintf( stderr, "[K-ODBC] INFO: SQLMoreResults returned SQL_SUCCESS_WITH_INFO\n" );
            }
            else if ( ret == SQL_ERROR )
            {
                fprintf( stderr, "[K-ODBC] ERROR: SQLMoreResults returned SQL_ERROR\n" );
            }
        }
        mr = 1;
        strcpy ((char*) szSepLine, "" ) ;

        /*
         * check to see if it has generated a result set
         */

        if ( SQLNumResultCols( hStmt, &cols ) != SQL_SUCCESS )
        {
            fprintf( stderr, "[K-ODBC] ERROR: Could not SQLNumResultCols\n" );
            SQLFreeStmt( hStmt, SQL_DROP );
            free(szSepLine);
            return 0;
        }

        if ( cols > 0 )
        {
            /****************************
             * WRITE HEADER
             ***************************/
             WriteHeaderNormal( hStmt, &szSepLine , res);

            /****************************
             * WRITE BODY
             ***************************/
            nRows = WriteBodyNormal( hStmt , res);
            
        }

        /****************************
         * WRITE FOOTER
         ***************************/
        WriteFooterNormal( hStmt, szSepLine, nRows , res);
    }
    while ( has_moreresults && ( ret = SQLMoreResults( hStmt )) != SQL_NO_DATA );

    /****************************
     * CLEANUP
     ***************************/
    SQLFreeStmt( hStmt, SQL_DROP );
    free(szSepLine);

    return 1;
}


//Close Database
static int CloseDatabase( SQLHENV hEnv, SQLHDBC hDbc )
{
    SQLDisconnect( hDbc );
    if ( version3 )
    {
        SQLFreeHandle( SQL_HANDLE_DBC, hDbc );
        SQLFreeHandle( SQL_HANDLE_ENV, hEnv );
    }
    else
    {
        SQLFreeConnect( hDbc );
        SQLFreeEnv( hEnv );
    }

    return 1;
}


static void WriteHeaderNormal(SQLHSTMT hStmt, SQLCHAR **szSepLine, std::string &res)
{
    SQLINTEGER nCol = 0;
    SQLSMALLINT nColumns = 0;
    SQLCHAR *szColumn;
    SQLCHAR *szColumnName;
    SQLCHAR *szHdrLine;
    SQLUINTEGER nOptimalDisplayWidth = 10;

    szColumnName = (SQLCHAR*)malloc(max_col_size + 1);
    if (!szColumnName)
    {
        mem_error(__LINE__);
    }
    szColumn = (SQLCHAR*)malloc(max_col_size + 3);
    if (!szColumn)
    {
        free(szColumnName);
        mem_error(__LINE__);
    }

    *szColumn = '\0';
    *szColumnName = '\0';

    if (SQLNumResultCols(hStmt, &nColumns) != SQL_SUCCESS)
        nColumns = -1;

    if (nColumns > 0)
    {
        szHdrLine = (SQLCHAR*)calloc(1, 512 + max_col_size * nColumns);
        if (!szHdrLine)
        {
            mem_error(__LINE__);
        }
        *szSepLine = (SQLCHAR*)realloc(*szSepLine, 512 + max_col_size * nColumns);
        if (!*szSepLine)
        {
            mem_error(__LINE__);
        }
    }
    else
    {
        szHdrLine = (SQLCHAR*)calloc(1, 32001);
    }

    for (nCol = 1; nCol <= nColumns; nCol++)
    {
        int sret;

        nOptimalDisplayWidth = OptimalDisplayWidth(hStmt, nCol, nUserWidth);
        SQLColAttribute(hStmt, nCol, SQL_DESC_LABEL, szColumnName, max_col_size, NULL, NULL);

        /* SEP */
        memset(szColumn, '\0', max_col_size + 2);
        memset(szColumn, '-', nOptimalDisplayWidth + 1);
        strcat((char *)*szSepLine, "+");
        strcat((char *)*szSepLine, (char *)szColumn);

        /* HDR */
        sret = sprintf((char *)szColumn, "| %-*.*s",
                       (int)nOptimalDisplayWidth, (int)nOptimalDisplayWidth, szColumnName);
        if (sret < 0)
            sprintf((char *)szColumn, "| %-*.*s",
                    (int)nOptimalDisplayWidth, (int)nOptimalDisplayWidth, "**ERROR**");
        strcat((char *)szHdrLine, (char *)szColumn);
    }
    strcat((char *)*szSepLine, "+\n");
    strcat((char *)szHdrLine, "|\n");

    fputs((char *)*szSepLine, stdout);
    res += (char *)*szSepLine;
    fputs((char *)szHdrLine, stdout);
    res += (char *)szHdrLine;
    fputs((char *)*szSepLine, stdout);
    res += (char *)*szSepLine;

    free(szHdrLine);
    free(szColumnName);
    free(szColumn);
}


static SQLLEN WriteBodyNormal(SQLHSTMT hStmt, std::string &res)
{
    SQLINTEGER nCol = 0;
    SQLSMALLINT nColumns = 0;
    SQLLEN nIndicator = 0;
    SQLCHAR *szColumn;
    SQLCHAR *szColumnValue;
    SQLRETURN nReturn = 0;
    SQLLEN nRows = 0;
    SQLUINTEGER nOptimalDisplayWidth = 10;

    szColumnValue = (SQLCHAR*)malloc(max_col_size + 1);
    if (!szColumnValue)
    {
        mem_error(__LINE__);
    }

    szColumn = (SQLCHAR*)malloc(max_col_size + 21);
    if (!szColumn)
    {
        free(szColumnValue);
        mem_error(__LINE__);
    }

    nReturn = SQLNumResultCols(hStmt, &nColumns);
    if (nReturn != SQL_SUCCESS && nReturn != SQL_SUCCESS_WITH_INFO)
        nColumns = -1;

    /* ROWS */
    nReturn = SQLFetch(hStmt);
    while (nReturn == SQL_SUCCESS || nReturn == SQL_SUCCESS_WITH_INFO)
    {
        /* COLS */
        for (nCol = 1; nCol <= nColumns; nCol++)
        {
            int sret;

            nOptimalDisplayWidth = OptimalDisplayWidth(hStmt, nCol, nUserWidth);
            nReturn = SQLGetData(hStmt, nCol, SQL_C_CHAR, (SQLPOINTER)szColumnValue, max_col_size + 1, &nIndicator);
            szColumnValue[max_col_size] = '\0';

            if (nReturn == SQL_SUCCESS && nIndicator != SQL_NULL_DATA)
            {
                sret = sprintf((char *)szColumn, "| %-*.*s",
                               (int)nOptimalDisplayWidth, (int)nOptimalDisplayWidth, szColumnValue);
                if (sret < 0)
                    sprintf((char *)szColumn, "| %-*.*s",
                            (int)nOptimalDisplayWidth, (int)nOptimalDisplayWidth, "**ERROR**");
            }
            else if (nReturn == SQL_SUCCESS_WITH_INFO)
            {
                sret = sprintf((char *)szColumn, "| %-*.*s...",
                               (int)nOptimalDisplayWidth - 3, (int)nOptimalDisplayWidth - 3, szColumnValue);
                if (sret < 0)
                    sprintf((char *)szColumn, "| %-*.*s",
                            (int)nOptimalDisplayWidth, (int)nOptimalDisplayWidth, "**ERROR**");
            }
            else if (nReturn == SQL_ERROR)
            {
                break;
            }
            else
            {
                sprintf((char *)szColumn, "| %-*s", (int)nOptimalDisplayWidth, "");
            }
            fputs((char *)szColumn, stdout);
            res += (char *)szColumn;
        } /* for columns */

        nRows++;
        printf("|\n");
        res += "|\n";
        nReturn = SQLFetch(hStmt);
    } /* while rows */

    free(szColumnValue);
    free(szColumn);

    return nRows;
}


static void
WriteFooterNormal(SQLHSTMT hStmt, SQLCHAR *szSepLine, SQLLEN nRows, std::string &res)
{
    SQLLEN nRowsAffected = -1;

    fputs((char *)szSepLine, stdout);
    res += (char *)szSepLine;

    SQLRowCount(hStmt, &nRowsAffected);
    printf("[K-ODBC] SQLRowCount returns %ld\n", nRowsAffected);

    if (nRows)
    {
        printf("%ld rows fetched\n", nRows);
    }
}


static void mem_error(int line)
{
    fprintf(stderr, "[K-ODBC] ERROR: memory allocation fail before line %d\n", line);
    exit(-1);
}

static SQLUINTEGER
OptimalDisplayWidth( SQLHSTMT hStmt, SQLINTEGER nCol, int nUserWidth )
{
    SQLUINTEGER nLabelWidth                     = 10;
    SQLULEN nDataWidth                      = 10;
    SQLUINTEGER nOptimalDisplayWidth            = 10;
    SQLCHAR     szColumnName[MAX_DATA_WIDTH+1]; 

    *szColumnName = '\0';

    SQLColAttribute( hStmt, nCol, SQL_DESC_DISPLAY_SIZE, NULL, 0, NULL, (SQLLEN*)&nDataWidth );
    SQLColAttribute( hStmt, nCol, SQL_DESC_LABEL, szColumnName, sizeof(szColumnName), NULL, NULL );
    nLabelWidth = strlen((char*) szColumnName );

    /*
     * catch sqlserver var(max) types
     */

    if ( nDataWidth == 0 ) {
        nDataWidth = max_col_size;
    }

    nOptimalDisplayWidth = max( nLabelWidth, nDataWidth );

    if ( nUserWidth > 0 )
        nOptimalDisplayWidth = min( nOptimalDisplayWidth, nUserWidth );

    if ( nOptimalDisplayWidth > max_col_size )
        nOptimalDisplayWidth = max_col_size;

    return nOptimalDisplayWidth;
}
