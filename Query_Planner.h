#pragma once
#include "Meta_Data_Manager.h"
#include "Parsed_Query.h"
#include "keti_log.h"

class Query_Planner {
public:
	Query_Planner() {}
    void Parse(Meta_Data_Manager &mata_data_manager,Parsed_Query &parsed_query){
    KETILOG::DEBUGLOG("Query Planner","Start Query Processing");
    KETILOG::DEBUGLOG("Query Planner","Parsing Query ...");

        if(parsed_query.Get_Ori_Query() == "TPC-H_01") { //TPC-H Query 1
            parsed_query.Set_Parsed_Query("SELECT l_returnflag,\n\
       l_linestatus,\n\
       SUM(l_quantity)                                           AS sum_qty,\n\
       SUM(l_extendedprice)                                      AS sum_base_price,\n\
       SUM(l_extendedprice * ( 1 - l_discount ))                 AS sum_disc_price,\n\
       SUM(l_extendedprice * ( 1 - l_discount ) * ( 1 + l_tax )) AS sum_charge,\n\
       Avg(l_quantity)                                           AS avg_qty,\n\
       Avg(l_extendedprice)                                      AS avg_price,\n\
       Avg(l_discount)                                           AS avg_disc,\n\
       Count(*)                                                  AS count_order\n\
FROM   lineitem\n\
WHERE  l_shipdate <= DATE ('1998-12-01') - interval '108' day\n\
GROUP  BY l_returnflag,\n\
          l_linestatus\n\
ORDER  BY l_returnflag,\n\
          l_linestatus;");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "TPC-H_02"){ //TPC-H Query 2
            parsed_query.Set_Parsed_Query("select \n\
    s_acctbal, \n\
    s_name, \n\
    n_name, \n\
    p_partkey, \n\
    p_mfgr, \n\
    s_address, \n\
    s_phone, \n\
    s_comment \n\
from \n\
    part, \n\
    supplier, \n\
    partsupp, \n\
    nation, \n\
    region \n\
where \n\
    p_partkey = ps_partkey and \n\
    s_suppkey = ps_suppkey and \n\
    p_size = 30 and \n\
    p_type like '%steel' and \n\
    s_nationkey = n_nationkey and\n\
    n_regionkey = r_regionkey and \n\
    r_name = 'asia' and \n\
    ps_supplycost = (\n\
        select \n\
            min(ps_supplycost) \n\
        from \n\
            partsupp, \n\
            supplier, \n\
            nation, \n\
            region \n\
        where \n\
            p_partkey = ps_partkey and \n\
            s_suppkey = ps_suppkey and \n\
            s_nationkey = n_nationkey and \n\
            n_regionkey = r_regionkey and \n\
            r_name = 'asia'\n\
    ) \n\
order by \n\
    s_acctbal desc, \n\
    n_name, \n\
    s_name, \n\
    p_partkey \n\
limit 100;");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "TPC-H_03"){ //TPC-H Query 3
            parsed_query.Set_Parsed_Query("SELECT   l_orderkey, \n\
         Sum(l_extendedprice * (1 - l_discount)) AS revenue,\n\
         o_orderdate,\n\
         o_shippriority\n\
FROM     customer,\n\
         orders,\n\
         lineitem\n\
WHERE    c_mktsegment = 'automobile'\n\
AND      c_custkey = o_custkey\n\
AND      l_orderkey = o_orderkey\n\
AND      o_orderdate < date '1995-03-13'\n\
AND      l_shipdate >  date '1995-03-13'\n\
GROUP BY l_orderkey,\n\
         o_orderdate,\n\
         o_shippriority\n\
ORDER BY revenue DESC,\n\
         o_orderdate\n\
LIMIT    10;");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "TPC-H_04"){ //TPC-H Query 4
            parsed_query.Set_Parsed_Query("select \n\
    o_orderpriority, count(*) as order_count \n\
from \n\
    orders \n\
where \n\
    o_orderdate >= date '1995-01-01' and \n\
    o_orderdate < date '1995-01-01' + interval '3' month and \n\
    exists (\n\
        select \n\
            * \n\
        from \n\
            lineitem \n\
        where \n\
            l_orderkey = o_orderkey and \n\
            l_commitdate < l_receiptdate\n\
        ) \n\
group by \n\
    o_orderpriority \n\
order by \n\
    o_orderpriority;");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "TPC-H_05"){ //TPC-H Query 5
            parsed_query.Set_Parsed_Query("select \n\
    n_name, \n\
    sum(l_extendedprice * (1 - l_discount)) as revenue \n\
from \n\
    customer, \n\
    orders, \n\
    lineitem, \n\
    supplier, \n\
    nation, \n\
    region \n\
where \n\
    c_custkey = o_custkey and \n\
    l_orderkey = o_orderkey and \n\
    l_suppkey = s_suppkey and \n\
    c_nationkey = s_nationkey and \n\
    s_nationkey = n_nationkey and \n\
    n_regionkey = r_regionkey and \n\
    r_name = 'middle east' and \n\
    o_orderdate >= date '1994-01-01' and \n\
    o_orderdate < date '1994-01-01' + interval '1' year\n\
group by \n\
    n_name \n\
order by \n\
    revenue desc;");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "TPC-H_06"){ //TPC-H Query 6
            parsed_query.Set_Parsed_Query("select\n\
	sum(l_extendedprice * l_discount) as revenue\n\
from\n\
	lineitem\n\
where\n\
	l_shipdate >= date ('1994-01-01')\n\
	and l_shipdate < date ('1994-01-01') + interval '1' year\n\
	and l_discount between 0.06 - 0.01 and 0.06 + 0.01\n\
	and l_quantity < 24;");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "TPC-H_07"){ //TPC-H Query 7
            parsed_query.Set_Parsed_Query("select \n\
    supp_nation, \n\
    cust_nation, \n\
    l_year, \n\
    sum(volume) as revenue \n\
from ( \n\
    select \n\
        n1.n_name as supp_nation, \n\
        n2.n_name as cust_nation, \n\
        extract(year from l_shipdate) as l_year, \n\
        l_extendedprice * (1 - l_discount) as volume \n\
    from \n\
        supplier, \n\
        lineitem, \n\
        orders, \n\
        customer, \n\
        nation n1, \n\
        nation n2 \n\
    where \n\
        s_suppkey = l_suppkey and \n\
        o_orderkey = l_orderkey and \n\
        c_custkey = o_custkey and \n\
        s_nationkey = n1.n_nationkey and\n\
        c_nationkey = n2.n_nationkey and \n\
        (\n\
            (n1.n_name = 'japan' and n2.n_name = 'india') or \n\
            (n1.n_name = 'india' and n2.n_name = 'japan')\n\
        ) and \n\
        l_shipdate between date '1995-01-01' and date '1996-12-31'\n\
    ) as shipping \n\
group by \n\
    supp_nation, \n\
    cust_nation, \n\
    l_year \n\
order by \n\
    supp_nation, \n\
    cust_nation, \n\
    l_year;");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "TPC-H_08"){ //TPC-H Query 8
            parsed_query.Set_Parsed_Query("select \n\
    o_year, \n\
    sum(case when nation = 'india' then volume else 0 end) / sum(volume) as mkt_share \n\
from (\n\
    select \n\
        extract(year from o_orderdate) as o_year,	\n\
        l_extendedprice * (1 - l_discount) as volume, \n\
        n2.n_name as nation \n\
    from \n\
        part, \n\
        supplier, \n\
        lineitem, \n\
        orders, \n\
        customer, \n\
        nation n1, \n\
        nation n2, \n\
        region \n\
    where \n\
        p_partkey = l_partkey and \n\
        s_suppkey = l_suppkey and \n\
        l_orderkey = o_orderkey and \n\
        o_custkey = c_custkey and \n\
        c_nationkey = n1.n_nationkey and \n\
        n1.n_regionkey = r_regionkey and \n\
        r_name = 'asia'	and \n\
        s_nationkey = n2.n_nationkey and \n\
        o_orderdate between date '1995-01-01' and date '1996-12-31'and \n\
        p_type = 'small plated copper'\n\
    ) as all_nations \n\
group by \n\
    o_year \n\
order by \n\
    o_year;");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "TPC-H_09"){ //TPC-H Query 9
            parsed_query.Set_Parsed_Query("select \n\
    nation, \n\
    o_year, \n\
    sum(amount) as sum_profit \n\
from (\n\
    select \n\
        n_name as nation, \n\
        extract(year from o_orderdate) as o_year, \n\
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount \n\
    from \n\
        part, \n\
        supplier, \n\
        lineitem, \n\
        partsupp, \n\
        orders, \n\
        nation \n\
    where \n\
        s_suppkey = l_suppkey and \n\
        ps_suppkey = l_suppkey and \n\
        ps_partkey = l_partkey and \n\
        p_partkey = l_partkey and \n\
        o_orderkey = l_orderkey and \n\
        s_nationkey = n_nationkey and \n\
        p_name like '%dim%'\n\
    ) as profit \n\
group by \n\
    nation, \n\
    o_year \n\
order by \n\
    nation, \n\
    o_year desc;");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "TPC-H_10"){ //TPC-H Query 10
            parsed_query.Set_Parsed_Query("select c_custkey,\n\
	c_name,\n\
	sum(l_extendedprice * (1 - l_discount)) as revenue,\n\
	c_acctbal,\n\
	n_name,\n\
	c_address,\n\
	c_phone,\n\
	c_comment\n\
from\n\
	customer,\n\
	orders,\n\
	lineitem,\n\
	nation\n\
where\n\
	c_custkey = o_custkey\n\
	and l_orderkey = o_orderkey\n\
	and o_orderdate >= date '1993-08-01'\n\
	and o_orderdate < date '1993-08-01' + interval '3' month\n\
	and l_returnflag = 'r'\n\
	and c_nationkey = n_nationkey\n\
group by\n\
	c_custkey,\n\
	c_name,\n\
	c_acctbal,\n\
	c_phone,\n\
	n_name,\n\
	c_address,\n\
	c_comment\n\
order by\n\
	revenue desc\n\
limit 20;");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "TPC-H_11"){ //TPC-H Query 11
            parsed_query.Set_Parsed_Query("SELECT ps_partkey,\n\
       Sum(ps_supplycost * ps_availqty) AS value\n\
FROM   partsupp,\n\
       supplier,\n\
       nation\n\
WHERE  ps_suppkey = s_suppkey\n\
       AND s_nationkey = n_nationkey\n\
       AND n_name = 'mozambique'\n\
GROUP  BY ps_partkey\n\
HAVING Sum(ps_supplycost * ps_availqty) > (SELECT\n\
       Sum(ps_supplycost * ps_availqty) * 0.0001000000\n\
                                           FROM   partsupp,\n\
                                                  supplier,\n\
                                                  nation\n\
                                           WHERE  ps_suppkey = s_suppkey\n\
                                                  AND s_nationkey = n_nationkey\n\
                                                  AND n_name = 'mozambique')\n\
ORDER  BY value DESC;");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "TPC-H_12"){ //TPC-H Query 12
            parsed_query.Set_Parsed_Query("SELECT l_shipmode,\n\
       SUM(CASE\n\
             WHEN o_orderpriority = '1-urgent'\n\
                   OR o_orderpriority = '2-high' THEN 1\n\
             ELSE 0\n\
           END) AS high_line_count,\n\
       SUM(CASE\n\
             WHEN o_orderpriority <> '1-urgent'\n\
                  AND o_orderpriority <> '2-high' THEN 1\n\
             ELSE 0\n\
           END) AS low_line_count\n\
FROM   orders,\n\
       lineitem\n\
WHERE  o_orderkey = l_orderkey\n\
       AND l_shipmode IN ( 'rail', 'fob' )\n\
       AND l_commitdate < l_receiptdate\n\
       AND l_shipdate < l_commitdate\n\
       AND l_receiptdate >= DATE '1997-01-01'\n\
       AND l_receiptdate < DATE '1997-01-01' + interval '1' year\n\
GROUP  BY l_shipmode\n\
ORDER  BY l_shipmode; ");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "TPC-H_13"){ //TPC-H Query 13
            parsed_query.Set_Parsed_Query("SELECT c_count,\n\
       Count(*) AS custdist\n\
FROM   (SELECT c_custkey,\n\
               Count(o_orderkey) AS c_count\n\
        FROM   customer\n\
               LEFT OUTER JOIN orders\n\
                            ON c_custkey = o_custkey\n\
                               AND o_comment NOT LIKE '%pending%deposits%'\n\
        GROUP  BY c_custkey) c_orders\n\
GROUP  BY c_count\n\
ORDER  BY custdist DESC,\n\
          c_count DESC; ");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "TPC-H_14"){ //TPC-H Query 14
            parsed_query.Set_Parsed_Query("SELECT 100.00 * SUM(CASE\n\
                      WHEN p_type LIKE 'promo%' THEN l_extendedprice *\n\
                                                     ( 1 - l_discount )\n\
                      ELSE 0\n\
                    END) / SUM(l_extendedprice * ( 1 - l_discount )) AS\n\
       promo_revenue\n\
FROM   lineitem,\n\
       part\n\
WHERE  l_partkey = p_partkey\n\
       AND l_shipdate >= DATE '1996-12-01'\n\
       AND l_shipdate < DATE '1996-12-01' + interval '1' month;");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "TPC-H_15"){ //TPC-H Query 15
            parsed_query.Set_Parsed_Query("CREATE VIEW revenue0\n\
(supplier_no, total_revenue)\n\
AS\n\
  SELECT l_suppkey,\n\
         SUM(l_extendedprice * ( 1 - l_discount ))\n\
  FROM   lineitem\n\
  WHERE  l_shipdate >= DATE '1997-07-01'\n\
         AND l_shipdate < DATE '1997-07-01' + interval '3' month\n\
  GROUP  BY l_suppkey;\n\
SELECT s_suppkey,\n\
       s_name,\n\
       s_address,\n\
       s_phone,\n\
       total_revenue\n\
FROM   supplier,\n\
       revenue0\n\
WHERE  s_suppkey = supplier_no\n\
       AND total_revenue = (SELECT Max(total_revenue)\n\
                            FROM   revenue0)\n\
ORDER  BY s_suppkey;\n\
DROP VIEW revenue0;");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "TPC-H_16"){ //TPC-H Query 16
            parsed_query.Set_Parsed_Query("SELECT p_brand,\n\
       p_type,\n\
       p_size,\n\
       Count(DISTINCT ps_suppkey) AS supplier_cnt\n\
FROM   partsupp,\n\
       part\n\
WHERE  p_partkey = ps_partkey\n\
       AND p_brand <> 'brand#34'\n\
       AND p_type NOT LIKE 'large brushed%'\n\
       AND p_size IN ( 48, 19, 12, 4,\n\
                       41, 7, 21, 39 )\n\
       AND ps_suppkey NOT IN (SELECT s_suppkey\n\
                              FROM   supplier\n\
                              WHERE  s_comment LIKE '%customer%complaints%')\n\
GROUP  BY p_brand,\n\
          p_type,\n\
          p_size\n\
ORDER  BY supplier_cnt DESC,\n\
          p_brand,\n\
          p_type,\n\
          p_size;");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "TPC-H_17"){ //TPC-H Query 17            
            parsed_query.Set_Parsed_Query("SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly \n\
FROM   lineitem, \n\
       part \n\
WHERE  p_partkey = l_partkey \n\
       AND p_brand = 'Brand#44' \n\
       AND p_container = 'WRAP PKG' \n\
       AND l_quantity < (SELECT 0.2 * AVG(l_quantity) \n\
                         FROM   lineitem \n\
                         WHERE  l_partkey = p_partkey);");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "TPC-H_18"){ //TPC-H Query 18
            parsed_query.Set_Parsed_Query("SELECT c_name,\n\
       c_custkey,\n\
       o_orderkey,\n\
       o_orderdate,\n\
       o_totalprice,\n\
       Sum(l_quantity)\n\
FROM   customer,\n\
       orders,\n\
       lineitem\n\
WHERE  o_orderkey IN (SELECT l_orderkey\n\
                      FROM   lineitem\n\
                      GROUP  BY l_orderkey\n\
                      HAVING Sum(l_quantity) > 314)\n\
       AND c_custkey = o_custkey\n\
       AND o_orderkey = l_orderkey\n\
GROUP  BY c_name,\n\
          c_custkey,\n\
          o_orderkey,\n\
          o_orderdate,\n\
          o_totalprice\n\
ORDER  BY o_totalprice DESC,\n\
          o_orderdate\n\
LIMIT  100; ");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "TPC-H_19"){ //TPC-H Query 19
            parsed_query.Set_Parsed_Query("SELECT Sum(l_extendedprice * ( 1 - l_discount )) AS revenue\n\
FROM   lineitem,\n\
       part\n\
WHERE  ( p_partkey = l_partkey\n\
         AND p_brand = 'brand#52'\n\
         AND p_container IN ( 'sm case', 'sm box', 'sm pack', 'sm pkg' )\n\
         AND l_quantity >= 4\n\
         AND l_quantity <= 4 + 10\n\
         AND p_size BETWEEN 1 AND 5\n\
         AND l_shipmode IN ( 'air', 'air reg' )\n\
         AND l_shipinstruct = 'deliver in person' )\n\
        OR ( p_partkey = l_partkey\n\
             AND p_brand = 'brand#11'\n\
             AND p_container IN ( 'med bag', 'med box', 'med pkg', 'med pack' )\n\
             AND l_quantity >= 18\n\
             AND l_quantity <= 18 + 10\n\
             AND p_size BETWEEN 1 AND 10\n\
             AND l_shipmode IN ( 'air', 'air reg' )\n\
             AND l_shipinstruct = 'deliver in person' )\n\
        OR ( p_partkey = l_partkey\n\
             AND p_brand = 'brand#51'\n\
             AND p_container IN ( 'lg case', 'lg box', 'lg pack', 'lg pkg' )\n\
             AND l_quantity >= 29\n\
             AND l_quantity <= 29 + 10\n\
             AND p_size BETWEEN 1 AND 15\n\
             AND l_shipmode IN ( 'air', 'air reg' )\n\
             AND l_shipinstruct = 'deliver in person' );");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "TPC-H_20"){ //TPC-H Query 20
            parsed_query.Set_Parsed_Query("SELECT s_name,\n\
       s_address\n\
FROM   supplier,\n\
       nation\n\
WHERE  s_suppkey IN (SELECT ps_suppkey\n\
                     FROM   partsupp\n\
                     WHERE  ps_partkey IN (SELECT p_partkey\n\
                                           FROM   part\n\
                                           WHERE  p_name LIKE 'green%')\n\
                            AND ps_availqty > (SELECT 0.5 * SUM(l_quantity)\n\
                                               FROM   lineitem\n\
                                               WHERE  l_partkey = ps_partkey\n\
                                                      AND l_suppkey = ps_suppkey\n\
                                                      AND l_shipdate >= DATE\n\
                                                          '1993-01-01'\n\
                                                      AND l_shipdate < DATE\n\
                                                          '1993-01-01' +\n\
                                                          interval\n\
                                                                       '1' year\n\
                                              ))\n\
       AND s_nationkey = n_nationkey\n\
       AND n_name = 'algeria'\n\
ORDER  BY s_name; ");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "TPC-H_21"){ //TPC-H Query 21
            parsed_query.Set_Parsed_Query("SELECT s_name,\n\
       Count(*) AS numwait\n\
FROM   supplier,\n\
       lineitem l1,\n\
       orders,\n\
       nation\n\
WHERE  s_suppkey = l1.l_suppkey\n\
       AND o_orderkey = l1.l_orderkey\n\
       AND o_orderstatus = 'f'\n\
       AND l1.l_receiptdate > l1.l_commitdate\n\
       AND EXISTS (SELECT *\n\
                   FROM   lineitem l2\n\
                   WHERE  l2.l_orderkey = l1.l_orderkey\n\
                          AND l2.l_suppkey <> l1.l_suppkey)\n\
       AND NOT EXISTS (SELECT *\n\
                       FROM   lineitem l3\n\
                       WHERE  l3.l_orderkey = l1.l_orderkey\n\
                              AND l3.l_suppkey <> l1.l_suppkey\n\
                              AND l3.l_receiptdate > l3.l_commitdate)\n\
       AND s_nationkey = n_nationkey\n\
       AND n_name = 'egypt'\n\
GROUP  BY s_name\n\
ORDER  BY numwait DESC,\n\
          s_name\n\
LIMIT  100; ");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "TPC-H_22"){ //TPC-H Query 22
            parsed_query.Set_Parsed_Query("SELECT cntrycode,\n\
       Count(*)       AS numcust,\n\
       Sum(c_acctbal) AS totacctbal\n\
FROM   (SELECT Substring(c_phone FROM 1 FOR 2) AS cntrycode,\n\
               c_acctbal\n\
        FROM   customer\n\
        WHERE  Substring(c_phone FROM 1 FOR 2) IN ( '20', '40', '22', '30',\n\
                                                    '39', '42', '21' )\n\
               AND c_acctbal > (SELECT Avg(c_acctbal)\n\
                                FROM   customer\n\
                                WHERE  c_acctbal > 0.00\n\
                                       AND Substring(c_phone FROM 1 FOR 2) IN (\n\
                                           '20', '40', '22', '30',\n\
                                           '39', '42', '21' ))\n\
               AND NOT EXISTS (SELECT *\n\
                               FROM   orders\n\
                               WHERE  o_custkey = c_custkey)) AS custsale\n\
GROUP  BY cntrycode\n\
ORDER  BY cntrycode;");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        } else if(parsed_query.Get_Ori_Query() == "test_index_scan1"){ //custom query jhk
            parsed_query.Set_Parsed_Query("");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        }else if(parsed_query.Get_Ori_Query() == "test_index_scan2"){
            parsed_query.Set_Parsed_Query("");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        }else if(parsed_query.Get_Ori_Query() == "test_lineitem"){
            parsed_query.Set_Parsed_Query("");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        }else if(parsed_query.Get_Ori_Query() == "test_customer"){
            parsed_query.Set_Parsed_Query("");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        }else if(parsed_query.Get_Ori_Query() == "test_orders"){
            parsed_query.Set_Parsed_Query("");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        }else if(parsed_query.Get_Ori_Query() == "test_supplier"){
            parsed_query.Set_Parsed_Query("");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        }else if(parsed_query.Get_Ori_Query() == "test_part"){
            parsed_query.Set_Parsed_Query("");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        }else if(parsed_query.Get_Ori_Query() == "test_partsupp"){
            parsed_query.Set_Parsed_Query("");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        }else if(parsed_query.Get_Ori_Query() == "test_region"){
            parsed_query.Set_Parsed_Query("");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        }else if(parsed_query.Get_Ori_Query() == "test_nation"){
            parsed_query.Set_Parsed_Query("");
          parsed_query.Set_Query_Type_As_PushdownQuery();
        }else { //Other Query
            parsed_query.Set_Parsed_Query(parsed_query.Get_Ori_Query().c_str());
        }
    } 

private:
};
