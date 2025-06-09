
#include "SQLDialect.h"

vector<vector<string>> SQLDialect::createSchemaStmt = {
        {
                // PostgreSQL
                "CREATE SCHEMA HAT",
                "CREATE TABLE HAT.PART (\n"
                "	P_PARTKEY INT NOT NULL,\n"
                "	P_NAME VARCHAR(22),\n"
                "	P_MFGR CHAR(6),\n"
                "	P_CATEGORY CHAR(7),\n"
                "	P_BRAND1 CHAR(9),\n"
                "	P_COLOR VARCHAR(11),\n"
                "	P_TYPE VARCHAR(25),\n"
                "	P_SIZE INT,\n"
                "	P_CONTAINER VARCHAR(10),\n"
                "	P_PRICE DECIMAL,\n"
                "	PRIMARY KEY (P_PARTKEY)\n"
                ")",
                "CREATE TABLE HAT.CUSTOMER (\n"
                "	C_CUSTKEY INT NOT NULL,\n"
                "	C_NAME VARCHAR(25),\n"
                "	C_ADDRESS VARCHAR(25),\n"
                "	C_CITY CHAR(10),\n"
                "	C_NATION VARCHAR(15),\n"
                "	C_REGION VARCHAR(12),\n"
                "	C_PHONE CHAR(15),\n"
                "	C_MKTSEGMENT VARCHAR(10),\n"
                "	C_PAYMENTCNT INTEGER,\n"
                "	PRIMARY KEY (C_CUSTKEY)\n"
                ")",
                "CREATE TABLE HAT.SUPPLIER (\n"
                "	S_SUPPKEY INTEGER NOT NULL,\n"
                "	S_NAME VARCHAR(25),\n"
                "	S_ADDRESS VARCHAR(25),\n"
                "	S_CITY CHAR(10),\n"
                "	S_NATION VARCHAR(15),\n"
                "	S_REGION CHAR(12),\n"
                "	S_PHONE CHAR(15),\n"
                "	S_YTD DECIMAL,\n"
                "	PRIMARY KEY (S_SUPPKEY)\n"
                ")",
                "CREATE TABLE HAT.DATE (\n"
                "	D_DATEKEY INTEGER NOT NULL,\n"
                "	D_DATE VARCHAR(18),\n"
                "	D_DATEOFWEEK VARCHAR(9),\n"
                "	D_MONTH VARCHAR(9),\n"
                "	D_YEAR INTEGER,\n"
                "	D_YEARMONTHNUM INTEGER,\n"
                "	D_YEARMONTH CHAR(7),\n"
                "	D_DAYNUMINWEEK INTEGER,\n"
                "	D_DAYNUMINMONTH INTEGER,\n"
                "	D_DAYNUMINYEAR INTEGER,\n"
                "	D_MONTHNUMINYEAR INTEGER,\n"
                "	D_WEEKNUMINYEAR INTEGER,\n"
                "	D_SELLINGSEASON CHAR(15),\n"
                "	D_LASTDAYINWEEKFL BOOLEAN,\n"
                "	D_LASTDAYINMONTHFL BOOLEAN,\n"
                "	D_HOLIDAYFL BOOLEAN,\n"
                "	D_WEEKDAYFL BOOLEAN,\n"
                "	PRIMARY KEY (D_DATEKEY)\n"
                ")",
                "CREATE TABLE HAT.LINEORDER (\n"
                "	LO_ORDERKEY INTEGER NOT NULL,\n"
                "	LO_LINENUMBER INTEGER NOT NULL,\n"
                "	LO_CUSTKEY INTEGER,\n"
                "	LO_PARTKEY INTEGER,\n"
                "	LO_SUPPKEY INTEGER,\n"
                "	LO_ORDERDATE INTEGER,\n"
                "	LO_ORDPRIORITY CHAR(15),\n"
                "	LO_SHIPPRIORITY CHAR(1),\n"
                "	LO_QUANTITY INTEGER,\n"
                "	LO_EXTENDEDPRICE DECIMAL,\n"
                "	LO_ORDTOTALPRICE DECIMAL,\n"
                "	LO_DISCOUNT INTEGER,\n"
                "	LO_REVENUE DECIMAL,\n"
                "	LO_SUPPLYCOST DECIMAL,\n"
                "	LO_TAX INTEGER,\n"
                "	LO_COMMITDATE INTEGER,\n"
                "	LO_SHIPMODE CHAR(10),\n"
                "	PRIMARY KEY (LO_ORDERKEY,LO_LINENUMBER)\n"
                ")",
                "CREATE TABLE HAT.HISTORY (\n"
                "	H_ORDERKEY INTEGER NOT NULL,\n"
                "	H_CUSTKEY INTEGER NOT NULL,\n"
                "	H_AMOUNT DECIMAL\n"
                ")",
                "CREATE OR REPLACE FUNCTION HAT.NEWORDER(orderKey INTEGER, n INTEGER, custName VARCHAR(25), \n"
                "TEXT,  TEXT, TEXT, TEXT,  TEXT, TEXT, TEXT,  TEXT, TEXT, TEXT, TEXT, TEXT, tableName TEXT, txnNum INTEGER) RETURNS void\n"
                "       LANGUAGE plpgsql\n"
                "       AS $$\n"
                "       DECLARE\n"
                "               partKeys INTEGER[] := string_to_array($4, ',');\n"
                "               suppNames VARCHAR(25)[] := string_to_array($5, ',');\n"
                "               dates VARCHAR(18)[] := string_to_array($6, '^');\n"
                "               ordPriorities CHAR(15)[] := string_to_array($7, ',');\n"
                "               shipPriorities CHAR(1)[] := string_to_array($8, ',');\n"
                "               quantities INTEGER[] := string_to_array($9, ',');\n"
                "               extendedPrices DECIMAL[] := string_to_array($10, ',');\n"
                "               discounts INTEGER[] := string_to_array($11, ',');\n"
                "               revenues DECIMAL[] := string_to_array($12, ',');\n"
                "               supplyCosts DECIMAL[] := string_to_array($13, ',');\n"
                "               taxes INTEGER[] := string_to_array($14, ',');\n"
                "               shipModes CHAR(10)[] := string_to_array($15, ',');\n"
                "               i INTEGER := 0;\n"
                "               price DECIMAL := 0;\n"
                "               supplier INTEGER := 0;\n"
                "               dateKey INTEGER := 0;\n"
                "               prices DECIMAL[] := array_fill(0.0 ,ARRAY[n]);\n"
                "               suppKeys INTEGER[] := array_fill(0 ,ARRAY[n]); \n"
                "               dateKeys INTEGER[] := array_fill(0, ARRAY[n]);\n"
                "               custKey INTEGER := 0;\n"
                "               totalOrderCost DECIMAL := 0.0;\n"
                "       BEGIN\n"
                "	        SELECT C_CUSTKEY INTO custKey FROM HAT.CUSTOMER WHERE C_NAME = custName;\n"
                "               EXECUTE format('UPDATE HAT.%I SET F_TXNNUM = %L', tableName, txnnum);\n"
                "	        LOOP\n"
                "		        exit when i = n;\n"
                "		        i := i + 1;\n"
                "		        SELECT P_PRICE INTO price FROM HAT.PART WHERE P_PARTKEY = partKeys[i];\n"
                "		        prices[i] := price;\n"	
                "		        SELECT S_SUPPKEY INTO supplier FROM HAT.SUPPLIER WHERE S_NAME = suppNames[i];\n"
                "		        suppKeys[i] := supplier;\n"
                "		        SELECT D_DATEKEY INTO dateKey FROM HAT.DATE WHERE D_DATE = dates[i];\n"
                "		        dateKeys[i] := dateKey;\n"
                "               extendedPrices[i] = extendedPrices[i]*prices[i];\n"
                "               revenues[i] = revenues[i]*extendedPrices[i];\n"
                "               totalOrderCost := totalOrderCost + revenues[i];\n"
                "               INSERT INTO HAT.LINEORDER(LO_ORDERKEY, LO_LINENUMBER, LO_CUSTKEY, LO_PARTKEY, LO_SUPPKEY, LO_ORDERDATE, \n"
                "                  LO_ORDPRIORITY, LO_SHIPPRIORITY, LO_QUANTITY, LO_EXTENDEDPRICE, LO_DISCOUNT, LO_REVENUE, LO_SUPPLYCOST, \n"
                "                  LO_TAX, LO_COMMITDATE, LO_SHIPMODE)\n"
                "               VALUES (orderKey, i, custKey, partKeys[i], suppKeys[i], dateKeys[i], ordPriorities[i], shipPriorities[i], \n"
                "                  quantities[i], extendedPrices[i], discounts[i], revenues[i], supplyCosts[i], taxes[i], dateKeys[i], shipModes[i]);\n"
                "          END LOOP;\n"
                "          UPDATE HAT.LINEORDER SET LO_ORDTOTALPRICE = totalOrderCost WHERE LO_ORDERKEY = orderKey;"
                "       END;\n"
                "       $$;\n",
                "CREATE FUNCTION HAT.PAYMENT(CustKey INTEGER, SuppKey INTEGER, amount DECIMAL, OrderKey INTEGER, tableName TEXT, txnNum INTEGER) RETURNS void\n"
                "       LANGUAGE plpgsql\n"
                "       AS $$\n"
                "       BEGIN\n"
                "               EXECUTE format('UPDATE HAT.%I SET F_TXNNUM = %L', tableName, txnnum);\n"
		"               UPDATE HAT.CUSTOMER\n"
                "                       SET C_PAYMENTCNT = C_PAYMENTCNT + 1\n"
                "                       WHERE C_CUSTKEY = CustKey;\n"                
                "               UPDATE HAT.SUPPLIER\n"
                "                       SET S_YTD = S_YTD + amount\n"
                "                       WHERE S_SUPPKEY = @SuppKey;\n"
                "               INSERT INTO HAT.HISTORY(H_ORDERKEY, H_CUSTKEY, H_AMOUNT)\n"
                "                       VALUES(OrderKey, CustKey, amount);\n"
                "       END;\n"
                "       $$;\n",
                "CREATE FUNCTION HAT.COUNTORDERS(CustName VARCHAR(25), tableName TEXT, txnNum INTEGER) RETURNS INTEGER\n" 
                "       LANGUAGE plpgsql\n"
                "       AS $$\n"
                "               DECLARE custKey INTEGER;\n"
                "               DECLARE cnt INTEGER;\n"
                "       BEGIN\n"
                "               EXECUTE format('UPDATE HAT.%I SET F_TXNNUM = %L', tableName, txnnum);\n"
		"               SELECT C_CUSTKEY INTO custKey FROM HAT.CUSTOMER WHERE C_NAME = CustName;\n"
                "               SELECT COUNT(DISTINCT LO_ORDERKEY) INTO cnt FROM HAT.LINEORDER WHERE LO_CUSTKEY = custKey;\n"
                "               RETURN cnt;\n"
                "       END;\n"
                "       $$;\n"

        }
};

vector<vector<string>> SQLDialect::dropSchemaStmt = {
        {
                // PostgreSQL
                "DROP INDEX hat.idx_p_category;",
                "DROP INDEX hat.idx_p_brand1;",
                "DROP INDEX hat.idx_p_mfgr;",
                "DROP INDEX hat.idx_s_region;",
                "DROP INDEX hat.idx_c_supplier;",
                "DROP INDEX hat.idx_c_city;",
                "DROP INDEX hat.idx_c_region;",
                "DROP INDEX hat.idx_c_nation;",
                "DROP INDEX hat.idx_c_customer;",
                "DROP INDEX hat.idx_d_year;",
                "DROP INDEX hat.idx_d_yearmonthnum;",
                "DROP INDEX hat.idx_d_weeknuminyear;",
                "DROP INDEX hat.idx_lo_quantity;",
                "DROP INDEX hat.idx_lo_discount;",
                "DROP INDEX hat.idx_lo_orderdate;",
                "DROP INDEX hat.idx_lo_partkey;",
                "DROP INDEX hat.idx_lo_suppkey;",
                "DROP INDEX hat.idx_lo_custkey;",
                "DROP INDEX hat.idx_c_name;",
                "DROP INDEX hat.idx_s_name;",
                "DROP INDEX hat.idx_d_date;",
                "DROP FUNCTION IF EXISTS HAT.NEWORDER(orderKey INTEGER, n INTEGER, custName VARCHAR(25), TEXT,  TEXT, TEXT, TEXT,  TEXT, TEXT, TEXT,  TEXT, TEXT, TEXT, TEXT, TEXT,  tableName TEXT, txnNum INTEGER)",
                "DROP FUNCTION IF EXISTS HAT.PAYMENT(CustKey INTEGER, SuppKey INTEGER, amount DECIMAL, OrderKey INTEGER, tableName TEXT, txnNum INTEGER)",
                "DROP FUNCTION IF EXISTS HAT.COUNTORDERS(CustName VARCHAR(25), tableName TEXT, txnNum INTEGER)",
                "DROP TABLE IF EXISTS HAT.HISTORY",
                "DROP TABLE IF EXISTS HAT.LINEORDER",
                "DROP TABLE IF EXISTS HAT.DATE",
                "DROP TABLE IF EXISTS HAT.PART",
                "DROP TABLE IF EXISTS HAT.CUSTOMER",
                "DROP TABLE IF EXISTS HAT.SUPPLIER",
                //"DROP TABLE IF EXISTS HAT.FRESHNESS",
		"DROP SCHEMA HAT CASCADE"
        },
        {
                // System-X
                "DROP PROCEDURE HAT.NEWORDER;",
                "DROP PROCEDURE HAT.PAYMENT;",
                "DROP PROCEDURE HAT.COUNTORDERS;",
                "DROP TABLE IF EXISTS HAT.HISTORY",
                "DROP TABLE IF EXISTS HAT.LINEORDER",
                "DROP TABLE IF EXISTS HAT.DATE",
                "DROP TABLE IF EXISTS HAT.PART",
                "DROP TABLE IF EXISTS HAT.CUSTOMER",
                "DROP SCHEMA IF EXISTS HAT"
        },
        {
                // TiDB 
                "DROP TABLE IF EXISTS HAT.HISTORY",
                "DROP TABLE IF EXISTS HAT.LINEORDER",
                "DROP TABLE IF EXISTS HAT.DATE",
                "DROP TABLE IF EXISTS HAT.PART",
                "DROP TABLE IF EXISTS HAT.CUSTOMER",
                "DROP TABLE IF EXISTS HAT.SUPPLIER",
                "DROP DATABASE HAT"
        },
        {        // MySQL
                "DROP TABLE IF EXISTS HAT.HISTORY",
                "DROP TABLE IF EXISTS HAT.LINEORDER",
                "DROP TABLE IF EXISTS HAT.DATE",
                "DROP TABLE IF EXISTS HAT.PART",
                "DROP TABLE IF EXISTS HAT.CUSTOMER",
                "DROP TABLE IF EXISTS HAT.SUPPLIER",
                "DROP DATABASE HAT"
        }
};

vector<vector<string>> SQLDialect::bulkLoadStmt = {
        {
                // PostgreSQL
                "COPY HAT.PART FROM'",
                "/part.bin' CSV DELIMITER '!'",
                "COPY HAT.SUPPLIER  FROM'",
                "/supplier.bin' CSV DELIMITER '!'",
                "COPY HAT.CUSTOMER  FROM'",
                "/customer.bin' CSV DELIMITER '!'",
                "COPY HAT.DATE  FROM'",
                "/date.bin' CSV DELIMITER '!'",
                "COPY HAT.LINEORDER  FROM'",
                "/lineorder.bin' CSV DELIMITER '!'",
                "COPY HAT.HISTORY  FROM'",
                "/history.bin' CSV DELIMITER '!'"

        }
};

vector<vector<string>> SQLDialect::createIndexStmt = {
        {
                // PostgreSQL-
                "ALTER TABLE hat.lineorder ADD CONSTRAINT fk_lo_custkey FOREIGN KEY (LO_CUSTKEY) REFERENCES HAT.CUSTOMER (C_CUSTKEY) MATCH FULL;",
                "ALTER TABLE hat.lineorder ADD CONSTRAINT fk_lo_partkey FOREIGN KEY (LO_PARTKEY) REFERENCES HAT.PART (P_PARTKEY) MATCH FULL;",
                "ALTER TABLE hat.lineorder ADD CONSTRAINT fk_lo_suppkey FOREIGN KEY (LO_SUPPKEY) REFERENCES HAT.SUPPLIER (S_SUPPKEY) MATCH FULL;",
                "ALTER TABLE hat.lineorder ADD CONSTRAINT fk_lo_orderdate FOREIGN KEY (LO_ORDERDATE) REFERENCES HAT.DATE (D_DATEKEY) MATCH FULL;",
                "ALTER TABLE hat.lineorder ADD CONSTRAINT fk_lo_commitdate FOREIGN KEY (LO_COMMITDATE) REFERENCES HAT.DATE (D_DATEKEY) MATCH FULL;",
                "CREATE INDEX idx_p_category ON hat.part USING btree (p_category);",
                "CREATE INDEX idx_p_brand1 ON hat.part USING btree (p_brand1);",
                "CREATE INDEX idx_p_mfgr ON hat.part USING btree (p_mfgr);",
                "CREATE INDEX idx_s_region ON hat.supplier USING btree (s_region);",
                "CREATE INDEX idx_c_supplier ON hat.supplier USING btree (s_nation);",
                "CREATE INDEX idx_c_city ON hat.supplier USING btree (s_city);",
                "CREATE INDEX idx_c_region ON hat.customer USING btree (c_region);",
                "CREATE INDEX idx_c_nation ON hat.customer USING btree (c_nation);",
                "CREATE INDEX idx_c_customer ON hat.customer USING btree (c_city);",
                "CREATE INDEX idx_d_year ON hat.date USING btree (d_year);",
                "CREATE INDEX idx_d_yearmonthnum ON hat.date USING btree (d_yearmonthnum);",
                "CREATE INDEX idx_d_weeknuminyear ON hat.date USING btree (d_weeknuminyear);",
                "CREATE INDEX idx_lo_quantity ON hat.lineorder USING btree (lo_quantity);",
                "CREATE INDEX idx_lo_discount ON hat.lineorder USING btree (lo_discount);",
                "CREATE INDEX idx_lo_orderdate ON hat.lineorder USING btree (lo_orderdate);",
                "CREATE INDEX idx_lo_partkey ON hat.lineorder USING btree (lo_partkey);",
                "CREATE INDEX idx_lo_suppkey ON hat.lineorder USING btree (lo_suppkey);",
                "CREATE INDEX idx_lo_custkey ON hat.lineorder USING btree (lo_custkey);",
                "CREATE INDEX idx_c_name ON hat.customer USING btree (c_name);",
                "CREATE INDEX idx_s_name ON hat.supplier USING btree (s_name);",
                "CREATE INDEX idx_d_date ON hat.date USING btree (d_date);",
                "ANALYZE hat.lineorder;",
                "ANALYZE hat.customer;",
                "ANALYZE hat.supplier;",
                "ANALYZE hat.part;",
                "ANALYZE hat.history;",
                "ANALYZE hat.date;"
        }
};

vector<string> SQLDialect::init = {
    "SELECT DISTINCT C_PHONE FROM HAT.CUSTOMER;",
    "SELECT DISTINCT S_PHONE FROM HAT.SUPPLIER;",
    "SELECT MAX(LO_ORDERKEY) FROM HAT.LINEORDER;"
};


vector<vector<string>> SQLDialect::deleteTuplesStmt = {
        {
                // PostgreSQL
                //"\\c hatrickbench;",
                "set search_path to hat;",
                "delete from hat.lineorder where lo_orderkey>",
                "delete from hat.history where h_orderkey>"
        }
};

vector<string> SQLDialect::createFreshnessTableStmt = {
                "CREATE TABLE HAT.\"FRESHNESS",
                "\"(F_TXNNUM INTEGER, F_CLIENTNUM INTEGER);"
};

vector<string> SQLDialect::deleteFreshnessTableStmt = {
                "DROP TABLE HAT.\"FRESHNESS",
		"\";"
};

vector<string> SQLDialect::populateFreshnessTableStmt = {
                "INSERT INTO HAT.\"FRESHNESS",
		"\"(F_TXNNUM, F_CLIENTNUM) VALUES(0,",
		");",
		"ALTER TABLE HAT.FRESHNESS",
	       	" SET TIFLASH REPLICA 1;"
};

vector<vector<string>> SQLDialect::transactionalQueries = {
        {  //Postgres         
                // New order transaction's commands
                "SELECT HAT.NEWORDER(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",
                // Payment transaction
                "SELECT HAT.PAYMENT(?,?,?,?,?,?);",
                // Count orders transaction's commands
                "SELECT * FROM HAT.COUNTORDERS(?,?,?);"
        }
};


vector<string> SQLDialect::freshnessCommands = {
    "UPDATE HAT.FRESHNESS",
    " SET F_TXNNUM = ? WHERE F_CLIENTNUM = ?"
};

vector<string> SQLDialect::analyticalQueries = {     // 13 SSB Benchmark queries
    // Q1 / Q1.1
    "SELECT SUM(LO_EXTENDEDPRICE*LO_DISCOUNT) AS REVENUE\n"
    "   FROM HAT.LINEORDER, HAT.DATE\n"
    "   WHERE LO_ORDERDATE = D_DATEKEY\n"
    "   AND D_YEAR = 1993\n"
    "   AND LO_DISCOUNT BETWEEN 1 AND 3\n"
    "   AND LO_QUANTITY<25",
    // Q2 / Q1.2
    "SELECT SUM(LO_EXTENDEDPRICE*LO_DISCOUNT) AS REVENUE\n"
    "   FROM HAT.LINEORDER, HAT.DATE\n"
    "   WHERE LO_ORDERDATE = D_DATEKEY\n"
    "   AND D_YEARMONTHNUM = 199401\n"
    "   AND LO_DISCOUNT BETWEEN 4 AND 6\n"
    "   AND LO_QUANTITY BETWEEN 26 AND 35",
    // Q3 / Q1.3
    "SELECT SUM(LO_EXTENDEDPRICE*LO_DISCOUNT) AS REVENUE\n"
    "   FROM HAT.LINEORDER, HAT.DATE\n"
    "   WHERE LO_ORDERDATE = D_DATEKEY\n"
    "   AND D_WEEKNUMINYEAR = 6\n"
    "   AND D_YEAR = 1994\n"
    "   AND LO_DISCOUNT BETWEEN 5 AND 7\n"
    "   AND LO_QUANTITY BETWEEN 26 AND 35",
    // Q4 / Q2.1
    "SELECT SUM(LO_REVENUE), D_YEAR, P_BRAND1\n"
    "   FROM HAT.LINEORDER, HAT.DATE, HAT.PART, HAT.SUPPLIER\n"
    "   WHERE LO_ORDERDATE = D_DATEKEY\n"
    "   AND LO_PARTKEY = P_PARTKEY\n"
    "   AND LO_SUPPKEY = S_SUPPKEY\n"
    "   AND P_CATEGORY = 'MFGR#12'\n"
    "   AND S_REGION = 'AMERICA'\n"
    "   GROUP BY D_YEAR, P_BRAND1\n"
    "   ORDER BY D_YEAR, P_BRAND1",
    // Q5 / Q2.2
    "SELECT SUM(LO_REVENUE), D_YEAR, P_BRAND1\n"
    "   FROM HAT.LINEORDER, HAT.DATE, HAT.PART, HAT.SUPPLIER\n"
    "   WHERE LO_ORDERDATE = D_DATEKEY\n"
    "   AND LO_PARTKEY = P_PARTKEY\n"
    "   AND LO_SUPPKEY = S_SUPPKEY\n"
    "   AND P_BRAND1 BETWEEN 'MFGR#2221' AND 'MFGR#2228'\n"
    "   AND S_REGION = 'ASIA'\n"
    "   GROUP BY D_YEAR, P_BRAND1\n"
    "   ORDER BY D_YEAR, P_BRAND1",
    // Q6 / Q2.3
    "SELECT SUM(LO_REVENUE), D_YEAR, P_BRAND1\n"
    "   FROM HAT.LINEORDER, HAT.DATE, HAT.PART, HAT.SUPPLIER\n"
    "   WHERE LO_ORDERDATE = D_DATEKEY\n"
    "   AND LO_PARTKEY = P_PARTKEY\n"
    "   AND LO_SUPPKEY = S_SUPPKEY\n"
    "   AND P_BRAND1 = 'MFGR#2221'\n"
    "   AND S_REGION = 'EUROPE'\n"
    "   GROUP BY D_YEAR, P_BRAND1\n"
    "   ORDER BY D_YEAR, P_BRAND1",
    // Q7 / Q3.1
    "SELECT C_NATION, S_NATION, D_YEAR, SUM(LO_REVENUE) AS REVENUE\n"
    "   FROM HAT.CUSTOMER, HAT.LINEORDER, HAT.SUPPLIER, HAT.DATE\n"
    "   WHERE LO_CUSTKEY = C_CUSTKEY\n"
    "   AND LO_SUPPKEY = S_SUPPKEY\n"
    "   AND LO_ORDERDATE = D_DATEKEY\n"
    "   AND C_REGION = 'ASIA' AND S_REGION = 'ASIA'\n"
    "   AND D_YEAR >= 1992 AND D_YEAR <= 1997\n"
    "   GROUP BY C_NATION, S_NATION, D_YEAR\n"
    "   ORDER BY D_YEAR ASC, REVENUE DESC",
    // Q8 / Q3.2
    "SELECT C_CITY, S_CITY, D_YEAR, SUM(LO_REVENUE) AS REVENUE\n"
    "   FROM HAT.CUSTOMER, HAT.LINEORDER, HAT.SUPPLIER, HAT.DATE\n"
    "   WHERE LO_CUSTKEY = C_CUSTKEY\n"
    "   AND LO_SUPPKEY = S_SUPPKEY\n"
    "   AND LO_ORDERDATE = D_DATEKEY\n"
    "   AND C_NATION = 'UNITED STATES'\n"
    "   AND S_NATION = 'UNITED STATES'\n"
    "   AND D_YEAR >= 1992 AND D_YEAR <= 1997\n"
    "   GROUP BY C_CITY, S_CITY, D_YEAR\n"
    "   ORDER BY D_YEAR ASC, REVENUE DESC",
    // Q9 / Q3.3
    "SELECT C_CITY, S_CITY, D_YEAR, SUM(LO_REVENUE) AS REVENUE\n"
    "   FROM HAT.CUSTOMER, HAT.LINEORDER, HAT.SUPPLIER, HAT.DATE\n"
    "   WHERE LO_CUSTKEY = C_CUSTKEY\n"
    "   AND LO_SUPPKEY = S_SUPPKEY\n"
    "   AND LO_ORDERDATE = D_DATEKEY\n"
    "   AND (C_CITY = 'UNITED KI1' OR C_CITY = 'UNITED KI5')\n"
    "   AND (S_CITY = 'UNITED KI1' OR S_CITY = 'UNITED KI5')\n"
    "   AND D_YEAR >= 1992 AND D_YEAR <= 1997\n"
    "   GROUP BY C_CITY, S_CITY, D_YEAR\n"
    "   ORDER BY D_YEAR ASC, REVENUE DESC",
    // Q10 / Q3.4
    "SELECT C_CITY, S_CITY, D_YEAR, SUM(LO_REVENUE) AS REVENUE\n"
    "   FROM HAT.CUSTOMER, HAT.LINEORDER, HAT.SUPPLIER, HAT.DATE\n"
    "   WHERE LO_CUSTKEY = C_CUSTKEY\n"
    "   AND LO_SUPPKEY = S_SUPPKEY\n"
    "   AND LO_ORDERDATE = D_DATEKEY\n"
    "   AND (C_CITY = 'UNITED KI1' OR C_CITY = 'UNITED KI5')\n"
    "   AND (S_CITY = 'UNITED KI1' OR S_CITY = 'UNITED KI5')\n"
    "   AND D_YEARMONTH = 'Dec1994'\n"
    "   GROUP BY C_CITY, S_CITY, D_YEAR\n"
    "   ORDER BY D_YEAR ASC, REVENUE DESC",
    // Q11 / Q4.1
    "SELECT D_YEAR, C_NATION, SUM(LO_REVENUE-LO_SUPPLYCOST) AS PROFIT\n"
    "   FROM HAT.DATE, HAT.CUSTOMER, HAT.SUPPLIER, HAT.PART, HAT.LINEORDER\n"
    "   WHERE LO_CUSTKEY = C_CUSTKEY\n"
    "   AND LO_SUPPKEY = S_SUPPKEY\n"
    "   AND LO_PARTKEY = P_PARTKEY\n"
    "   AND LO_ORDERDATE = D_DATEKEY\n"
    "   AND C_REGION = 'AMERICA'\n"
    "   AND S_REGION = 'AMERICA'\n"
    "   AND (P_MFGR = 'MFGR#1' OR P_MFGR = 'MFGR#2')\n"
    "   GROUP BY D_YEAR, C_NATION\n"
    "   ORDER BY D_YEAR, C_NATION",
    // Q12 / Q4.2
    "SELECT D_YEAR, C_NATION, P_CATEGORY, SUM(LO_REVENUE-LO_SUPPLYCOST) AS PROFIT\n"
    "   FROM HAT.DATE, HAT.CUSTOMER, HAT.SUPPLIER, HAT.PART, HAT.LINEORDER\n"
    "   WHERE LO_CUSTKEY = C_CUSTKEY\n"
    "   AND LO_SUPPKEY = S_SUPPKEY\n"
    "   AND LO_PARTKEY = P_PARTKEY\n"
    "   AND LO_ORDERDATE = D_DATEKEY\n"
    "   AND C_REGION = 'AMERICA'\n"
    "   AND S_REGION = 'AMERICA'\n"
    "   AND (D_YEAR = 1997 OR D_YEAR = 1998)\n"
    "   AND (P_MFGR = 'MFGR#1' OR P_MFGR = 'MFGR#2')\n"
    "   GROUP BY D_YEAR, C_NATION, P_CATEGORY\n"
    "   ORDER BY D_YEAR, C_NATION, P_CATEGORY",
    // Q13 / Q4.3
    "SELECT D_YEAR, S_CITY, P_BRAND1, SUM(LO_REVENUE-LO_SUPPLYCOST) AS PROFIT\n"
    "   FROM HAT.DATE, HAT.CUSTOMER, HAT.SUPPLIER, HAT.PART, HAT.LINEORDER\n"
    "   WHERE LO_CUSTKEY = C_CUSTKEY\n"
    "   AND LO_SUPPKEY = S_SUPPKEY\n"
    "   AND LO_PARTKEY = P_PARTKEY\n"
    "   AND LO_ORDERDATE = D_DATEKEY\n"
    "   AND C_REGION = 'AMERICA'\n"
    "   AND S_NATION = 'UNITED STATES'\n"
    "   AND (D_YEAR = 1997 OR D_YEAR = 1998)\n"
    "   AND P_CATEGORY = 'MFGR#14'\n"
    "   GROUP BY D_YEAR, S_CITY, P_BRAND1\n"
    "   ORDER BY D_YEAR, S_CITY, P_BRAND1"
};

