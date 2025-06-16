
#ifndef HATTRICKBENCH_TRANSACTIONALCLIENT_H
#define HATTRICKBENCH_TRANSACTIONALCLIENT_H
#include "Driver.h"
#include "SQLDialect.h"
#include "Globals.h"
#include "DataSrc.h"
#include "LinkedList.h"
#include <thread>
#include <iostream>
#include <algorithm>
#include <vector>
#include <utility>
using namespace std;

class TransactionalClient{
private:
    thread::id threadNum;           // id of thread
    int loOrderKey;                 // current lo_orderkey
    SQLHSTMT tStmt = 0;             // for stored procedures
    SQLHSTMT freshStmt = 0;
    int clientNum = 0;              
    vector<vector<double>> latencyVector = vector<vector<double>>(3);    // history of response time for the 3 transactions
    int txn_num[3] = {0};           // number of txns executed from each category
    int localCounter = 0;           // local counter of the transactions that the current client is running
    const int numTries = 3;
    int failCounter = 0;
    int totalFailCounter = 0;
public:
    TransactionalClient();
    int NewOrderTransactionPS(SQLHDBC& dbc);   // NewOrder txn w/ store procedures for PostgreSQL
    int PaymentTransactionSP(SQLHDBC& dbc);    // Payment txn w/ stored procedures, same for PostgreSQL and SQL Server
    SQLHSTMT& GetTransactionStmt();
    void SetLoOrderKey(int& num);
    int& GetLoOrderKey();
    void SetClientNum(int& num);     // Set the number of the client
    int& GetClientNum();             // Get the number of the client
    void SetThreadNum(thread::id num);
    thread::id GetThreadNum();
    void SetLatency(double latency, int tType);
    double GetLatencySum(int tType);
    int GetLatencySize(int tType);
    std::vector<double>& GetLatencies(int tType);
    void IncrementLocalCounter();
    int& GetLocalCounter();
    void IncrementFailCounter();
    int& GetFailCounter();
    void IncrementTotalFailCounter();
    int& GetTotalFailCounter();
    void PrepareFreshnessStmt(SQLHDBC &dbc);
    SQLHSTMT& GetFreshnessStmt();
};
#endif //HATTRICKBENCH_TRANSACTIONALCLIENT_H
