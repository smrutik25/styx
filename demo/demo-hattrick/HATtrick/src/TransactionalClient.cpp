
#include "TransactionalClient.h"

TransactionalClient::TransactionalClient(){}

void TransactionalClient::PrepareFreshnessStmt(SQLHDBC &dbc){
   Driver::prepareStmt(dbc, GetFreshnessStmt(), (SQLDialect::freshnessCommands[0]+
			   std::to_string(GetClientNum())+
			   SQLDialect::freshnessCommands[1]).c_str());
}

int TransactionalClient::NewOrderTransactionPS(SQLHDBC& dbc){
    // Create a random LO_CUSTNAME.
    int tries = 0;
    int custkey = DataSrc::uniformIntDist(1, UserInput::getCustSize());
    ostringstream ckey;
    ckey << setw(9) << setfill('0') << custkey;
    string c = "Customer#";
    char* custName = &(c.append(ckey.str()))[0];
    // Choose a random number of orders 
    int numOrders = DataSrc::uniformIntDist(1, 7);
    char* partKeys = 0; char* suppNames = 0;  char* dateNames = 0;  char* ordPriorities = 0;  char* shipPriorities = 0;  \
    char* quantities = 0; char* extendedPrices = 0;  char* discounts = 0;  char* revenues = 0;  char* supplyCosts = 0; \
    char* taxes = 0;  char* shipModes = 0;
    string partKeysBuf, suppNamesBuf, dateNamesBuf, ordPrioritiesBuf, shipPrioritiesBuf, quantitiesBuf, \
    extendedPricesBuf, discountsBuf, revenuesBuf, supplyCostsBuf, taxesBuf, shipModesBuf;
    int suppKey = 0, month = 0, quantity = 0, discount = 0, client_num = 0, txn_num=0;
    int orderKey =  GetLoOrderKey();
    int ret = -1;
    for(int i=0; i<numOrders; i++){
        // Create a random LO_PARTKEY.
        if(i!=0){
            partKeysBuf.append(",");
            suppNamesBuf.append(",");
            dateNamesBuf.append("^");
            ordPrioritiesBuf.append(",");
            shipPrioritiesBuf.append(",");
            quantitiesBuf.append(",");
            extendedPricesBuf.append(",");
            discountsBuf.append(",");
            revenuesBuf.append(",");
            supplyCostsBuf.append(",");
            taxesBuf.append(",");
            shipModesBuf.append(",");
        }
        partKeysBuf.append(to_string(DataSrc::uniformIntDist(1, UserInput::getPartSize())));
        // Create a random LO_SUPPNAME.
        suppKey = DataSrc::uniformIntDist(1, UserInput::getSuppSize());
        ostringstream skey;
        skey << setw(9) << setfill('0') << suppKey;
        suppNamesBuf.append("Supplier#" + skey.str());
        // Create a random LO_DATENAME.
        month = DataSrc::uniformIntDist(1, 12);
        dateNamesBuf.append(DataSrc::getMonthName(month-1) + " " + \
                        to_string(DataSrc::uniformIntDist(1,DataSrc::getMonthDay(month-1))) + \
                        ", " + DataSrc::getYear(DataSrc::uniformIntDist(1, 7)-1));
        // Create the other data of the current lineorder randomly.
        ordPrioritiesBuf.append(DataSrc::getOrdPriority(DataSrc::uniformIntDist(0,4)));
        shipPrioritiesBuf.append(to_string(DataSrc::uniformIntDist(0,1)));
        quantity = DataSrc::uniformIntDist(1, 50);
        quantitiesBuf.append(to_string(quantity));
        extendedPricesBuf.append(to_string(quantity)); 
        discount = DataSrc::uniformIntDist(0, 10);
        discountsBuf.append(to_string(discount));
        revenuesBuf.append(to_string(double(100-discount)/100)); 
        supplyCostsBuf.append(to_string(DataSrc::uniformRealDist(1.00, 1000.00)));
        taxesBuf.append(to_string(DataSrc::uniformIntDist(0,8)));
        shipModesBuf.append(DataSrc::getShipMode(DataSrc::uniformIntDist(0,6)));
    }
    partKeys = &(partKeysBuf)[0];
    suppNames = &(suppNamesBuf)[0];
    dateNames = &(dateNamesBuf)[0];
    ordPriorities =  &(ordPrioritiesBuf)[0];
    shipPriorities = &(shipPrioritiesBuf)[0];
    quantities = &(quantitiesBuf)[0];
    extendedPrices = &(extendedPricesBuf)[0];
    discounts = &(discountsBuf)[0];
    revenues = &(revenuesBuf)[0];
    supplyCosts = &(supplyCostsBuf)[0];
    taxes = &(taxesBuf)[0];
    shipModes = &(shipModesBuf)[0];
    client_num = GetClientNum();
    txn_num = GetLocalCounter();
    string table = "FRESHNESS";
    char* tableName = &(table.append(to_string(client_num)))[0];
    // Call the NewOrder txn
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &GetTransactionStmt());
    Driver::bindIntParam(GetTransactionStmt(), orderKey, 1);
    Driver::bindIntParam(GetTransactionStmt(), numOrders, 2);
    Driver::bindCharParam(GetTransactionStmt(), custName, 25, 3);
    Driver::bindCharParam(GetTransactionStmt(),  partKeys, 0, 4);
    Driver::bindCharParam(GetTransactionStmt(),  suppNames, 0, 5);
    Driver::bindCharParam(GetTransactionStmt(),  dateNames, 0, 6);
    Driver::bindCharParam(GetTransactionStmt(),  ordPriorities, 0, 7);
    Driver::bindCharParam(GetTransactionStmt(),  shipPriorities, 0, 8);
    Driver::bindCharParam(GetTransactionStmt(),  quantities, 0, 9);
    Driver::bindCharParam(GetTransactionStmt(),  extendedPrices, 0, 10);
    Driver::bindCharParam(GetTransactionStmt(),  discounts, 0, 11);
    Driver::bindCharParam(GetTransactionStmt(),  revenues, 0, 12);
    Driver::bindCharParam(GetTransactionStmt(),  supplyCosts, 0, 13);
    Driver::bindCharParam(GetTransactionStmt(),  taxes, 0, 14);
    Driver::bindCharParam(GetTransactionStmt(),  shipModes, 0, 15);
    Driver::bindCharParam(GetTransactionStmt(), tableName, 0, 16);
    Driver::bindIntParam(GetTransactionStmt(), txn_num, 17);
    while(ret != 0){
    	ret = Driver::executeStmtDiar(GetTransactionStmt(), SQLDialect::transactionalQueries[UserInput::getdbChoice()][0].c_str());
    	if(ret != 0){
    	    IncrementTotalFailCounter();
    	}
    	tries++ ;
    	if(tries >= numTries) break;
    }
    Driver::freeStmtHandle(GetTransactionStmt());
    if (ret == 0) return 1;
    else return 0;
}

int TransactionalClient::PaymentTransactionSP(SQLHDBC& dbc){
    int tries = 0;
    // Get random customer key CUSTKEY
    int custkey  = DataSrc::uniformIntDist(1, UserInput::getCustSize());
    // Get random supplier key SUPPKEY
    int suppkey = DataSrc::uniformIntDist(1, UserInput::getSuppSize());
    // Get random paymnet amount X, TODO: set max value for the amount
    double payAmount =  DataSrc::uniformRealDist(1.00, 104950.00);
    int client_num = GetClientNum();
    int txn_num = GetLocalCounter();
    string table = "FRESHNESS";
    char* tableName = &(table.append(to_string(client_num)))[0];
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &GetTransactionStmt());
    Driver::bindIntParam(GetTransactionStmt(), custkey, 1);
    Driver::bindIntParam(GetTransactionStmt(), suppkey, 2);
    Driver::bindDecParam(GetTransactionStmt(), payAmount, 3);  
    Driver::bindIntParam(GetTransactionStmt(), GetLoOrderKey(), 4);  
    Driver::bindCharParam(GetTransactionStmt(), tableName, 0, 5);
    Driver::bindIntParam(GetTransactionStmt(), txn_num, 6);
    [[maybe_unused]] int ret = -1;
    while(ret != 0){
    	ret = Driver::executeStmtDiar(GetTransactionStmt(), SQLDialect::transactionalQueries[UserInput::getdbChoice()][1].c_str());
    	if(ret != 0){
    	    IncrementTotalFailCounter();
    	}
    	tries++ ;
    	if(tries >= numTries) break;
    }
    Driver::freeStmtHandle(GetTransactionStmt());
    if (ret == 0) return 1;
    else return 0;
}

SQLHSTMT& TransactionalClient::GetTransactionStmt(){
    return tStmt;
}

void TransactionalClient::SetLoOrderKey(int& key){
    loOrderKey = key;
}

int& TransactionalClient::GetLoOrderKey(){
    return loOrderKey;
}

void TransactionalClient::SetClientNum(int& num){
    clientNum = num;
}

int& TransactionalClient::GetClientNum(){
    return clientNum;
}

void TransactionalClient::SetThreadNum(thread::id num){
    threadNum = num;
}

thread::id TransactionalClient::GetThreadNum(){
    return threadNum;
}

void TransactionalClient::SetLatency(double latency, int tType){
    latencyVector[tType-1].push_back(latency);
}

double TransactionalClient::GetLatencySum(int tType){
    double sum=0.0;
    for(int i=0; i<GetLatencySize(tType); i++)
        sum += latencyVector[tType-1][i];
    return sum;
}

int TransactionalClient::GetLatencySize(int tType){
    return latencyVector[tType-1].size();
}

std::vector<double>& TransactionalClient::GetLatencies(int tType){
    return latencyVector[tType-1];
}

void TransactionalClient::IncrementLocalCounter(){
    localCounter++;
}

int& TransactionalClient::GetLocalCounter(){
    return localCounter;
}

void TransactionalClient::IncrementFailCounter(){
    failCounter++;
}

int& TransactionalClient::GetFailCounter(){
    return failCounter;
}

void TransactionalClient::IncrementTotalFailCounter(){
    totalFailCounter++;
}

int& TransactionalClient::GetTotalFailCounter(){
    return totalFailCounter;
}

SQLHSTMT& TransactionalClient::GetFreshnessStmt(){
    return freshStmt;
}
