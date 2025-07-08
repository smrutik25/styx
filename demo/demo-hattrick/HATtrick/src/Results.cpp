
#include "Results.h"
#include <filesystem>

namespace fs = std::filesystem;

void Results::setTotalQueries(vector<AnalyticalClient*>& a){
    for(int i=0; i<UserInput::getAnalClients(); i++) {
        totalQueries += a[i]->GetQueriesNum();
    }
}

void Results::setTotalTxns(vector<TransactionalClient*>& t){
    for(int i=0; i<UserInput::getTranClients(); i++) {
        totalTxns += t[i]->GetLocalCounter();
        totalFailedTxns += t[i]->GetFailCounter();
        totalFails += t[i]->GetTotalFailCounter();
    }
}

double Results::getTransactionalThroughput(){
    return t_throughput;
}

double Results::getAnalyticalThroughput(){
    return a_throughput;
}

void Results::setTransactionalThroughput(double& tt){
    t_throughput = tt;
}

void Results::setAnalyticalThroughput(double& at){
    a_throughput = at;
}

void Results::saveResults(bool frontier_calc) {
    fs::create_directory("results");	
    double tt = 0, at = 0, ft = 0;
    if(totalQueries != 0)
        at = (double)totalQueries/testDuration;
    setAnalyticalThroughput(at);
    if(totalTxns != 0)
        tt = (double)totalTxns/UserInput::getTestDuration();
    if ((totalFailedTxns + totalTxns) != 0)
        ft = (double)totalFailedTxns/(totalFailedTxns + totalTxns) * 100;
    setTransactionalThroughput(tt);

    resultsStream.open("results/results-SF"+to_string(UserInput::getSF())+".txt", ofstream::out | ofstream::app | ofstream::binary);
    resultsStream << "--------------------------------------------" << endl;
    resultsStream << "Available threads in the system: " << thread::hardware_concurrency() << endl;
    resultsStream << "Anal. Threads #: " << UserInput::getAnalClients() << endl;
    resultsStream << "Tran. Threads #: " << UserInput::getTranClients() << endl;
    resultsStream << "Total # of transactions executed: " << totalTxns << endl;
    resultsStream << "Total # of transactions failed: " << totalFailedTxns << endl;
    resultsStream << "Percentage # of transactions failed: " << ft << endl;
    resultsStream << "Total failures (inc. retries): " << totalFails << endl;
    resultsStream << "Total # of queries executed: " << totalQueries << endl;
    resultsStream << "Anal. Throughput [queries/sec]: " << getAnalyticalThroughput()  << endl;
    resultsStream << "Tran. Throughput [transactions/sec]: " << getTransactionalThroughput() << endl;
    resultsStream << "Avg. Latency for New-Order Tran: " << txnLatency[0] << endl;
    resultsStream << "Avg. Latency for Payment Tran: " << txnLatency[1] << endl;
    resultsStream << "Avg. Latency for Count-Orders Tran: " << txnLatency[2] << endl;
    resultsStream << "Avg. Execution Time for Q1: " << queryExecTime[0] << endl;
    resultsStream << "Avg. Execution Time for Q2: " << queryExecTime[1] << endl;
    resultsStream << "Avg. Execution Time for Q3: " << queryExecTime[2] << endl;
    resultsStream << "Avg. Execution Time for Q4: " << queryExecTime[3] << endl;
    resultsStream << "Avg. Execution Time for Q5: " << queryExecTime[4] << endl;
    resultsStream << "Avg. Execution Time for Q6: " << queryExecTime[5] << endl;
    resultsStream << "Avg. Execution Time for Q7: " << queryExecTime[6] << endl;
    resultsStream << "Avg. Execution Time for Q8: " << queryExecTime[7] << endl;
    resultsStream << "Avg. Execution Time for Q9: " << queryExecTime[8] << endl;
    resultsStream << "Avg. Execution Time for Q10: " << queryExecTime[9] << endl;
    resultsStream << "Avg. Execution Time for Q11: " << queryExecTime[10] << endl;
    resultsStream << "Avg. Execution Time for Q12: " << queryExecTime[11] << endl;
    resultsStream << "Avg. Execution Time for Q13: " << queryExecTime[12] << endl;
    resultsStream << "Percentage of fresh queries during the test [%]: " << 100-fresh_score << endl;
    resultsStream << "Real test duration: " << testDuration << endl;
    resultsStream.close();
    resultsStream.clear();
    if (frontier_calc) {
        resultsStream.open("results/frontier-SF"+to_string(UserInput::getSF())+".csv", ofstream::out | ofstream::app | ofstream::binary);
        resultsStream << tt << "," << at <<  endl;
        resultsStream.close();
        resultsStream.open("results/txn-failures-SF"+to_string(UserInput::getSF())+".csv", ofstream::out | ofstream::app | ofstream::binary);
        resultsStream << ft << "," << totalFails << endl;
        resultsStream.close();
        resultsStream.open("results/txn-latency-SF"+to_string(UserInput::getSF())+".csv", ofstream::out | ofstream::app | ofstream::binary);
        resultsStream << txnLatencyAll << "," << txnLatencyAll95 << "," << txnLatencyAll99 << endl;
        resultsStream.close();
        resultsStream.open("results/ana-latency-SF"+to_string(UserInput::getSF())+".csv", ofstream::out | ofstream::app | ofstream::binary);
        resultsStream << queryExecTimeAll << "," << queryExecTimeAll95 << "," << queryExecTimeAll99 << endl;
        resultsStream.close();
    if(UserInput::getAnalClients()>0){
    	resultsStream.clear();
    	resultsStream.open("results/freshness-SF"+to_string(UserInput::getSF())+"-"+
			to_string(UserInput::getTranClients())+"-"+
			to_string(UserInput::getAnalClients())+".csv", 
			ofstream::out | ofstream::app | ofstream::binary);
    	for(unsigned int i =0; i<(unsigned int)probFresh.size(); i++){
    		resultsStream << probFresh[i] << ", " <<  freshValues[i] <<  endl;
    	}
    	resultsStream.close();
    }
    }

}

void Results::getQueryExecTime(vector<AnalyticalClient*>& a){
    double sum[13] = {0.0};
    int q[13] = {0};
    std::vector<double> allExecTimes[13];
    std::vector<double> combinedExecTimes;
    for(int i=0; i<UserInput::getAnalClients(); i++){
        for(int j=0; j<13; j++){
            sum[j] += a[i]->GetExecutionTimeSum(j);
            q[j] += a[i]->GetExecutionTimeSize(j);
            const std::vector<double>& clientExecTimes = a[i]->GetExecutionTimes(j);
            allExecTimes[j].insert(allExecTimes[j].end(),
                                   clientExecTimes.begin(),
                                   clientExecTimes.end());
            combinedExecTimes.insert(combinedExecTimes.end(),
                                     clientExecTimes.begin(),
                                     clientExecTimes.end());
        }
    }
    for(int i=0; i<13; i++)
        queryExecTime[i] = (sum[i]*1e-9)/q[i];

    if (!combinedExecTimes.empty()) {
        double totalSum = 0.0;
        for (const auto& val : combinedExecTimes) {
            if (!std::isnan(val)) // check for NaN just in case
                totalSum += val;
        }
        double averageAll = totalSum / combinedExecTimes.size();

        std::sort(combinedExecTimes.begin(), combinedExecTimes.end());

        int idx95 = static_cast<int>(0.95 * combinedExecTimes.size());
        if(idx95 >= combinedExecTimes.size()) idx95 = combinedExecTimes.size() - 1;

        int idx99 = static_cast<int>(0.99 * combinedExecTimes.size());
        if(idx99 >= combinedExecTimes.size()) idx99 = combinedExecTimes.size() - 1;

        queryExecTimeAll = averageAll*1e-9;
        queryExecTimeAll95 = combinedExecTimes[idx95]*1e-9;
        queryExecTimeAll99 = combinedExecTimes[idx99]*1e-9;
    } else {
        queryExecTimeAll = queryExecTimeAll95 = queryExecTimeAll99 = 0.0;
    }

}

void Results::getTxnLatency(vector<TransactionalClient*>& t){
    double sum[3] = {0.0};
    int tran[3] = {0};
    std::vector<double> allLatencies[3];
    std::vector<double> combinedLatencies;
    for(int i=0; i<UserInput::getTranClients(); i++) {
        for(int j=0; j<3; j++){
            sum[j] +=  t[i]->GetLatencySum(j+1);
            tran[j] += t[i]->GetLatencySize(j+1);
            const std::vector<double>& clientLatencies = t[i]->GetLatencies(j + 1);
            allLatencies[j].insert(allLatencies[j].end(),
                                   clientLatencies.begin(),
                                   clientLatencies.end());
            combinedLatencies.insert(combinedLatencies.end(),
                                     clientLatencies.begin(),
                                     clientLatencies.end());
        }
    }
    for(int i=0; i<3; i++)
            txnLatency[i] = (sum[i]*1e-9)/tran[i];
    if (!combinedLatencies.empty()) {
        double totalSum = 0.0;
        for (const auto& val : combinedLatencies) {
            if (!std::isnan(val)) // handle possible NaNs
                totalSum += val;
        }
        double averageAll = totalSum / combinedLatencies.size();

        std::sort(combinedLatencies.begin(), combinedLatencies.end());

        int idx95 = static_cast<int>(0.95 * combinedLatencies.size());
        if(idx95 >= combinedLatencies.size()) idx95 = combinedLatencies.size() - 1;

        int idx99 = static_cast<int>(0.99 * combinedLatencies.size());
        if(idx99 >= combinedLatencies.size()) idx99 = combinedLatencies.size() - 1;

        txnLatencyAll = averageAll*1e-9;
        txnLatencyAll95 = combinedLatencies[idx95]*1e-9;
        txnLatencyAll99 = combinedLatencies[idx99]*1e-9;
    } else {
        txnLatencyAll = txnLatencyAll95 = txnLatencyAll99 = 0.0;
    }
}

void Results::getFreshness(vector<AnalyticalClient*>& a){
    int index = 0;
    for(int i=0; i<UserInput::getAnalClients(); i++){
	for(unsigned int j=0; j<a[i]->GetFreshness().size(); j++){
            freshness.push_back(a[i]->GetFreshness()[j]);
	}
    }
    sort(freshness.begin(), freshness.end());
    index = ceil(0.95*freshness.size());
    fresh_score = freshness[index-1];

    vector<double> ceil_freshness;
    for(int unsigned i=0; i<(unsigned int)freshness.size(); i++)
	    ceil_freshness.push_back(ceil(freshness[i]));
    double current, previous=-1;
    for(int unsigned i=0; i<(unsigned int)ceil_freshness.size(); i++){
	current  = ceil_freshness[i];
	if(current != previous){
		probFresh.push_back((double)count(ceil_freshness.begin(), ceil_freshness.end(), current)/freshness.size());
		freshValues.push_back(current);
		previous = current;
	}
    }
}

void Results::getTestDuration(vector<AnalyticalClient*>& a){
    for(int i=0; i<UserInput::getAnalClients(); i++){
        if(a[i]->GetTestDuration() > testDuration)
            testDuration  = a[i]->GetTestDuration();
    }
}

void Results::computeResults(vector<TransactionalClient*>& t, vector<AnalyticalClient*>& a, bool frontier_calc){
    setTotalQueries(a);
    setTotalTxns(t);
    getTxnLatency(t);
    getQueryExecTime(a);
    if(UserInput::getAnalClients()>0){
    	getFreshness(a);
    }
    getTestDuration(a);
    saveResults(frontier_calc);
}
