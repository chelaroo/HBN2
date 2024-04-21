#include "patterndetect.hpp"
#include <iostream>
#include <fstream>
#include <map>
#include <set>
#include <vector>

using namespace std;

#define FLOAT_TOLERANCE 0.01f
#define MIN_RANDOM_CHAIN_LEN 2

vector<Transaction *> transactions;

Transaction *read_transaction(ifstream &data)
{
    time_t time = -1;
    string from;
    string to;
    double recieved = 0;
    double paid = 0;

    if (data.eof())
    {
        // cout << "EOF\n";
        return nullptr;
    }

    data >> time;
    data >> from;
    data >> to;
    data >> recieved;
    data >> paid;

    // cout << "t: " << time
    //      << " f: " << from
    //      << " t: " << to
    //      << " r: " << recieved
    //      << " p: " << paid
    //      << "\n";

    Transaction *pTransaction = new Transaction(time, from, to, recieved, paid);
    return pTransaction;
}

void load_data(string path)
{
    uint64_t iter = 0;
    ifstream data(path);
    Transaction *pTransaction = nullptr;

    if (!data)
    {
        // cout << "Error: file could not be opened" << endl;
        exit(1);
    }

    while (pTransaction = read_transaction(data))
    {
        if (iter++ % 10000000 == 0)
        {
            // cout << "iter: " << iter << "\n";
        }
        transactions.push_back(pTransaction);
    }
}

Pattern get_pattern(uint32_t transactionId, time_t period, double transactionThreshold, double launderingEfficiency)
{
    // cout << "ti: " << transactionId
    //      << " p: " << period
    //      << " th: " << transactionThreshold
    //      << " l: " << launderingEfficiency
    //      << "\n";
    Pattern simplePattern = UNDECIDED;
    Pattern complexPattern = UNDECIDED;

    // for (auto const &pTransaction : transactions)
    // {
    //     cout << pTransaction->time << " ";
    //     cout << pTransaction->from << " ";
    //     cout << pTransaction->to << " ";
    //     cout << pTransaction->paid << " ";
    //     cout << pTransaction->recieved << "\n";
    // }

    if (transactions[transactionId]->paid < transactionThreshold)
    {
        transactions[transactionId]->pattern = CLEAN;
        return CLEAN;
    }

    if (UNDECIDED != transactions[transactionId]->pattern)
    {
        return transactions[transactionId]->pattern;
    }

    simplePattern = getSimplePattern(transactionId, transactions[transactionId]->paid, transactionId, period,
                                     transactionThreshold, launderingEfficiency, 0);
    if (UNDECIDED != simplePattern)
    {
        return simplePattern;
    }

    complexPattern = getComplexPattern(transactionId, period, transactionThreshold, launderingEfficiency);
    if (UNDECIDED != complexPattern)
    {
        return complexPattern;
    }

    return CLEAN;
}

Pattern getSimplePattern(uint32_t initiatorId, double sumToLaunder, uint32_t transactionId, time_t period,
                         double transactionThreshold, double launderingEfficiency, uint32_t depth)
{
    // cout << __func__ << endl;
    if (transactions[transactionId]->pattern != UNDECIDED)
    {
        return UNDECIDED;
    }

    if (transactions[transactionId]->paid < transactionThreshold)
    {
        transactions[transactionId]->pattern = CLEAN;
        return UNDECIDED;
    }

    if (transactions[initiatorId]->time + period > transactions[transactionId]->time)
    {
        return UNDECIDED;
    }

    if (transactions[transactionId]->to == transactions[initiatorId]->from)
    {
        if (sumToLaunder * launderingEfficiency >= transactions[transactionId]->paid)
        {
            transactions[transactionId]->pattern = CYCLE;
            return CYCLE;
        }
    }

    for (uint32_t id = transactionId + 1;
         initiatorId < transactions.size() && transactions[initiatorId]->time + period > transactions[id]->time;
         id++)
    {
        Pattern pattern = getSimplePattern(initiatorId, sumToLaunder, id, period, transactionThreshold, launderingEfficiency, depth + 1);
        if (pattern == CYCLE || pattern == RANDOM)
        {
            transactions[transactionId]->pattern = pattern;
            return pattern;
        }
    }

    if (depth >= MIN_RANDOM_CHAIN_LEN)
    {
        if (sumToLaunder * launderingEfficiency >= transactions[transactionId]->paid)
        {
            transactions[transactionId]->pattern = RANDOM;
            return RANDOM;
        }
    }
    throw std::runtime_error("unhandled simple pattern?");
}

// set<string> getOutputNames(vector<Transaction *> fanOut)
// {
//     set<string> names;
//     for (auto const &pTransaction : fanOut)
//     {
//         names.insert(pTransaction->to);
//     }
//     return names;
// }

// set<string> getInputNames(vector<Transaction *> fanIn)
// {
//     set<string> names;
//     for (auto const &pTransaction : fanIn)
//     {
//         names.insert(pTransaction->from);
//     }
//     return names;
// }

Pattern getComplexPattern(uint32_t transactionId, time_t period, double transactionThreshold, double launderingEfficiency)
{
    // cout << __func__ << endl;
    map<string, vector<Transaction *>> fanOutTransactions;
    map<string, vector<Transaction *>> fanInTransactions;

    map<string, set<string>> fanOutNames;
    map<string, set<string>> fanInNames;

    // map<string, double> totalPaid;
    // map<string, double> totalRecieved;
    time_t maxTime = transactions[transactionId]->time + period;

    // cout << "Finding fan ins/outs\n";
    for (uint32_t id = transactionId;
         id < transactions.size() && transactions[id]->time < maxTime;
         id++)
    {
        if (transactions[id]->pattern == RANDOM ||
            transactions[id]->pattern == CYCLE ||
            transactions[id]->pattern == CLEAN)
        {
            continue;
        }
        if (transactions[id]->paid >= transactionThreshold)
        {
            transactions[id]->pattern = FAN_IN;

            fanOutTransactions[transactions[id]->from].push_back(transactions[id]);
            fanOutNames[transactions[id]->from].insert(transactions[id]->to);
            // if (totalPaid.find(transactions[id]->from) == totalPaid.end())
            //     totalPaid[transactions[id]->from] = 0;
            // totalPaid[transactions[id]->from] += transactions[id]->paid;

            fanInTransactions[transactions[id]->to].push_back(transactions[id]);
            fanInNames[transactions[id]->to].insert(transactions[id]->from);
            // if (totalRecieved.find(transactions[id]->to) == totalRecieved.end())
            //     totalRecieved[transactions[id]->to] = 0;
            // totalRecieved[transactions[id]->to] += transactions[id]->recieved;
        }
    }

    // GATHER SCATTER
    // cout << "Finding GATHER SCATTER\n";
    for (auto const &fanOut : fanOutTransactions)
    {
        if (fanInTransactions.find(fanOut.first) != fanInTransactions.end())
        {
            for (auto const &pTransaction : fanOut.second)
            {
                pTransaction->pattern = GATHER_SCATTER;
            }

            for (auto const &pTransaction : fanInTransactions[fanOut.first])
            {
                pTransaction->pattern = GATHER_SCATTER;
            }
        }
    }

    // SCATTER GATHER
    // cout << "Finding SCATTER GATHER\n";
    for (auto const &fanOut : fanOutTransactions)
    {
        // cout << "OUT " << fanOut.first << endl;
        for (auto const &fanIn : fanInTransactions)
        {
            // cout << "IN " << fanIn.first << endl;
            if (fanOutNames[fanOut.first] == fanInNames[fanIn.first])
            {
                for (auto const &pTransaction : fanOut.second)
                {
                    pTransaction->pattern = SCATTER_GATHER;
                }

                for (auto const &pTransaction : fanIn.second)
                {
                    pTransaction->pattern = SCATTER_GATHER;
                }
                break;
            }
        }
    }

    // BIPARTITE
    // cout << "Finding BIPARTITE\n";
    for (auto const &party1 : fanOutTransactions)
    {
        for (auto const &party2 : fanOutTransactions)
        {
            if (party1.first == party2.first)
            {
                continue;
            }
            if (fanOutNames[party1.first] == fanInNames[party2.first])
            {
                for (auto const &pTransaction : party1.second)
                {
                    pTransaction->pattern = BIPARTITE;
                }

                for (auto const &pTransaction : party2.second)
                {
                    pTransaction->pattern = BIPARTITE;
                }
                break;
            }
        }
    }

    // BIPARTITE (other way around)
    // cout << "Finding BIPARTITE invers\n";
    for (auto const &party1 : fanInTransactions)
    {
        for (auto const &party2 : fanInTransactions)
        {
            if (party1.first == party2.first)
            {
                continue;
            }
            if (fanOutNames[party1.first] == fanInNames[party2.first])
            {
                for (auto const &pTransaction : party1.second)
                {
                    pTransaction->pattern = BIPARTITE;
                }

                for (auto const &pTransaction : party2.second)
                {
                    pTransaction->pattern = BIPARTITE;
                }
                break;
            }
        }
    }

    return transactions[transactionId]->pattern;
}

extern "C"
{
    void extern_load_data(char *path)
    {
        // cout << "Loading data from" << path << "\n";
        load_data(path);
        // cout << "Loaded data\n";
    }
    int extern_get_pattern(uint32_t transactionId, time_t period, double transactionThreshold, double launderingEfficiency)
    {
        return get_pattern(transactionId, period, transactionThreshold, launderingEfficiency);
    }
}
