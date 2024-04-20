#include "patterndetect.hpp"
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
    time_t time;
    string from;
    string to;
    double recieved;
    double paid;

    if (!data.eof())
    {
        return nullptr;
    }

    data >> time;
    data >> from;
    data >> to;
    data >> recieved;
    data >> paid;

    Transaction *pTransaction = new Transaction(time, from, to, recieved, paid);
    return pTransaction;
}

void load_data(string path)
{
    ifstream data(path);
    Transaction *pTransaction = nullptr;

    while (pTransaction = read_transaction(data))
    {
        transactions.push_back(pTransaction);
    }
}

Pattern get_pattern(uint32_t transactionId, time_t period, double transactionThreshold, double launderingEfficiency)
{
    Pattern simplePattern = UNDECIDED;
    Pattern complexPattern = UNDECIDED;

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

set<string> getOutputNames(vector<Transaction *> fanOut)
{
    set<string> names;
    for (auto const &pTransaction : fanOut)
    {
        names.insert(pTransaction->to);
    }
    return names;
}

set<string> getInputNames(vector<Transaction *> fanIn)
{
    set<string> names;
    for (auto const &pTransaction : fanIn)
    {
        names.insert(pTransaction->from);
    }
    return names;
}

Pattern getComplexPattern(uint32_t transactionId, time_t period, double transactionThreshold, double launderingEfficiency)
{
    map<string, vector<Transaction *>> fanOuts;
    map<string, vector<Transaction *>> fanIns;
    map<string, double> totalPaid;
    map<string, double> totalRecieved;
    time_t maxTime = transactions[transactionId]->time + period;

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

            fanOuts[transactions[id]->from].push_back(transactions[id]);
            if (totalPaid.find(transactions[id]->from) == totalPaid.end())
                totalPaid[transactions[id]->from] = 0;
            totalPaid[transactions[id]->from] += transactions[id]->paid;

            fanIns[transactions[id]->to].push_back(transactions[id]);
            if (totalRecieved.find(transactions[id]->to) == totalRecieved.end())
                totalRecieved[transactions[id]->to] = 0;
            totalRecieved[transactions[id]->to] += transactions[id]->recieved;
        }
    }

    // GATHER SCATTER
    for (auto const &fanOut : fanOuts)
    {
        if (fanIns.find(fanOut.first) != fanIns.end())
        {
            for (auto const &pTransaction : fanOut.second)
            {
                pTransaction->pattern = GATHER_SCATTER;
            }

            for (auto const &pTransaction : fanIns[fanOut.first])
            {
                pTransaction->pattern = GATHER_SCATTER;
            }
        }
    }

    // SCATTER GATHER
    for (auto const &fanOut : fanOuts)
    {
        for (auto const &fanIn : fanIns)
        {
            if (getOutputNames(fanOut.second) == getInputNames(fanIn.second))
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
    for (auto const &party1 : fanOuts)
    {
        for (auto const &party2 : fanOuts)
        {
            if (party1.first == party2.first)
            {
                continue;
            }
            if (getOutputNames(party1.second) == getInputNames(party2.second))
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
    for (auto const &party1 : fanIns)
    {
        for (auto const &party2 : fanIns)
        {
            if (party1.first == party2.first)
            {
                continue;
            }
            if (getOutputNames(party1.second) == getInputNames(party2.second))
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
    void extern_load_data(char *path) { load_data(path); }
    int extern_get_pattern(uint32_t transactionId, time_t period, double transactionThreshold, double launderingEfficiency)
    {
        return get_pattern(transactionId, period, transactionThreshold, launderingEfficiency);
    }
}
