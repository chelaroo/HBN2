#pragma once
#include <cstdint>
#include <ctime>
#include <string>

using namespace std;

enum Pattern
{
    UNDECIDED,
    CLEAN,
    FAN_OUT,
    FAN_IN,
    GATHER_SCATTER,
    SCATTER_GATHER,
    CYCLE,
    RANDOM,
    BIPARTITE,
    STACK,
};

class Transaction
{
public:
    time_t time;     // UNIX timestamp
    string from;     // From UID
    string to;       // To UID
    double recieved; // Amount recieved
    double paid;     // Amount Paid
    Pattern pattern; // Type of transaction

    Transaction(time_t time, string from, string to, double recieved, double paid);
    ~Transaction();
};

Transaction::Transaction(time_t time, string from, string to, double recieved, double paid)
    : time(time), from(from), to(to), recieved(recieved), paid(paid), pattern(UNDECIDED) {}

Transaction::~Transaction() {}

void load_data(string path);

Pattern get_pattern(uint32_t transactionId, time_t period, double transactionThreshold, double launderingEfficiency);

Pattern getSimplePattern(uint32_t initiatorId, double sumToLaunder, uint32_t transactionId, time_t period,
                         double transactionThreshold, double launderingEfficiency, uint32_t depth = 0);
Pattern getComplexPattern(uint32_t transactionId, time_t period, double transactionThreshold, double launderingEfficiency);

extern "C"
{
    void extern_load_data(char *path);
    int extern_get_pattern(uint32_t transactionId, time_t period, double transactionThreshold, double launderingEfficiency);
}
