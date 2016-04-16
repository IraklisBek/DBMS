#ifndef recordPos_H_INCLUDED
#define recordPos_H_INCLUDED

#include "dbtproj.h"

typedef struct {
    int block;
    int record;
} recordPos;

bool operator==(const recordPos &recPos1, const recordPos &recordPos2);
bool operator!=(const recordPos &recPos1, const recordPos &recordPos2);
bool operator>(const recordPos &recPos1, const recordPos &recordPos2);
bool operator<(const recordPos &recPos1, const recordPos &recordPos2);
bool operator>=(const recordPos &recPos1, const recordPos &recordPos2);
bool operator<=(const recordPos &recPos1, const recordPos &recordPos2);
int operator-(const recordPos &recPos1, const recordPos &recordPos2);
recordPos operator+(const recordPos &recordPos, int offset);
recordPos operator-(const recordPos &recordPos, int offset);

record_t getRecord(block_t *buffer, recordPos recordPos);
void setRecord(block_t *buffer, record_t rec, recordPos recordPos);

recordPos getRecordPos(int offset);

int getOffset(recordPos recordPos);

void incr(recordPos &recordPos);
void decr(recordPos &recordPos);

void swapRecordsPos(block_t *buffer, recordPos recPos1, recordPos recordPos2);
int compareRecords(record_t rec1, record_t rec2, unsigned char field);
#endif // recordPos_H_INCLUDED
