#include <string>
#include "dbtproj.h"
#include "records.h"


//overloading operators so we can make recordPos struct comparisons.
bool operator==(const recordPos &recordPos1, const recordPos &recordPos2) {
    if (recordPos1.block == recordPos2.block && recordPos1.record == recordPos2.record) {
        return true;
    } else {
        return false;
    }
}

bool operator!=(const recordPos &recordPos1, const recordPos &recordPos2) {
    return !(recordPos1 == recordPos2);
}

bool operator>(const recordPos &recordPos1, const recordPos &recordPos2) {
    if (recordPos1.block == recordPos2.block) {
        if (recordPos1.record > recordPos2.record) {
            return true;
        } else {
            return false;
        }
    } else {
        if (recordPos1.block > recordPos2.block) {
            return true;
        } else {
            return false;
        }
    }
}

bool operator<(const recordPos &recordPos1, const recordPos &recordPos2) {
    if (recordPos1.block == recordPos2.block) {
        if (recordPos1.record < recordPos2.record) {
            return true;
        } else {
            return false;
        }
    } else {
        if (recordPos1.block < recordPos2.block) {
            return true;
        } else {
            return false;
        }
    }
}

bool operator>=(const recordPos &recordPos1, const recordPos &recordPos2) {
    if (recordPos1 > recordPos2 || recordPos1 == recordPos2) {
        return true;
    } else {
        return false;
    }
}

bool operator<=(const recordPos &recordPos1, const recordPos &recordPos2) {
    if (recordPos1 < recordPos2 || recordPos1 == recordPos2) {
        return true;
    } else {
        return false;
    }
}

int operator-(const recordPos &recordPos1, const recordPos &recordPos2) {
    return (recordPos1.block * MAX_RECORDS_PER_BLOCK+ recordPos1.record) - (recordPos2.block * MAX_RECORDS_PER_BLOCK+ recordPos2.record);
}

recordPos operator+(const recordPos &recPos, int offset) {
    recordPos result;
    result.block = recPos.block + offset / MAX_RECORDS_PER_BLOCK;
    int rest = offset % MAX_RECORDS_PER_BLOCK;

    if (recPos.record + rest >= MAX_RECORDS_PER_BLOCK) {
        result.block += 1;
        result.record = recPos.record + rest - MAX_RECORDS_PER_BLOCK;
    } else {
        result.record = recPos.record + rest;
    }
    return result;
}

recordPos operator-(const recordPos &recPos, int offset) {
    recordPos result;

    result.block = recPos.block - offset / MAX_RECORDS_PER_BLOCK;
    int rest = offset % MAX_RECORDS_PER_BLOCK;
    if (recPos.record - rest < 0) {
        result.block -= 1;
        result.record = MAX_RECORDS_PER_BLOCK+ recPos.record - rest;
    } else {
        result.record = recPos.record - rest;
    }
    return result;
}

//gets a record by its position in the buffer.
record_t getRecord(block_t *buffer, recordPos recPos) {
    return buffer[recPos.block].entries[recPos.record];
}

//changes the value of a record
void setRecord(block_t *buffer, record_t rec, recordPos recPos) {
    buffer[recPos.block].entries[recPos.record] = rec;
}

//return the position of a record.
recordPos getRecordPos(int offset) {
    recordPos zero;
    zero.block = 0;
    zero.record = 0;
    return zero + offset;
}

// increases the recPos so that it points to the next record.
// if pointing at the end of a block, moves to the start of the next.
void incr(recordPos &recPos) {
    if (recPos.record < MAX_RECORDS_PER_BLOCK- 1) {
        recPos.record += 1;
    } else {
        recPos.record = 0;
        recPos.block += 1;
    }
}

// decreases the recPos so that it points to the previous record.
// if pointing at the start of a block, moves to the end of the previous.
void decr(recordPos &recPos) {
    if (recPos.record > 0) {
        recPos.record -= 1;
    } else if (recPos.block > 0) {
        recPos.record = MAX_RECORDS_PER_BLOCK- 1;
        recPos.block -= 1;
    }
}

//swaps two recordPos
void swapRecordsPos(block_t *buffer, recordPos recordPos1, recordPos recordPos2) {
    record_t tmp = getRecord(buffer, recordPos1);
    setRecord(buffer, getRecord(buffer, recordPos2), recordPos1);
    setRecord(buffer, tmp, recordPos2);
}

//compare records according to the field.
int compareRecords(record_t rec1, record_t rec2, unsigned char field) {
    switch (field) {
        case 0:
            if (rec1.recid < rec2.recid) {
                return -1;
            } else if (rec1.recid > rec2.recid) {
                return 1;
            }
            return 0;
        case 1:
            if (rec1.num < rec2.num) {
                return -1;
            } else if (rec1.num > rec2.num) {
                return 1;
            }
            return 0;
        case 2:
            if (strcmp(rec1.str, rec2.str) < 0) {
                return -1;
            } else if (strcmp(rec1.str, rec2.str) > 0) {
                return 1;
            }
            return 0;
        case 3:
            if ((rec1.num < rec2.num || (rec1.num == rec2.num && strcmp(rec1.str, rec2.str) < 0))) {
                return -1;
            } else if ((rec1.num > rec2.num || (rec1.num == rec2.num && strcmp(rec1.str, rec2.str) > 0))) {
                return 1;
            }
            return 0;
    }
}
