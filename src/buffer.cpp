#include "files.h"
#include "dbtproj.h"
#include "buffer.h"
#include "records.h"

#include <math.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
void emptyBlock(block_t *block){
    for (int i = 0; i < MAX_RECORDS_PER_BLOCK; i++) {
        (*block).entries[i].valid = false;
    }
    (*block).nreserved = 0;
}

void emptyBuffer(block_t *buffer, int memBlocks){
    for(int i=0; i<memBlocks; i++){
        emptyBlock(buffer + i);//adiazei diadoxika tis theseis mnhmhs
        buffer[i].valid=true;
    }
}

int readBlock(block_t *buffer, FILE *infilee, int offset){
    //printf("^^^^^^^^^\n");
    //FILE* infilee = fopen(infile, "rb");
    fseek(infilee, sizeof(block_t)*offset, SEEK_SET);
    fread(buffer , 1, sizeof(block_t), infilee);
    int nreserved = buffer->nreserved;
    for (int i=0; i<nreserved; ++i) {
            //printf("this is block id: %d, record id: %d, num: %d\n",buffer->blockid, buffer->entries[i].recid, buffer->entries[i].num);//, buffer->entries[i].str);
    }
    //fclose(infilee);
    return 1;
}

int readBuffer(block_t *buffer, FILE *infile, int offset, int memBlocks){
	//FILE* infilee = fopen(infile, "rb");
	int nreserved;
    fseek(infile, sizeof(block_t)*offset, SEEK_SET);
    fread(buffer , 1, sizeof(block_t)*memBlocks, infile);
    nreserved = buffer->nreserved;
    //printf("@@@@@@@@@@@@@@\n");
    for(int j=0; j<memBlocks; j++){
        for (int i=0; i<nreserved; ++i) {
            //printf("this is block id: %d, record id: %d, num: %d\n",buffer->blockid, buffer->entries[i].recid, buffer->entries[i].num);//, buffer->entries[i].str);
        }
        *buffer++;
    }
	//fclose(infilee);
	return memBlocks;
}

int writeBlock(FILE* infilee, block_t *buffer){
    //FILE* infilee = fopen(infile, "ab");
    fwrite(buffer , 1, sizeof(block_t), infilee);
    //printf("$$$$$$$$$$$$$$$\n");
    for (int i=0; i<buffer->nreserved; ++i) {
        //printf("this is block id: %d, record id: %d, num: %d\n",buffer->blockid, buffer->entries[i].recid, buffer->entries[i].num);
    }
    //fclose(infilee);
    return 1;
}

int writeBuffer(FILE* infilee, block_t *buffer, int offset, int memBlocks, int print){
    //FILE* infilee = fopen(infile, "ab");
    int nreserved;
    fseek(infilee, sizeof(block_t)*offset, SEEK_SET);
    fwrite(buffer , 1, sizeof(block_t)*memBlocks, infilee);
    nreserved = buffer->nreserved;
    for(int j=0; j<memBlocks; j++){
        for (int i=0; i<nreserved; i++) {
            //printf("this is block id: %d, record id: %d, num: %d\n",buffer->blockid, buffer->entries[i].recid, buffer->entries[i].num);//, buffer->entries[i].str);
        }
        *buffer++;
    }
    //fclose(infilee);
    return memBlocks;
}

recordPos part(block_t *buffer, recordPos left, recordPos right, unsigned char field){
    record_t pivot = getRecord(buffer,left);
    while (left <= right) {
        while (compareRecords(getRecord(buffer, left), pivot, field) < 0) {
            incr(left);
        }
        while (compareRecords(getRecord(buffer, right), pivot, field) > 0) {
            decr(right);
        }
        if (left <= right) {
            swapRecordsPos(buffer, left, right);
            incr(left);
            decr(right);
        }
    }
    return left;
}

void quickSort(block_t *buffer, recordPos left, recordPos right, unsigned char field) {
        recordPos leftIndex = part(buffer, left, right, field);

        if(left < leftIndex -1){
            quickSort(buffer, left, leftIndex - 1,1);
        }
        if(right > leftIndex){
            quickSort(buffer,leftIndex, right, 1);
        }
}

void sortBuffer(block_t* buffer, unsigned char field, int memBlocks) {
    recordPos first = getRecordPos(0);
    recordPos last = getRecordPos(MAX_RECORDS_PER_BLOCK*memBlocks) - 1;
    quickSort(buffer, first, last, field);
    for (int i = 0; i <= last.block; i++) {
        buffer[i].nreserved = MAX_RECORDS_PER_BLOCK;
        buffer[i].blockid = i;
    }
}

