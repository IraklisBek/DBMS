#include "files.h"
#include "dbtproj.h"
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
//STR_LENGTH = 120
//MAX_RECORDS_PER_BLOCK = 100
char* getRandomString() {
    char* str = (char*) malloc(STR_LENGTH);
    const char text[] = "abcdefghijklmnopqrstuvwxyz";
    int length = rand() % (STR_LENGTH - 1);
    int i;
    for (i = 0; i < length; ++i) {
        str[i] = text[rand() % (sizeof (text) - 1)];
    }
    str[i] = '\0';
    return str;
}

void create(char *filename, int nblocks) {
    FILE* outfile = fopen(filename, "wb");

    int recid = 0;
    record_t record;
    block_t block;
    for (int b = 0; b < nblocks; ++b) { // for each block
        block.blockid = b;
        for (int r = 0; r < MAX_RECORDS_PER_BLOCK; ++r) { // for each record
            record.recid = recid += 1;// id of record
            record.num = rand() % 500;// num of record.
            strcpy(record.str, getRandomString()); // string of record.
            record.valid = true;// ?
            memcpy(&block.entries[r], &record, sizeof (record_t)); // copy record to block
        }
        block.nreserved = MAX_RECORDS_PER_BLOCK;
        block.valid = true;
        fwrite(&block, 1, sizeof (block_t), outfile);
    }
    fclose(outfile);
}


void createTwoFiles(char filename1[], int nblocks1, char filename2[], int nblocks2){
    create(filename1, nblocks1);
    create(filename2, nblocks2);
}
int getSize(char *filename) {
    struct stat st;
    stat(filename, &st);
    return st.st_size / sizeof (block_t);
}

void printFile(char *filename){

    printf("!!!!!!!!!!!!!!!!!!!!!!\n");
    block_t block;
	record_t record;
	FILE *infile;
    infile = fopen(filename, "rb");
	while (!feof(infile)) { // while end-of-file has not been reached ...
		fread(&block, 1, sizeof(block_t), infile); // read the next block
		// print block contents
		for (int i=0; i<block.nreserved; ++i) {
			printf("this is block id: %d, record id: %d, num: %d\n",block.blockid, block.entries[i].recid, block.entries[i].num);
		}

	}
	fclose(infile);
}
