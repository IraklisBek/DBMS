#include <iostream>
#include <time.h>

#include "files.h"
#include "dbtproj.h"
#include "buffer.h"

int main(){
    time_t timer;
    char date[26];
    struct tm* tm_info;


    char infile1[] = "infile1.bin";
    char infile2[] = "infile2.bin";
    char outfile1[] = "sortedRecords.bin";
    char outfile2[] = "sortedEliminatedRecords.bin";
    char outfile3[] = "mergedJoinRecords.bin";
    char outfile4[] = "hashedJoinRecords.bin";

    createTwoFiles(infile1, 54000, infile2, 36000);
    unsigned int nsorted_segs = 0, npasses = 0, nios = 0, nres = 0, nunique = 0, nmem_blocks = 20;
    block_t* buffer = (block_t*) malloc(nmem_blocks * sizeof (block_t));

    printf("Test with nmem_blocks: %d, infile1 size: 750MB infile2 size: 500MB.\nfield: 1\n\n", nmem_blocks);

    time(&timer);
    tm_info = localtime(&timer);
    strftime(date, 260, "%H:%M", tm_info);
    printf("MergeSort start time: %s \n", date);
    MergeSort(infile1, 1, buffer, nmem_blocks, outfile1, &nsorted_segs, &npasses, &nios);
    printf("nios = %d, npasses = %d, nsorted_segs = %d\n", nios, npasses, nsorted_segs);
    time(&timer);
    tm_info = localtime(&timer);
    strftime(date, 260, "%H:%M", tm_info);
    printf("MergeSort stop time: %s\n\n", date);


    printf("EliminateDuplicates start time: %s\n", date);
    EliminateDuplicates(infile1, 1, buffer, nmem_blocks, outfile2, &nunique, &nios);
    printf("nios = %d, nunique = %d\n", nios, nunique);
    time(&timer);
    tm_info = localtime(&timer);
    strftime(date, 260, "%H:%M", tm_info);
    printf("EliminateDuplicates stop time: %s\n\n", date);


    printf("MergeJoin start time: %s\n", date);
    MergeJoin(infile1, infile2, 1, buffer, nmem_blocks, outfile3, &nres, &nios);
    printf("nios = %d, nres = %d\n", nios, nres);
    printf("After MergeJoin time: %s\n\n", date);


    printf("HashJoin start time: %s\n", date);
    HashJoin (infile1, infile2, 0, buffer, nmem_blocks, outfile4, &nres, &nios);
    printf("nios = %d, nres = %d\n", nios, nres);
    time(&timer);
    tm_info = localtime(&timer);
    strftime(date, 260, "%H:%M", tm_info);
    printf("After HashJoin time: %s\n", date);

    return 0;
}
