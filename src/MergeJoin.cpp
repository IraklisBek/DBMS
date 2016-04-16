#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <stddef.h>

#include "dbtproj.h"
#include "records.h"
#include "buffer.h"
#include "files.h"



void MergeJoin (char *infile1, char *infile2, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nres, unsigned int *nios){
    //memSize of buffer is -2 cause 2 last blocks is used for, one reading block from big file and two writing to output
    int memSize = nmem_blocks - 2;
    //get sizes of files
    int infile1Size = getSize(infile1);
    int infile2Size = getSize(infile2);

    unsigned int noneed1=0, noneed2=0, ios=0;
    *nres = 0;
    *nios = 0;

    FILE *out = fopen(outfile, "ab");

    char outfile1[] = "outfile1.bin";
    char outfile2[] = "outfile2.bin";



    MergeSort(infile1, 1, buffer, nmem_blocks, outfile1, &noneed1, &noneed2, &ios);
    (*nios) += ios;

    MergeSort(infile2, 1, buffer, nmem_blocks, outfile2, &noneed1, &noneed2, &ios);
    (*nios) += ios;

    //if file1 is bigger switch files cause next we assume that file1 is the small one.
    if(infile1Size > infile2Size){
        char temp0 = outfile1[7];
        outfile1[7] = outfile2[7];
        outfile2[7] = temp0;
        int temp = infile1Size;
        infile1Size = infile2Size;
        infile2Size = temp;
    }
    FILE *input1 = fopen(outfile1, "rb");
    FILE *input2 = fopen(outfile2, "rb");
    ////printfile(outfile1);
    ////printfile(outfile2);

    block_t *bigFileBlock = buffer + memSize;
    block_t *outputBlock = buffer + memSize + 1;
    (*outputBlock).blockid = 0;
    //offset that changes every time we need new block from big file to check
    int bigFileBlockOffset=0;
    //counts the records of buffer that has same value of a record of bigFileBlock
    int countSameBufferEntries=0;
    //number of blocks in big file. Useful to end the main loop.
    int blocks = infile2Size - 1;
    //printf("%d", blocks);
    //this will represent the id of the first block of the buffer
    int firstBlockId = 0;
    int lastBlockId = memSize-1;
    //at first we read first blocks of small and big file so we have something to compare in first loop
    (*nios) += readBuffer(buffer, input1, 0, memSize);
    (*nios) += readBlock(bigFileBlock, input2, 0);
    recordPos bufferRecPos = getRecordPos(0);
    record_t tempBufferRec;
    int tempBufferBlock=0;
    record_t bigFileBlockRec;
    while(blocks>0){
        /*
        General:
            if(bufferRecPos.block%memSize==firstBlockId%memSize)
                this condition checks if we have reached i circle in the buffer.

            firstBlockId%memSize
                defines the first block of the buffer and makes it easy to replace it with another block if necessary with:
                    buffer + firstBlockId%memSize



        */

        for(int blockEntrie=0; blockEntrie<MAX_RECORDS_PER_BLOCK; blockEntrie++){

            bigFileBlockRec = (*bigFileBlock).entries[blockEntrie];

            if(compareRecords(tempBufferRec, bigFileBlockRec, field)==0){
                //Here we  have to go back to the block and record of tempBufferRec.
                for(int i=0; i<countSameBufferEntries; i++){
                    decr(bufferRecPos);
                    ////printf("%d\n", bufferRecPos.block);
                }
                int i=0;//keeps i for load2
                int load, load2;
                if(countSameBufferEntries/memSize > memSize){
                    load = memSize;
                    load2=0;
                }else{
                    load = countSameBufferEntries/memSize;
                    load2 = countSameBufferEntries%memSize;
                }
                for( ;i<load; i++){
                    (*nios) += readBlock(buffer + (tempBufferBlock + i) % memSize, input1, tempBufferBlock + i);
                }
                if(load2!=0){//we have to read one more block.
                    (*nios) += readBlock(buffer + (tempBufferBlock + i) % memSize, input1, tempBufferBlock + i);
                }
                //return to firstBlockID and lastBlockID their previous values
                firstBlockId = tempBufferBlock ;
                lastBlockId = firstBlockId + memSize - 1;
            }

            while(compareRecords(getRecord(buffer, bufferRecPos), bigFileBlockRec, field) < 0){
                //Here we have to pass the records in the small file that are smaller than the bigFileRec
                incr(bufferRecPos);
                if (bufferRecPos.record == 0) {
                     if(bufferRecPos.block%memSize==firstBlockId%memSize){
                        if (lastBlockId < infile1Size - 1) {
                            (*nios) += readBlock(buffer + firstBlockId%memSize, input1, lastBlockId + 1);
                            firstBlockId += 1;
                            lastBlockId += 1;
                        }else{
                            blocks=0;//No point to continue merging as all next records are greater than the last of buffer.
                            break;
                        }
                    }
                }
            }

            if(compareRecords(getRecord(buffer, bufferRecPos), bigFileBlockRec, field) > 0){
                    continue;//...
            }


            tempBufferRec = getRecord(buffer, bufferRecPos);
            tempBufferBlock = bufferRecPos.block;
            countSameBufferEntries=0;

            while(compareRecords(getRecord(buffer, bufferRecPos), bigFileBlockRec, field)==0){
                //Here we add in output the merges.
                (*outputBlock).entries[(*outputBlock).nreserved++] = bigFileBlockRec;
                (*outputBlock).entries[(*outputBlock).nreserved++] = getRecord(buffer, bufferRecPos);
                (*nres)++;
                if ((*outputBlock).nreserved == MAX_RECORDS_PER_BLOCK) {
                    (*nios) += writeBlock(out, outputBlock);
                    emptyBlock(outputBlock);
                    (*outputBlock).blockid += 1;
                }

                countSameBufferEntries++;
                incr(bufferRecPos);

                if(bufferRecPos.record==0){
                    if(bufferRecPos.block%memSize==firstBlockId%memSize){
                        if (lastBlockId < infile1Size - 1) {
                            (*nios) += readBlock(buffer + firstBlockId%memSize, input1, lastBlockId + 1);
                            firstBlockId++;
                            lastBlockId++;
                        }else {//take always the same value because it is the last value to compare with last big file's blocks
                            if (bufferRecPos.block == 0) {
                                bufferRecPos.block = memSize - 1;
                            } else {
                                bufferRecPos.block -= 1;
                            }
                            bufferRecPos.record = MAX_RECORDS_PER_BLOCK - 1;
                            break;
                        }
                    }
                }
                ////printf("fsdfds");
            }


        }
        //records in bigFileBlock are over and we read the next one.
        bigFileBlockOffset++;
        blocks--;
        (*nios) += readBlock(bigFileBlock, input2, bigFileBlockOffset);
    }








}
