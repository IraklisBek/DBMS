#include <stdio.h>
#include <stdlib.h>

#include "dbtproj.h"
#include "records.h"
#include "buffer.h"
#include "files.h"



void MergeSort(char* infile, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char* outfile, unsigned int* nsorted_segs, unsigned int* npasses, unsigned int* nios) {


    (*nsorted_segs) = 0;
    (*npasses) = 0;
    (*nios) = 0;
    //memory of buffer without the last block which will be used for sorting records and output them.
    int memSize = nmem_blocks - 1;
    //# of blocks in file.
    int blocks = getSize(infile);
    //# of segments in file.
    int segments = blocks / nmem_blocks;
    //# of blocks in the last segment.
    int remainingSegment = blocks % nmem_blocks;
    //The segments size.
    int segmentSize = nmem_blocks;

    FILE *input;
    input = fopen(infile, "rb");
    //explanations of files handle at the end of source file.
    FILE *output;
    char sortedSegments1[] = "o.bin";
    output = fopen(sortedSegments1, "ab");

    char sortedSegments2[] = "t.bin";

    for(int i=0; i<=segments; i++){//for every segment, read buffer sort it and write it to a sortedSegments2 output.
        if (segments == i) {//consider the last segment.
            if (remainingSegment != 0) {
                segmentSize = remainingSegment;
            } else {
                break;
            }
        }
        (*nios) += readBuffer(buffer, input, i*segmentSize, segmentSize);
        sortBuffer(buffer, field, segmentSize);
        (*nios) += writeBuffer(output, buffer, i*segmentSize, segmentSize, 1);
        (*nsorted_segs) += 1;
        emptyBuffer(buffer, nmem_blocks);
    }
    (*npasses) += 1;


    fclose(input);
    fclose(output);
    //in case of remaining segment != 0 where the segment size changes here it is initialized again.
    segmentSize = nmem_blocks;
    //for convenience.
    int nSortedSegs = (*nsorted_segs);
    //define the size of last segment.
    int lastSegmentSize;
    if (remainingSegment == 0) {
        lastSegmentSize = nmem_blocks;
    } else {
        lastSegmentSize = remainingSegment;
    }
    //Last block of buffer that will be used for sorting records. Always true.
    //minBlock refers to that slot of buffer.
    buffer[memSize].valid = true;

    while (nSortedSegs > 1) {//while there are segments to merge
        remove(sortedSegments2);
        input = fopen(sortedSegments1, "rb");
        output = fopen(sortedSegments2, "ab");
        //sorted segments that are to be created.
        int newSortedSegs = 0;
        //merges than needs to be done at this pass.
        int passMerges = nSortedSegs / memSize;
        //# of last segments that are to be merged.
        //Example, if sorted segments are 7 and memory size 4 then we merge the first four and there are 3 left.
        int lastMergeSegs = nSortedSegs % memSize;
        //printf("nSortedSegs: %d\n", nSortedSegs);
        //printf("lastMergePass: %d\n", lastMergeSegs);
        // array that holds the number of blocks l%deft to a sorted segment during merging
        int *blocksLeft = (int*) malloc(memSize * sizeof (int));
        //# of segments to be merged.
        int segsToMerge = memSize;
        //refers to the case that we are at the last merge of this pass.
        bool lastMerge = false;

        for (int m = 0; m <= passMerges; m++) {
            int segmentOffset = m * memSize * segmentSize;

            if (m == passMerges - 1 && lastMergeSegs == 0) {//If there are no other segments to be merged.
                lastMerge = true;
            } else if (m == passMerges) {
                // if during the last merge the buffer is not fully utilised,//////////////////////
                // changes the numbers of segsToMerge and sets lastMerge as true/////////////////////
                if (lastMergeSegs != 0) {
                    segsToMerge = lastMergeSegs;
                    lastMerge = true;
                } else {
                    break;
                }
            }
            //recordsToSort that shows to the next record of the segment that is to be merged.
            //we get the right record with the help of getRecord that returns a record of a block.
            recordPos *recordsToSort = (recordPos*) malloc(segsToMerge * sizeof (recordPos));
            //Reads first blocks of each segment and puts them in the buffer.
            for (int i = 0; i < segsToMerge; i++) {
                (*nios) += readBlock(buffer + i, input, (segmentOffset + i * segmentSize));
                recordsToSort[i] = getRecordPos(i * MAX_RECORDS_PER_BLOCK);
                blocksLeft[i] = segmentSize - 1;// -1 because the first block does not count
            }
            //in the case o last merge we initialize the blocks left for sorting same way as for general purpose.
            //size of last segment will be blocks left + 1.
            int sizeOfLastSeg;
            if (lastMerge) {
                blocksLeft[segsToMerge - 1] = lastSegmentSize - 1;
                sizeOfLastSeg = blocksLeft[segsToMerge - 1] + 1;
            }

            //Here we keep the sorted records MAX_RECS_PER_BLOCK at a time.
            //we use the last block of buffer.
            block_t *minBlock = buffer + memSize;
            //nreserved=0 and every entrie is valid false.

            emptyBlock(minBlock);
            (*minBlock).blockid = 0;
            //blocksWritten in minBlock.
            int blocksWritten = 0;
            //Copy the value of segsToMerge because we must use the segsToMerge without making any change
            //and the segmentsLeft counts the number of merges that had been done.
            int segmentsLeft = segsToMerge;

            while (segmentsLeft != 0) {//while all of the segments have not been merged.
                int i;
                // finds the first valid block which means that finds a block that has not been used previously.
                for (i = 0; i < segsToMerge; i++) {
                    if (buffer[i].valid) {
                        break;
                    }
                }
                //setting its record as min and takes the block index.
                record_t minRec = getRecord(buffer, recordsToSort[i]);
                int minBlockIndex = i;
                //compares records with minRec. Edv isws prepei na kanw min heap kathe fora pou teleiwnei to block.
                for (int j = i + 1; j < segsToMerge; j++) {
                    if (buffer[j].valid && compareRecords(getRecord(buffer, recordsToSort[j]), minRec, field) < 0) {
                        minRec = getRecord(buffer, recordsToSort[j]);
                        minBlockIndex = j;
                    }
                }
                //put the minimum record in minBlock.
                (*minBlock).entries[(*minBlock).nreserved++] = minRec;
                //if minBlock is full write it to output and empty it.
                if ((*minBlock).nreserved == MAX_RECORDS_PER_BLOCK) {
                    (*nios) += writeBlock(output, minBlock);
                    (*minBlock).blockid += 1;
                    blocksWritten += 1;
                    emptyBlock(minBlock);
                }
                //take next record
                incr(recordsToSort[minBlockIndex]);

                if (recordsToSort[minBlockIndex].record == 0) {//if recordsToSort changes block
                    recordsToSort[minBlockIndex].block -= 1;//then take another block.
                    //printf("%d\n", recordsToSort[minBlockIndex].block);
                    if (blocksLeft[minBlockIndex] > 0) {//if there are blocks left in the segment
                        int blockOffset;
                        if (lastMerge && minBlockIndex == segsToMerge - 1) {//if we are on the last merge and all blocks had been passed for check

                            blockOffset = segmentOffset + segmentSize * minBlockIndex + sizeOfLastSeg - blocksLeft[minBlockIndex];
                        } else {
                            //block offset will be equal with the segment offset
                            //plus segments size * min block index so we can get to the segments where the block with the current min record is
                            //plus segment size - blocks left in the segment so we can get the correct block.
                            blockOffset = segmentOffset + segmentSize * minBlockIndex + segmentSize - blocksLeft[minBlockIndex];
                        }

                        //read the block and minor blocks left.
                        (*nios) += readBlock(buffer + minBlockIndex, input, blockOffset);
                        blocksLeft[minBlockIndex] -= 1;
                    } else {
                        //set it as false so it wont take it in the next loop to compare its records
                        // because all records from the block had been taken and we do not need this block anymore.
                        buffer[minBlockIndex].valid = false;
                        segmentsLeft -= 1;
                    }
                }
            }
            free(recordsToSort);
            //wright in output records left without full minBlock.
            if ((*minBlock).nreserved !=0)
                (*nios) += writeBlock(output, minBlock);
            newSortedSegs += 1;
        }
        free(blocksLeft);

        // updates variables for the next pass
        if (lastMergeSegs == 0) {
            lastSegmentSize = (memSize - 1) * segmentSize + lastSegmentSize;//+ lastSegmentsSize add the correct value we missing from -1
        } else {
            lastSegmentSize = (lastMergeSegs - 1) * segmentSize + lastSegmentSize;
        }

        segmentSize *= memSize;
        nSortedSegs = newSortedSegs;
        (*npasses) += 1;

        fclose(input);
        fclose(output);

        // swaps the files. If during this pass "o.bin" was used as input, next
        // pass it will be used as output and "t.bin" as input
        //every time a file has larger sorted segments.
        char tmp = sortedSegments1[0];
        sortedSegments1[0] = sortedSegments2[0];
        sortedSegments2[0] = tmp;
    }

	if((*npasses) % 2 == 1){
        rename("o.bin", outfile);
        remove("t.bin");
        remove("o.bin");
	}else{
	    rename("t.bin", outfile);
	    remove("o.bin");
	    remove("t.bin");
	}
    //printFile(outfile);

}
