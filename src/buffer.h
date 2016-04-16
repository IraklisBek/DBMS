#ifndef BUFFERINFO_H_INCLUDED
#define BUFFERINFO_H_INCLUDED

#include <iostream>
#include "dbtproj.h"

void emptyBlock(block_t *buffer);
void emptyBuffer(block_t *buffer, int bufferSize);

int readBlock(block_t *block, FILE *infile, int offset);
int readBuffer(block_t *buffer, FILE *infile, int offset, int memBlocks);

int writeBlock(FILE* filename, block_t *block);
int writeBuffer(FILE* filename, block_t *buffer, int offset, int memBlocks, int print);

void sortBuffer(block_t* buffer, unsigned char field, int memBlocks);
#endif // BUFFERINFO_H_INCLUDED
