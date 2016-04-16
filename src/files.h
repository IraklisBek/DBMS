#ifndef FILESINFO_H_INCLUDED
#define FILESINFO_H_INCLUDED
#include <iostream>
#include "dbtproj.h"

void createTwoFiles(char filename1[], int nblocks1, char filename2[], int nblocks2);
int getSize(char *filename);
void printFile(char *filename);
#endif // FILESINFO_H_INCLUDED
