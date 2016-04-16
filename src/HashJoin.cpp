#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <cstring>
#include "dbtproj.h"
#include "records.h"
#include "buffer.h"
#include "files.h"
#define MEM_PARTITION 3.0/4.0
using namespace std;



void printBlock(block_t t)
{
    cout<<"***********"<<endl;
    cout<<"Block id: "<<t.blockid<<endl;
    cout<<"Block nreserved: "<<t.nreserved<<endl;
    for(int i=0;i<t.nreserved;i++)
        cout<<"\trecid["<<i<<"]="<<t.entries[i].recid<<
        "   num["<<i<<"]="<<t.entries[i].num<<
        "   str="<<t.entries[i].str<<
        "   dum1="<<t.entries[i].dummy1<<
        "   dum2="<<t.entries[i].dummy2<<endl;
}
void bufferOverview(int x, block_t *buffer)
{
    for(int i=0;i<x;i++)
        printBlock(buffer[i]);
}
int hashFunction(block_t t,int field,int nhash)
{
    switch(field)
    {
        case 0://idi 1-1 antistoixia gia recid
            return t.entries[0].recid%nhash;
        case 1:
            return (t.entries[0].num*3)%nhash;
        case 2:
            return (strlen(t.entries[0].str)*10)%nhash;
        case 3:
            return (t.entries[0].num+strlen(t.entries[0].str))%nhash;
    }
}
block_t createNewOutputBlock(int index)
{
    block_t temp;
    temp.blockid=index;
    temp.nreserved=0;//is used as index
    temp.valid=true;
    temp.misc='e';
    return temp;
}
/**
    Hybrid Hash Join
*/
void HashJoin (char *infile1, char *infile2, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nres, unsigned int *nios){

    emptyBuffer(buffer,nmem_blocks);
    (*nios) = 0;

    //nmem_hash: number of blocks that hash table uses in memory
    int nmem_hash=nmem_blocks-2;//(int)floor(MEM_PARTITION*nmem_blocks);
    cout<<"nmem_hash: "<<nmem_hash<<endl;
    //nmem_probing: remaining blocks for probing
    int nmem_probing=nmem_blocks-nmem_hash;
    cout<<"nmem_probing: "<<nmem_probing<<endl;
    //Number of blocks in file
    int blocks=getSize(infile1);
    cout<<"blocks: "<<blocks<<endl;

    FILE *input=fopen(infile1,"rb");
    FILE *hashOut=fopen("hashOut.bin","wb");
    //read and create hashtable
    /** CREATE HASH TABLE **/
    for(int i=0;i<blocks;i++){
        /*
            read a block from disk
            check infile for ID.
            place in buffer in correct position
        */
        //read Next Block
        fseek(input,sizeof(block_t)*i,SEEK_SET);
        block_t tempBuffer;
        fread(&tempBuffer,1,sizeof(block_t),input);
        (*nios)++;
        int index=hashFunction(tempBuffer,field,nmem_hash);
        if(buffer[index].nreserved==0)
            buffer[index]=tempBuffer;
        else
        {
            fwrite(&buffer[index],1,sizeof(block_t),hashOut);
            (*nios)++;
            buffer[index]=tempBuffer;
        }
    }
    fclose(input);
    fclose(hashOut);
    int index=0;
    /*
    dimiourgw block_t sto telos tou buffer, to opoio molis ftasei sta
    MAX_RECODS_PER_BLOCK tha eggrafei sto output file.
    */
    buffer[nmem_blocks-1]=createNewOutputBlock(index);
    /*******  READ INFILE2  ********/
    int blocks2=getSize(infile2);
    FILE *output=fopen(outfile,"wb");
    FILE *read=fopen(infile2,"rb");
    int counter=0;
    int hashTableSize=getSize("hashOut.bin");
    int block_segments=hashTableSize/nmem_hash;
    int remaining_segments=hashTableSize%nmem_hash;
    int segmentsRead=0;
    hashOut=fopen("hashOut.bin","rb");
    bool exitLoop=true;
    do{
        for(int i=0;i<blocks2;i++)
        {
            /*
            diabazw ena-ena ta blocks apo to infile2 kai ta topothetw sthn pro-teleutaia thesi
            tou buffer.
            oi upoloipes theseis einai gemates me blocks_t apo ton
            pinaka katakermatismou tou prwtou arxeiou
            */
            fseek(read,sizeof(block_t)*i,SEEK_SET);
            block_t tempBuffer;
            fread(&tempBuffer,1,sizeof(block_t),read);
            (*nios)++;
            buffer[nmem_blocks-2]=tempBuffer;
            int hash_index=hashFunction(buffer[nmem_blocks-2],field,nmem_hash);
            /*
                Gia kathe stoixeio sto record[] tou block_t apo ta infile2
            */
            for(int j=0;j<MAX_RECORDS_PER_BLOCK;j++)
            {
                record_t r1=buffer[hash_index].entries[j];
                /*
                    gia kathe record[]
                    tou block_t pou proekupse apo to index meta thn hashFunction()
                */
                for(int k=0;k<MAX_RECORDS_PER_BLOCK;k++)
                {
                    record_t r2=buffer[nmem_blocks-2].entries[k];
                    bool control=false;
                    /*
                        Ginetai sugkrisi ena pros ena ta stoixeia
                        records[] metaksu twn block_t
                    */
                    switch(field)
                    {
                        case 0:
                            if(r1.recid==r2.recid)
                                control=true;
                            break;
                        case 1:
                            if(r1.num==r2.num)
                                control=true;
                            break;
                        case 2:
                            if(strcmp(r1.str,r2.str)==0)
                                control=true;
                            break;
                        case 3:
                            if(r1.num==r2.num && strcmp(r1.str,r2.str)==0)
                                control=true;
                            break;
                    }
                    /*
                        pername to stoixeio sto output block_t
                    */
                    if(control)
                    {
                        counter++;
                        int output_record_index=buffer[nmem_blocks-1].nreserved;
                        record_t r;
                        r.recid=r1.recid;
                        r.dummy1=r2.recid;
                        r.num=r1.num;
                        r.dummy2=r2.num;
                        strcpy(r.str,r1.str);
                        memcpy(&buffer[nmem_blocks-1].entries[output_record_index],&r,sizeof(record_t));
                        buffer[nmem_blocks-1].nreserved++;
                    }
                    /*
                        An exei gemisei to output block_t, to eggrafoume sto output file
                        kai ftiaxnoume kainourgio adeio block_t
                    */
                    if(buffer[nmem_blocks-1].nreserved==MAX_RECORDS_PER_BLOCK)
                    {
                        fwrite(&buffer[nmem_blocks-1],1,sizeof(block_t),output);
                        (*nios)++;
                        index++;
                        buffer[nmem_blocks-1]=createNewOutputBlock(index);
                    }
                }
            }
        }
        /*
            fill/replace hash table blocks from buffer
            with new ones.
        */
        if(segmentsRead==hashTableSize)
            exitLoop=false;
        else
        {
            cout<<"Changing Buffer"<<endl;
            cout<<"Segments Read Before Replacement: "<<segmentsRead<<endl;
            int bound=segmentsRead>=(block_segments*nmem_hash) ? remaining_segments : nmem_hash;
            for(int i=0;i<bound;i++)
            {
                fseek(hashOut,sizeof(block_t)*segmentsRead,SEEK_SET);
                block_t temp;
                fread(&temp,1,sizeof(block_t),hashOut);
                (*nios)++;
                buffer[i]=temp;
                segmentsRead++;
            }
            cout<<"Segments Read After Replacement: "<<segmentsRead<<endl;
        }
    }while(exitLoop);
    fclose(hashOut);
    //One last flush
    fwrite(&buffer[nmem_blocks-1],1,sizeof(block_t),output);
    (*nios)++;
    cout<<"Number of joins="<<counter<<endl;
    cout<<"number of blocks read: "<<blocks<<endl;
    cout<<"NIOS="<<(*nios)<<endl;
    fclose(output);
    fclose(read);
}
