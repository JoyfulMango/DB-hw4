#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


const Status BufMgr::allocBuf(int & frame) 
{

    //Index before incrementing clock for first time
    int startIndex = clockHand;
    //Counter to count how many pages are pinned every full loop 
    int pinCounter = 0;

    //Perma loop
    while(1)
    {
        
        advanceClock();

        BufDesc curr = bufTable[clockHand]
        //If frame is empty then return frame
        if( curr.valid == false )
        {
            frame = &bufPool[clockHand];
            return OK;

        }
        //If refbit is true then set to false and move to next
        if( curr.refBit == true )
        {
            bufTable.refBit = false;
            continue;

        }
        //If pin count is greater than 0, move on to next
        if( curr.pinCnt > 0 )
        {
            //increment counter to see how many frames are full
            pinCounter++;
         
            //If back where we started, and every frame is full, then return bufferexceeded
            if( clockHand == startIndex ) 
            {
                if(pinCounter == numBufs) 
                {
                    return BUFFEREXCEEDED;
                }
                else
                {
                    pinCounter = 0;

                }
            }

            continue;
        }
        //if dirty, write back
        if( curr.dirty == true )
        {
            Status write = curr.file->writePage(curr.pageNo, &bufPool[clockHand] );
            if( write != OK )
            {
                return UNIXERR;
            }

        }

        frame = &bufPool[clockHand];
        return OK;
    }
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{





}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{





}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{

    Status status;
    
    if(file->allocatePage(pageNo) != OK)
    {
        return UNIXERR;
    }

    int freshFrame;
    status = allocBuf(freshFrame);
    if(status != OK)
    {
        return status;
    }

    status = hashTable.insert(file, pageNo, freshFrame);
    if(status != OK)
    {
        return status;
    }

    bufTable[freshFrame].Set(file, pageNo);

    page = &bufPool[freshFrame];

    return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


