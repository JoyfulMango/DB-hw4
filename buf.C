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

        //If frame is empty then return frame
        if( bufTable[clockHand].valid == false )
        {
            frame = clockHand;
            return OK;

        }
        //If refbit is true then set to false and move to next
        if( bufTable[clockHand].refbit == true )
        {
            bufTable[clockHand].refbit = false;
            continue;

        }
        //If pin count is greater than 0, move on to next
        if( bufTable[clockHand].pinCnt > 0 )
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
                //if not bufferexceeded, reset counter for next full loop
                else
                {
                    pinCounter = 0;

                }
            }

            continue;
        }
        //if dirty, write back
        if( bufTable[clockHand].dirty == true )
        {
            //Write file back to disk
            Status write = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, &bufPool[clockHand]);
            if( write != OK )
            {
                return UNIXERR;
            }
        }

        //remove page from hashtable and clear frame desc
        hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);
        bufTable[clockHand].Clear();

        //frame = &bufPool[clockHand];
        frame = clockHand;
        return OK;
    }
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo = 0;
    Status status;

    if(hashTable->lookup(file, PageNo, frameNo) == OK)
    {
        bufTable[frameNo].refbit = true;
        bufTable[frameNo].pinCnt++;
        page = &bufPool[frameNo];
        status = OK;
        //printSelf();

    }
    else
    {
        int frame;
        Status ab = allocBuf(frame);
        if(ab == OK)
        {
            Status rp = file->readPage(PageNo, &(bufPool[frame]));
            if( rp != OK)
            {
                return rp;
            }
            if( hashTable->insert(file, PageNo, frame) == OK )
            {
                bufTable[frame].Set(file, PageNo);
                page = &(bufPool[frame]); 
                status = OK;

            }
            else
            {
                status = HASHTBLERROR;
            }

        }
        else if( ab == UNIXERR )
        {
            status =  UNIXERR;
        }
        else if( ab == BUFFEREXCEEDED)
        {
            status = BUFFEREXCEEDED;
        }


    }

    return status;

}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{

    int frameNo = 0;
    Status status = hashTable->lookup(file, PageNo, frameNo);
    // BufDesc frame = bufTable[frameNo];

    if (status == HASHNOTFOUND) 
    {
        return HASHNOTFOUND;
    }

    int pinCount = bufTable[frameNo].pinCnt;
    if (pinCount == 0) 
    {
        return PAGENOTPINNED;
    }

    if(dirty == true) 
    {
        bufTable[frameNo].dirty = true;
    }

    bufTable[frameNo].pinCnt--;

    return OK;

}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{

    Status status = OK;
    
    if(file->allocatePage(pageNo) != OK)
    {
        return UNIXERR;
    }

    int freshFrame = 0;
    status = allocBuf(freshFrame);
    if(status != OK)
    {
        return status;
    }

    status = hashTable->insert(file, pageNo, freshFrame);
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
            cout << "\tvalid\t";
        cout << tmpbuf->file;
        cout << endl;
    };
}


