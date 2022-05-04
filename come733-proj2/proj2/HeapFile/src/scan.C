/*
 * implementation of class Scan for HeapFile project.
 * $Id: scan.C,v 1.1 1997/01/02 12:46:42 flisakow Exp $
 */

#include <stdio.h>
#include <stdlib.h>

#include "heapfile.h"
#include "scan.h"
#include "hfpage.h"
#include "buf.h"
#include "db.h"

#define DIRTY 1
#define CLEAN 0









extern vector<HFPage*> directoryPages; //current opened directory pages
char *record;
int recLen;

// *******************************************
// The constructor pins the first page in the file
// and initializes its private data members from the private data members from hf


Scan::Scan (HeapFile *hf, Status& status)
{
  // put your code here
  init(hf);
  status = OK;
  
}

// *******************************************
// The deconstructor unpin all pages.
Scan::~Scan()
{
    Status status;
    status = reset();
}


// *******************************************
// Retrieve the next record in a sequential scan.
// Also returns the RID of the retrieved record.
Status Scan::getNext(RID& rid, char *recPtr, int& recLen)
{
   
    while(scanIsDone!=0) {
        MINIBASE_BM->unpinPage(dirPageId,CLEAN,_hf->fileName);
        MINIBASE_BM->unpinPage(dataPageId,CLEAN,_hf->fileName);

        return DONE;
        break;
    }


    Status sts = dataPage->returnRecord(userRid,record,recLen);

    memcpy(recPtr,record,recLen);
    rid = userRid;
    if(dataPage->nextRecord(rid,userRid)!=OK) {
       sts = nextDataPage();
       if(sts!=OK){
           scanIsDone=1;
       }
    }
    return OK;
}


// *******************************************
// Do all the constructor work.
Status Scan::init(HeapFile *hf)
{
  // put your code here
  this->_hf = hf;
  firstDataPage();
  return OK;
}

// *******************************************
// Reset everything and unpin all pages.
Status Scan::reset()
{
  // put your code here
  return OK;
}



// *******************************************
// Copy data about first page in the file.
Status Scan::firstDataPage()
{
    
    Page *page1, *page2;
    DataPageInfo *info;
    dirPage = directoryPages[0];
    
    
    page1 = (Page *)dirPage;
    
    if(MINIBASE_BM->pinPage(dirPage->page_no(),page1,CLEAN,_hf->fileName)!=OK){
        Status pinNow =MINIBASE_BM->pinPage(dirPage->page_no(),page1,CLEAN,_hf->fileName);
        return MINIBASE_CHAIN_ERROR(BUFMGR,pinNow);
        
    }else if(dirPage->firstRecord(dataPageRid)!=OK){
        Status sts = dirPage->firstRecord(dataPageRid);
        return sts;
    }
    


    dirPageId = dirPage->page_no();
    while(dirPage->returnRecord(dataPageRid,record,recLen)!=OK){
        return MINIBASE_FIRST_ERROR(HEAPFILE,RECNOTFOUND);
        break;
    }



    info = (DataPageInfo *)record;
    dataPageId = info->pageId;
    while(MINIBASE_BM->pinPage(dataPageId,page2,CLEAN,_hf->fileName)!=OK){
        Status pinNow = MINIBASE_BM->pinPage(dataPageId,page2,CLEAN,_hf->fileName);
        return MINIBASE_CHAIN_ERROR(BUFMGR,pinNow);
        break;
    }



    dataPage = (HFPage *)page2;
    scanIsDone=0;
    nxtUserStatus= dataPage->firstRecord(this->userRid);

    return OK;
}





// *******************************************
// Retrieve the next data page.
Status Scan::nextDataPage()
{
    RID nextDP;
    Page *page;
    
    
    while(dirPage->nextRecord(dataPageRid,nextDP)!=OK){
       return nextDirPage();
       break;
    }



    dataPageRid = nextDP;
    
    if(MINIBASE_BM->unpinPage(dataPageId,CLEAN,_hf->fileName)!=OK){
        Status pinNow = MINIBASE_BM->unpinPage(dataPageId,CLEAN,_hf->fileName);
        return MINIBASE_CHAIN_ERROR(BUFMGR,pinNow);

    }else if(this->dirPage->returnRecord(nextDP,record,recLen)!=OK){
        return MINIBASE_CHAIN_ERROR(HEAPFILE,RECNOTFOUND);
    }
    


    DataPageInfo *info = (DataPageInfo *)record;
    dataPageId = info->pageId;
    while(MINIBASE_BM->pinPage(dataPageId,page,CLEAN,_hf->fileName)!=OK){
        return MINIBASE_CHAIN_ERROR(HEAPFILE,RECNOTFOUND);
        break;
    }
    


    dataPage = (HFPage *)page;
    userRid.pageNo = info->pageId;
    nxtUserStatus= dataPage->firstRecord(userRid);
   
   return OK;
}





// *******************************************
// Retrieve the next directory page.
Status Scan::nextDirPage() {



    Page *page1, *page2;
    HFPage *hfpg, *dirPage;
    DataPageInfo *info;

    int dirPgId = dirPageId;
    int idx;
    char *record;
    int recLen;
    int i = 0;
    int d = directoryPages.size();

    while(d>i)
    {
        hfpg = directoryPages[i];
        if(hfpg->page_no()==dirPgId) {
            idx = i;
            break;
        }
        i++;
    }

    int dpsize = directoryPages.size()-1;
    if(idx==dpsize){
        return DONE;
    }

    
    if(MINIBASE_BM->unpinPage(dirPgId,CLEAN,_hf->fileName)!=OK){
        Status pinNow = MINIBASE_BM->unpinPage(dirPgId,CLEAN,_hf->fileName);
        return MINIBASE_CHAIN_ERROR(BUFMGR,pinNow);

    } else if(MINIBASE_BM->unpinPage(dataPage->page_no(),CLEAN,_hf->fileName)!=OK){
        Status pinNow = MINIBASE_BM->unpinPage(dataPage->page_no(),CLEAN,_hf->fileName);
        return MINIBASE_CHAIN_ERROR(BUFMGR,pinNow);
    }
    
    idx++;
    this->dirPageId = directoryPages[idx]->page_no();
    this->dirPage = directoryPages[idx];
    page1 = (Page *)directoryPages[idx];
    
    
    
    while(MINIBASE_BM->pinPage(dirPageId,page1,CLEAN,_hf->fileName)!=OK){
        Status pinNow = MINIBASE_BM->pinPage(dirPageId,page1,CLEAN,_hf->fileName);
        return MINIBASE_CHAIN_ERROR(BUFMGR,pinNow);
        break;
    }
    

    dirPage = directoryPages[idx];
    dirPage->firstRecord(dataPageRid);
    
    
    while(dirPage->returnRecord(dataPageRid,record,recLen)!=OK){
        return MINIBASE_CHAIN_ERROR(HEAPFILE,RECNOTFOUND);
        break;
    }
    
    info = (DataPageInfo *)record;
    dataPageId = info->pageId;
    
    while(MINIBASE_BM->pinPage(info->pageId,page2,CLEAN,_hf->fileName)!=OK){
        Status pinNow = MINIBASE_BM->pinPage(info->pageId,page2,CLEAN,_hf->fileName);
        return MINIBASE_CHAIN_ERROR(BUFMGR,pinNow);
        break;
    }

    dataPage = (HFPage *)page2;
    nxtUserStatus=dataPage->firstRecord(userRid);

  
  return OK;
}
