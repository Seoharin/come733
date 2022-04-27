#include "heapfile.h"
#include <vector>
#include <memory.h>
#define DIRTY 1
#define CLEAN 0
#define T 1
#define F 0
// ******************************************************
// Error messages for the heapfile layer

static const char *hfErrMsgs[] = {
    "bad record id",
    "bad record pointer", 
    "end of file encountered",
    "invalid update operation",
    "no space on page for record", 
    "page is empty - no records",
    "last record on page",
    "invalid slot number",
    "file has already been deleted",
};

typedef struct{
    PageId headerPageId;
    vector<HFPage*> pages;
}directory;          //mapping of page IDs to directory pages

vector<directory> dirs;    //list of all directories for all files

vector<HFPage*> directoryPages;   //holds the Directory pages for currently open file
static error_string_table hfTable( HEAPFILE, hfErrMsgs );



int recCount = 0;

// ********************************************************
// Constructor
HeapFile::HeapFile( const char *name, Status& returnStatus )
{
   PageId start_pg;
   int i, namelen;
    
   namelen = strlen(name);
   if (namelen > MAX_NAME) {
        returnStatus = FAIL;
        return;
    }
    
    this->file_deleted = F;
    
    this->fileName = (char*) malloc(sizeof(char)*namelen);
    for(i=0;i<namelen;i++)
        this->fileName[i]=name[i];

    if( MINIBASE_DB->get_file_entry(name, start_pg)!=OK) directoryPages.clear();
    else{
           i=0;
       while(1){
           if(dirs[i].headerPageId==start_pg) break;
           i++;
       }
       directoryPages=dirs[i].pages;
    }
 
    returnStatus = OK;
   
}

// ******************
// Destructor
HeapFile::~HeapFile()
{
   // fill in the body 

}

// *************************************
// Return number of records in heap file
int HeapFile::getRecCnt()
{
   // fill in the body
    int rcnt = 0;
    HFPage *hfpage;
    Page * page;
    RID rid, temp;
    DataPageInfo* pinfo;
    char* recptr;
    int recLen;
    int i=0;
    
    while(i<directoryPages.size()){
        hfpage = directoryPages[i];
        page = (Page *)hfpage;
        
        MINIBASE_BM->pinPage(hfpage->page_no(), page, hfpage->empty(), this->fileName);
      
        
        if((hfpage->firstRecord(rid))==OK)
        {
            do{
            hfpage->returnRecord(rid, recptr, recLen);
            temp = rid;
            pinfo = (DataPageInfo*)recptr;
            rcnt = rcnt+pinfo->recct;
          
            }while((hfpage->nextRecord(temp,rid))==OK); 
        }
        
        MINIBASE_BM->unpinPage(hfpage->page_no(), CLEAN, this->fileName);
      
        i++;
    }
    
    return rcnt;
}


// *****************************
// Insert a record into the file
Status HeapFile::insertRecord(char* recPtr, int recLen, RID& outRid)
{
    if (recPtr == NULL)
        return MINIBASE_FIRST_ERROR(HEAPFILE, BAD_REC_PTR);

    if (recLen >= MINIBASE_PAGESIZE)
        return MINIBASE_FIRST_ERROR(HEAPFILE, NO_SPACE);
    
    
    HFPage* hfpage,*hfdatapage;
    Page *page,*datapage,*newpage;
    DataPageInfo* pinfo, *newinfo;
    RID rid,currid;
    char *recptr;
    int reclen;
    for (int i = 0; i < directoryPages.size(); i++) {
        hfpage = directoryPages[i];
        page = (Page*)hfpage;
        MINIBASE_BM->pinPage(hfpage->page_no(), page, hfpage->empty(), this->fileName);
       
        
        hfpage->firstRecord(rid);
        
        while (1) {
            hfpage->returnRecord(rid, recptr, reclen);
           
            pinfo = (DataPageInfo*)recptr;
            if (pinfo->availspace > recLen) {
                outRid.pageNo = pinfo->pageId;
                
                MINIBASE_BM->pinPage(pinfo->pageId, datapage, 0, this->fileName);
                hfdatapage = (HFPage*)datapage;
                
                if(hfdatapage->insertRecord(recPtr,recLen,outRid)==OK){
                     pinfo->availspace = hfdatapage->available_space();
                     pinfo->recct += 1;
                     recCount += 1;

                     MINIBASE_BM->unpinPage(pinfo->pageId, DIRTY, this->fileName);
                     MINIBASE_BM->unpinPage(hfpage->page_no(), DIRTY, this->fileName);
               
                     return OK;
                    
                }else return MINIBASE_CHAIN_ERROR(HEAPFILE, insertStatus);
                
            }
            currid = rid;
            if(hfpage->nextRecord(currid,rid)!=OK) break;
           
        }
        MINIBASE_BM->unpinPage(hfpage->page_no(), CLEAN, this->fileName);
        
    }
   
    
    newinfo = (DataPageInfo*)malloc(sizeof(DataPageInfo));
  
    newDataPage(newinfo);
   
    newinfo->recct =newinfo->recct+ 1;
    recCount ==recCount+ 1;

    
    
    PageId dirId, dataId;
    RID dirRid;
    Page* pg;

    //create directory entry for new page
    allocateDirSpace(newinfo, dirId, dirRid);
    MINIBASE_BM->pinPage(dirId, pg, 0, this->fileName);

    HFPage* dirhfpage = (HFPage*)pg;
    char* rec;
    int reclength;
    DataPageInfo *info1;
    if(dirhfpage->returnRecord(dirRid,rec,reclength)==OK){
        info1 = (DataPageInfo*)rec;
        
    }else return MINIBASE_FIRST_ERROR(HEAPFILE, RECNOTFOUND);
    

    //pin datapage for insertion


    MINIBASE_BM->pinPage(newinfo->pageId, newpage, 0, this->fileName);
   
    HFPage* newhfpage = (HFPage*)newpage;
    Status insertStatus = newhfpage->insertRecord(recPtr, recLen, outRid);

    info1->availspace = newhfpage->available_space();

    MINIBASE_BM->unpinPage(dirId, DIRTY, this->fileName);
    MINIBASE_BM->unpinPage(newinfo->pageId, DIRTY, this->fileName);

   
    return insertStatus;
}


// ***********************
// delete record from file
Status HeapFile::deleteRecord (const RID& rid)
{
  if(rid.slotNo < 0)
        return MINIBASE_FIRST_ERROR(HEAPFILE,INVALID_SLOTNO);
  // fill in the body
    HFPage *hfpage;
    Page *page;
    DataPageInfo *pinfo;
    RID currid,temp;
    char *recptr;
    int i,reclen;
    Status status;

    
    for(int i=0;i<directoryPages.size();i++)
    {
        hfpage = directoryPages[i];
        page = (Page *)hfpage;
        MINIBASE_BM->pinPage(hfpage->page_no(), page, 0, this->fileName);
      
        if(hfpage->firstRecord(currid)==OK)
        {
            while(1)
            {
                if(hfpage->returnRecord(currid,recptr,reclen)==OK)
                {
                    pinfo = (DataPageInfo*)recptr;
                    if(pinfo->pageId==rid.pageNo) break;
                    temp=currid;
                    if(hfpage->nextRecord(temp,currid)!=OK) break;
                }
            }
        }
        
        if(hfpage->returnRecord(currid,recptr,reclen)==OK)
        {
            Page *dataPage;
            
            MINIBASE_BM->pinPage(rid.pageNo,dataPage,0,this->fileName);
            HFPage *dp = (HFPage *)dataPage;
            
            if(dp->deleteRecord(rid)==OK)
            {
                MINIBASE_BM->unpinPage(rid.pageNo,DIRTY,this->fileName);
                pinfo->availspace = dp->available_space();
                pinfo->recct = pinfo->recct- 1;
                MINIBASE_BM->unpinPage(hfpage->page_no(),DIRTY,this->fileName);
                return OK;
            }else return FAIL;
            
        }
       MINIBASE_BM->unpinPage(hfpage->page_no(), CLEAN, this->fileName);
  
    }
    return DONE;

}

// *******************************************
// updates the specified record in the heapfile.
Status HeapFile::updateRecord(const RID& rid, char* recPtr, int recLen)
{
    if (recPtr == NULL)
        return MINIBASE_FIRST_ERROR(HEAPFILE, BAD_REC_PTR);
    if (rid.slotNo < 0)
    {
        return MINIBASE_FIRST_ERROR(HEAPFILE, INVALID_SLOTNO);
    }
    HFPage * hfpage;
    Page * page;
    DataPageInfo * pinfo;
    RID currid, temp;
    char* recptr;
    int reclen;

    for (int i = 0; i < directoryPages.size(); i++)
    {
        hfpage = directoryPages[i];
        page = (Page*)hfpage;
        
        
        MINIBASE_BM->pinPage(hfpage->page_no(), page, 0, this->fileName);
       
        if(hfpage->firstRecord(currid)==OK) {
            while(1){
                if(hfpage->returnRecord(currid, recptr,reclen)==OK){
                    pinfo = (DataPageInfo*) recptr;
                    if(pinfo->pageId == rid.pageNo){
                         Page* dataPage;
                         Status pinStatus = MINIBASE_BM->pinPage(rid.pageNo, dataPage, 0, this->fileName);
                         if (pinStatus != OK)
                            return MINIBASE_CHAIN_ERROR(BUFMGR, pinStatus);
                         HFPage* hfDataPage = (HFPage*)dataPage;
                         char* origin;
                         int len;
                         if(hfDataPage->returnRecord(rid,origin,len)==OK){
                             if(len==recLen){
                                 memmove(origin, recPtr, recLen);
                                 MINIBASE_BM->unpinPage(rid.pageNo, DIRTY, this->fileName);
                                 MINIBASE_BM->unpinPage(hfpage->page_no(), CLEAN, this->fileName);
                                 
                             }else return MINIBASE_FIRST_ERROR(HEAPFILE, INVALID_UPDATE);
                             
                         }else return MINIBASE_FIRST_ERROR(HEAPFILE, RECNOTFOUND);
                         return OK;
                    }
                    temp = currid;
                    if(hfpage->nextRecord(temp,currid)!=OK) break;
                    
                }else return MINIBASE_FIRST_ERROR(HEAPFILE, RECNOTFOUND);
            }
                
        }
       
        MINIBASE_BM->unpinPage(hfpage->page_no(), CLEAN, this->fileName);
       
    }

    // fill in the body
    return OK;
}
// ***************************************************
// read record from file, returning pointer and length
Status HeapFile::getRecord (const RID& rid, char *recPtr, int& recLen)
{
  // fill in the body
  HFPage *hfpage;
  Page *page;
  DataPageInfo * pinfo;
  RID currid, temp;
  char *recptr;
  int reclen;
    
  for(int i=0;i<directoryPages.size();i++)
  {
      hfpage = directoryPages[i];
      page = (Page *)hfpage;
      MINIBASE_BM->pinPage(hfpage->page_no(),page,0,this->fileName);
      
      if(hfpage->firstRecord(currid)==OK){
          while(1){
              if(hfpage->returnRecord(currid,recptr,reclen)==OK){
                   pinfo = (DataPageInfo *)recptr;
                   if(pinfo->pageId == rid.pageNo) {
                   Page *dataPage;
                   MINIBASE_BM->pinPage(rid.pageNo,dataPage,0,this->fileName);
             
                   HFPage *dp = (HFPage *)dataPage;
                   char *record;
                   if(dp->returnRecord(rid,record,recLen)==OK){
                       memmove(recPtr,record,recLen);
                       MINIBASE_BM->unpinPage(rid.pageNo,CLEAN,this->fileName);
                       MINIBASE_BM->unpinPage(hfpage->page_no(),CLEAN,this->fileName);  
                   }else return MINIBASE_CHAIN_ERROR(BUFMGR,FAIL);
                       
                   return OK;
                   }
              temp = currid;
              if(hfpage->nextRecord(temp,currid)!=OK) break;
              }else return MINIBASE_FIRST_ERROR(HEAPFILE,RECNOTFOUND);
              
            
          }
      }
      
      MINIBASE_BM->unpinPage(hfpage->page_no(),CLEAN,this->fileName);
     
  }
  return DONE;
}

// **************************
// initiate a sequential scan
Scan *HeapFile::openScan(Status& status)
{
  Scan *scan = new Scan(this,status);


  return scan;
}

// ****************************************************
// Wipes out the heapfile from the database permanently. 
Status HeapFile::deleteFile()
{
    
    MINIBASE_DB->delete_file_entry(this->fileName) ;
    this->file_deleted=T;
    
    int i=0;
    while(i<dirs.size()){
        if(dirs[i++].headerPageId == directoryPages[0]->page_no()) break;
    }
    dirs.erase(dirs.begin()+i);
    return OK;
    // fill in the body
}

// ****************************************************************
// Get a new datapage from the buffer manager and initialize dpinfo
// (Allocate pages in the db file via buffer manager)
Status HeapFile::newDataPage(DataPageInfo *dpinfop)
{
    
    if(dpinfop==NULL) return FAIL;
    
    int dataPageId;
    Page *dataPageVar;
    HFPage *dataPage;
    MINIBASE_BM->newPage(dataPageId,dataPageVar,1);
    
    dataPage = (HFPage *)dataPageVar;
    dataPage->init(dataPageId);


    dpinfop->availspace = dataPage->available_space();
    dpinfop->pageId = dataPageId;
    dpinfop->recct = 0;

    MINIBASE_BM->unpinPage(dataPageId,DIRTY,this->fileName);
   
    return OK;
}

// ************************************************************************
// Internal HeapFile function (used in getRecord and updateRecord): returns
// pinned directory page and pinned data page of the specified user record
// (rid).
//
// If the user record cannot be found, rpdirpage and rpdatapage are 
// returned as NULL pointers.
//
Status HeapFile::findDataPage(const RID& rid,
                    PageId &rpDirPageId, HFPage *&rpdirpage,
                    PageId &rpDataPageId,HFPage *&rpdatapage,
                    RID &rpDataPageRid)
{
    rpdirpage = NULL;
    rpdatapage = NULL;
    // fill in the body
    HFPage *hfpage;
    Page *page, *dp;
    DataPageInfo* pinfo;
    RID currid,temp;
    char *recptr;
    int reclen, pageNo;
    
    
    for(int i=0;i<directoryPages.size();i++)
    {
        hfpage = directoryPages[i];
        pageNo = hfpage->page_no();
        
        if(hfpage->firstRecord(currid)==OK){
            while(1){
                if(hfpage->getRecord(currid,recptr,reclen)==OK){
                    pinfo = (DataPageInfo*)recptr;
                    if(pinfo->pageId==rid.pageNo){
                        MINIBASE_BM->pinPage(pageNo,page,0,this->fileName);
                        MINIBASE_BM->pinPage(rid.pageNo,dp,0,this->fileName);
                        rpdatapage = (HFPage *)dp;
                        rpdirpage = (HFPage *)page;
                        rpDirPageId = pageNo;
                        rpDataPageId = rid.pageNo;
                        rpDataPageRid = rid;
                        return OK;
                    }
                }else return MINIBASE_CHAIN_ERROR(HEAPFILE,FAIL);
                RID temp = currid;
                if(hfpage->nextRecord(temp,currid)!=OK) break;
                
            }
            
        }
    }

    return DONE;
}

// *********************************************************************
// Allocate directory space for a heap file page 
Status HeapFile::allocateDirSpace(struct DataPageInfo * dpinfop,/* data page information*/
                            PageId &allocDirPageId,/*Directory page having the first data page record*/
                            RID &allocDataPageRid)
{
    HFPage *hfpage;
    Page *page;
    RID rid;
    int pageId;
    int i;
    
    //check existing pages
    for(int i=0;i<directoryPages.size();i++)
    {
        hfpage = directoryPages[i];

        if(hfpage->available_space()>sizeof(DataPageInfo))
        {
            page = (Page*) hfpage;
            allocDirPageId = hfpage->page_no();
            MINIBASE_BM->pinPage(allocDirPageId,page,hfpage->empty(),this->fileName);
           
            
            if(hfpage->insertRecord((char *)dpinfop,sizeof(DataPageInfo),allocDataPageRid)==OK){
                MINIBASE_BM->unpinPage(allocDirPageId,DIRTY,this->fileName);
                return OK;
            }else  return MINIBASE_CHAIN_ERROR(HEAPFILE,FAIL);
        }
    }


    //no space, so create new directory page
    HFPage *newpage;
    MINIBASE_BM->newPage(pageId,page,1);
   
    newpage = (HFPage *)page;
    newpage->init(pageId);
    allocDirPageId = newpage->page_no();
    
    if(newpage->insertRecord((char *)dpinfop, sizeof(DataPageInfo),allocDataPageRid)==OK){
         MINIBASE_BM->unpinPage(allocDirPageId,DIRTY,this->fileName);
    
         directoryPages.push_back(newpage);
         if(directoryPages.size()==1) {
            MINIBASE_DB->add_file_entry(this->fileName,newpage->page_no());
            directory d;
            d.pages = directoryPages;
            d.headerPageId = newpage->page_no();
            dirs.push_back(d);
        } else {
        HFPage *hpg = directoryPages[0];
        for(int i=0;i<dirs.size();i++) {
            if(dirs[i].headerPageId==hpg->page_no())
                dirs[i].pages = directoryPages;
            }
        }
        
     }else return MINIBASE_CHAIN_ERROR(HEAPFILE,FAIL);
    
    return OK;
}



// *******************************************
