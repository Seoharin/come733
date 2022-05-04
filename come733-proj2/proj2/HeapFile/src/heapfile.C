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


vector<HFPage*> directoryPages;   //current opened directory pages

typedef struct{
    PageId headerPageId;
    vector<HFPage*> pages;
}pagedirectory;        
//pagedirectory has headerpageid and pages

vector<pagedirectory> all_directories;    //lise of all directories
static error_string_table hfTable( HEAPFILE, hfErrMsgs );



int reccnt = 0;

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
           if(all_directories[i].headerPageId==start_pg) break;
           i++;
       }
       directoryPages=all_directories[i].pages;
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
    
    PageId directoryid;
    HFPage* hfpage,*hfdatapage, *directoryhfpage;
    Page *page,*datapage,*newpage,*pg;
    DataPageInfo* pinfo, *newinfo,*pinfo2;
    RID rid,currid,directoryrid;
    char *recptr,*recptr2;
    int reclen,reclen2;
    
    
    //if datapage has space 
    for (int i = 0; i < directoryPages.size(); i++) {
        hfpage = directoryPages[i];
        hfpage->firstRecord(rid);
        
        while (1) {
            hfpage->returnRecord(rid, recptr, reclen);
           
            pinfo = (DataPageInfo*)recptr;
            if (pinfo->availspace > recLen) {
                outRid.pageNo = pinfo->pageId;
                
                MINIBASE_BM->pinPage(hfpage->page_no(), (Page*&)hfpage, hfpage->empty(), this->fileName);
                MINIBASE_BM->pinPage(pinfo->pageId, (Page*&)hfpage,  hfpage->empty(), this->fileName);
                //hfdatapage = (HFPage*)datapage;
                
                if(hfpage->insertRecord(recPtr,recLen,outRid)==OK){
                     pinfo->availspace = hfpage->available_space();
                     pinfo->recct += 1;
                     reccnt += 1;

                     MINIBASE_BM->unpinPage(pinfo->pageId, DIRTY, this->fileName);
                     MINIBASE_BM->unpinPage(hfpage->page_no(), DIRTY, this->fileName);
               
                     return OK;
                    
                }else return MINIBASE_CHAIN_ERROR(HEAPFILE, FAIL);
                
            } 
            currid = rid;
            if(hfpage->nextRecord(currid,rid)!=OK) break;
           
        }
     
        
    }
   
    
    //if datapage hasn't space, create new data page
    newinfo = (DataPageInfo*)malloc(sizeof(DataPageInfo));
  
    newDataPage(newinfo);
   
    newinfo->recct =newinfo->recct+ 1;
    reccnt ==reccnt+ 1;

    
    
    allocateDirSpace(newinfo, directoryid, directoryrid);
    MINIBASE_BM->pinPage(directoryid, pg, 0, this->fileName);

    directoryhfpage = (HFPage*)pg;
    
    if(directoryhfpage->returnRecord(directoryrid,recptr2,reclen2)==OK){
        pinfo2 = (DataPageInfo*)recptr2;
        
    }else return MINIBASE_FIRST_ERROR(HEAPFILE, RECNOTFOUND);
    

    MINIBASE_BM->pinPage(newinfo->pageId, newpage, 0, this->fileName);
   
    HFPage* newhfpage = (HFPage*)newpage;
    if(newhfpage->insertRecord(recPtr,recLen,outRid)==OK){
        pinfo2->availspace = newhfpage->available_space();
        MINIBASE_BM->unpinPage(directoryid, DIRTY, this->fileName);
        MINIBASE_BM->unpinPage(newinfo->pageId, DIRTY, this->fileName);
        return OK;
    }else return FAIL;
  
}


// ***********************
// delete record from file


Status HeapFile::deleteRecord (const RID& rid)
{
   if(rid.slotNo < 0)
        return MINIBASE_FIRST_ERROR(HEAPFILE,INVALID_SLOTNO);
  // fill in the body
    HFPage *hfdirpage, *hfdatapage;
    PageId dirpid, datapid;
    RID currid;
    char *recptr;
    int reclen;
    
    if(findDataPage(rid, dirpid, hfdirpage, datapid, hfdatapage, currid)==OK){
        
        MINIBASE_BM->pinPage(datapid,(Page*&)hfdatapage,hfdatapage->empty(),this->fileName);
        hfdatapage->returnRecord(rid,recptr,reclen);
        hfdatapage->deleteRecord(rid);
        MINIBASE_BM->unpinPage(datapid,DIRTY,this->fileName);
       
    }else return FAIL;
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
    
    HFPage *hfdirpage, *hfdatapage;
    PageId dirpid, datapid;
    RID currid;
    char *recptr;
    int reclen;
    
    if(findDataPage(rid, dirpid, hfdirpage, datapid, hfdatapage, currid)==OK){
        
        MINIBASE_BM->pinPage(datapid,(Page*&)hfdatapage,hfdatapage->empty(),this->fileName);
        hfdatapage->returnRecord(rid,recptr,reclen);
        if(reclen==recLen){
             memmove(recptr, recPtr, recLen);
             MINIBASE_BM->unpinPage(datapid,DIRTY,this->fileName);
             return OK;
        }else return MINIBASE_FIRST_ERROR(HEAPFILE, INVALID_UPDATE);
    }else return FAIL;
    
    
}
// ***************************************************
// read record from file, returning pointer and length
Status HeapFile::getRecord (const RID& rid, char *recPtr, int& recLen)
{
  // fill in the body
   
  
   HFPage *hfdirpage, *hfdatapage;
   PageId dirpid, datapid;
   RID currid;
   char *recptr;
   int reclen;
    
   if(findDataPage(rid, dirpid, hfdirpage, datapid, hfdatapage, currid)==OK){
        
       MINIBASE_BM->pinPage(datapid,(Page*&)hfdatapage,hfdatapage->empty(),this->fileName);
       hfdatapage->returnRecord(rid,recptr,reclen);
       if(reclen==recLen){
            memmove(recPtr, recptr, recLen);
            MINIBASE_BM->unpinPage(datapid,DIRTY,this->fileName);
            return OK;
       }else return MINIBASE_FIRST_ERROR(HEAPFILE, INVALID_UPDATE);
   }else return FAIL;
    
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
    while(i<all_directories.size()){
        if(all_directories[i++].headerPageId == directoryPages[0]->page_no()) break;
    }
    all_directories.erase(all_directories.begin()+i);
    return OK;
   
}

// ****************************************************************
// Get a new datapage from the buffer manager and initialize dpinfo
// (Allocate pages in the db file via buffer manager)
Status HeapFile::newDataPage(DataPageInfo *dpinfop)
{
    
    if(dpinfop==NULL) return FAIL;
    
    int pageid;
    Page *newpage;
    HFPage *newhfpage;
    MINIBASE_BM->newPage(pageid,newpage,1);
    
    newhfpage = (HFPage *)newpage;
    newhfpage->init(pageid);

    
    dpinfop->pageId = pageid;
    dpinfop->recct = 0;
    dpinfop->availspace = newhfpage->available_space();
    
    

    MINIBASE_BM->unpinPage(pageid,DIRTY,this->fileName);
   
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
                if(hfpage->returnRecord(currid,recptr,reclen)==OK){
                    pinfo = (DataPageInfo*)recptr;
                    if(pinfo->pageId==rid.pageNo){
                        MINIBASE_BM->pinPage(pageNo,page,0,this->fileName);
                        MINIBASE_BM->pinPage(rid.pageNo,dp,0,this->fileName);
                        
                        rpDirPageId = pageNo;
                        rpdirpage = (HFPage *)page;
                        
                        
                        rpDataPageId = pinfo->pageId;
                        rpdatapage = (HFPage *)dp;
                        
                        rpDataPageRid = rid;
                        MINIBASE_BM->unpinPage(rid.pageNo);
                        MINIBASE_BM->unpinPage(pageNo);
                        return OK;
                    }
                }else return MINIBASE_CHAIN_ERROR(HEAPFILE,FAIL);
                temp = currid;
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
            pagedirectory pd;
            pd.pages = directoryPages;
            pd.headerPageId = newpage->page_no();
            all_directories.push_back(pd);
        } else {
        HFPage *hpg = directoryPages[0];
        
        i=0;
        while(i<all_directories.size()){
            if(all_directories[i].headerPageId==hpg->page_no())
                all_directories[i++].pages=directoryPages; 
        }
      }
        
     }else return MINIBASE_CHAIN_ERROR(HEAPFILE,FAIL);
    
    return OK;
}



// *******************************************
