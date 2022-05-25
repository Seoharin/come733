/*****************************************************************************/
/*************** Implementation of the Buffer Manager Layer ******************/
/*****************************************************************************/

#include "minirel.h"
#include "buf.h"
#include <vector>
#define A 2
#define B 3

// Define buffer manager error messages here
//enum bufErrCodes  {...};

// Define error message here
static const char* bufErrMsgs[] = { 
  // error message strings go here
  "Not enough memory to allocate hash entry",
  "Inserting a duplicate entry in the hash table",
  "Removing a non-existing entry from the hash table",
  "Page not in hash table",
  "Not enough memory to allocate queue node",
  "Poping an empty queue",
  "OOOOOOPS, something is wrong",
  "Buffer pool full",
  "Not enough memory in buffer manager",
  "Page not in buffer pool",
  "Unpinning an unpinned page",
  "Freeing a pinned page"
};

// Create a static "error_string_table" object and register the error messages
// with minibase system 
static error_string_table bufTable(BUFMGR,bufErrMsgs);

//*************************************************************
//** This is the implementation of BufMgr
//************************************************************


typedef struct description{
  PageId page_number;
  int pin_count;
  bool dirty;
}description;


description* bufDescr; // array of buffer discriptions

typedef struct bucket{
  PageId page_number;
  int frame_number;
}bucket;

bucket* hashtable;

vector<PageId>MRU;   
vector<PageId>LRU;   

BufMgr::BufMgr (int numbuf, Replacer *replacer) {
  
  this->numBuffers = numbuf;
  //allocate #numbuf * sizeof(Page) to bufPool
  bufPool = (Page*)malloc(numbuf*sizeof(Page));
  //allocate #numbuf * sizeof(description) to bufDescr
  bufDescr = (description*)malloc(numbuf*sizeof(description));
  for(int i=0;i<this->numBuffers;i++){
    bufDescr[i].page_number=INVALID_PAGE;
    bufDescr[i].pin_count = 0;
    bufDescr[i].dirty = FALSE;
  }


  
}

//*************************************************************
//** This is the implementation of ~BufMgr
//************************************************************
BufMgr::~BufMgr(){
  // put your code here
}

//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage) {
  //check if this page is in buffer pool
 
  for(int i=0;i<this->numBuffers;i++){
    if(bufDescr[i].page_number==PageId_in_a_DB){
       page = &bufPool[i];
       bufDescr[i].pin_count +=1;
      
       return OK;
    }
  }
  
    //otherwise, find a frame for this page
    //using LOVE/HATE replacement policy
  

}//end pinPage

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId page_num, int dirty=FALSE, int hate = FALSE){
  // put your code here
  for(int i =0;i<this->numBuffers;i++){
    if(bufDescr[i].page_number==page_num) {
      if(bufDescr[i].pin_count==0) {
        MINIBASE_SHOW_ERRORS();
        return FAIL;
        }

      bufDescr[i].pin_count-=1;
      if(bufDescr[i].pin_count==0)

      bufDescr[i].dirty = dirty;
      break;
    }
    
  }
  

  return OK;
}

//*************************************************************
//** This is the implementation of newPage
//************************************************************
Status BufMgr::newPage(PageId& firstPageId, Page*& firstpage, int howmany) {
  // put your code here
  MINIBASE_DB->allocate_page(firstPageId);
  if(pinPage(firstPageId,firstpage,TRUE)!=OK){

    MINIBASE_FIRST_ERROR(BUFMGR,BUFMGRMEMORYERROR);
    MINIBASE_DB->deallocate_page(firstPageId);
    return FAIL;
  }

  return OK;
}

//*************************************************************
//** This is the implementation of freePage
//************************************************************
Status BufMgr::freePage(PageId globalPageId){
  // put your code here
  for(int i=0;i<this->numBuffers;i++){
    if(bufDescr[i].page_number==globalPageId) {
      bufDescr[i].page_number = INVALID_PAGE; 
      bufDescr[i].pin_count=0;
      bufDescr[i].dirty=FALSE;
      // deallocate bufPool[i]
      MINIBASE_DB->deallocate_page(i);
      return OK;
    } 
  }
  return FAIL;
}

//*************************************************************
//** This is the implementation of flushPage
//************************************************************
Status BufMgr::flushPage(PageId pageid) {
  // memory -> disk 
  for(int i=0;i<this->numBuffers;i++){
    if(bufDescr[i].page_number == pageid){
      MINIBASE_DB->write_page(i,&bufPool[i]);
      bufDescr[i].dirty=FALSE;
      return OK;
    }
  }

  return FALSE;
}
    
//*************************************************************
//** This is the implementation of flushAllPages
//************************************************************
Status BufMgr::flushAllPages(){
  //put your code here
  for(int i=0;i<this->numBuffers;i++){
    if(bufDescr[i].dirty==TRUE){
      flushPage(bufDescr[i].page_number);
      } 
    }
    return OK; 
 
}


/*** Methods for compatibility with Projects 1-2 ***/
//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage, const char *filename){
  //put your code here

  return OK;
}

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename){
  //put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of getNumUnpinnedBuffers
//************************************************************
unsigned int BufMgr::getNumUnpinnedBuffers(){
  //put your code here
  int unpincnt=0;
  for(int i=0;i<this->numBuffers;i++){
    if(bufDescr[i]->pin_count ==0) unpincnt++;
  }
  return unpincnt;
}

int BufMgr::HashFunction(PageId page_number){
    return(A*page_number+B)/HTSIZE;
}
