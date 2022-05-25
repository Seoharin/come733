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
  bucket* next_page;
}bucket;


bucket **hashtable;



vector<PageId>MRU;   
vector<PageId>LRU;   

BufMgr::BufMgr (int numbuf, Replacer *replacer) {
  
  this->numBuffers = numbuf;
  //allocate #numbuf * sizeof(Page) to bufPool
  bufPool = (Page*)malloc(numbuf*sizeof(Page));
  //allocate #numbuf * sizeof(description) to bufDescr
  bufDescr = (description*)malloc(numbuf*sizeof(description));

  for(int i=0;i<this->numBuffers;i++){
    //bufPool[i] = NULL;
    bufDescr[i].page_number=INVALID_PAGE;
    bufDescr[i].pin_count = 0;
    bufDescr[i].dirty = FALSE;
  }

  hashtable = new bucket*[HTSIZE];
  for(int i=0;i<HTSIZE;i++){
    hashtable[i]=NULL;
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
  if(PageId_in_a_DB == INVALID_PAGE) 
    return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTPINNED);

  for(int i=0;i<this->numBuffers;i++){
    if(bufDescr[i].page_number==PageId_in_a_DB){
       page = &bufPool[i];
       bufDescr[i].pin_count +=1; 
       return OK;
    }
  }
  
    //otherwise, find a frame for this page
    //using LOVE/HATE replacement policy
  PageId replace_page_number = this->Find_Replacement_Page();

   for(int i=0;i<this->numBuffers;i++){
      if(bufDescr[i].page_number==replace_page_number){
        if(bufDescr[i].dirty) flushPage(replace_page_number);
        
        int frame = this->FindFrame(replace_page_number);

        this->delete_hash_table(replace_page_number);
        this->write_hash_table(PageId_in_a_DB, frame);
        MINIBASE_DB->read_page(PageId_in_a_DB, &bufPool[PageId_in_a_DB]);
        page = &bufPool[PageId_in_a_DB];
        
        bufDescr[i].page_number = PageId_in_a_DB;
        bufDescr[i].pin_count++;
        bufDescr[i].dirty=false;
      }
      
    }
    return OK;
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
        {
          if(hate == TRUE){
            //MRU
            MRU.push_back(page_num);
          }else{
            LRU.push_back(page_num);
          }
        }
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

  return FAIL;
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
  pinPage(PageId_in_a_DB,page,emptyPage);
  return OK;
}

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename){
  //put your code here
  unpinPage(globalPageId_in_a_DB,dirty);
  return OK;
}

//*************************************************************
//** This is the implementation of getNumUnpinnedBuffers
//************************************************************
unsigned int BufMgr::getNumUnpinnedBuffers(){
  //put your code here
  int unpincnt=0;
  for(int i=0;i<this->numBuffers;i++){
    if(bufDescr[i].pin_count ==0) unpincnt++;
  }
  return unpincnt;
}

int BufMgr::HashFunction(PageId page_number){
    return(A*page_number+B)/HTSIZE;
}

int BufMgr::FindFrame(PageId page_number){
  int hash = HashFunction(page_number);
  bucket* temp;
  temp = hashtable[hash];
  while(1){
    if(temp->page_number == page_number) return temp->frame_number;
    temp = temp->next_page;
  }
}

void BufMgr::write_hash_table(PageId page_number, int frame_number){
  int hash = HashFunction(page_number);
  bucket* temp;
  temp = hashtable[hash];
   while(1){
    if(temp->page_number == INVALID_PAGE){
      temp->page_number = page_number;
      temp->frame_number = frame_number;
      return;
    } 
    temp = temp->next_page;
  }
}

void BufMgr::delete_hash_table(PageId page_number){
  int hash = HashFunction(page_number);
  bucket* temp;
  bucket* prev;
  bucket* next;
  temp = hashtable[hash];
  
  if(temp->page_number == page_number){
    //맨 앞
    hashtable[hash] = temp->next_page;
    free(temp);
     
  }else{
    while(1){
     prev = temp;
     temp = temp->next_page;

     if(temp->page_number == page_number){
      next = temp->next_page;
      prev->next_page = next;
      free(temp);
      return;
    } 
    temp = temp->next_page;
  }
}

}

PageId BufMgr::Find_Replacement_Page(){
  PageId pageid;
  if(MRU.size()>0){
    int msize = MRU.size();
    pageid = MRU.front();
    for(int i=0;i<msize-1;i++){
      MRU[i]=MRU[i+1];
    }
    MRU.resize(msize-1);
    return pageid;

  }else if(LRU.size()>0){
    int lsize = LRU.size();
    pageid = LRU.back();
    LRU.resize(lsize-1);
    return pageid;

  }else return INVALID_PAGE;

}

  
