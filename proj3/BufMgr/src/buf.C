/*****************************************************************************/
/*************** Implementation of the Buffer Manager Layer ******************/
/*****************************************************************************/

#include "minirel.h"
#include "buf.h"
#include <vector>
#define A 1
#define B 1

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
  memset(bufPool, 0, numbuf * sizeof(Page));
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
    //버퍼풀에 요청한 페이지가 있으면
    if(bufDescr[i].page_number==PageId_in_a_DB){
       int frame = FindFrame(PageId_in_a_DB);
       page = bufPool+frame;
       bufDescr[i].pin_count +=1; 
       return OK;
    }
  }
   //버퍼풀에 요청한 페이지가 없고, 비어있는 프레임이 있으면
  for(int i=0;i<this->numBuffers;i++){
    if(bufDescr[i].page_number==INVALID_PAGE){
      int frame = i;
      write_hash_table(PageId_in_a_DB,frame);
      if(emptyPage!=True){
         MINIBASE_DB->read_page(PageId_in_a_DB,page);
         memmove(bufPool+frame, page, sizeof(Page));
      }
     
      bufDescr[i].pin_count++;
      bufDescr[i].page_number=PageId_in_a_DB;
      bufDescr[i].dirty=false;
      return OK;
    }
  }
    //otherwise, find a frame for this page
    //using LOVE/HATE replacement policy
    //버퍼풀에 요청한 페이지가 없고, 비어있는 프레임도 없으면
  PageId replace_page_number = this->Find_Replacement_Page();

   for(int i=0;i<this->numBuffers;i++){
      if(bufDescr[i].page_number==replace_page_number){
        if(bufDescr[i].dirty==True) flushPage(replace_page_number);
        
        int frame = this->FindFrame(replace_page_number);

        this->delete_hash_table(replace_page_number);
        this->write_hash_table(PageId_in_a_DB, frame);
        if(emptyPage!=True){
          MINIBASE_DB->read_page(PageId_in_a_DB, page);
          memmove(bufPool+frame,page,sizeof(Page));
        }
      
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
  for(int i=0;i<this->numBuffers;i++){
    if(bufDescr[i].page_number==INVALID_PAGE){
       MINIBASE_DB->allocate_page(firstPageId,howmany);
       int frame = FindFrame(firstPageId);
       write_hash_table(firstPageId,frame);
       
       bufDescr[i].page_number = firstPageId;
       bufDescr[i].pin_count=1;
       bufDescr[i].dirty=false;

       
       memmove(bufPool+frame, firstpage, sizeof(Page));
    }
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
      MINIBASE_DB->deallocate_page(globalPageId);
      delete_hash_table(globalPageId);
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
      int frame = FindFrame(pageid);
      delete_hash_table(pageid);
      MINIBASE_DB->write_page(pageid,&bufPool[frame]);
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
    if(bufDescr[i].dirty==TRUE&&bufDescr[i].page_number!=INVALID_PAGE){
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

  while(temp!=NULL){
    if(temp->page_number==page_number){
      return temp->frame_number;
    }
    temp=temp->next_page;
  }
}

void BufMgr::write_hash_table(PageId page_number, int frame_number){
  int hash = HashFunction(page_number);
  bucket* temp;
  temp = hashtable[hash];
  if(temp==NULL) {
    hashtable[hash] =(bucket*)malloc(sizeof(bucket));
    temp = (bucket*)malloc(sizeof(bucket));
    temp->page_number=page_number;
    temp->frame_number=frame_number;
    temp->next_page = NULL;
    hashtable[hash]=temp;
  }
  
  else{
    while(temp->next_page!=NULL){
      temp = temp->next_page;
    }
    temp->next_page = (bucket*)malloc(sizeof(bucket));

  }
    temp->page_number=page_number;
    temp->frame_number=frame_number;
    temp->next_page = NULL;
}

void BufMgr::delete_hash_table(PageId page_number){
  int hash = HashFunction(page_number);
  bucket* temp;
  bucket* curr;
  curr = hashtable[hash];
  temp = curr;
  
  //if(curr==NULL){
  //  return MINIBASE_FIRST_ERROR(BUFMGR, HASHREMOVEERROR);
  // }

  if(curr->page_number==page_number){
    //맨 앞
    curr = curr->next_page;
    hashtable[hash] = curr;
    free(temp);
    return;
  }else{
    while(curr->next_page!=NULL){
      if(curr->next_page->page_number==page_number){
        temp = curr->next_page;
        curr->next_page= temp->next_page;
        free(temp);
        return;
      }
      curr = curr->next_page;
    }
  }

}

PageId BufMgr::Find_Replacement_Page(){
  PageId pageid;
  int msize = MRU.size();
  int lsize = LRU.size();
  if(msize>0){
    
    pageid = MRU.back();
    MRU.resize(msize-1);
    if(pageid==LRU.front()){
      for(int i=0;i<lsize-1;i++){
      LRU[i]=LRU[i+1];
    }
    LRU.resize(lsize-1);
    }
    return pageid;

  }else if(lsize>0){
   
    pageid = LRU.front();
    for(int i=0;i<lsize-1;i++){
      LRU[i]=LRU[i+1];
    }
    LRU.resize(lsize-1);
    return pageid;

  }else return INVALID_PAGE;

}
