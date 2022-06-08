#include <string.h>
#include <assert.h>
#include "sortMerge.h"

// Error Protocall:


enum ErrCodes {SORT_FAILED, HEAPFILE_FAILED};

static const char* ErrMsgs[] =  {
  "Error: Sort Failed.",
  "Error: HeapFile Failed."
  // maybe more ...
};


char *fname1 = "sorted file1";
char *fname2 = "sorted file2";
HeapFile* file1, *file2, *mergedfile;

static error_string_table ErrTable( JOINS, ErrMsgs );

// sortMerge constructor
sortMerge::sortMerge(
    char*           filename1,      // Name of heapfile for relation R
    int             len_in1,        // # of columns in R.
    AttrType        in1[],          // Array containing field types of R.
    short           t1_str_sizes[], // Array containing size of columns in R
    int             join_col_in1,   // The join column of R 

    char*           filename2,      // Name of heapfile for relation S
    int             len_in2,        // # of columns in S.
    AttrType        in2[],          // Array containing field types of S.
    short           t2_str_sizes[], // Array containing size of columns in S
    int             join_col_in2,   // The join column of S

    char*           filename3,      // Name of heapfile for merged results
    int             amt_of_mem,     // Number of pages available
    TupleOrder      order,          // Sorting order: Ascending or Descending
    Status&         s               // Status of constructor
){
  // search rows under condition,
	// sort results respectively
  int roffset=0, soffset=0;
  int rsize=0, ssize=0;
	
  for(int i=0;i<join_col_in1;i++){
    roffset+=t1_str_sizes[i];
  }
  for(int i=0;i<join_col_in2;i++){
    soffset+=t2_str_sizes[i];
  }
  for(int i=0;i<len_in1;i++){
    rsize+=t1_str_sizes[i];
  }
  for(int i=0;i<len_in2;i++){
    ssize+=t2_str_sizes[i];
  }

  Sort(filename1, fname1, len_in1, in1,  t1_str_sizes,  join_col_in1,  order,  amt_of_mem, s);
	Sort(filename2, fname2, len_in2, in2,  t2_str_sizes,  join_col_in2,  order,  amt_of_mem, s);

  file1 = new HeapFile(fname1,s);
  file2 = new HeapFile(fname2,s);
  mergedfile = new HeapFile(filename3,s);
  
  char *rrec = (char*)malloc(rsize);
  char *srec = (char*)malloc(ssize);
  char *mergerec = (char*)malloc(rsize+ssize);
  RID rid, sid, mid;
  int rlen, slen;
  
  int cmp = 0;
  Status st1,st2;
  
  Scan *rscan = file1->openScan(s);
  st1 = rscan->getNext(rid, rrec, rlen);
  while(st1 ==OK){

    Scan *sscan = file2->openScan(s);
    st2 = sscan->getNext(sid, srec, slen); 
    while(st2 ==OK){
      int* temp1 = (int*)(rrec+roffset);
      int* temp2 = (int*)(srec+soffset);
      cmp = tupleCmp(temp1,temp2);
      if(cmp==0){
        memmove(mergerec, rrec,rlen);
        memmove(mergerec+rlen, srec, slen);
        mergedfile->insertRecord(mergerec, rlen+slen,mid);
      
      st2 = sscan->getNext(sid, srec, slen); 

    }
    st1 = rscan->getNext(rid, rrec, rlen);
  }

 
 
  file1->deleteFile();
  file2->deleteFile();
  }
}
// sortMerge destructor
sortMerge::~sortMerge()
{
	// fill in the body
}
