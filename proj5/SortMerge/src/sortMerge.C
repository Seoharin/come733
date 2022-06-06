
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
  
  int s_len=0;
  int r_len=0;
  int merge_len=0;
  
  for(int i=0;i<len_in1;i++){
    s_len += t1_str_sizes[i];
  }
  for(int i=0;i<len_in2;i++){
    r_len += t2_str_sizes[i];
  }
  merge_len= s_len+r_len;

  Sort(filename1, fname1, len_in1, in1,  t1_str_sizes,  join_col_in1,  order,  amt_of_mem, s);
	Sort(filename2, fname2, len_in2, in2,  t2_str_sizes,  join_col_in2,  order,  amt_of_mem, s);

  file1 = new HeapFile(flie1,s);
  file2 = new HeapFile(file2,s);
  mergedfile = new HeapFile(filename3,s);
  
  //if(t1_str_sizes[join_col_in1]!=t2_str_sizes[join_col_in2]){
  //  s = DONE;
  //  return;
  //}

  Scan *sscan = file1->openScan(s);
  Scan *rscan = file2->openScan(s);
  
  char *srec = (char*)malloc(sizeof(s_len));
  char *rrec = (char*)malloc(sizeof(r_len));
  
  RID sid, rid, mid;
  int slen, rlen;

  int cmp = 0;
  Status st1,st2;
  st1 = rscan->getNext(rid, rrec, rlen);
  st2 = sscan->getNext(sid, srec,slen); 
  while(1){
    if(st1!=OK || st2!=OK) break;
    
    cmp = tupleCmp(rrec,srec);

    if(cmp>0) st2 = sscan->getNext(sid, srec, slen);
    else if(cmp<0) st1 = rscan->getNext(rid,rrec,rlen);
    else{
      char *mergerec = (char*)malloc(merge_len);
      memmove(mergerec, rrec, rlen);
      memmove(mergerec+rlen, srec, slen);
      mergedfile->insertRecord(mergerec, rlen+slen);

    }
  }
  delete sscan;
  file1->deleteFile();
  delete file1;

  delete rscan;
  file2->deleteFile();
  delete file2;
}

// sortMerge destructor
sortMerge::~sortMerge()
{
	// fill in the body
}
