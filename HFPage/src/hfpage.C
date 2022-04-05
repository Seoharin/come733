#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "hfpage.h"
#include "buf.h"
#include "db.h"


// **********************************************************
// page class constructor

void HFPage::init(PageId pageNo)
{
// nextPage, PrevPage, slotCnt, curPage, usedPtr, freeSpace
// should set

nextPage = INVALID_PAGE;
prevPage = INVALID_PAGE;

slotCnt= 0;
curPage = pageNo;

//usedPtr : offset into the data array, not a pointer
usedPtr = MAX_SPACE - DPFIXED;
freeSpace = MAX_SPACE - DPFIXED + sizeof(slot_t);

slot[0].length = EMPTY_SLOT;
slot[0].offset = 0;
}

// **********************************************************
// dump page utlity
void HFPage::dumpPage()
{
int i;

cout << "dumpPage, this: " << this << endl;
cout << "curPage= " << curPage << ", nextPage=" << nextPage << endl;
cout << "usedPtr=" << usedPtr << ", freeSpace=" << freeSpace
<< ", slotCnt=" << slotCnt << endl;
for (i=0; i < slotCnt; i++) {
cout << "slot["<< i <<"].offset=" << slot[i].offset
<< ", slot["<< i << "].length=" << slot[i].length << endl;
}
}

// **********************************************************
PageId HFPage::getPrevPage()
{
// fill in the body
return prevPage;
}

// **********************************************************
void HFPage::setPrevPage(PageId pageNo)
{

// fill in the body
prevPage = pageNo;
}

// **********************************************************
PageId HFPage::getNextPage()
{
// fill in the body
return nextPage;
}

// **********************************************************
void HFPage::setNextPage(PageId pageNo)
{
// fill in the body
nextPage = pageNo;
}

// **********************************************************
// Add a new record to the page. Returns OK if everything went OK
// otherwise, returns DONE if sufficient space does not exist
// RID of the new record is returned via rid parameter.
Status HFPage::insertRecord(char* recPtr, int recLen, RID& rid)
{
// fill in the body
if(available_space()<recLen) return DONE;
//search empty space

int num=0;
while(num<slotCnt){
if(slot[num].length == EMPTY_SLOT) break;
num++;
}

// input length, offset in slot

usedPtr -= recLen;

slot[num].length =recLen;
slot[num].offset = usedPtr;
if(num==slotCnt) slotCnt++;


//input pageid, slotnum in rid
rid.pageNo = curPage;
rid.slotNo = num;

//memcpy(dest, src, sizeof(src))
//memory copy form recPtr to data area in page
memmove(data+usedPtr, recPtr, recLen);

//after insert record, change freespace and usedptr
freeSpace -= recLen;

return OK;
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID& rid)
{
slot_t *drec = &slot[rid.slotNo];

if (slotCnt == 0 || rid.slotNo <0 || rid.slotNo >= slotCnt
|| drec->length == EMPTY_SLOT)
return FAIL;

int doffset = drec->offset;
int dlength = drec->length;
//memcpy(dest, src, sizeof(src))
memmove(data+usedPtr+dlength, data+usedPtr, doffset-usedPtr);

drec->length = EMPTY_SLOT;
usedPtr = usedPtr+dlength;
freeSpace = freeSpace+dlength;

int temp = slotCnt;
while(temp){
     if(slot[temp-1].length != EMPTY_SLOT) break;
     slotCnt--;
     temp--;
}

if(rid.slotNo < slotCnt){
for(int i = rid.slotNo+1 ; i<slotCnt; i++){
slot[i].offset+=dlength;
}
}

return OK;
}

// **********************************************************
// returns RID of first record on page
// returns RID of first record on page
// returns RID of first record on page
// returns RID of first record on page
Status HFPage::firstRecord(RID& firstRid)
{
// fill in the body
if (empty()) {
return DONE;
}

if (slotCnt == 0)
return FAIL;

bool hasRecord = false;

for (int i = 0; i < slotCnt; i++) {
if (slot[i].length != EMPTY_SLOT) {
firstRid.slotNo = i;
firstRid.pageNo = curPage;
hasRecord = true;
break;
}
}

if (!hasRecord) {
return DONE;
}

return OK;
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
Status HFPage::nextRecord (RID curRid, RID& nextRid)
{
// fill in the body

if (curRid.pageNo != curPage) {
return FAIL;
}

if (empty()) {
return FAIL;
}

bool foundNext = false;

for (int i = curRid.slotNo + 1; i < slotCnt; i++) {
if (slot[i].length != EMPTY_SLOT) {
nextRid.slotNo = i;
nextRid.pageNo = curPage;
foundNext = true;
break;
}
}

if (foundNext) {
return OK;
}

return DONE;
}



// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char* recPtr, int& recLen)
{
// fill in the body
slot_t *tempslot = &slot[rid.slotNo];
if(rid.pageNo!=curPage||!slotCnt||rid.slotNo<0||rid.slotNo>=slotCnt||
tempslot->length == EMPTY_SLOT) return FAIL;

for(int i=0;i<tempslot->length;i++){
*recPtr = data[tempslot->offset +i];
recPtr++;
}
recLen = tempslot->length;

return OK;
}

// **********************************************************
// returns length and pointer to record with RID rid. The difference
// between this and getRecord is that getRecord copies out the record
// into recPtr, while this function returns a pointer to the record
// in recPtr.
Status HFPage::returnRecord(RID rid, char*& recPtr, int& recLen)
{
// fill in the body
slot_t *tempslot = &slot[rid.slotNo];
if(rid.pageNo!=curPage||!slotCnt||rid.slotNo<0||rid.slotNo>=slotCnt||
tempslot->length == EMPTY_SLOT) return FAIL;


recPtr = &data[tempslot->offset];
recLen = tempslot->length;
return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void)
{
// fill in the body
short avalspace =0;
if(slotCnt!=0) avalspace = freeSpace - slotCnt*sizeof(slot_t);
else avalspace = freeSpace- sizeof(slot_t);
return avalspace;
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void)
{
// fill in the body
int temp = slotCnt;
while(temp>=0){
if(slot[temp].length !=EMPTY_SLOT) return 0;
temp--;
}
return 1;
}
