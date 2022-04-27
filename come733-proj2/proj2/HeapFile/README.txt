Team 3 : Kang Yeondae, Seoharin

[heapfile.c]
- heapfile
1. check len(name)<MAX_NAME
2. assign name to fileName
3. initialize file_deleted = False
4. check whether DB has given name file if DB has assign header file else clear current dirctory page


- getRecCnt
for all directory page, calculate the number of records using DataPageInfo->recct

- insertRecord
check whether space for insert records is exist in datapage.
if exist insert , if not exist create new data page for insert and then insert

- deleteRecord
search record matching at given rid, then delete

- updateRecord
search record to update, then update it using memmove

- getRecord
search record matching at given rid, then get

- openScan
initialize a sequential scan

- deleteFile
delete heapfile 
search headpage to use erase()

- newDataPage
create new data page and set dpinfop-pageid,recct,availspace

- findDataPage
search datapage matching at given pageNo 

- allocateDirSpace
allocate dirctory space for given page



[scan.c]

- getNext
return nextDataPage and give tag for scaned page

- firstDataPage
Copy data about first page
check whether pin page is clean or not

- nextDataPage
return nextDirPage when next record is not ready
make errors unpin page is not ready

- nextDirPage
check heap file and take directory information

