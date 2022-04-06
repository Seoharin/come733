Team 3 : Kang Yeondae, Seoharin

HFPage :: init
initially,
nextPage and prevPage = INVAILD_PAGE,
slotCnt = 0,
curPage = pageNo,
usedPtr = MAX_SPACE - DPFIXED,
freeSpace = MAX_SPACE - DPFIXED + sizeof(slot_t),

and because first slot are empty, slot[0].length = -1 , slot[0].offset = 0

HFPage :: getPrevPage
return prevPage

HFPage :: setPrevPage
set pageNo to prevPage

HFPage :: getNextPage
return nextPage

HFPage :: setNextPage
set pageNo to nextPage

HFPage :: insertRecord
step1) check whether available space is exist in current page.
step2) search empty slot to insert record
step3) usedptr offset change
step4) slot diretory update
step5) insert record was inserted in lastest slot, slotCnt++
step6) rid update
step7) memory move
step8) freespace change

HFPage :: deleteRecord
step1) check whether record delegation is available
step2) memory move
step3) slot diretory update
step4) if slot is empty, slotCnt--
step5) if record to delete is not lastest in slot, offset change


we use memmove for memory move
because memmove considers about memory overlap


HFPage :: firstRecord
step1) declare and initialize variables
step2) check empty page
step3) check slot error
step4) find empty slot to record
stop5) return slot number and page

HFPage :: nextRecord
step1) declare and initialize variables
step2) check page number used before
step3) check empty page
step4) find empty slot to record
stop5) return slot number and page


HFPage :: getRecord
according to receieved rid, copy data of record in recPtr, length of data in recLen

HFPage :: return Record
according to receieved rid, return offset and length of slot

HFPage :: available_space
if slotCnt is zero, available_space = freeSpace - sizeof(slot_t)
else available_space = freeSpace - slotCnt*sizeof(slot_t)

HFPage :: empty
all slots are empty -> return 1, else return 0
