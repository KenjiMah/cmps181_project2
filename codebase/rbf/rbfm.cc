#include "rbfm.h"
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <string.h>
#include <iomanip>
RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {
    // Retrieve the specified page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData)){
        return RBFM_READ_FAILED;
    }
    
    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    
    if(slotHeader.recordEntriesNumber < rid.slotNum){
        return RBFM_SLOT_DN_EXIST;
    }
    
    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    
    //copy the records before the record we are about to delete
    memcpy((char*)pageData + slotHeader.freeSpaceOffset + recordEntry.length, (char*)pageData + slotHeader.freeSpaceOffset, recordEntry.offset - slotHeader.freeSpaceOffset);

    SlotDirectoryRecordEntry newRecordSlot;
    for(int i = 0; i < slotHeader.recordEntriesNumber; i++){
        newRecordSlot = getSlotDirectoryRecordEntry(pageData, i);
        if(newRecordSlot.offset <= recordEntry.offset){
            newRecordSlot.offset += recordEntry.length;
            setSlotDirectoryRecordEntry(pageData, i, newRecordSlot);
        }
    }
    //update slot header
    newRecordSlot = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    newRecordSlot.length = 0;
    newRecordSlot.offset = 1; // i just set it to one cause we might need to to be 0
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordSlot);
    //delete the last slots that are not used anymore (clean-up)
    if(rid.slotNum == slotHeader.recordEntriesNumber - 1){ //aka if its the last slot clear up and previous slots
        for(int i = slotHeader.recordEntriesNumber - 1; i >= 0; i --){
            newRecordSlot = getSlotDirectoryRecordEntry(pageData, i);
            if(newRecordSlot.length == 0){
                break;
            }
            slotHeader.recordEntriesNumber -= 1;
        }
    }
    slotHeader.freeSpaceOffset += recordEntry.length;
    setSlotDirectoryHeader(pageData, slotHeader);
    
    //rewrite the entire page
    if (fileHandle.writePage(rid.pageNum, pageData)){
        return RBFM_WRITE_FAILED;
    }
    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::deleteRecordUnclean(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {
    // Retrieve the specified page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData)){
        return RBFM_READ_FAILED;
    }
    
    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    
    if(slotHeader.recordEntriesNumber < rid.slotNum){
        return RBFM_SLOT_DN_EXIST;
    }
    
    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    
    //copy the records before the record we are about to delete
    memcpy((char*)pageData + slotHeader.freeSpaceOffset + recordEntry.length, (char*)pageData + slotHeader.freeSpaceOffset, recordEntry.offset - slotHeader.freeSpaceOffset);
    
    SlotDirectoryRecordEntry newRecordSlot;
    for(int i = 0; i < slotHeader.recordEntriesNumber; i++){
        newRecordSlot = getSlotDirectoryRecordEntry(pageData, i);
        if(newRecordSlot.offset <= recordEntry.offset){
            newRecordSlot.offset += recordEntry.length;
            setSlotDirectoryRecordEntry(pageData, i, newRecordSlot);
        }
    }
    //update slot header
    newRecordSlot = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    newRecordSlot.length = 0;
    newRecordSlot.offset = 1; // i just set it to one cause we might need to to be 0
    setSlotDirectoryHeader(pageData, slotHeader);
    
    //rewrite the entire page
    if (fileHandle.writePage(rid.pageNum, pageData)){
        return RBFM_WRITE_FAILED;
    }
    free(pageData);
    return SUCCESS;
}
//negative offset represents page number(tombsone only)
//length turns into the slotnumber (tombsone only) + 1 becuase 0 denotes an empty slot
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
    void * pageData = malloc(PAGE_SIZE);
    void * pageDataExtra = malloc(PAGE_SIZE);//page data extra represnets the page where the tombstone points to
    if (fileHandle.readPage(rid.pageNum, pageData)){
        return RBFM_READ_FAILED;
    }
    
    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    SlotDirectoryHeader slotHeaderExtra;//slot header extra represents where the tombstone points to
    SlotDirectoryRecordEntry newRecordEntry;
    if(slotHeader.recordEntriesNumber < rid.slotNum){
        return RBFM_SLOT_DN_EXIST;
    }
    
    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    RID tempRID;
    bool isTombstone = false;
    
    // check if its a tombstone
    if(recordEntry.offset < 0){
        isTombstone = true;
        //tempRID points to where the actual data is stored
        tempRID.pageNum = abs(recordEntry.offset);
        tempRID.slotNum = recordEntry.length - 1;
        if (fileHandle.readPage(tempRID.pageNum, pageDataExtra)){
            return RBFM_READ_FAILED;
        }
        
        // Checks if the specific slot id exists in the page
        slotHeaderExtra = getSlotDirectoryHeader(pageData);
        
        if(slotHeaderExtra.recordEntriesNumber < tempRID.slotNum){
            return RBFM_SLOT_DN_EXIST;
        }
        recordEntry = getSlotDirectoryRecordEntry(pageDataExtra, tempRID.slotNum); // now record entry points to the slot that the tombstone points to
    }
    
    unsigned int newRecordSize = getRecordSize(recordDescriptor, data);
    unsigned int oldRecordSize = recordEntry.length;
    int freespace;
    if(isTombstone){
        freespace = getPageFreeSpaceSize(pageDataExtra);
    }
    else{
        freespace = getPageFreeSpaceSize(pageData);
    }
    //if new record is the same size then just replace the old one
    if(newRecordSize == oldRecordSize){
        if(!isTombstone){
            setRecordAtOffset (pageData, recordEntry.offset, recordDescriptor, data);
            if (fileHandle.writePage(rid.pageNum, pageData)){
                return RBFM_WRITE_FAILED;
            }
        }
        else{
            //record entry changed automatically if its a tombstone
            setRecordAtOffset (pageDataExtra, recordEntry.offset, recordDescriptor, data);
            if (fileHandle.writePage(tempRID.pageNum, pageDataExtra)){
                return RBFM_WRITE_FAILED;
            }
        }
    }
    //if new record is a different size but there is still freespace within that page to fit the new record then delete record and put the new record in the same page
    else if(freespace >= newRecordSize - oldRecordSize){
        if(!isTombstone){
            deleteRecordUnclean(fileHandle, recordDescriptor, rid);
            //remember to update slot header after delete
            if (fileHandle.readPage(rid.pageNum, pageData)){
                return RBFM_READ_FAILED;
            }
            slotHeader = getSlotDirectoryHeader(pageData);
            
            // Adding the new record reference in the slot directory.
            
            newRecordEntry.length = newRecordSize;
            newRecordEntry.offset = slotHeader.freeSpaceOffset - newRecordSize;
            setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);
            
            //set the record at the offset
            setRecordAtOffset (pageData, slotHeader.freeSpaceOffset - newRecordSize, recordDescriptor, data);
            if (fileHandle.writePage(rid.pageNum, pageData)){
                return RBFM_WRITE_FAILED;
            }
        }
        else{
            deleteRecordUnclean(fileHandle, recordDescriptor, tempRID);
            //remember to update slot header after delete
            if (fileHandle.readPage(tempRID.pageNum, pageDataExtra)){
                return RBFM_READ_FAILED;
            }
            slotHeader = getSlotDirectoryHeader(pageDataExtra);
            
            // Adding the new record reference in the slot directory.
            newRecordEntry.length = newRecordSize;
            newRecordEntry.offset = slotHeader.freeSpaceOffset - newRecordSize;
            setSlotDirectoryRecordEntry(pageDataExtra, tempRID.slotNum, newRecordEntry);
            
            //set the record at the offset
            setRecordAtOffset (pageDataExtra, slotHeader.freeSpaceOffset - newRecordSize, recordDescriptor, data);
            if (fileHandle.writePage(tempRID.pageNum, pageDataExtra)){
                return RBFM_WRITE_FAILED;
            }
        }
    }
    //if new record will not fit back into the page then create a tombstone
    else{
        //do tombstone stuff
        RID newRID;
        if(!isTombstone){
            deleteRecordUnclean(fileHandle, recordDescriptor, rid);
            insertRecord(fileHandle, recordDescriptor, data, newRID);
            //remember to update slot header after delete
            if (fileHandle.readPage(rid.pageNum, pageData)){
                return RBFM_READ_FAILED;
            }
            slotHeader = getSlotDirectoryHeader(pageData);
            
            // Adding the new record reference in the slot directory.
            newRecordEntry.length = newRID.slotNum + 1;
            newRecordEntry.offset = (-1) * newRID.pageNum;
            setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);
        }
        else{
            deleteRecordUnclean(fileHandle, recordDescriptor, tempRID);
            insertRecord(fileHandle, recordDescriptor, data, newRID);
            //remember to update slot header after delete
            if (fileHandle.readPage(tempRID.pageNum, pageDataExtra)){
                return RBFM_READ_FAILED;
            }
            slotHeader = getSlotDirectoryHeader(pageDataExtra);
            
            // Adding the new record reference in the slot directory.
            newRecordEntry.length = newRID.slotNum + 1;
            newRecordEntry.offset = (-1) * newRID.pageNum;
            setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);
        }
    }
        
    free(pageData);
    free(pageDataExtra);
    return SUCCESS;
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                     
                     
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){
    // Retrieve the specified page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData)){
        return RBFM_READ_FAILED;
    }
    
    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    
    if(slotHeader.recordEntriesNumber < rid.slotNum){
        return RBFM_SLOT_DN_EXIST;
    }
    
    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    
    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, (char *)pageData + recordEntry.offset, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);
    char nullIndicator[recordNullIndicatorSize];
    memset(nullIndicator, 0, recordNullIndicatorSize);
    memcpy(nullIndicator, (char *)pageData + recordEntry.offset + sizeof(RecordLength), recordNullIndicatorSize);
    
    // We've read in the null indicator, so we can skip past it now
    ColumnOffset startPointer;
    ColumnOffset endPointer;
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++){
        if(recordDescriptor[i].name == attributeName){
            // Skip null fields
            if (fieldIsNull(nullIndicator, i)){
                memset(data, 1, 1);
                return SUCCESS;
            }
            else if(i == 0){
                memcpy(data, 0, 1);
                memcpy(&endPointer, (char *)pageData + recordEntry.offset + sizeof(RecordLength) + recordNullIndicatorSize + i * sizeof(ColumnOffset), sizeof(ColumnOffset));
                memcpy((char *)data + sizeof(char), (char *)pageData + recordEntry.offset + sizeof(RecordLength) + recordNullIndicatorSize, endPointer - sizeof(RecordLength) + recordNullIndicatorSize);
            }
            else{
                memset(data, 0, 1);
                // Grab pointer to end of this column
                memcpy(&startPointer, (char *)pageData + recordEntry.offset + sizeof(RecordLength) + recordNullIndicatorSize + i * sizeof(ColumnOffset), sizeof(ColumnOffset));
                memcpy(&endPointer, (char *)pageData + recordEntry.offset + sizeof(RecordLength) + recordNullIndicatorSize + i * sizeof(ColumnOffset), sizeof(ColumnOffset));
                memcpy((char *)data + sizeof(char), (char *)pageData + recordEntry.offset + startPointer, endPointer - startPointer);
            }
        }
    }
    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;
    newRecordBasedPage(firstPageData);
    // Adds the first record based page.

    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    // Gets the size of the record.
    unsigned recordSize = getRecordSize(recordDescriptor, data);

    // Cycles through pages looking for enough free space for the new entry.
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    bool pageFound = false;
    unsigned i;
    unsigned numPages = fileHandle.getNumberOfPages();
    for (i = 0; i < numPages; i++)
    {
        if (fileHandle.readPage(i, pageData))
            return RBFM_READ_FAILED;

        // When we find a page with enough space (accounting also for the size that will be added to the slot directory), we stop the loop.
        if (getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + recordSize)
        {
            pageFound = true;
            break;
        }
    }

    // If we can't find a page with enough space, we create a new one
    if(!pageFound)
    {
        newRecordBasedPage(pageData);
    }

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    SlotDirectoryRecordEntry newRecordEntry;
    // Setting up the return RID.
    rid.pageNum = i;
    bool freeSlot = false;
    for(int j = 0; j < slotHeader.recordEntriesNumber; j++){
        newRecordEntry = getSlotDirectoryRecordEntry(pageData, j);
        if(newRecordEntry.length == 0){
            rid.slotNum = j;
            freeSlot = true;
            break;
        }
    }
    if(!freeSlot){
        rid.slotNum = slotHeader.recordEntriesNumber;
    }
    // Adding the new record reference in the slot directory.
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    if(!freeSlot){
        slotHeader.recordEntriesNumber += 1;
    }
    setSlotDirectoryHeader(pageData, slotHeader);

    // Adding the record data.
    setRecordAtOffset (pageData, newRecordEntry.offset, recordDescriptor, data);

    // Writing the page to disk.
    if (pageFound)
    {
        if (fileHandle.writePage(i, pageData))
            return RBFM_WRITE_FAILED;
    }
    else
    {
        if (fileHandle.appendPage(pageData))
            return RBFM_APPEND_FAILED;
    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    // Retrieve the specified page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    /////////////////////////////////////
    //check for tombstones
    if(recordEntry.offset < 0){// if the record offset is less than 0 then its a tombstone
        if (fileHandle.readPage(abs(recordEntry.offset), pageData))
            return RBFM_READ_FAILED;
        slotHeader = getSlotDirectoryHeader(pageData);
        
        if(slotHeader.recordEntriesNumber < recordEntry.length)
            return RBFM_SLOT_DN_EXIST;
    }
    ////////////////////////////////////
    // Retrieve the actual entry data
    getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    // Parse the null indicator and save it into an array
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);
    
    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    cout << "----" << endl;
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        cout << setw(10) << left << recordDescriptor[i].name << ": ";
        // If the field is null, don't print it
        bool isNull = fieldIsNull(nullIndicator, i);
        if (isNull)
        {
            cout << "NULL" << endl;
            continue;
        }
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                uint32_t data_integer;
                memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
                offset += INT_SIZE;

                cout << "" << data_integer << endl;
            break;
            case TypeReal:
                float data_real;
                memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
                offset += REAL_SIZE;

                cout << "" << data_real << endl;
            break;
            case TypeVarChar:
                // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
                uint32_t varcharSize;
                memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;

                // Gets the actual string.
                char *data_string = (char*) malloc(varcharSize + 1);
                if (data_string == NULL)
                    return RBFM_MALLOC_FAILED;
                memcpy(data_string, ((char*) data + offset), varcharSize);

                // Adds the string terminator.
                data_string[varcharSize] = '\0';
                offset += varcharSize;

                cout << data_string << endl;
                free(data_string);
            break;
        }
    }
    cout << "----" << endl;

    return SUCCESS;
}

SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void * page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader)
{
    // Setting the slot directory header.
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy  (
            &recordEntry,
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            sizeof(SlotDirectoryRecordEntry)
            );

    return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
    // Setting the slot directory entry data.
    memcpy  (
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            &recordEntry,
            sizeof(SlotDirectoryRecordEntry)
            );
}

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void * page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    SlotDirectoryHeader slotHeader;
    slotHeader.freeSpaceOffset = PAGE_SIZE;
    slotHeader.recordEntriesNumber = 0;
	memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Offset into *data. Start just after the null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize it to the size of the header
    unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                size += INT_SIZE;
                offset += INT_SIZE;
            break;
            case TypeReal:
                size += REAL_SIZE;
                offset += REAL_SIZE;
            break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                size += varcharSize;
                offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    return size;
}

// Calculate actual bytes for null-indicator for the given field counts
int RecordBasedFileManager::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page) 
{
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
void RecordBasedFileManager::getRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, void *data)
{
    // Pointer to start of record
    char *start = (char*) page + offset;

    // Allocate space for null indicator.
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
    memcpy(data, nullIndicator, nullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;
    
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;
        
        // Grab pointer to end of this column
        ColumnOffset endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

        // rec_offset keeps track of start of column, so end-start = total size
        uint32_t fieldSize = endPointer - rec_offset;

        // Special case for varchar, we must give data the size of varchar first
        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
            data_offset += VARCHAR_LENGTH_SIZE;
        }
        // Next we copy bytes equal to the size of the field and increase our offsets
        memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
        rec_offset += fieldSize;
        data_offset += fieldSize;
    }
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset (nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Points to start of record
    char *start = (char*) page + offset;

    // Offset into *data
    unsigned data_offset = nullIndicatorSize;
    // Offset into page header
    unsigned header_offset = 0;

    RecordLength len = recordDescriptor.size();
    memcpy(start + header_offset, &len, sizeof(len));
    header_offset += sizeof(len);

    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;

    // Keeps track of the offset of each record
    // Offset is relative to the start of the record and points to the END of a field
    ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!fieldIsNull(nullIndicator, i))
        {
            // Points to current position in *data
            char *data_start = (char*) data + data_offset;

            // Read in the data for the next column, point rec_offset to end of newly inserted data
            switch (recordDescriptor[i].type)
            {
                case TypeInt:
                    memcpy (start + rec_offset, data_start, INT_SIZE);
                    rec_offset += INT_SIZE;
                    data_offset += INT_SIZE;
                break;
                case TypeReal:
                    memcpy (start + rec_offset, data_start, REAL_SIZE);
                    rec_offset += REAL_SIZE;
                    data_offset += REAL_SIZE;
                break;
                case TypeVarChar:
                    unsigned varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                    memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                    // We also have to account for the overhead given by that integer.
                    rec_offset += varcharSize;
                    data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
                break;
            }
        }
        // Copy offset into record header
        // Offset is relative to the start of the record and points to END of field
        memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
        header_offset += sizeof(ColumnOffset);
    }
}
