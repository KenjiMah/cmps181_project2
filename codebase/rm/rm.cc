
#include "rm.h"
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <string.h>
#include <iomanip>

RelationManager* RelationManager::_rm = 0;

RecordBasedFileManager* RelationManager::_rbf_manager = NULL;

FileHandle tables;
FileHandle columns;

int tableNum = 0;

vector<Attribute> tableAttrs;
vector<Attribute> columnAttrs;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}


// RM_ScanIterator::RM_ScanIterator()
// {
// }

// RM_ScanIterator::~RM_ScanIterator()
// {

// }

// RC RM_ScanIterator::getNextTuple(RID &rid, void * data)
// {
//     return -1;
// }

// RC RM_ScanIterator::close()
// {
//     return 0;
// }

RelationManager::RelationManager()
{
    int rc;
    rc = _rbf_manager->openFile("Tables.tbl", tables);
    rc = _rbf_manager->openFile("Columns.tbl", columns);
}

RelationManager::~RelationManager()
{
}

void RelationManager::updateCatalog(const string &tableName, const vector<Attribute> &attrs){
    // Holds the table and column records format
    // Insert record for the new table
    RID test;
    void * data = calloc(1, 4096);
    int offset = 0;
    int nameLength = tableName.length();
    // Set the null bit to 0 as no null fields
    offset += 1;
    // Set table-id field
    tableNum += 1;
    memcpy((char *)data+offset, (char*)&tableNum, sizeof(int));
    offset += 4;
    // Set table-name field
    memcpy((char *)data + offset, (char*)&nameLength, sizeof(int));
    offset += 4;
    memcpy((char *)data+offset,tableName.c_str(), nameLength);
    offset += nameLength;
    // Set file-name field
    memcpy((char *)data + offset, (char*)&nameLength, sizeof(int));
    offset += 4;
    memcpy((char *)data+offset,tableName.c_str(), nameLength);
    offset += nameLength;

    _rbf_manager->insertRecord(tables, tableAttrs, data, test);
    // Inserts a record in columns for each attr in new table
    for(int i = 0; i < (int)attrs.size(); i++){
        // Reset variables
        offset = 0;
        memset(data, 0, 4096);
        nameLength = attrs[i].name.length();

        // Set the null bit to 0 as no null fields
        memset(data,0,1);
        offset += 1;
        // Set table-id field
        memcpy((char *)data+offset, &tableNum, sizeof(int));
        offset += 4;
        // Set column-name field
        memcpy((char *)data + offset, &nameLength, sizeof(int));
        offset += 4;
        memcpy((char *)data+offset, attrs[i].name.c_str(), nameLength);
        offset += nameLength;
        // Set column-type field
        memcpy((char *)data+offset, &attrs[i].type, sizeof(int));
        offset += 4;
        // Set column-length field
        memcpy((char *)data+offset, &attrs[i].length, sizeof(int));
        offset += 4;
        // Set column-position field
        memcpy((char *)data+offset, &i, sizeof(int));
        offset += 4;
        _rbf_manager->insertRecord(columns, columnAttrs, data, test);
    }
    free(data);
}

void RelationManager::setAttrs(){
    
    tableAttrs.clear();
    // Set up table attributes
    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    tableAttrs.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tableAttrs.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tableAttrs.push_back(attr);
    
    columnAttrs.clear();
    // Set up column attributes
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnAttrs.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    columnAttrs.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnAttrs.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnAttrs.push_back(attr);

    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnAttrs.push_back(attr);
}

RC RelationManager::createCatalog()
{
    if(_rbf_manager->createFile("Tables.tbl")){
        return RBFM_CREATE_FAILED;
    }
    if(_rbf_manager->createFile("Columns.tbl")){
        return RBFM_CREATE_FAILED;
    }
    int rc;
    rc = _rbf_manager->openFile("Tables.tbl", tables);
    if(rc){
        return RBFM_OPEN_FAILED;
    }
    rc = _rbf_manager->openFile("Columns.tbl", columns);
    if(rc){
        return RBFM_OPEN_FAILED;
    }
    if(tableAttrs.size() == 0 || columnAttrs.size() == 0){
        setAttrs();
    }
    //Create the table catalog
    updateCatalog("Tables", tableAttrs);
    //Create the column catalog
    updateCatalog("Columns", columnAttrs);

    return 0;
}

RC RelationManager::deleteCatalog()
{
    int rc;
    rc = _rbf_manager->destroyFile("Tables.tbl");
    if(rc){
        return -1;
    }
    rc = _rbf_manager->destroyFile("Columns.tbl");
    if(rc){
        return -1;
    }
    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    int rc;
    const string fileName = tableName + ".tbl";
    rc = _rbf_manager->createFile(fileName);
    if(rc){
        return RBFM_CREATE_FAILED;
    }
    updateCatalog(tableName, attrs);
    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
    if(tableName == "Table" || tableName == "Column"){
        fprintf(stderr, "You cannot delete from the catalog tables\n");
        return -1;
    }
    if(_rbf_manager->destroyFile(tableName + ".tbl")){
        fprintf(stderr, "Failure destroying table\n");
        return -1;
    }
    RM_ScanIterator scanner;
    vector<string> getAttrs;
    getAttrs.push_back("table-id");
    scan("Tables", "table-name", EQ_OP, tableName.c_str(), getAttrs, scanner);
    vector<RID> rids;
    RID rid;
    void * data = calloc(1, 4096);
    if(scanner.getNextTuple(rid, data) == RM_EOF){
        fprintf(stderr, "Table doesn't exist\n");
        return -1;
    }  
    FileHandle newt;
    _rbf_manager->openFile("Tables.tbl", newt);
    if(_rbf_manager->deleteRecord(newt, tableAttrs, rid)){
        fprintf(stderr, "Error deleting record in table catalog\n");
        return -1;
    }
    getAttrs.clear();
    RM_ScanIterator scanner2;
    int * hold = (int *)calloc(1, sizeof(int));
    memcpy(hold, (char*)data+1, sizeof(int));
    scan("Columns", "table-id", EQ_OP, hold, getAttrs, scanner2);
    memset(data, 0, 4096);  
    while(scanner2.getNextTuple(rid, data) != RM_EOF){
        rids.push_back(rid);
    }
    FileHandle newts;
    _rbf_manager->openFile("Columns.tbl", newts);
    for(int i = 0; i < (int)rids.size(); i++){
        if(_rbf_manager->deleteRecord(newts, columnAttrs, rids[i])){
            fprintf(stderr, "Error deleting record in column catalog\n");
            return -1;
        }
    }
    free(data);
    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    if(tableAttrs.size() == 0 || columnAttrs.size() == 0){
        setAttrs();
    }
    RM_ScanIterator scanner;
    vector<string> getAttrs;
    getAttrs.push_back("table-id");
    scan("Tables", "table-name", EQ_OP, tableName.c_str(), getAttrs, scanner);
    RID rid;
    void * data = calloc(1, 4096);
    if(scanner.getNextTuple(rid, data) == RM_EOF){
        fprintf(stderr, "Table doesn't exist\n");
        return -1;
    }
    getAttrs.clear();
    getAttrs.push_back("column-name");
    getAttrs.push_back("column-type");
    getAttrs.push_back("column-length");
    RM_ScanIterator scanner2;
    int * hold = (int *)calloc(1, sizeof(int));
    memcpy(hold, (char*)data+1, sizeof(int));
    scan("Columns", "table-id", EQ_OP, hold, getAttrs, scanner2);
    memset(data, 0, 4096);
    int *offset = (int *) calloc(1, 4);
    while(scanner2.getNextTuple(rid, data) != RM_EOF){
        Attribute attr;
        memcpy(offset, (char*)data+1, sizeof(int));
        char * name = (char *) calloc(1, offset[0]);
        memcpy(name, (char*)data+5, offset[0]);
        attr.name = name;
        offset[0] += 5;
        AttrType *type = (AttrType *) calloc(1, sizeof(AttrType));
        memcpy(type, (char*)data+offset[0], sizeof(int));
        attr.type = *type;
        offset[0] += 4;
        AttrLength *len = (AttrLength *) calloc(1, sizeof(AttrLength));
        memcpy(len, (char*)data+offset[0], sizeof(int));
        attr.length = *len;
        attrs.push_back(attr);
        // free(name);
        // free(type);
        // free(len);
    }
    free(offset);
    free(hold);
    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    FileHandle table;
    const string fileName = tableName + ".tbl";
    int rc = _rbf_manager->openFile(fileName, table);
    if(rc){
        return RBFM_OPEN_FAILED;
    }
    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    rc = _rbf_manager->insertRecord(table, attrs, data, rid);
    if(rc){
        return -1;
    }
    // RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);
    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    if(tableName == "Table" || tableName == "Column"){
        fprintf(stderr, "You cannot delete from the catalog tables\n");
        return -1;
    }
    FileHandle table;
    if(_rbf_manager->openFile(tableName + ".tbl", table)){
        fprintf(stderr, "File does not exist\n");
        return -1;
    }
    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    int rc;
    rc = _rbf_manager->deleteRecord(table, attrs, rid);
    if(rc){
        fprintf(stderr, "Error deleting tuple\n");
        return -1;
    }
    // RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);
    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    if(tableName == "Table" || tableName == "Column"){
        fprintf(stderr, "You cannot update the catalog tables\n");
        return -1;
    }
    FileHandle table;
    if(_rbf_manager->openFile(tableName + ".tbl", table)){
        fprintf(stderr, "File does not exist\n");
        return -1;
    }
    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    int rc;
    rc = _rbf_manager->updateRecord(table, attrs, data, rid);
    if(rc){
        fprintf(stderr, "Error updating tuple\n");
        return -1;
    }
    
    //RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    FileHandle table;
    if(_rbf_manager->openFile(tableName + ".tbl", table)){
        fprintf(stderr, "File does not exist\n");
        return -1;
    }
    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    int rc;
    cout << "reading" << endl;
    rc = _rbf_manager->readRecord(table, attrs, rid, data);
    if(rc){
        fprintf(stderr, "Error reading tuple\n");
        return -1;
    }
    cout << "dnoe reading" << endl;
    // RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);
    return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    int rc;
    rc = _rbf_manager->printRecord(attrs, data);
    if(rc){
        fprintf(stderr, "Error printing tuple\n");
        return -1;
    }
    // RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);
    return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    FileHandle table;
    if(_rbf_manager->openFile(tableName + ".tbl", table)){
        fprintf(stderr, "File does not exist\n");
        return -1;
    }
    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    int rc;
    rc = _rbf_manager->readAttribute(table, attrs, rid, attributeName, data);
    if(rc){
        fprintf(stderr, "Error reading tuple\n");
        return -1;
    }
    return 0;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    // Check if the table passed through actually exists
    int rc;
    rc = _rbf_manager->openFile(tableName + ".tbl", rm_ScanIterator.table);
    if(rc){
        fprintf(stderr, "File does not exist");
        return -1;
    }
    // Check if they are searching through the table or column catalogs
    if(tableName == "Tables"){
        rm_ScanIterator.attrs = tableAttrs;
    }else if(tableName == "Columns"){
        rm_ScanIterator.attrs = columnAttrs;
    }else{
        getAttributes(tableName, rm_ScanIterator.attrs);
    }
    rm_ScanIterator.conditionAttr = conditionAttribute;
    rm_ScanIterator.compOp = compOp;
    rm_ScanIterator.value = value;
    rm_ScanIterator.attrNames = attributeNames;
    rm_ScanIterator._rbf_manager = _rbf_manager;
    // Check whether the condtion attribute is a string or not
    // Used in getnexttuple
    rm_ScanIterator.conditionType = false;
    // cout << rm_ScanIterator.attrs.size() << endl;
    for(int i = 0; i < (int)rm_ScanIterator.attrs.size(); i++){
        if(rm_ScanIterator.attrs[i].name == conditionAttribute){
            if(rm_ScanIterator.attrs[i].type == TypeVarChar){
                rm_ScanIterator.conditionType = true; 
            }
        }
    }
    rm_ScanIterator.scannedRID.slotNum = 0;
    rm_ScanIterator.scannedRID.pageNum = 0;
    return 0;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void * data){
    int i = scannedRID.pageNum;
    int j = scannedRID.slotNum + 1;
    for(i; i < table.getNumberOfPages(); i++){
        uint16_t slots;
        void * page = calloc(1, 4096);
        table.readPage(i, page);
        memcpy(&slots, (char*)page+sizeof(uint16_t), sizeof(uint16_t));
        for(j; j < slots; j++){
            // Check record corresponding to RIDS
            RID checkRID;
            checkRID.pageNum = i;
            checkRID.slotNum = j;
            // Check if rid exists
            uint32_t ridExists;
            memcpy  (
            &ridExists,
            ((char*) page + sizeof(SlotDirectoryHeader) + j * sizeof(SlotDirectoryRecordEntry)),
            sizeof(uint32_t)
            );
            if(ridExists == 0){
                continue;
            }

            // If rid exists, read in record
            void* record = calloc(1, 4096);
            _rbf_manager->readAttribute(table, attrs, checkRID, conditionAttr, record);
            // Buffer to hold attribute to be checked
            char *nullChar = (char*)calloc(1,sizeof(char));
            memcpy(nullChar, record, sizeof(char));
            void* attrCheck = nullptr;
            int *offset = nullptr;
            int *valueInt = nullptr;
            if(nullChar != 0){
                attrCheck = calloc(1, 4096);
                offset = (int *) calloc(1, sizeof(int));       
                // cout << "Page: " << i << "Record: " << j << endl;
                // If attribute to be checked is a string account for it
                if(conditionType){
                    memcpy(offset, (char *)record+1, sizeof(int));
                    memcpy(attrCheck, (char *)record+5, *offset);
                }else{
                    memcpy(offset, (char *)record+1, sizeof(int));
                    valueInt = (int *)calloc(1, sizeof(int));
                    memcpy(valueInt, value, sizeof(int));
                    // cout << valueInt[0] << " :: " << offset[0] << endl;
                }
            }
            // Switch based on scan operator
            bool valid = false;
            switch(compOp){
                case EQ_OP :
                    if(conditionType){
                        if(strcmp((char*)attrCheck,(char*)value) == 0){
                            rid = checkRID;
                            valid = true;
                            returnData(rid, data);
                        }
                    }else{    
                        if(offset[0] == valueInt[0]){
                            rid = checkRID;
                            valid = true;
                            returnData(rid, data);
                        }
                    }
                    break; // no condition// = 
                case LT_OP : 
                    if(conditionType){
                        if(strcmp((char*)attrCheck,(char*)value) < 0){
                            rid = checkRID;
                            valid = true;
                            returnData(rid, data);
                        }
                    }else{    
                        if(attrCheck < value){
                            rid = checkRID;
                            valid = true;
                            returnData(rid, data);
                        }
                    }
                    break; // <
                case LE_OP :
                    if(conditionType){
                        if(strcmp((char*)attrCheck,(char*)value) <= 0){
                            rid = checkRID;
                            valid = true;
                            returnData(rid, data);
                        }
                    }else{    
                        if(attrCheck <= value){
                            rid = checkRID;
                            valid = true;
                            returnData(rid, data);
                        }
                    }
                    break; // <=
                case GT_OP :
                    if(conditionType){
                        if(strcmp((char*)attrCheck,(char*)value) > 0){
                            rid = checkRID;
                            valid = true;
                            returnData(rid, data);
                        }
                    }else{    
                        if(attrCheck > value){
                            rid = checkRID;
                            valid = true;
                            returnData(rid, data);
                        }
                    }
                    break; // >
                case GE_OP :
                    if(conditionType){
                        if(strcmp((char*)attrCheck,(char*)value) >= 0){
                            rid = checkRID;
                            valid = true;
                            returnData(rid, data);
                        }
                    }else{    
                        if(attrCheck >= value){
                            rid = checkRID;
                            valid = true;
                            returnData(rid, data);
                        }
                    }
                    break; // >=
                case NE_OP :
                    if(conditionType){
                        if(strcmp((char*)attrCheck,(char*)value) != 0){
                            rid = checkRID;
                            valid = true;
                            returnData(rid, data);
                        }
                    }else{    
                        if(attrCheck != value){
                            rid = checkRID;
                            valid = true;
                            returnData(rid, data);
                        }
                    }
                    break; // !=
                case NO_OP : 
                    rid = checkRID;
                    valid = true;
                    returnData(rid, data);
                    break;
            }
            free(record);
            if(valueInt != nullptr)free(valueInt);
            if(attrCheck != nullptr)free(attrCheck);
            if(offset != nullptr)free(offset);
            if(valid){
                free(page);
                scannedRID = rid;
                return 0;
            }
        }
        free(page);
    }
    return RM_EOF;
}

void RM_ScanIterator::returnData(RID &rid, void* data){
    vector<Attribute> returnAttrs;
    int nullBytes = ceil((double)attrNames.size()/8);
    int offset = nullBytes;
    char *nullChar = (char*)calloc(1,sizeof(char));
    char *nullBytesIndicators = (char*)calloc(1, nullBytes);
    void* checkBuffer = calloc(1, 4096);
    for(int i = 0; i < attrNames.size(); i++){
        _rbf_manager->readAttribute(table, attrs, rid, attrNames[i], checkBuffer);
        memcpy(nullChar, checkBuffer, sizeof(char));
        // check if att is null
        if(nullChar[0] != 0){
            nullBytesIndicators[(int)(i/8)] = nullBytesIndicators[(int)(i/8)] | (1 << (8-(i%8) - 1));
        }else{
            for (int j = 0; j < attrs.size(); j++)
            {
                if (attrNames[i] == attrs[j].name)
                {
                    if (attrs[j].type == TypeVarChar)
                    {
                        int length;
                        memcpy(&length, (char *)checkBuffer + 1, sizeof(int));
                        memcpy((char *)data + offset, (char *)checkBuffer + 1, sizeof(int) + length);
                        offset += length + 4;
                        break;
                    }
                    else
                    {
                        memcpy((char *)data + offset, (char *)checkBuffer + 1, sizeof(int));
                        offset += 4;
                        break;
                    }
                }
            }
        }
        memset(checkBuffer, 0, 4096);
    }
    free(nullChar);
    free(nullBytesIndicators);
    free(checkBuffer);
}

    // scan calling rbfm scan

    // int rc;
    // vector<Attribute> attrs;
    // rc = getAttributes(tableName, attrs);
    // if(rc){
    //     return -1;
    // }
    // FileHandle table;
    // rc = _rbf_manager->openFile(tableName + ".tbl", table);
    // if(rc){
    //     return -1;
    // }
    // // RBFM_ScanIterator scanner;
    // // rc = _rbf_manager->scan(table, attrs, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.scanner);
    // if(rc){
    //     return -1;
    // }