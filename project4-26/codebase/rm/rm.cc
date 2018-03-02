#include "rm.h"
#include "../ix/ix.h"
#include <string.h>
#include <stdlib.h>
#include <iostream>
#include <math.h>

RelationManager* RelationManager::_rm = 0;
RecordBasedFileManager* recordBasedFileManager = 0;
IndexManager* indexManagerptr = 0;

//vector<Attribute> tableAttributes;
//vector<Attribute> columnAttributes;


RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
	recordBasedFileManager = RecordBasedFileManager::instance();
    indexManagerptr = IndexManager::instance();


	//fill the Tables Tbl attributes
	Attribute attr;
	attr.name = "table-id";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	tableAttributes.push_back(attr);

	attr.name = "table-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)50;
	tableAttributes.push_back(attr);

	attr.name = "file-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)50;
	tableAttributes.push_back(attr);

	attr.name = "no-of-columns";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	tableAttributes.push_back(attr);

	//fill the Columns Tbl attributes
	attr.name = "table-id";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	columnAttributes.push_back(attr);

	attr.name = "column-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)50;
	columnAttributes.push_back(attr);

	attr.name = "column-type";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	columnAttributes.push_back(attr);

	attr.name = "column-length";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	columnAttributes.push_back(attr);

	attr.name = "column-position";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	columnAttributes.push_back(attr);

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    indexAttributes.push_back(attr);

    attr.name = "index-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    indexAttributes.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)100;
    indexAttributes.push_back(attr);
}

RelationManager::~RelationManager()
{
}


void RelationManager::addRowsToTblTables(int tableId, const string &tableName, const string &fileName, int noOfColumns)
{
	RID rid;
	void* tableRecord = malloc(100);
	int offset = 0;
	int nullFieldsIndicatorActualSize = 1;
	unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
	memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

	memcpy(tableRecord, nullsIndicator, nullFieldsIndicatorActualSize);
	offset = offset + nullFieldsIndicatorActualSize;
	memcpy((char*)tableRecord + offset, &tableId, sizeof(int));
	offset = offset + sizeof(int);
	int sizeOfVarchar = strlen(tableName.c_str());
	memcpy((char*)tableRecord + offset, &sizeOfVarchar, sizeof(int));
	offset = offset + sizeof(int);
	memcpy((char*)tableRecord + offset, tableName.c_str(), sizeOfVarchar);
	offset = offset + sizeOfVarchar;
	sizeOfVarchar = strlen(fileName.c_str());
	memcpy((char*)tableRecord + offset, &sizeOfVarchar, sizeof(int));
	offset = offset + sizeof(int);
	memcpy((char*)tableRecord + offset, fileName.c_str(), sizeOfVarchar);
	offset = offset + sizeOfVarchar;
	memcpy((char*)tableRecord + offset, &noOfColumns, sizeof(int));
	offset = offset + sizeof(int);
	insertTuple("Tables", tableRecord, rid, true);

	int length;
	memcpy(&length, (char*)tableRecord + 5, sizeof(int));
	void* tblName = malloc(6);
	memcpy(tblName, (char*)tableRecord + 9, 6);
	free(tblName);
	free(tableRecord);
	free(nullsIndicator);
}


void RelationManager::addRowsToTblIndex(int tableId, const string &attributeName, const string &fileName)
{
	RID rid;
	void* tableRecord = malloc(1000);
	int offset = 0;
	int nullFieldsIndicatorActualSize = 1;
	unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
	memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

	memcpy(tableRecord, nullsIndicator, nullFieldsIndicatorActualSize);
	offset = offset + nullFieldsIndicatorActualSize;

    memcpy((char*)tableRecord + offset, &tableId, sizeof(int));
	offset = offset + sizeof(int);

	int sizeOfVarchar = strlen(attributeName.c_str());
	memcpy((char*)tableRecord + offset, &sizeOfVarchar, sizeof(int));
	offset = offset + sizeof(int);
	memcpy((char*)tableRecord + offset, attributeName.c_str(), sizeOfVarchar);
	offset = offset + sizeOfVarchar;

    sizeOfVarchar = strlen(fileName.c_str());
	memcpy((char*)tableRecord + offset, &sizeOfVarchar, sizeof(int));
	offset = offset + sizeof(int);
	memcpy((char*)tableRecord + offset, fileName.c_str(), sizeOfVarchar);
	offset = offset + sizeOfVarchar;

    insertTuple("Index", tableRecord, rid, true);

	free(tableRecord);
	free(nullsIndicator);
}

void RelationManager::prepareTblTables()
{
	addRowsToTblTables(1, "Tables", "Tables", 4);
	addRowsToTblTables(2, "Columns", "Columns", 5);
    ////////////////////change///////////////////////
	addRowsToTblTables(3, "Index", "Index", 3);
}


void RelationManager::addRowsToTblColumns(int tableId, const string &columnName, AttrType type, int size, int index)
{
	RID rid;
	void* columnRecord = malloc(200);
	int offset = 0;
	int nullFieldsIndicatorActualSize = 1;
	unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
	memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

	memcpy(columnRecord, nullsIndicator, nullFieldsIndicatorActualSize);
	offset = offset + 1;
	memcpy((char*)columnRecord + offset, &tableId, sizeof(int));
	offset = offset + sizeof(int);
	int sizeOfVarchar = strlen(columnName.c_str());
	memcpy((char*)columnRecord + offset, &sizeOfVarchar, sizeof(int));
	offset = offset + sizeof(int);
	memcpy((char*)columnRecord + offset, columnName.c_str(), sizeOfVarchar);		//table-name
	offset = offset + sizeOfVarchar;
	int enumNum = static_cast <int>(type);
	memcpy((char*)columnRecord + offset, &enumNum, sizeof(int));
	offset = offset + sizeof(int);
	memcpy((char*)columnRecord + offset, &size, sizeof(int));
	offset = offset + sizeof(int);
	memcpy((char*)columnRecord + offset, &index, sizeof(int));
	offset = offset + sizeof(int);

	insertTuple("Columns", columnRecord, rid, true);
	free(columnRecord);
	free(nullsIndicator);
}

void RelationManager::prepareTblColumns()
{
	addRowsToTblColumns(1, "table-id", TypeInt, 4, 1);
	addRowsToTblColumns(1, "table-name", TypeVarChar, 50, 2);
	addRowsToTblColumns(1, "file-name", TypeVarChar, 50, 3);
	addRowsToTblColumns(1, "no-of-columns", TypeInt, 4, 4);
	addRowsToTblColumns(2, "table-id", TypeInt, 4, 1);
	addRowsToTblColumns(2, "column-name", TypeVarChar, 50, 2);
	addRowsToTblColumns(2, "column-type", TypeInt, 4, 3);
	addRowsToTblColumns(2, "column-length", TypeInt, 4, 4);
	addRowsToTblColumns(2, "column-position", TypeInt, 4, 5);

	////////////////////change///////////////////////
	addRowsToTblColumns(3, "table-id", TypeInt, 4, 1);
	addRowsToTblColumns(3, "attribute-name", TypeVarChar, 50, 2);
	addRowsToTblColumns(3, "file-name", TypeVarChar, 100, 3);
}

RC RelationManager::createCatalog()
{
	if(recordBasedFileManager->createFile("Tables") == -1)
	{
		return -1;
	}
	prepareTblTables();

	recordBasedFileManager->createFile("Columns");
	prepareTblColumns();

	////////////////////change///////////////////////
	recordBasedFileManager->createFile("Index");
    return 0;
}

RC RelationManager::deleteCatalog()
{
	recordBasedFileManager->destroyFile("Tables");
	recordBasedFileManager->destroyFile("Columns");
	////////////////////change///////////////////////
	recordBasedFileManager->destroyFile("Index");
    return 0;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
    string fileName;
    fileName = tableName + "_" + attributeName;
    // cout<<"FILE NAME BEING CREATED  : "<<fileName<<endl;

    //craete File name.
    indexManagerptr->createFile(fileName); //created the index file on the attribute name.

    //inserting data into index catalog table.
    //The table id needs to be retrieved from the tables's table for given table name.
    int tableId;
    RID rid;
    getTableIdForTable(tableName, rid, tableId);
    // cout<<"TABLE ID got : "<<tableId<<endl;
    addRowsToTblIndex(tableId, attributeName, fileName);

    //check if no of keys in the table is greater than one...
    //if it is we need to add the entries to the index file.

    FileHandle fileHandle;
    if(recordBasedFileManager->openFile(tableName, fileHandle) != 0)		//table does not exist
    {
    	return -1;
    }
    recordBasedFileManager->closeFile(fileHandle);

    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    RM_ScanIterator rmsi;
    string attr= "";
    vector<string> attributes;
    int positionOfAttribute;
    for(unsigned int i=0;i<recordDescriptor.size();i++)
    {
		attributes.push_back(recordDescriptor[i].name);
		if(recordDescriptor[i].name==attributeName)
		{
			positionOfAttribute = i;
		}

    }

    void *FILENAME = malloc(100);
    scan(tableName, attr, NO_OP, FILENAME, attributes, rmsi);
    RID indexTblRid;
    void * returnedData = malloc(100);

    while(rmsi.getNextTuple(indexTblRid, returnedData) != RM_EOF)
    {
        IXFileHandle ixFileHandle;
        indexManagerptr -> openFile(fileName,ixFileHandle);
        Attribute indexAttr = recordDescriptor[positionOfAttribute];

        void *key = malloc(1000);
        extractedKey(key,recordDescriptor,positionOfAttribute, returnedData); //extracting key value from data
   //      if(recordDescriptor[positionOfAttribute].type == TypeReal)
   //      {
   //      	float fKey;
			// memcpy(&fKey, key, sizeof(float));
			// cout<<"FLOAT KEY BEING INSERTED : "<<fKey<<endl;
   //      }
   //      else if(recordDescriptor[positionOfAttribute].type == TypeInt)
   //      {
   //      	int iKey;
  	// 	    memcpy(&iKey, key, sizeof(int));
  	// 		cout<<"INT KEY BEING INSERTED : "<<iKey<<endl;
   //      }

        // cout<<"INDEX ATTR : "<<indexAttr.name<<":"<<indexAttr.type<<endl;
        RC  indexInsertResult = indexManagerptr->insertEntry(ixFileHandle,indexAttr,key,indexTblRid);
        if(indexInsertResult == -1)
        {
        	 // cout<<"Failure to insert!"<<endl;
            free(key);
        	indexManagerptr -> closeFile(ixFileHandle);
        	return indexInsertResult;
        }
        // indexManagerptr -> printBtree(ixFileHandle,indexAttr);
        free(key);

    	indexManagerptr -> closeFile(ixFileHandle);
    }
    rmsi.close();
    free(FILENAME);
    free(returnedData);
    return 0;


//    //testing.
//
//    RM_ScanIterator rmsi;
//    string attr = "file-name";
//    vector<string> attributes;
//    attributes.push_back("file-name");
//
//    //    vector<string> attrs;
////    attrs.push_back("attr5");
////    attrs.push_back("attr12");
////    attrs.push_back("attr28");
//
//    void *FILENAME = malloc(100);
//    int s =  strlen(fileName.c_str());
//    memcpy(FILENAME,&s, sizeof(int));
//    memcpy((char *)FILENAME+ sizeof(int),fileName.c_str(),s);
//    scan("Index", attr, EQ_OP, FILENAME, attributes, rmsi);
//    RID indexTblRid;
//    void * returnedData = malloc(100);
//    RID toBeDeletedRid;
//
//    while(rmsi.getNextTuple(indexTblRid, returnedData) != RM_EOF)
//    {
//        void *attrName;
//        int attrNameLength;
//        memcpy(&attrNameLength, (char*)returnedData+sizeof(char), sizeof(int));
//        memcpy(attrName, (char*)returnedData +(sizeof(char)+sizeof(int)), 6);
//
//        string varcharKey = string((char*)attrName, attrNameLength);
//        cout<<varcharKey<<endl;
//
//    }
//    exit(0);



}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
    //create filename
    string fileName = tableName + "_" + attributeName;

    int tableId;
    RID rid;
    getTableIdForTable(tableName, rid, tableId);

    RM_ScanIterator rmsi;
    string attr = "table-id";
    vector<string> scanAttributes;
    string scanAttr = "index-name";
    scanAttributes.push_back(scanAttr);

    scan("Index", attr, EQ_OP, &tableId, scanAttributes, rmsi);
    RID indexTblRid;

    void * returnedData = malloc(100);
    RID toBeDeletedRid;
    bool indexFound = false;

    while(rmsi.getNextTuple(indexTblRid, returnedData) != RM_EOF)
    {
        int attrNameLength;
        memcpy(&attrNameLength, (char*)returnedData + sizeof(char), sizeof(int));
        void* attrName = malloc(attrNameLength);
        memcpy(attrName, (char*)returnedData + sizeof(char) + sizeof(int), attrNameLength);

        string varcharKey = string((char*)attrName, attrNameLength);
//        cout<<"Index Name Found : "<<varcharKey<<endl;
        free(attrName);

        if(varcharKey.compare(attributeName) == 0) {
            toBeDeletedRid = indexTblRid;
            indexFound = true;
            break;
        }

    }
    rmsi.close();
    free(returnedData);
    if(indexFound)
    {
    	deleteTuple("Index", toBeDeletedRid, true);
    	return indexManagerptr->destroyFile(fileName); //destroyed the index file on the attribute name
    }
    else
    {
        return -1;
    }
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *data)
{
    RC scanResult = ixScanIterator.getNextEntry(rid, data);
    if(scanResult == IX_EOF)
    {
        indexManagerptr->closeFile(*ixScanIterator.ixfileHandle);
    }
    return scanResult;
}

RC RM_IndexScanIterator::close()
{
	return ixScanIterator.close();
}


RC RelationManager::indexScan(const string &tableName,
             const string &attributeName,
             const void *lowKey,
             const void *highKey,
             bool lowKeyInclusive,
             bool highKeyInclusive,
             RM_IndexScanIterator &rm_IndexScanIterator
)
{
    string fileName = tableName + "_" + attributeName;
    FileHandle fileHandle;
    if(recordBasedFileManager->openFile(tableName, fileHandle) != 0)
    {
    	return -1;
    }
    recordBasedFileManager->closeFile(fileHandle);
    IXFileHandle ixFileHandle;
    IX_ScanIterator ixScanIterator;
    rm_IndexScanIterator.ixScanIterator = ixScanIterator;

    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    Attribute attribute;
    for(unsigned int i = 0 ; i < attrs.size(); i++)
    {
    	// cout<<attrs[i].name<<endl;
        if(attrs[i].name.compare(attributeName) == 0)
        {
        	attribute.name = attrs[i].name;
        	attribute.length = attrs[i].length;
        	attribute.type = attrs[i].type;
            break;
        }
    }

    if(indexManagerptr->openFile(fileName, ixFileHandle) != 0)		//make sure the file exists before scanning it
    {
            return -1;
    }

    return indexManagerptr->scan(ixFileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ixScanIterator);
}



RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	FileHandle fileHandle;

	if(recordBasedFileManager->openFile(tableName, fileHandle) == 0)		//table already exists
	{
		recordBasedFileManager->closeFile(fileHandle);
		return -1;
	}

	recordBasedFileManager->openFile("Tables", fileHandle);

	void* data = malloc(sizeof(int) + sizeof(char));
	unsigned int totalNoOfPagesInFile = recordBasedFileManager->getNumberOfPagesInFile(fileHandle);
	RID rid;
	short lengthOfDirectory = recordBasedFileManager->getNumberOfSlots(fileHandle, totalNoOfPagesInFile - 1);
	rid.pageNum = totalNoOfPagesInFile - 1;
	rid.slotNum = PAGE_SIZE - 3*sizeof(short) - lengthOfDirectory* 2*sizeof(short);
	int temp = 1;
	while(readAttribute("Tables", rid, "table-id", data) != 0 && temp <= lengthOfDirectory)
	{
		rid.slotNum = rid.slotNum + 2*sizeof(short);
		temp = temp + 1;
	}
	int lastInsertedTableId;
	memcpy(&lastInsertedTableId, (char*)data + sizeof(char), sizeof(int));
	addRowsToTblTables(lastInsertedTableId + 1, tableName, tableName, attrs.size());

	int i = 1;
	for (vector<Attribute>::const_iterator  it = attrs.begin() ; it != attrs.end(); ++it)
	{
		addRowsToTblColumns(lastInsertedTableId + 1, it->name, it->type, it->length, i++);
	}
	return recordBasedFileManager->createFile(tableName);
}

RC RelationManager::deleteTable(const string &tableName)
{
	FileHandle tableFileHandle;
	if(recordBasedFileManager->openFile(tableName, tableFileHandle) != 0)
	{
		return -1;
	}
	recordBasedFileManager->closeFile(tableFileHandle);
	if(tableName.compare("Tables") == 0 || tableName.compare("Columns") == 0 || tableName.compare("Index") == 0)
	{
		return -1;
	}
	RID tablesTblRid;
	int tableId;
	getTableIdForTable(tableName, tablesTblRid, tableId);		//this tableRid will help to delete this entry from tbl Tables

	RID colsTblRid;
	RM_ScanIterator rmsi;
	string attr = "table-id";
	vector<string> attributes;
	attributes.push_back(attr);
	string tableColumns = "Columns";
	scan(tableColumns, attr, EQ_OP, &tableId, attributes, rmsi);
	void * returnedData = malloc(100);
	vector<RID> toBeDeletedRids;
	while(rmsi.getNextTuple(colsTblRid, returnedData) != RM_EOF)
	{
		int colsTableId;
		memcpy(&colsTableId, (char*)returnedData + sizeof(char), sizeof(int));
		toBeDeletedRids.push_back(colsTblRid);
	}

	for(unsigned int i = 0; i < toBeDeletedRids.size(); i++)
	{
		deleteTuple("Columns", toBeDeletedRids[i], true);
	}
	deleteTuple("Tables", tablesTblRid, true);


	recordBasedFileManager->destroyFile(tableName);			//finally delete the tableName file
    return 0;
}

void RelationManager::getTableIdForTable(const string &tableName, RID &rid, int &tableId)
{
	FileHandle fileHandle;
	recordBasedFileManager->openFile("Tables", fileHandle);

	rid.slotNum = PAGE_SIZE - 3*sizeof(short);				//start scanning from the beginning
	rid.pageNum = 0;

	unsigned int totalPagesInFile = fileHandle.getNumberOfPages();

	unsigned int pageNum = 0;

	while(true)
	{
		short numOfSlots = recordBasedFileManager->getNumberOfSlots(fileHandle, pageNum);
		short temp = 1;
		while(temp <= numOfSlots )			// two slots used by tbl Tables & tbl Columns
		{
			rid.slotNum = rid.slotNum - 2*sizeof(short);
			void *data = malloc(100);
			if(recordBasedFileManager->readAttribute(fileHandle, tableAttributes, rid, "table-name", data) != 0)
			{
				temp = temp + 1;
				free(data);
				continue;
			}
//			unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
//			memcpy(nullsIndicator, (char*)data, nullFieldsIndicatorActualSize);
//			bool nullBit = nullsIndicator[0] & 1 << 7;
//			if(nullBit)
//			{
//
//			}
			void *readTableName = malloc(50);
			int varcharLen;
			memcpy(&varcharLen, (char*)data + sizeof(char), sizeof(int));
			memcpy(readTableName, (char*)data + sizeof(char) + sizeof(int), varcharLen);
			if((string((char*)readTableName, varcharLen)).compare(tableName) == 0)
			{
				recordBasedFileManager->readAttribute(fileHandle, tableAttributes, rid, "table-id", data);
				memcpy(&tableId, (char*)data + sizeof(char), sizeof(int));
				free(readTableName);
				break;
			}
			free(data);
			free(readTableName);
			temp = temp + 1;
		}
		if(temp <= numOfSlots)		//means attribute value was found
		{
			break;
		}
		pageNum = pageNum + 1;
		if(pageNum > totalPagesInFile - 1)
		{
			tableId = -1;
			break;
		}
	}
	recordBasedFileManager->closeFile(fileHandle);
}


RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	if(tableName.compare("Tables") == 0)
	{
		attrs = tableAttributes;
	}
	else if (tableName.compare("Columns") == 0)
	{
		attrs = columnAttributes;
	}
	else if (tableName.compare("Index") == 0)
	{
		attrs = indexAttributes;
	}
	else
	{
		RID rid;
		int tableId;
		getTableIdForTable(tableName, rid, tableId);
		FileHandle fileHandle;
		recordBasedFileManager->openFile("Columns", fileHandle);
		vector<string> attributeNames;
		for (vector<Attribute>::const_iterator  it = columnAttributes.begin() ; it != columnAttributes.end(); ++it)
		{
			attributeNames.push_back(it->name);
		}
		RBFM_ScanIterator rbfm_ScanIterator;
		recordBasedFileManager->scan(fileHandle, columnAttributes, "table-id", EQ_OP, &tableId, attributeNames, rbfm_ScanIterator);

		void* storedRecord = malloc(PAGE_SIZE);
		RID recordRid;
		while(rbfm_ScanIterator.getNextRecord(recordRid, storedRecord) != RBFM_EOF)
		{
			int readOffset = 0;
			Attribute attr;
			int nullBitIndicatorSize = ceil(float(columnAttributes.size())/CHAR_BIT);
			unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullBitIndicatorSize);
			memcpy(nullFieldsIndicator, (char*)storedRecord + nullBitIndicatorSize, nullBitIndicatorSize);
			readOffset = readOffset + nullBitIndicatorSize;

			int i = 0;
			for (vector<Attribute>::const_iterator  it = columnAttributes.begin() ; it != columnAttributes.end(); ++it, i++)
			{
				bool nullBit = nullFieldsIndicator[i/CHAR_BIT] & (1 << (7 - (i%CHAR_BIT)));		//nullBitIndicatorSize in bytes, so multiply by 8 for bits
				if(!nullBit)
				{
					if(it->type == TypeVarChar)
					{
						int varcharLen;
						memcpy(&varcharLen, (char *)storedRecord + readOffset, sizeof(int));
						readOffset = readOffset + sizeof(int);
						void* varchar = malloc(varcharLen);
						memcpy((char*)varchar, (char*)(storedRecord) + readOffset, varcharLen);
						readOffset  = readOffset + varcharLen;
						attr.name = string((char *)varchar,varcharLen);
					}
					else if (it->type == TypeInt)
					{
						int number;
						memcpy(&number, (char *)storedRecord + readOffset, sizeof(int));
						readOffset =  readOffset + sizeof(int);

						if(it->name.compare("column-type") == 0)
						{
							if(number == TypeInt)
							{
								attr.type = TypeInt;
							}
							else if(number == TypeReal)
							{
								attr.type = TypeReal;
							}
							else
							{
								attr.type =TypeVarChar;
							}
						}
						if(it->name.compare("column-length")==0)
						{
							attr.length = number;

						}
					}
				}
			}

			attrs.push_back(attr);

		}
		free(storedRecord);
		rbfm_ScanIterator.close();
		recordBasedFileManager->closeFile(fileHandle);
    }
	return 0;
}


RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid, bool isInternal)
{
	if((tableName.compare("Tables") == 0 || tableName.compare("Columns") == 0 || tableName.compare("Index") == 0) && !isInternal)
	{
		return -1;
	}
	FileHandle fileHandle;
	if(recordBasedFileManager->openFile(tableName, fileHandle) != 0)
	{
		return -1;
	}

	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	RC insertResult = recordBasedFileManager->insertRecord(fileHandle, recordDescriptor, data, rid);
	recordBasedFileManager->closeFile(fileHandle);

	if(tableName.compare("Tables") == 0 || tableName.compare("Columns") == 0  || tableName.compare("Index") == 0)
	{
		return insertResult;
	}

	FileHandle indexFileHandle;
	if(recordBasedFileManager->openFile("Index", indexFileHandle) != 0)			//check if the file exists. THEN CLOSE IT!
	{
		return insertResult;
	}
	recordBasedFileManager->closeFile(indexFileHandle);

    //scan to find which indexes to enter into.

    //I have the record descriptor of inserted data, RID of inserted data and table name.
    // For loop is used to iterate through the index table and insert index value for each
    // corresponding filename found in the index table.

    for(unsigned int i = 0; i<recordDescriptor.size(); i++)
    {
        RM_ScanIterator rmsi;
        string fileName = tableName+"_"+recordDescriptor[i].name;
        string attr = "file-name";
        vector<string> attributes;
        attributes.push_back("file-name");
        int s =  strlen(fileName.c_str());
        void *FILENAME = malloc(s);
        memcpy(FILENAME,&s, sizeof(int));
        memcpy((char *)FILENAME+ sizeof(int),fileName.c_str(),s);
        scan("Index", attr, EQ_OP, FILENAME, attributes, rmsi);
        RID indexTblRid;
        void *returnedData = malloc(100);
        while (rmsi.getNextTuple(indexTblRid, returnedData) != RM_EOF)
        {
            IXFileHandle ixFileHandle;
            indexManagerptr -> openFile(fileName,ixFileHandle);
            Attribute indexAttr = recordDescriptor[i];
            void *key = malloc(1000);
            extractedKey(key,recordDescriptor,i, data); //extracting key value from data
            // float checker;
            // memcpy(&checker, key, sizeof(float));
            // cout<<"KEy being inserted  : "<<checker<<endl;
            RC indexInsertResult = indexManagerptr->insertEntry(ixFileHandle,indexAttr,key,rid);
            if(indexInsertResult == -1)
            {
            	free(key);
				indexManagerptr -> closeFile(ixFileHandle);
            	return -1;
            }
          	// indexManagerptr -> printBtree(ixFileHandle,indexAttr);
            free(key);
            indexManagerptr -> closeFile(ixFileHandle);

        }
        free(returnedData);
        free(FILENAME);
        rmsi.close();
    }

	return insertResult;
}


void RelationManager::extractedKey(void *key, vector<Attribute> recordDescriptor, int i, const void *data)
{
	int nullBitIndicatorSize = ceil(float(recordDescriptor.size())/CHAR_BIT);
	int startOffset=nullBitIndicatorSize;
    for(int j=0;j<i;j++)
    {
        if(recordDescriptor[j].type==TypeInt || recordDescriptor[j].type==TypeReal )
        {
            startOffset = startOffset+4;
        }
        else
        {
            int varcharLen;
            memcpy(&varcharLen,(char *)data+ startOffset, sizeof(int));
            startOffset = startOffset + sizeof(int);
            startOffset = startOffset + varcharLen;
        }

    }

    if(recordDescriptor[i].type==TypeInt || recordDescriptor[i].type==TypeReal )
    {
//        float key1;
//		memcpy(&key1,(char *)data + startOffset, sizeof(int));
//		cout<<endl<<"inside extracted key"<<key1<<endl;
		memcpy(key,(char *)data + startOffset, sizeof(int));

    } else
    {
        int varcharLen;
        memcpy(&varcharLen,(char *)data+ startOffset, sizeof(int));
        memcpy(key,(char *)data + startOffset+ sizeof(int),varcharLen);
    }
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid, bool isInternal)
{
	if((tableName.compare("Tables") == 0 || tableName.compare("Columns") == 0 || tableName.compare("Index") == 0) && !isInternal)
	{
		return -1;
	}
	FileHandle fileHandle;
	if(recordBasedFileManager->openFile(tableName, fileHandle) != 0)
	{
		return -1;
	}

	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
    void *data = malloc(PAGE_SIZE);
    int resultFromReadTuple = readTuple(tableName,rid,data);
    RC deleteResult = recordBasedFileManager->deleteRecord(fileHandle, recordDescriptor, rid);
    recordBasedFileManager->closeFile(fileHandle);

    //going to delete key from index also.
    for(unsigned int i=0;i<recordDescriptor.size();i++) {

        RM_ScanIterator rmsi;
        string fileName = tableName+"_"+recordDescriptor[i].name;
        string attr = "file-name";
        vector<string> attributes;
        attributes.push_back("file-name");
        int s =  strlen(fileName.c_str());
        void *FILENAME = malloc(s);
        memcpy(FILENAME,&s, sizeof(int));
        memcpy((char *)FILENAME+ sizeof(int),fileName.c_str(),s);
        scan("Index", attr, EQ_OP, FILENAME, attributes, rmsi);
        RID indexTblRid;
        void *returnedData = malloc(1000);


        while (rmsi.getNextTuple(indexTblRid, returnedData) != RM_EOF) { //if given tablename.attribute name file exists

            IXFileHandle ixFileHandle;
            indexManagerptr -> openFile(fileName,ixFileHandle);
            Attribute indexAttr = recordDescriptor[i];
            void *key = malloc(1000);
            extractedKey(key,recordDescriptor,i, data); //extracting key value from data
//            int ket;
//            memcpy(&ket,key,sizeof(int));
//            cout<<endl<<ket<<endl;
//
            RC  indexDeleteResult = indexManagerptr -> deleteEntry(ixFileHandle,indexAttr,key,rid);
//            cout<<indexDeleteResult<<endl;
//            indexManagerptr -> printBtree(ixFileHandle,indexAttr);
            free(key);

            indexManagerptr -> closeFile(ixFileHandle);

        }

        rmsi.close();
        free(FILENAME);
        free(returnedData);
    }

    free(data);
	return deleteResult;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid, bool IsInternal)
{
	FileHandle fileHandle;
	if(recordBasedFileManager->openFile(tableName, fileHandle) != 0)
	{
		return -1;
	}

	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
    void *beforeData = malloc(PAGE_SIZE);
    int resultFromBeforeReadTuple = readTuple(tableName,rid,beforeData);

    RC updateResult = recordBasedFileManager->updateRecord(fileHandle, recordDescriptor, data, rid);
    recordBasedFileManager->closeFile(fileHandle);

    //going to delete key from index.
    for(unsigned int i=0;i<recordDescriptor.size();i++) {

        RM_ScanIterator rmsi;
        string fileName = tableName+"_"+recordDescriptor[i].name;
        string attr = "file-name";
        vector<string> attributes;
        attributes.push_back("file-name");
        int s =  strlen(fileName.c_str());
        void *FILENAME = malloc(s);
        memcpy(FILENAME,&s, sizeof(int));
        memcpy((char *)FILENAME+ sizeof(int),fileName.c_str(),s);
        scan("Index", attr, EQ_OP, FILENAME, attributes, rmsi);
        RID indexTblRid;
        void *returnedData = malloc(100);


        while (rmsi.getNextTuple(indexTblRid, returnedData) != RM_EOF) { //if given tablename.attribute name file exists

            IXFileHandle ixFileHandle;
            indexManagerptr -> openFile(fileName,ixFileHandle);
            Attribute indexAttr = recordDescriptor[i];
            void *key = malloc(1000);
            extractedKey(key,recordDescriptor,i, beforeData); //extracting key value from data
            RC  indexInsertResult = indexManagerptr -> deleteEntry(ixFileHandle,indexAttr,key,rid);
            //indexManagerptr -> printBtree(ixFileHandle,indexAttr);
            free(key);

            indexManagerptr -> closeFile(ixFileHandle);

        }
        rmsi.close();
        free(FILENAME);
        free(returnedData);
    }
    free(beforeData);

    //going to update the new key value.
    for(unsigned int i=0;i<recordDescriptor.size();i++) {

        RM_ScanIterator rmsi;
        string fileName = tableName+"_"+recordDescriptor[i].name;
        string attr = "file-name";
        vector<string> attributes;
        attributes.push_back("file-name");
        int s =  strlen(fileName.c_str());
        void *FILENAME = malloc(s);
        memcpy(FILENAME,&s, sizeof(int));
        memcpy((char *)FILENAME+ sizeof(int),fileName.c_str(),s);
        scan("Index", attr, EQ_OP, FILENAME, attributes, rmsi);
        RID indexTblRid;
        void *returnedData = malloc(100);


        while (rmsi.getNextTuple(indexTblRid, returnedData) != RM_EOF) { //if given tablename.attribute name file exists

            IXFileHandle ixFileHandle;
            indexManagerptr -> openFile(fileName,ixFileHandle);
            Attribute indexAttr = recordDescriptor[i];
            void *key = malloc(1000);
            extractedKey(key,recordDescriptor,i, data); //extracting key value from data
            RC  indexInsertResult = indexManagerptr -> insertEntry(ixFileHandle,indexAttr,key,rid);
//            indexManagerptr -> printBtree(ixFileHandle,indexAttr);
            free(key);

            indexManagerptr -> closeFile(ixFileHandle);

        }
        rmsi.close();
        free(FILENAME);
        free(returnedData);
    }

	return updateResult;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	FileHandle fileHandle;
	if(recordBasedFileManager->openFile(tableName, fileHandle) != 0)
	{
		return -1;
	}

	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	RC readResult = recordBasedFileManager->readRecord(fileHandle, recordDescriptor, rid, data);

	recordBasedFileManager->closeFile(fileHandle);
	return readResult;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return recordBasedFileManager->printRecord(attrs, data);
//	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	FileHandle fileHandle;
	if(recordBasedFileManager->openFile(tableName, fileHandle) != 0)
	{
		return -1;
	}
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	RC readAttrResult = recordBasedFileManager->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
	recordBasedFileManager->closeFile(fileHandle);
    return readAttrResult;
}

RC RM_ScanIterator::close()
{
	return rbfm_ScanIterator.close();
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
	RC scanResult = rbfm_ScanIterator.getNextRecord(rid, data);
	if(scanResult == RBFM_EOF)
	{
		recordBasedFileManager->closeFile(rbfm_ScanIterator.fileHandle);
	}
	return scanResult;
}


RC RelationManager::scan(const string &tableName,
    const string &conditionAttribute,
    const CompOp compOp,
    const void *value,
    const vector<string> &attributeNames,
    RM_ScanIterator &rm_ScanIterator)
{
	FileHandle fileHandle;
	if(recordBasedFileManager->openFile(tableName, fileHandle) != 0)
	{
		return -1;
	}
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	RBFM_ScanIterator rbfm_ScanIterator;
	rm_ScanIterator.rbfm_ScanIterator = rbfm_ScanIterator;
	return recordBasedFileManager->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfm_ScanIterator);
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}


