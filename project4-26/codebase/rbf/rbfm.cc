#include "rbfm.h"

#include <math.h>
#include <iostream>
#include <string.h>
#include <stdlib.h>
#include <map>
#include <bitset>
#include <algorithm>
#include <sstream>


RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;
PagedFileManager* pfm = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
	pfm = PagedFileManager::instance();
	updateFlag = false;
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {

	return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {

    return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {

	return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {

	return pfm->closeFile(fileHandle);
}

short RecordBasedFileManager::getNumberOfSlots(FileHandle &fileHandle, unsigned pageNum)
{
	void* storedPage = malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum, storedPage);
	short numOfSlots;
	memcpy(&numOfSlots, (char*)storedPage + PAGE_SIZE - 3 * sizeof(short), sizeof(short));
	free(storedPage);
	return numOfSlots;
}

int RecordBasedFileManager::getNumberOfPagesInFile(FileHandle &fileHandle)
{
	return fileHandle.getNumberOfPages();
}

void RecordBasedFileManager::insertRecordOnNewPage(FileHandle &fileHandle, const void* insertData, const short insertOffset, RID &rid)
{
	short recordStartsAt = 0;
	short lenOfDirectory = 1;
	short lenOfRecord = insertOffset;
	short freeSpace = PAGE_SIZE - 5*sizeof(short) - lenOfRecord;
	memcpy((char*)(insertData) + PAGE_SIZE - sizeof(short), &freeSpace, sizeof(short));
	memcpy((char*)(insertData) + PAGE_SIZE - 2*sizeof(short), &insertOffset , sizeof(short));
	memcpy((char*)(insertData) + PAGE_SIZE - 3*sizeof(short), &lenOfDirectory, sizeof(short)); //length of directory = 1 after we insert
	memcpy((char*)(insertData) + PAGE_SIZE - 4*sizeof(short), &lenOfRecord, sizeof(short)); //length of record
	memcpy((char*)(insertData) + PAGE_SIZE - 5*sizeof(short), &recordStartsAt, sizeof(short)); //offset from beginning of file for record
	fileHandle.appendPage(insertData);
	rid.pageNum = fileHandle.getNumberOfPages() - 1;
	rid.slotNum = PAGE_SIZE - 5* sizeof(short);
}

RC RecordBasedFileManager::insertRecordOnPage(FileHandle &fileHandle, const void * insertData, const short insertOffset, const int pageNo, RID &rid)
{
	void *storedData = malloc(PAGE_SIZE);
	fileHandle.readPage(pageNo, storedData);

	short freeBytes;
	short freeBytesStartAt;
	short lenOfDirectory;

	memcpy(&freeBytes, (char*)storedData + PAGE_SIZE - sizeof(short), sizeof(short));
	memcpy(&freeBytesStartAt, (char*)storedData + PAGE_SIZE - 2*sizeof(short), sizeof(short));
	memcpy(&lenOfDirectory, (char*)storedData + PAGE_SIZE - 3*sizeof(short), sizeof(short));

	int spaceLeft = freeBytes - insertOffset - 2* sizeof(short);
	if(spaceLeft >= 0)
	{
		memcpy((char*)storedData + freeBytesStartAt, insertData, insertOffset);	//insert the record

		int slot = PAGE_SIZE - 3* sizeof(short) - (2 * sizeof(short) * lenOfDirectory) - 2*sizeof(short);	 //the last 2 * sizeof(short) is to get the entry pos of new record slot//each directory entry : offset from start for record + len of record
		memcpy((char*)storedData + slot, &freeBytesStartAt, sizeof(short));
		memcpy((char*)storedData + slot + sizeof(short), &insertOffset, sizeof(short));

		freeBytes = freeBytes  - insertOffset - 2* sizeof(short);
		lenOfDirectory = lenOfDirectory + 1;
		freeBytesStartAt = freeBytesStartAt + insertOffset;
		rid.pageNum = pageNo;
		rid.slotNum = slot;

		memcpy((char*)storedData + PAGE_SIZE - sizeof(short), &freeBytes, sizeof(short));
		memcpy((char*)storedData + PAGE_SIZE - 2*sizeof(short), &freeBytesStartAt, sizeof(short));
		memcpy((char*)storedData + PAGE_SIZE - 3*sizeof(short), &lenOfDirectory, sizeof(short));

		fileHandle.writePage(pageNo, storedData);
		free(storedData);
		return 0;
	}
	else
	{
		free(storedData);
		return -1;
	}
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {

	void *insertData = malloc(PAGE_SIZE); //this is a page
	short insertOffset = 0;

	map<short,void*> offsetVarcharMap;	// Offset  - Data Map where data is mainly varchars
	map<short,short> offsetVarcharLenMap;	// offset -Length(Varchar) map

	int nullBitIndicatorSize = ceil(float(recordDescriptor.size())/CHAR_BIT);
	short i = 0;
	short readOffset = 0;
	bool nullBit = false;

    if(updateFlag == true)
    {
        char updateStatus = '1';
        memcpy(insertData, &updateStatus, sizeof(char));
        //add first byte as 1
    }
    else
    {
        char updateStatus = '0';
        memcpy(insertData, &updateStatus, sizeof(char));
        //add first byte as 0
	}

    insertOffset = insertOffset + sizeof(char);

    unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullBitIndicatorSize);
	memcpy(nullFieldsIndicator, data, nullBitIndicatorSize);
	memcpy((char *)(insertData)+insertOffset, nullFieldsIndicator, nullBitIndicatorSize);
	insertOffset = insertOffset + nullBitIndicatorSize;
	readOffset = readOffset + nullBitIndicatorSize;

	for (vector<Attribute>::const_iterator  it = recordDescriptor.begin() ; it != recordDescriptor.end(); ++it, i++)
	{
		nullBit = nullFieldsIndicator[i/CHAR_BIT] & (1 << (7 - (i%CHAR_BIT)));		//nullBitIndicatorSize in bytes, so multiply by 8 for bits
		if(!nullBit)
		{
			if(it->type == TypeVarChar)
			{
				int varcharLenInt;
				memcpy(&varcharLenInt, (char *)data + readOffset, sizeof(int));
//				if((unsigned int)varcharLenInt > it->length)
//				{
//					cout<<"Cannot insert varchar of length greater than defined in schema"<<endl;
//					return -1;
//				}
				short varcharLenShort = short(varcharLenInt);
				memcpy((char*)(insertData) + insertOffset, &varcharLenShort, sizeof(short));	//store length of varchar
				readOffset = readOffset + sizeof(int);
				insertOffset = insertOffset + sizeof(short);

				//check if length of string  = 0 i.e it is empty set offset to some value - say -1

				if(varcharLenInt == 0)
				{
					short offsetForEmptyVarchar = -1;
					memcpy((char*)(insertData) + insertOffset, &offsetForEmptyVarchar, sizeof(short));
				}
				else
				{
					void* varchar = malloc(varcharLenInt);
					memcpy(varchar, (char *)data + readOffset, varcharLenInt);
					readOffset = readOffset + varcharLenInt;
					offsetVarcharMap[insertOffset] = varchar;
					offsetVarcharLenMap[insertOffset] = varcharLenShort;
				}

				insertOffset += sizeof(short);	//space used to store an offset to the varchar , it is filled late when the offset id known or already filled with -1 if string is empty

			}
			else if (it->type == TypeInt)			//insert integers and real values directly, no offset needed
			{
				memcpy((char*)(insertData) + insertOffset, (char *)(data) + readOffset, sizeof(int));
				readOffset += sizeof(int);
				insertOffset += sizeof(int);

			}
			else if (it->type == TypeReal)
			{
				memcpy((char*)(insertData) + insertOffset, (char *)data + readOffset, sizeof(float));
				readOffset += sizeof(float);
				insertOffset += sizeof(float);
			}
		}

	}

	//iterate over all entries in the map created for varchars and fill the offsets and the varchars in the record
	for (map<short,void*>::const_iterator  it = offsetVarcharMap.begin() ; it != offsetVarcharMap.end(); ++it)
	{
		memcpy((char*)(insertData) + it->first, &insertOffset, sizeof(short));		//copies the offset address into the offset store location
		memcpy((char*)(insertData) + insertOffset, (char*)it->second, offsetVarcharLenMap[it->first]); //copies the data into the memory address
		insertOffset += offsetVarcharLenMap[it->first];
	}

	//Added to pad the record incase it is smaller than 4 bytes - defined in header as MIN_RECORD_SIZE

	if(insertOffset < MIN_RECORD_SIZE)
	{
		short paddedSpace = MIN_RECORD_SIZE - insertOffset;
		void* padCharacters = malloc(paddedSpace);
		memset(padCharacters, '\0', paddedSpace);
		memcpy((char*)(insertData) + insertOffset, padCharacters, paddedSpace);
		insertOffset = insertOffset + paddedSpace;
		free(padCharacters);
	}

	if(insertOffset > PAGE_SIZE - 5 * sizeof(short))		//record is bigger than the page
	{
		free(insertData);
		return -1;
	}

	int currentPage = fileHandle.getNumberOfPages() - 1;
	if(currentPage == -1)			//no pages in the file
	{
		insertRecordOnNewPage(fileHandle, insertData, insertOffset, rid);
	}
	else
	{
		if(insertRecordOnPage(fileHandle, insertData, insertOffset, currentPage, rid) != 0)		//if record not successfully inserted on current page
		{
			int i;
			for(i  = 0; i < currentPage; i++)
			{
				if(insertRecordOnPage(fileHandle, insertData, insertOffset, i, rid) == 0)
				{
					break;
				}
			}
			if(i == currentPage)			///we couldn't find any page to insert the record on
			{
				insertRecordOnNewPage(fileHandle, insertData, insertOffset, rid);
			}
		}
	}
	free(insertData);
	return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

	int pageNo = rid.pageNum;
	void *storedPage = malloc(PAGE_SIZE);
	fileHandle.readPage(pageNo, storedPage);
	short offsetToRecord;
	short lenOfRecord;
    memcpy(&offsetToRecord, (char*)storedPage + rid.slotNum, sizeof(short));
    memcpy(&lenOfRecord, (char*)storedPage + rid.slotNum + sizeof(short), sizeof(short));


	if(lenOfRecord == -4)
	{
		//reading tombstone
		short pageNum;
		memcpy(&pageNum, (char*)storedPage + offsetToRecord, sizeof(short));
		short slotNum;
		memcpy(&slotNum, (char*)storedPage + offsetToRecord+ sizeof(short), sizeof(short));

		RID rid2;
		rid2.pageNum = pageNum;
		rid2.slotNum = slotNum;
		readRecord(fileHandle,recordDescriptor,rid2,data);  //calling tombstone RID

		free(storedPage);
		return 0;
	}

	if(offsetToRecord == -1)
	{
		free(storedPage);
		return -1;
	}

    int readOffset = offsetToRecord + sizeof(char); //ignoring the first byte.
    int writeOffset = 0;

    bool nullBit = false;
	int nullBitIndicatorSize = ceil(float(recordDescriptor.size())/CHAR_BIT);

	unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullBitIndicatorSize);
	memcpy(nullFieldsIndicator, (char*)storedPage + readOffset, nullBitIndicatorSize);
	memcpy(data, nullFieldsIndicator, nullBitIndicatorSize);
	readOffset =  readOffset + nullBitIndicatorSize;
	writeOffset = writeOffset + nullBitIndicatorSize;

	int i = 0;
    for (vector<Attribute>::const_iterator  it = recordDescriptor.begin() ; it != recordDescriptor.end(); ++it,i++)
    {
    	nullBit = nullFieldsIndicator[i/CHAR_BIT] & (1 << (7 - (i%CHAR_BIT)));
		if(!nullBit)
		{
			if(it->type == TypeVarChar)
			{
				short varcharLen;
				memcpy(&varcharLen, (char *)storedPage + readOffset, sizeof(short));
				readOffset = readOffset + sizeof(short);
				int varcharLenInt = (int)varcharLen;			//convert into int to print

				short varOffset;
				memcpy(&varOffset, (char*)storedPage + readOffset, sizeof(short));
				readOffset = readOffset + sizeof(short);

				//Writing to memory
				memcpy((char*)data + writeOffset, &varcharLenInt, sizeof(int));
				writeOffset = writeOffset + sizeof(int);

				if(varcharLenInt != 0)
				{
					void* varchar = malloc(varcharLen);
					memcpy((char*)varchar, (char*)(storedPage) + offsetToRecord + varOffset, varcharLen);		//since varchar offset starts from beginning of record

					memcpy((char*)(data) + writeOffset, varchar, varcharLen);
					writeOffset = writeOffset + varcharLen;
				}
			}
			else if (it->type == TypeInt)
			{
				int number;
				memcpy(&number, (char *)storedPage + readOffset, sizeof(int));
				readOffset =  readOffset + sizeof(int);
				memcpy((char *)data + writeOffset, &number, sizeof(int));
				writeOffset =  writeOffset + sizeof(int);
			}
			else if (it->type == TypeReal)
			{
				float floatValue;
				memcpy(&floatValue, (char *)storedPage + readOffset, sizeof(float));
				readOffset += sizeof(float);
				memcpy((char *)data + writeOffset, &floatValue, sizeof(float));
				writeOffset =  writeOffset + sizeof(float);
			}
		}
	}

	free(storedPage);
	return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {

	int nullBitIndicatorSize = ceil(float(recordDescriptor.size())/CHAR_BIT);
	int i = 0;
	int offset = 0;
	bool nullBit = false;

	unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullBitIndicatorSize);
	memcpy(nullFieldsIndicator, (char *)data + offset, nullBitIndicatorSize);
	offset = offset + nullBitIndicatorSize;

	for (vector<Attribute>::const_iterator  it = recordDescriptor.begin() ; it != recordDescriptor.end(); ++it, i++)
	{
	    nullBit = nullFieldsIndicator[i/CHAR_BIT] & (1 << (7 - (i%CHAR_BIT)));		//nullBitIndicatorSize in bytes, so multiply by 8 for bits

		if(nullBit)
		{
			cout<<it->name<<":"<<"NULL"<<endl;
		}
		else
		{
			if(it->type == TypeVarChar)
			{
				cout<<it->name<<" : ";
				int varcharLen;
				memcpy(&varcharLen, (char *)data + offset, sizeof(int));
				offset = offset + sizeof(int);

				if(varcharLen != 0)
				{
					void* varchar = malloc(varcharLen);
					memcpy((char*)varchar, (char *)data + offset, varcharLen);
					offset += varcharLen;
					cout<<string((char*)varchar, varcharLen)<<endl;
					free(varchar);
				}
				else
				{
					cout<<string("", varcharLen)<<endl;
				}
			}
			else if (it->type == TypeInt)
			{
				cout<<it->name<<" : ";
				int number;
				memcpy(&number, (char *)data + offset, sizeof(int));
				cout<<number<<endl;
				offset += sizeof(int);
			}
			else if (it->type == TypeReal)
			{
				cout<<it->name<<" : ";
				float floatValue;
				memcpy(&floatValue, (char *)data + offset, sizeof(float));
				cout<<floatValue<<endl;
				offset += sizeof(float);
			}
		}
	}
	return 0;
}

RC RecordBasedFileManager :: deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
	void* pageData = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, pageData);
	short offset;
	short lengthOfRecord;
	short freeBytesStartAt;
	memcpy(&offset, (char*)pageData + rid.slotNum, sizeof(short));
	memcpy(&lengthOfRecord, (char*)pageData + rid.slotNum + sizeof(short), sizeof(short));
	memcpy(&freeBytesStartAt, (char*)pageData + PAGE_SIZE - 2*sizeof(short), sizeof(short));


	if(offset == -1)
	{
		return -1;
	}
	if(lengthOfRecord == -4)
	{
		//reading tombstone
		short pageNum;
		memcpy(&pageNum, (char*)pageData + offset, sizeof(short));
		short slotNum;
		memcpy(&slotNum, (char*)pageData + offset + sizeof(short), sizeof(short));
		RID rid2;
		rid2.pageNum = pageNum;
		rid2.slotNum = slotNum;
		RC deleteRecordResult = deleteRecord(fileHandle,recordDescriptor,rid2);  //calling tombstone RID

		short newOffset = -1;
		short newLengthOfRecord = 0;
		memcpy((char*)pageData + rid.slotNum, &newOffset, sizeof(short));
		memcpy((char*)pageData + rid.slotNum + sizeof(short), &newLengthOfRecord, sizeof(short));
		fileHandle.writePage(rid.pageNum, pageData);
		free(pageData);
		return deleteRecordResult;
	}

	int sizeOfDataAfterRecord = freeBytesStartAt - (offset + lengthOfRecord);

	short lengthOfDirectory;
	memcpy(&lengthOfDirectory, (char*)pageData + (PAGE_SIZE - 3*sizeof(short)), sizeof(short));

	short freeSpace;
	memcpy(&freeSpace, (char*)pageData + (PAGE_SIZE - sizeof(short)), sizeof(short));

	if(sizeOfDataAfterRecord != 0)
	{
		void* dataAfterRecord = malloc(sizeOfDataAfterRecord);
		memcpy(dataAfterRecord, (char*)pageData + offset + lengthOfRecord, sizeOfDataAfterRecord);
		memcpy((char*)pageData + offset, dataAfterRecord, sizeOfDataAfterRecord);	//delete the record by over writing

		//Now go through the directory and find slots that have an offset greater than the record just modified
		//We dont rely on the directory entries only after the deleted record's slot id, because we will reuse
		//Rids from deleted records later on.

		short temp  = 1;
		short tempDiff = PAGE_SIZE - 5* sizeof(short);	//this comes to the slot of the first record
		while(temp <= lengthOfDirectory)
		{
			short tempOffset;
			memcpy(&tempOffset, (char*)pageData + tempDiff, sizeof(short));
			if(tempOffset > offset)
			{
				tempOffset =  tempOffset - lengthOfRecord;
				memcpy((char*)pageData + tempDiff, &tempOffset, sizeof(short));
			}
			tempDiff = tempDiff - 2*sizeof(short);
			temp =  temp + 1;
		}

		free(dataAfterRecord);
	}

	freeSpace = freeSpace + lengthOfRecord;
	freeBytesStartAt = freeBytesStartAt - lengthOfRecord;

	memcpy((char*)pageData + (PAGE_SIZE - sizeof(short)), &freeSpace, sizeof(short));
	memcpy((char*)pageData + (PAGE_SIZE - 2*sizeof(short)), &freeBytesStartAt, sizeof(short));

	offset = -1;
	lengthOfRecord = 0;

	memcpy((char*)pageData + rid.slotNum, &offset, sizeof(short));
	memcpy((char*)pageData + rid.slotNum + sizeof(short), &lengthOfRecord, sizeof(short));


	fileHandle.writePage(rid.pageNum, pageData);

	free(pageData);

	return 0;
}


RC RecordBasedFileManager::sizeOfRecord(const void* data,const vector<Attribute> &recordDescriptor, void *insertData)
{
	short insertOffset = 0;

	map<short,void*> offsetVarcharMap;	// Offset  - Data Map where data is mainly varchars
	map<short,short> offsetVarcharLenMap;	// offset -Length(Varchar) map

	int nullBitIndicatorSize = ceil(float(recordDescriptor.size())/CHAR_BIT);
	short i = 0;
	short readOffset = 0;
	bool nullBit = false;

	char updateStatus = '0';
	memcpy(insertData, &updateStatus, sizeof(char));
	insertOffset = insertOffset + sizeof(char);
	//add first byte as 1

	unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullBitIndicatorSize);
	memcpy(nullFieldsIndicator, (char *)data, nullBitIndicatorSize);
	memcpy((char *)insertData + insertOffset, nullFieldsIndicator, nullBitIndicatorSize);
	insertOffset = insertOffset + nullBitIndicatorSize;
	readOffset = readOffset + nullBitIndicatorSize;

	for (vector<Attribute>::const_iterator  it = recordDescriptor.begin() ; it != recordDescriptor.end(); ++it, i++)
	{
		nullBit = nullFieldsIndicator[i/8] & (1 << (7-(i%8)));		//nullBitIndicatorSize in bytes, so multiply by 8 for bits
		if(!nullBit)
		{
			if(it->type == TypeVarChar)
			{
				int varcharLenInt;
				memcpy(&varcharLenInt, (char *)data + readOffset, sizeof(int));
				if((unsigned int)varcharLenInt > it->length)
				{
					return -1;
				}
				short varcharLenShort = short(varcharLenInt);
				memcpy((char*)(insertData) + insertOffset, &varcharLenShort, sizeof(short));	//store length of varchar
				readOffset = readOffset + sizeof(int);
				insertOffset = insertOffset + sizeof(short);

				//check if length of string  = 0 i.e it is empty, set offset to some value - say -1

				if(varcharLenInt == 0)
				{
					short offsetForEmptyVarchar = -1;
					memcpy((char*)(insertData) + insertOffset, &offsetForEmptyVarchar, sizeof(short));
				}
				else
				{
					void* varchar = malloc(varcharLenInt);
					memcpy(varchar, (char *)data + readOffset, varcharLenInt);
					readOffset = readOffset + varcharLenInt;
					offsetVarcharMap[insertOffset] = varchar;
					offsetVarcharLenMap[insertOffset] = varcharLenShort;
				}

				insertOffset += sizeof(short);	//space used to store an offset to the varchar , it is filled late when the offset id known or already filled with -1 if string is empty

			}
			else if (it->type == TypeInt)			//insert integers and real values directly, no offset needed
			{
				memcpy((char*)(insertData) + insertOffset, (char *)(data) + readOffset, sizeof(int));
				readOffset += sizeof(int);
				insertOffset += sizeof(int);

			}
			else if (it->type == TypeReal)
			{
				memcpy((char*)(insertData) + insertOffset, (char *)data + readOffset, sizeof(float));
				readOffset += sizeof(float);
				insertOffset += sizeof(float);
			}
		}

	}
	//iterate over all entries in the map created for varchars and fill the offsets and the varchars in the record
	for (map<short,void*>::const_iterator  it = offsetVarcharMap.begin() ; it != offsetVarcharMap.end(); ++it)
	{
		memcpy((char*)(insertData) + it->first, &insertOffset, sizeof(short));		//copies the offset address into the offset store location
		memcpy((char*)(insertData) + insertOffset, (char*)it->second, offsetVarcharLenMap[it->first]); //copies the data into the memory address
		insertOffset += offsetVarcharLenMap[it->first];
	}
	return insertOffset;
}


// Assume the RID does not change after an update
RC RecordBasedFileManager :: updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
	short pageNo = rid.pageNum;
	void *storedPage = malloc(PAGE_SIZE);
	void *insertData = malloc(PAGE_SIZE);

	fileHandle.readPage(pageNo, storedPage);
	short offsetToRecord;

	short lenOfRecord; // contains size of old record
	short freeBytesStartAt;
	short freeBytes;
	short lengthOfDirectory;

	memcpy(&lengthOfDirectory, (char*)storedPage + (PAGE_SIZE - 3*sizeof(short)), sizeof(short));
	memcpy(&freeBytesStartAt, (char*)storedPage + PAGE_SIZE - 2*sizeof(short), sizeof(short));
	memcpy(&offsetToRecord, (char*)storedPage + rid.slotNum, sizeof(short));

	memcpy(&lenOfRecord, (char*)storedPage + rid.slotNum + sizeof(short), sizeof(short)); //old record size
	memcpy(&freeBytes, (char*)storedPage + PAGE_SIZE - sizeof(short), sizeof(short));


    //updateFlag = true;

    short size = sizeOfRecord(data,recordDescriptor, insertData); //contain size of 'data' argument passed (new data to be updated)

	//updateFlag = false;


//    cout<<size<<endl;
//    cout<<lenOfRecord<<endl;


//    short first;
//    memcpy(&first, (char*)insertData, sizeof(short));
//    cout<<endl<<"Stop Already at update"<<first;
//    exit(EXIT_SUCCESS);




	if(size<=lenOfRecord) // if old data is larger
		{
		short mem_left_move_size = freeBytesStartAt - (offsetToRecord+ lenOfRecord);
		memcpy((char*)storedPage + offsetToRecord,insertData, size); //copying data
		memmove((char*)storedPage + offsetToRecord + size, (char*)storedPage +offsetToRecord +lenOfRecord ,mem_left_move_size);



            //updating freebytes

		freeBytes = freeBytes + (lenOfRecord - size);
		memcpy((char*)storedPage + PAGE_SIZE - sizeof(short), &freeBytes, sizeof(short));

		//updating freebytesstartat
		freeBytesStartAt = freeBytesStartAt - (lenOfRecord-size);
		memcpy((char*)storedPage + PAGE_SIZE - 2*sizeof(short), &freeBytesStartAt, sizeof(short));

		//updating size of record
		memcpy((char*)storedPage + rid.slotNum+sizeof(short),&size,sizeof(short));

//		updating offset values for each subsequent slots
		short temp  = 1;
		short offset = offsetToRecord;
		short tempDiff = PAGE_SIZE - 5* sizeof(short);	//this comes to the ptr of the first record
		while(temp <= lengthOfDirectory)
		{
			short tempOffset; //old offset
			memcpy(&tempOffset, (char*)storedPage + tempDiff, sizeof(short));//old offsets going from slot 1 to end
			if(tempOffset > offset)     // to make sure you update slots whose offset is greater than start of record
			{
				tempOffset =  tempOffset - (lenOfRecord- size);
				memcpy((char*)storedPage + tempDiff, &tempOffset, sizeof(short));
			}
			tempDiff = tempDiff - 2*sizeof(short); //moving to next slot
			temp =  temp + 1;
		}

		fileHandle.writePage(pageNo, storedPage);
	}
	else // if new data is larger
	{

//		cout<<"we are here";

		if(freeBytes>(size-lenOfRecord)) // if there is enough space in the page
		{
			short mem_right_move_size = freeBytesStartAt - (offsetToRecord+ lenOfRecord);
			memmove((char*)storedPage + offsetToRecord + size, (char*)storedPage +offsetToRecord +lenOfRecord ,mem_right_move_size);
			memcpy((char*)storedPage + offsetToRecord,insertData, size); //copying data

			char first;
			memcpy(&first,insertData,1);

			cout<<endl<<"InserData first "<<first;
			short ffset;
			memcpy(&ffset,(char *)storedPage+rid.slotNum, sizeof(short));
			memcpy(&first,(char *)storedPage+ffset,1);



			//updating freebytes
			freeBytes = freeBytes - (size-lenOfRecord);
			memcpy((char*)storedPage + PAGE_SIZE - sizeof(short), &freeBytes, sizeof(short));

			//updating freebytesstartat
			freeBytesStartAt = freeBytesStartAt + (size-lenOfRecord);
			memcpy((char*)storedPage + PAGE_SIZE - 2*sizeof(short), &freeBytesStartAt, sizeof(short));


			//updating size of record
			memcpy((char*)storedPage + rid.slotNum+sizeof(short),&size,sizeof(short));


			//updating offset values for each subsequent slots
			short temp  = 1;
			short offset = offsetToRecord;
			short tempDiff = PAGE_SIZE - 5* sizeof(short);	//this comes to the ptr of the first record
			while(temp <= lengthOfDirectory)
			{
				short tempOffset; //old offset
				memcpy(&tempOffset, (char*)storedPage + tempDiff, sizeof(short));//old offsets going from slot 1 to end
				if(tempOffset > offset)     // to make sure you update slots whose offset is greater than start of record
				{
					tempOffset =  tempOffset + (size - lenOfRecord);
					memcpy((char*)storedPage + tempDiff, &tempOffset, sizeof(short));
				}
				tempDiff = tempDiff - 2*sizeof(short); //moving to next slot
				temp =  temp + 1;
			}
			fileHandle.writePage(pageNo, storedPage);

		}
		else
		{ //case when there is no space in page

			RID rid2;

			// call insert get RID back
            updateFlag = true;
			insertRecord(fileHandle, recordDescriptor, data, rid2);
            updateFlag = false;
			short pageNum = rid2.pageNum;
			short slotNum = rid2.slotNum;
			short mem_left_move_size = freeBytesStartAt - (offsetToRecord+ lenOfRecord);

			//inserting tombstone

			//	 -----------------------
			//	|   Page No|  Slot No  |
			//	|__________|___________|

			memcpy((char*)storedPage + offsetToRecord, &pageNum, sizeof(short)); //adding page number
			memcpy((char*)storedPage + offsetToRecord+sizeof(short), &slotNum, sizeof(short)); //adding slot number

			// moving data to cover up empty place.
			memmove((char*)storedPage + offsetToRecord + 4, (char*)storedPage +offsetToRecord +lenOfRecord ,mem_left_move_size);

			//updating freebytes
			freeBytes = freeBytes + (lenOfRecord - 4);
			memcpy((char*)storedPage + PAGE_SIZE - sizeof(short), &freeBytes, sizeof(short));

			//updating freebytesstartat
			freeBytesStartAt = freeBytesStartAt - (lenOfRecord-4);
			memcpy((char*)storedPage + PAGE_SIZE - 2*sizeof(short), &freeBytesStartAt, sizeof(short));


			//updating size of record
			short sizeofRecord = -4;
			memcpy((char*)storedPage + rid.slotNum+sizeof(short),&sizeofRecord,sizeof(short));

			// updating the offset to each slot

			short temp  = 1;
			short offset = offsetToRecord;
			short tempDiff = PAGE_SIZE - 5* sizeof(short);	//this comes to the ptr of the first record
			while(temp <= lengthOfDirectory)
			{
				short tempOffset; //old offset
				memcpy(&tempOffset, (char*)storedPage + tempDiff, sizeof(short));//old offsets going from slot 1 to end
				if(tempOffset > offset)     // to make sure you update slots whose offset is greater than start of record
				{
					tempOffset =  tempOffset - (lenOfRecord-4);
					memcpy((char*)storedPage + tempDiff, &tempOffset, sizeof(short));
				}
				tempDiff = tempDiff - 2*sizeof(short); //moving to next slot
				temp =  temp + 1;
			}


			fileHandle.writePage(rid.pageNum, storedPage);
		}
	}
	return 0;
}


RC RecordBasedFileManager :: readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data)
{
	int i  = 0;
	bool nullBit = false;

	void* storedPage = malloc(PAGE_SIZE);

	RC readResult = fileHandle.readPage(rid.pageNum, storedPage);
	if(readResult != 0)
	{
		return -1;
	}
	short offset;
	memcpy(&offset, (char*) storedPage + rid.slotNum, sizeof(short));
    short lengthOfRecord;
	memcpy(&lengthOfRecord, (char*)storedPage + rid.slotNum + sizeof(short), sizeof(short));
	if(offset == -1)			//record was deleted
	{
		return -1;
	}

	int nullBitIndicatorSize = ceil(float(recordDescriptor.size())/CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullBitIndicatorSize);
	memcpy(nullFieldsIndicator, (char*)storedPage + offset +  sizeof(char), nullBitIndicatorSize);

	short readOffset = 0;
	short writeOffset = 0;
	unsigned char *writeNullBitIndicator = (unsigned char *) malloc(1);
	memset(writeNullBitIndicator, 0, sizeof(char));

	for (vector<Attribute>::const_iterator  it = recordDescriptor.begin() ; it != recordDescriptor.end(); ++it, i++)
	{
		nullBit = nullFieldsIndicator[i/CHAR_BIT] & (1 << (7 - (i%CHAR_BIT)));		//nullBitIndicatorSize in bytes, so multiply by 8 for bits
		if(!nullBit)
		{
			if(attributeName.compare(it->name) == 0)
			{
				int attributePos  = sizeof(char) + nullBitIndicatorSize + readOffset;

				memcpy(data, writeNullBitIndicator, sizeof(char));
				writeOffset = writeOffset + 1;

				if(it->type == TypeInt)
				{
//					int attribute;
//					memcpy(&attribute, (char*)storedPage + offset + attributePos, sizeof(int));
					memcpy((char*)data + writeOffset, (char*)storedPage + offset + attributePos, sizeof(int));
					writeOffset = writeOffset + sizeof(int);
//					cout<<"Attribute Read : "<<attribute<<endl;
					break;
				}
				else if(it->type == TypeReal)
				{
//					float attribute;
//					memcpy(&attribute, (char*)storedPage + offset + attributePos, sizeof(float));
					memcpy((char*)data + writeOffset, (char*)storedPage + offset + attributePos, sizeof(float));
					writeOffset = writeOffset + sizeof(float);
//					cout<<"Attribute Read : "<<attribute<<endl;
					break;
				}
				else
				{
					void* value = malloc(it->length);
					short varcharOffset;
					short varcharLength;
					memcpy(&varcharLength, (char*)storedPage + offset + attributePos, sizeof(short));
					memcpy(&varcharOffset, (char*)storedPage + offset + attributePos + sizeof(short), sizeof(short));
					memcpy(value, (char*)storedPage + offset + varcharOffset, varcharLength);
					int varcharLenInt = (int)varcharLength;
					memcpy((char*)data + writeOffset, &varcharLenInt, sizeof(int));
					writeOffset = writeOffset + sizeof(int);
//					cout<<"Attribute Read : "<<string((char*)value, varcharLength)<<endl;
					memcpy((char*)data + writeOffset, (char*)storedPage + offset + varcharOffset, varcharLenInt);
					writeOffset = writeOffset + varcharLength;
					break;
				}
			}
			readOffset = readOffset +  sizeof(int);		//do not hard code 4, hence int
		}

	}
	//means that the atttribute to be read  is null for that record
	if(writeOffset == 0)
	{
		writeNullBitIndicator[0] = 128;
		memcpy(data, writeNullBitIndicator, sizeof(char));
	}
	free(storedPage);
	return 0;
}

RC RBFM_ScanIterator :: prepareScanData(void* data, const RID &rid)
{
	if(recordDescriptor.size() == attributeNames.size())
	{
		RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
		rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
		return 0;
	}
	int i  = 0;
	bool nullBit = false;

	void* storedPage = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, storedPage);
	short offset;
	memcpy(&offset, (char*) storedPage + rid.slotNum, sizeof(short));

	int nullBitIndicatorSize = ceil(float(recordDescriptor.size())/CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullBitIndicatorSize);
	memcpy(nullFieldsIndicator, (char*)storedPage + offset + sizeof(char), nullBitIndicatorSize);

	int returnNullBitIndicatorSize = ceil(float(attributeNames.size())/CHAR_BIT);
	short readOffset = nullBitIndicatorSize + sizeof(char);
	short writeOffset = 0;
	unsigned char *writeNullBitIndicator = (unsigned char *) malloc(returnNullBitIndicatorSize);
	memset(writeNullBitIndicator, 0, returnNullBitIndicatorSize);

	memcpy(data, writeNullBitIndicator, returnNullBitIndicatorSize);
	writeOffset = writeOffset + returnNullBitIndicatorSize;

	int returnFieldsParsed = 0;

	for (vector<Attribute>::const_iterator  it = recordDescriptor.begin() ; it != recordDescriptor.end(); ++it, i++)
	{
		nullBit = nullFieldsIndicator[i/CHAR_BIT] & (1 << (7 - (i%CHAR_BIT)));		//nullBitIndicatorSize in bytes, so multiply by 8 for bits

		if(std::find(attributeNames.begin(), attributeNames.end(), it->name) == attributeNames.end())	//couldn't find attr
		{
			if(!nullBit)
			{
				readOffset = readOffset + sizeof(int);
			}
			continue;
		}
		returnFieldsParsed = returnFieldsParsed + 1;		//we found a field whose value has to be returned
		if(!nullBit)
		{
			if(it->type == TypeInt)
			{
				int attribute;
				memcpy(&attribute, (char*)storedPage + offset + readOffset, sizeof(int));
				memcpy((char*)data + writeOffset, (char*)storedPage + offset + readOffset, sizeof(int));
				writeOffset = writeOffset + sizeof(int);
//				cout<<"Attribute Read : "<<attribute<<endl;
			}
			else if(it->type == TypeReal)
			{
				float attribute;
				memcpy(&attribute, (char*)storedPage + offset + readOffset, sizeof(float));
				memcpy((char*)data + writeOffset, (char*)storedPage + offset + readOffset, sizeof(float));
				writeOffset = writeOffset + sizeof(float);
//				cout<<"Attribute Read : "<<attribute<<endl;
			}
			else
			{
				void* value = malloc(it->length);
				short varcharOffset;
				short varcharLength;
				memcpy(&varcharLength, (char*)storedPage + offset + readOffset, sizeof(short));
				memcpy(&varcharOffset, (char*)storedPage + offset + readOffset + sizeof(short), sizeof(short));
				memcpy(value, (char*)storedPage + offset + varcharOffset, varcharLength);
				int varcharLenInt = (int)varcharLength;
				memcpy((char*)data + writeOffset, &varcharLenInt, sizeof(int));
				writeOffset = writeOffset + sizeof(int);
//				cout<<"Attribute Read : "<<string((char*)value, varcharLength)<<endl;
				memcpy((char*)data + writeOffset, (char*)storedPage + offset + varcharOffset, varcharLenInt);
				writeOffset = writeOffset + varcharLength;
			}
			readOffset = readOffset +  sizeof(int);		//do not hard code 4, hence int
		}
		else
		{
			writeNullBitIndicator[returnFieldsParsed/CHAR_BIT] = writeNullBitIndicator[returnFieldsParsed/CHAR_BIT] | 1 << (CHAR_BIT - returnFieldsParsed);		//set numm bit indicator for that field
		}
	}
	memcpy(data, writeNullBitIndicator, returnNullBitIndicatorSize);
	return 0;
}

// Scan returns an iterator to allow the caller to go through the results one by one.
RC RecordBasedFileManager :: scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator)
{
	rbfm_ScanIterator.fileHandle = fileHandle;
	rbfm_ScanIterator.recordDescriptor = recordDescriptor;
	rbfm_ScanIterator.conditionAttribute = conditionAttribute;
	rbfm_ScanIterator.compOp = compOp;
	rbfm_ScanIterator.attributeNames = attributeNames;
	rbfm_ScanIterator.value = value;
	rbfm_ScanIterator.storedRid.pageNum = 0;
	rbfm_ScanIterator.storedRid.slotNum = PAGE_SIZE - 5*sizeof(short);
	rbfm_ScanIterator.currentSlot = 0;
	rbfm_ScanIterator.counter = 0;
	return 0;
}

RC RBFM_ScanIterator :: close()
{
	return 0;
}

RC RBFM_ScanIterator :: getNextRecord(RID &rid, void* data)
{
	for(unsigned int i = storedRid.pageNum; i < fileHandle.getNumberOfPages(); i++)
	{
		void *pageData = malloc(PAGE_SIZE);
		fileHandle.readPage(i, pageData);
		short totalSlots;
		memcpy(&totalSlots, (char*) pageData + (PAGE_SIZE - 3 * sizeof(short)), sizeof(short));
		for(int j = currentSlot; j < totalSlots; j++) // going through each slot
		{
			short offsetToRecord;
			short lenOfRecord;
			memcpy(&offsetToRecord, (char*) pageData + storedRid.slotNum, sizeof(short));
			memcpy(&lenOfRecord, (char*) pageData + storedRid.slotNum + sizeof(short), sizeof(short));
			if(offsetToRecord == -1)			//record was deleted, so you ignore that slot and continue scanning
			{
				storedRid.slotNum = storedRid.slotNum - 2*sizeof(short);
				continue;
			}

            //here read and continue if first bit is 1;

            char firstByte;
            memcpy(&firstByte, (char *)pageData+offsetToRecord, sizeof(char));
            if(firstByte=='1')
            {
                storedRid.slotNum = storedRid.slotNum - 2*sizeof(short);
                continue;
            }



//			std::ostringstream checkPageStream;
//			std::ostringstream checkSlotStream;
//			checkSlotStream << storedRid.slotNum;
//			checkPageStream << storedRid.pageNum;
//			string checkRID = checkPageStream.str() + ":" + checkSlotStream.str();
//			if(traversedUpdatedRids.find(checkRID) != traversedUpdatedRids.end()) // RID was traversed earlier
//			{
//				counter++;
//				storedRid.slotNum = storedRid.slotNum - 2*sizeof(short);
//				traversedUpdatedRids.erase(checkRID);
//				continue;
//			}

			RID tempRid;
			tempRid.slotNum = storedRid.slotNum;
			tempRid.pageNum = storedRid.pageNum;


			if(lenOfRecord == -4)
			{
				//reading tombstone
				short newPageNum;
				memcpy(&newPageNum, (char*)pageData + offsetToRecord, sizeof(short));
				short newSlotNum;
				memcpy(&newSlotNum, (char*)pageData + offsetToRecord+ sizeof(short), sizeof(short));

				tempRid.pageNum = newPageNum;
				tempRid.slotNum = newSlotNum;
//				std::ostringstream pageStream;
//				std::ostringstream slotStream;
//				slotStream << tempRid.slotNum;
//				pageStream << tempRid.pageNum;
//				traversedUpdatedRids.insert(pageStream.str() + ":" + slotStream.str());
			}

			if(conditionAttribute == "")
			{ //
				prepareScanData(data, tempRid);
				rid.pageNum = storedRid.pageNum;
				rid.slotNum = storedRid.slotNum;
				storedRid.slotNum = storedRid.slotNum - 2*sizeof(short);
				currentSlot = j + 1;
				free(pageData);
				return 0;
			}
			void* requiredData = malloc(100);
			RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

			rbfm->readAttribute(fileHandle, recordDescriptor, tempRid, conditionAttribute, requiredData);

			int nullBitIndicatorSize = 1;
			unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullBitIndicatorSize);
			memcpy(nullFieldsIndicator, (char*)requiredData, nullBitIndicatorSize);

			bool nullBit = nullFieldsIndicator[0] & (1 << 7);		//nullBitIndicatorSize in bytes, so multiply by 8 for bits

			if(nullBit)
			{
				storedRid.slotNum = storedRid.slotNum - 2*sizeof(short);
				free(requiredData);
				continue;
			}

			bool satisfied = false;
			for (vector<Attribute>::const_iterator  it = recordDescriptor.begin() ; it != recordDescriptor.end(); ++it)
			{
				if(conditionAttribute.compare(it->name) == 0)
				{
					if(it->type == TypeInt)
					{
						int intValue;
						memcpy(&intValue, (char*) requiredData + sizeof(char), sizeof(int));

						int searchValue;
						memcpy(&searchValue, value, sizeof(int));
//						searchValue = 5;
						switch(compOp)
						 {
						 	 case EQ_OP:
								 if(intValue == searchValue)
								 {
									 satisfied = true;
								 }
								 break;

						 	 case LT_OP:
								 if(intValue < searchValue)
								 {
									 satisfied = true;
								 }
								 break;
						 	 case LE_OP:
						 		 if(intValue <= searchValue)
								 {
									 satisfied = true;
								 }
						 		break;
						 	case GT_OP:
								 if(intValue > searchValue)
								 {
									 satisfied = true;
								 }
								break;
						 	case GE_OP:
								 if(intValue >= searchValue)
								 {
									 satisfied = true;
								 }
								break;
						 	case NE_OP:
								 if(intValue != searchValue)
								 {
									 satisfied = true;
								 }
								break;
						 	case NO_OP:
								 satisfied = true;
								break;
						 }   //switch ends here
					}	// end of if for typeInt
					else if(it->type == TypeReal)
					{
						float floatValue;
						memcpy(&floatValue, (char*) requiredData + sizeof(char), sizeof(float));

						float searchValue;
						memcpy(&searchValue, value, sizeof(float));

						 switch(compOp)
						 {
						 	 case EQ_OP:
								 if(floatValue == searchValue)
								 {
									 satisfied = true;
								 }
								 break;

						 	 case LT_OP:
								 if(floatValue < searchValue)
								 {
									 satisfied = true;
								 }
								 break;
						 	 case LE_OP:
						 		 if(floatValue <= searchValue)
								 {
									 satisfied = true;
								 }
						 		break;
						 	case GT_OP:
								 if(floatValue > searchValue)
								 {
									 satisfied = true;
								 }
								break;
						 	case GE_OP:
								 if(floatValue >= searchValue)
								 {
									 satisfied = true;
								 }
								break;
						 	case NE_OP:
								 if(floatValue != searchValue)
								 {
									 satisfied = true;
								 }
								break;
						 	case NO_OP:
								 satisfied = true;
								break;
						 }
					}
					else
					{
						int varcharLength;
						memcpy(&varcharLength, (char*) requiredData + sizeof(char), sizeof(int));
						void* varchar = malloc(varcharLength);
						memcpy(varchar, (char*) requiredData + sizeof(char) + sizeof(int), varcharLength);
						string varcharValue = string((char*)varchar, varcharLength);

						int searchValueLength;
						memcpy(&searchValueLength, (char*)value, sizeof(int));
						void* searchVarchar = malloc(searchValueLength);
						memcpy(searchVarchar, (char*) value + sizeof(int), searchValueLength);
						string searchValue = string((char*)searchVarchar, searchValueLength);

//						cout<<"Varchar retrieved : "<<varcharValue<<endl;
//						cout<<"Search Value : "<<searchValue<<endl;
						switch(compOp)
						 {
							 case EQ_OP:
								 if(varcharValue.compare(searchValue) == 0)
								 {
									 satisfied = true;
								 }
								 break;

							 case LT_OP:
								 if(varcharValue.compare(searchValue) < 0)
								 {
									 satisfied = true;
								 }
								 break;
							 case LE_OP:
								 if(varcharValue.compare(searchValue) <= 0 )
								 {
									 satisfied = true;
								 }
								break;
							case GT_OP:
								 if(varcharValue.compare(searchValue) > 0)
								 {
									 satisfied = true;
								 }
								break;
							case GE_OP:
								 if(varcharValue.compare(searchValue) >= 0)
								 {
									 satisfied = true;
								 }
								break;
							case NE_OP:
								 if(varcharValue.compare(searchValue) != 0)
								 {
									 satisfied = true;
								 }
								break;
							case NO_OP:
								 satisfied = true;
								break;
						 }
						free(varchar);
						free(searchVarchar);
					}

					if(!satisfied)
					{
						break;
					}
					else
					{
						prepareScanData(data, tempRid);
						rid.pageNum = storedRid.pageNum;
						rid.slotNum = storedRid.slotNum;
						storedRid.slotNum = storedRid.slotNum - 2*sizeof(short);
						currentSlot = j + 1;
						free(requiredData);
						free(pageData);
						return 0;
					}
				} //if to compare attribute name with iterator
			} //for record descriptor iteration

			storedRid.slotNum = storedRid.slotNum - 2*sizeof(short);
			free(requiredData);
		}//slot for loop
		currentSlot = 0;
		storedRid.slotNum = PAGE_SIZE - 5*sizeof(short);
		storedRid.pageNum = i + 1;
		free(pageData);
	}// page for loop


	return RBFM_EOF;

}


