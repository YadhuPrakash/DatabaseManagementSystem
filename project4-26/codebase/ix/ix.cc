
#include "ix.h"
#include "../rbf/rbfm.h"

IndexManager* IndexManager::_index_manager = 0;
PagedFileManager* pagedFileManager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
	pagedFileManager = PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
	return pagedFileManager->createFile(fileName);
}

RC IndexManager::destroyFile(const string &fileName)
{
	return pagedFileManager->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
	ixfileHandle.fileName = fileName;
	return pagedFileManager->openFile(fileName, ixfileHandle.fileHandle);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	if(checkIfValidFileHandle(ixfileHandle) != 0)			//file was not destroyed earlier
	{
		return -1;
	}
	return pagedFileManager->closeFile(ixfileHandle.fileHandle);
}

//int IndexManager::getIntKey(void* key)
//{
//	int intKey;
//	memcpy(&intKey, key, sizeof(int));
//	return intKey;
//}
//
//float IndexManager::getFloatKey(void* key)
//{
//	float floatKey;
//	memcpy(&floatKey, key, sizeof(float));
//	return floatKey;
//}
//
//void* IndexManager::getVarcharKey(void* key, short &varcharLen)
//{
//	memcpy(&varcharLen, key, sizeof(int));
//
//	void* varchar = malloc(varcharLen);
//	memcpy(varchar, (char*)key + sizeof(int), varcharLen);
//	return varchar;
//}



RC IndexManager::createTree(IXFileHandle &ixfileHandle, const Attribute &attribute, const void* key, const RID &rid)
{
	void* rootNode = malloc(PAGE_SIZE);
	char typeOfNode = '1';								// 1 because root is first a leaf when number of records < no of keys permissible in pg
	memcpy(rootNode, &typeOfNode, sizeof(char));
	int writeOffset = sizeof(char);

	short noOfKeys = 1;
	memcpy((char*)rootNode + writeOffset, &noOfKeys, sizeof(short));
	writeOffset = writeOffset + sizeof(short);

	short freeBytesLeft = 0;						//will be set later after we insert the key
	int freeBytesLeftPos = writeOffset;
	writeOffset = writeOffset + sizeof(short);

	int pointerToNextPage = -1;
	memcpy((char*)rootNode + writeOffset, &pointerToNextPage, sizeof(int));
	writeOffset = writeOffset + sizeof(int);

	if(attribute.type == TypeInt)
	{
		int intKey;
		memcpy(&intKey, key, sizeof(int));
		memcpy((char*)rootNode + writeOffset, &intKey, sizeof(int));
		writeOffset =  writeOffset + sizeof(int);
	}
	else if (attribute.type == TypeReal)
	{
		float floatKey;
		memcpy(&floatKey, key, sizeof(float));
		memcpy((char*)rootNode + writeOffset, &floatKey, sizeof(float));
		writeOffset =  writeOffset + sizeof(float);
	}
	else		//type varchar
	{
		int varcharLen;
		memcpy(&varcharLen, key, sizeof(int));
		short varcharLenShort = (short)varcharLen;
		void* varchar = malloc(varcharLen);
		memcpy(varchar, (char*)key + sizeof(int), varcharLen);
		memcpy((char*)rootNode + writeOffset, &varcharLenShort, sizeof(short));
		writeOffset = writeOffset + sizeof(short);
		memcpy((char*)rootNode + writeOffset, varchar, varcharLen);
		writeOffset = writeOffset + varcharLen;
	}

	short noOfRids = 1;
	memcpy((char*)rootNode + writeOffset, &noOfRids, sizeof(short));
	writeOffset = writeOffset + sizeof(short);

	memcpy((char*)rootNode + writeOffset, &rid.pageNum, sizeof(int));
	writeOffset = writeOffset + sizeof(int);

	memcpy((char*)rootNode + writeOffset, &rid.slotNum, sizeof(int));
	writeOffset = writeOffset + sizeof(int);

	freeBytesLeft = PAGE_SIZE - writeOffset;
	//cout<<"Inside create tree : Free bytes left : "<<freeBytesLeft<<endl;
	//cout<<"Write offset for free bytes left : "<<freeBytesLeftPos<<endl;
	memcpy((char*)rootNode + freeBytesLeftPos, &freeBytesLeft, sizeof(short));

	RC insertResult = ixfileHandle.fileHandle.appendPage(rootNode);
	free(rootNode);

	void* checker = malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(0, checker);

	char typeOfPage = '1';
	int readOffset = 0;
	getCommonMetaData(checker, typeOfPage, noOfKeys, freeBytesLeft, readOffset);
	int nextPageId;
	memcpy(&nextPageId, (char*)checker + readOffset, sizeof(int));
	readOffset = readOffset + sizeof(int);
	// short len;
	// memcpy(&len, (char*)checker + readOffset, sizeof(short));
	// readOffset = readOffset + sizeof(short);
	// void* checkerVarchar = malloc(len);
	// memcpy(checkerVarchar,(char*)checker + readOffset, len);
	// cout<<"NO OF KEYS : "<<noOfKeys<<endl;
	// cout<<"KEY :"<<string((char*)checkerVarchar, len)<<endl;
	// cout<<"KEY LEN : "<<len<<endl;
	// float check;
	// memcpy(&check, (char*)checker + readOffset, sizeof(float));
	// cout<<"KEY : "<<check<<endl;
	// readOffset = readOffset+ sizeof(float);
	// short checkNoOfRids;
	// memcpy(&checkNoOfRids, (char*)checker + readOffset, sizeof(short));
	// readOffset +=sizeof(short);
	// cout<<"NO OF RIDS : "<<checkNoOfRids<<endl;
	// int slotNum;
	// memcpy(&slotNum, (char*)checker + readOffset + sizeof(int), sizeof(int));
	// cout<<"SLOT NUM  : "<<slotNum<<endl;


	return insertResult;
}

void IndexManager::insertNewKeyVarcharAtOffset(void* node, short &noOfKeys, void *varcharKey, short varcharLength, int &writeOffset, const RID &rid)
{
	noOfKeys = noOfKeys + 1;
	memcpy((char*)node + sizeof(char), &noOfKeys, sizeof(short));		//modify no of keys

	memcpy((char*)node + writeOffset, &varcharLength, sizeof(short));
	writeOffset = writeOffset + sizeof(short);

	memcpy((char*)node + writeOffset, varcharKey, varcharLength);
	writeOffset = writeOffset + varcharLength;

	int newNoOfRids = 1;
	memcpy((char*)node + writeOffset, &newNoOfRids, sizeof(short));
	writeOffset = writeOffset + sizeof(short);

	memcpy((char*)node + writeOffset, &rid.pageNum, sizeof(int));
	writeOffset = writeOffset + sizeof(int);

	memcpy((char*)node + writeOffset, &rid.slotNum, sizeof(int));
	writeOffset = writeOffset + sizeof(int);
}

void IndexManager::insertNewKeyFloatAtOffset(void* node, short &noOfKeys, float &floatKey, int &writeOffset, const RID &rid)
{
	noOfKeys = noOfKeys + 1;
	memcpy((char*)node + sizeof(char), &noOfKeys, sizeof(short));		//modify no of keys

	memcpy((char*)node + writeOffset, &floatKey, sizeof(float));
	writeOffset = writeOffset + sizeof(float);

	int newNoOfRids = 1;
	memcpy((char*)node + writeOffset, &newNoOfRids, sizeof(short));
	writeOffset = writeOffset + sizeof(short);

	memcpy((char*)node + writeOffset, &rid.pageNum, sizeof(int));
	writeOffset = writeOffset + sizeof(int);

	memcpy((char*)node + writeOffset, &rid.slotNum, sizeof(int));
	writeOffset = writeOffset + sizeof(int);
}


void IndexManager::insertNewKeyIntAtOffset(void* node, short &noOfKeys, int &intKey, int &writeOffset, const RID &rid)
{
	noOfKeys = noOfKeys + 1;
	memcpy((char*)node + sizeof(char), &noOfKeys, sizeof(short));		//modify no of keys

	memcpy((char*)node + writeOffset, &intKey, sizeof(int));
	writeOffset = writeOffset + sizeof(int);

	int newNoOfRids = 1;
	memcpy((char*)node + writeOffset, &newNoOfRids, sizeof(short));
	writeOffset = writeOffset + sizeof(short);

	memcpy((char*)node + writeOffset, &rid.pageNum, sizeof(int));
	writeOffset = writeOffset + sizeof(int);

	memcpy((char*)node + writeOffset, &rid.slotNum, sizeof(int));
	writeOffset = writeOffset + sizeof(int);
}


void IndexManager::fillCommonMetaData(void* newNode, int &writeOffset, char &typeOfPage)
{
	memcpy(newNode, &typeOfPage, sizeof(char));
	writeOffset =  writeOffset + sizeof(char);

	short noOfKeys = 0;
	memcpy((char*)newNode + writeOffset, &noOfKeys, sizeof(short));
	writeOffset = writeOffset + sizeof(short);

	short freeBytesLeft = PAGE_SIZE - sizeof(char) - 2*sizeof(short);
	memcpy((char*)newNode + writeOffset, &freeBytesLeft, sizeof(short));
	writeOffset = writeOffset + sizeof(short);
}

void IndexManager::checkIfNewKeyInt(void* node, char typeOfPage, int intKey, int &readOffset, short freeBytesStartAt, short &noOfKeys, bool &isNewKey, short &splitKey, short &keyCounter)
{
	if(typeOfPage == '1')
	{
		short noOfRids;
		int currentKey;
		while(readOffset < freeBytesStartAt)
		{
			memcpy(&currentKey, (char*)node + readOffset, sizeof(int));
			//cout<<"Current Key : "<<currentKey<<endl;
			if(intKey < currentKey)
			{
				isNewKey = true;
				break;
			}

			if(intKey  == currentKey)
			{
				//int to finish key read and short to finish noOfRids reading
				readOffset = readOffset + sizeof(int) + sizeof(short);		//point to the place where RIDS start for the currentKey
				break;
			}
			//go to the next key
			keyCounter = keyCounter + 1;
			// cout<<"KeyCounter :"<<keyCounter<<endl;
			readOffset = readOffset + sizeof(int);
			memcpy(&noOfRids, (char*)node + readOffset, sizeof(short));
			// cout<<"No Of rids :"<<noOfRids<<endl;
			readOffset = readOffset + sizeof(short) + noOfRids * 2 * sizeof(int);	//each RID is always 8 bytes
		}

		if(readOffset >= freeBytesStartAt)
		{
			isNewKey = true;
		}

		if(isNewKey)
		{
			noOfKeys = noOfKeys + 1;
		}

		splitKey = floor(noOfKeys/2);
	}
	else		//find position for a key in the index leaf when it's child was split
	{
		int currentKey;
		// cout<<"Key to insert : "<<intKey<<endl;

		readOffset = readOffset + sizeof(int); 		//skip the pageID

		while(readOffset < freeBytesStartAt)
		{
			memcpy(&currentKey, (char*)node + readOffset, sizeof(int));
			// cout<<"Current Key : "<<currentKey<<endl;
			if(intKey < currentKey)
			{
				isNewKey = true;
				break;
			}
			//go to the next key
			keyCounter = keyCounter + 1;
			// cout<<"KeyCounter :"<<keyCounter<<endl;
			readOffset = readOffset + sizeof(int);		//read the key
			// int checkPgId;
			// memcpy(&checkPgId, (char*)node + readOffset, sizeof(int));
			// cout<<"Page id : "<<checkPgId<<endl;
			readOffset = readOffset + sizeof(int); 		//skip the pageId
		}

		if(readOffset >= freeBytesStartAt)
		{
			isNewKey = true;
		}

		if(isNewKey)
		{
			noOfKeys = noOfKeys + 1;
		}

		splitKey = floor(noOfKeys/2);
	}
}


void IndexManager::checkIfNewKeyVarchar(void* node, char typeOfPage, void* varcharPtr, int varcharLength, int &readOffset, short freeBytesStartAt, short &noOfKeys, bool &isNewKey, short &splitKey, short &keyCounter)
{
	string varcharKey = string((char*)varcharPtr, varcharLength);

	// cout<<"VARCHAR KEY  : "<<varcharKey<<endl;
	if(typeOfPage == '1')
	{
		short noOfRids;
		string currentKey;

		while(readOffset < freeBytesStartAt)
		{
			short currentKeyLength;
			memcpy(&currentKeyLength, (char*)node + readOffset, sizeof(short));

			void* currentKeyVarchar = malloc(currentKeyLength);
			memcpy(currentKeyVarchar, (char*)node + readOffset + sizeof(short), currentKeyLength);

			string currentKey = string((char*)currentKeyVarchar, currentKeyLength);

			if(varcharKey.compare(currentKey) < 0)
			{
				isNewKey = true;
				break;
			}

			if(varcharKey.compare(currentKey) == 0)
			{
				//finish key read and short to finish noOfRids reading
				readOffset = readOffset + sizeof(short) + currentKeyLength + sizeof(short);		//point to the place where RIDS start for the currentKey
				break;
			}
			//go to the next key
			keyCounter = keyCounter + 1;
			// cout<<"KeyCounter :"<<keyCounter<<endl;
			readOffset = readOffset + sizeof(short) + currentKeyLength;
			memcpy(&noOfRids, (char*)node + readOffset, sizeof(short));
			// cout<<"No Of rids :"<<noOfRids<<endl;
			readOffset = readOffset + sizeof(short) + noOfRids * 2 * sizeof(int);	//each RID is always 8 bytes
		}

		if(readOffset >= freeBytesStartAt)
		{
			isNewKey = true;
		}

		if(isNewKey)
		{
			noOfKeys = noOfKeys + 1;
		}

		splitKey = floor(noOfKeys/2);
	}
	else		//find position for a key in the index leaf when it's child was split
	{
		string currentKey;
		// cout<<"Key to insert : "<<intKey<<endl;

		readOffset = readOffset + sizeof(int); 		//skip the pageID

		while(readOffset < freeBytesStartAt)
		{
			short currentKeyLength;
			memcpy(&currentKeyLength, (char*)node + readOffset, sizeof(short));

			void* currentKeyVarchar = malloc(currentKeyLength);
			memcpy(currentKeyVarchar, (char*)node + readOffset + sizeof(short), currentKeyLength);

			string currentKey = string((char*)currentKeyVarchar, currentKeyLength);

			if(varcharKey.compare(currentKey) < 0)
			{
				isNewKey = true;
				break;
			}
			//go to the next key
			keyCounter = keyCounter + 1;
			// cout<<"KeyCounter :"<<keyCounter<<endl;
			readOffset = readOffset + sizeof(short) + currentKeyLength;		//read the key
			// int checkPgId;
			// memcpy(&checkPgId, (char*)node + readOffset, sizeof(int));
			// cout<<"Page id : "<<checkPgId<<endl;
			readOffset = readOffset + sizeof(int); 		//skip the pageId
		}

		if(readOffset >= freeBytesStartAt)
		{
			isNewKey = true;
		}

		if(isNewKey)
		{
			noOfKeys = noOfKeys + 1;
		}

		splitKey = floor(noOfKeys/2);
	}
}

void IndexManager::insertKeyRidAtOffsetInt(int intKey, const RID &rid, void* node, int &offset, bool isNewKey)
{
	if(isNewKey)
	{
		memcpy((char*)node + offset, &intKey, sizeof(int));
		offset = offset + sizeof(int);

		short newKeyNoOfRids = 1;
		memcpy((char*)node + offset, &newKeyNoOfRids, sizeof(short));
		offset = offset + sizeof(short);

		memcpy((char*)node + offset, &rid.pageNum, sizeof(int));
		offset = offset + sizeof(int);
		memcpy((char*)node + offset, &rid.slotNum, sizeof(int));
		offset = offset + sizeof(int);
	}
	else
	{
		short newKeyNoOfRids;
		memcpy(&newKeyNoOfRids, (char*)node + offset - sizeof(short), sizeof(short));
		newKeyNoOfRids = newKeyNoOfRids + 1;
		memcpy((char*)node + offset - sizeof(short), &newKeyNoOfRids, sizeof(short));

		memcpy((char*)node + offset, &rid.pageNum, sizeof(int));
		offset = offset + sizeof(int);

		memcpy((char*)node + offset, &rid.slotNum, sizeof(int));
		offset = offset + sizeof(int);
	}
}

void IndexManager::insertKeyRidAtOffsetVarchar(void* varcharKey, short varcharLength, const RID &rid, void* node, int &offset, bool isNewKey)
{
	if(isNewKey)
	{
		memcpy((char*)node + offset, &varcharLength, sizeof(short));
		offset = offset + sizeof(short);

		memcpy((char*)node + offset, varcharKey, varcharLength);
		offset = offset + varcharLength;

		short newKeyNoOfRids = 1;
		memcpy((char*)node + offset, &newKeyNoOfRids, sizeof(short));
		offset = offset + sizeof(short);

		memcpy((char*)node + offset, &rid.pageNum, sizeof(int));
		offset = offset + sizeof(int);
		memcpy((char*)node + offset, &rid.slotNum, sizeof(int));
		offset = offset + sizeof(int);
	}
	else
	{
		short newKeyNoOfRids;
		memcpy(&newKeyNoOfRids, (char*)node + offset - sizeof(short), sizeof(short));
		newKeyNoOfRids = newKeyNoOfRids + 1;
		memcpy((char*)node + offset - sizeof(short), &newKeyNoOfRids, sizeof(short));

		memcpy((char*)node + offset, &rid.pageNum, sizeof(int));
		offset = offset + sizeof(int);

		memcpy((char*)node + offset, &rid.slotNum, sizeof(int));
		offset = offset + sizeof(int);
	}
}


void IndexManager::checkIfNewKeyFloat(void* node, char typeOfPage, float floatKey, int &readOffset, short freeBytesStartAt, short &noOfKeys, bool &isNewKey, short &splitKey, short &keyCounter)
{
	if(typeOfPage == '1')
	{
		short noOfRids;
		float currentKey;
		while(readOffset < freeBytesStartAt)
		{
			memcpy(&currentKey, (char*)node + readOffset, sizeof(float));
			//cout<<"Current Key : "<<currentKey<<endl;
			if(floatKey < currentKey)
			{
				isNewKey = true;
				break;
			}

			if(floatKey  == currentKey)
			{
				//int to finish key read and short to finish noOfRids reading
				readOffset = readOffset + sizeof(float) + sizeof(short);		//point to the place where RIDS start for the currentKey
				break;
			}
			//go to the next key
			keyCounter = keyCounter + 1;
			// cout<<"KeyCounter :"<<keyCounter<<endl;
			readOffset = readOffset + sizeof(float);
			memcpy(&noOfRids, (char*)node + readOffset, sizeof(short));
			// cout<<"No Of rids :"<<noOfRids<<endl;
			readOffset = readOffset + sizeof(short) + noOfRids * 2 * sizeof(int);	//each RID is always 8 bytes
		}

		if(readOffset >= freeBytesStartAt)
		{
			isNewKey = true;
		}

		if(isNewKey)
		{
			noOfKeys = noOfKeys + 1;
		}

		splitKey = floor(noOfKeys/2);
	}
	else		//find position for a key in the index leaf when it's child was split
	{
		float currentKey;

		readOffset = readOffset + sizeof(int); 		//skip the pageID

		while(readOffset < freeBytesStartAt)
		{
			memcpy(&currentKey, (char*)node + readOffset, sizeof(float));
			// cout<<"Current Key : "<<currentKey<<endl;
			if(floatKey < currentKey)
			{
				isNewKey = true;
				break;
			}
			//go to the next key
			keyCounter = keyCounter + 1;
			// cout<<"KeyCounter :"<<keyCounter<<endl;
			readOffset = readOffset + sizeof(float);		//read the key
			// int checkPgId;
			// memcpy(&checkPgId, (char*)node + readOffset, sizeof(int));
			// cout<<"Page id : "<<checkPgId<<endl;
			readOffset = readOffset + sizeof(int); 		//skip the pageId
		}

		if(readOffset >= freeBytesStartAt)
		{
			isNewKey = true;
		}

		if(isNewKey)
		{
			noOfKeys = noOfKeys + 1;
		}

		splitKey = floor(noOfKeys/2);
	}
}

void IndexManager::insertKeyRidAtOffsetFloat(float floatKey, const RID &rid, void* node, int &offset, bool isNewKey)
{
	if(isNewKey)
	{
		memcpy((char*)node + offset, &floatKey, sizeof(float));
		offset = offset + sizeof(float);

		short newKeyNoOfRids = 1;
		memcpy((char*)node + offset, &newKeyNoOfRids, sizeof(short));
		offset = offset + sizeof(short);

		memcpy((char*)node + offset, &rid.pageNum, sizeof(int));
		offset = offset + sizeof(int);
		memcpy((char*)node + offset, &rid.slotNum, sizeof(int));
		offset = offset + sizeof(int);
	}
	else
	{
		short newKeyNoOfRids;
		memcpy(&newKeyNoOfRids, (char*)node + offset - sizeof(short), sizeof(short));
		newKeyNoOfRids = newKeyNoOfRids + 1;
		memcpy((char*)node + offset - sizeof(short), &newKeyNoOfRids, sizeof(short));

		memcpy((char*)node + offset, &rid.pageNum, sizeof(int));
		offset = offset + sizeof(int);

		memcpy((char*)node + offset, &rid.slotNum, sizeof(int));
		offset = offset + sizeof(int);
	}
}

void IndexManager::insertEntryRecursive(IXFileHandle &ixfileHandle, int pageNo, const Attribute &attribute, const void* key, const RID &rid, SplitData &splitData)
{
	void* node = malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(pageNo, node);

	char typeOfPage;
	short noOfKeys;
	short freeBytesLeft;
	int readOffset = 0;
	short freeBytesStartAt;

	getCommonMetaData(node, typeOfPage, noOfKeys, freeBytesLeft, readOffset);

	freeBytesStartAt = PAGE_SIZE - freeBytesLeft;
	// cout<<"Free bytes left : "<<freeBytesLeft<<endl;
	// cout<<"Free bytes start at : "<<freeBytesStartAt<<endl;
	short keyRidSize = 0;
	if(typeOfPage == '1')		//check if it is a leaf
	{
		int nextPageId;
		memcpy(&nextPageId, (char*)node + readOffset, sizeof(int));
		readOffset = readOffset + sizeof(int); 			//stored nextPageID

		if(attribute.type == TypeInt)
		{
			keyRidSize = 3 * sizeof(int) + sizeof(short);		//key, rid and noOfRids
		}
		else if (attribute.type == TypeReal)
		{
			keyRidSize = sizeof(float) + 2*sizeof(int) + sizeof(short);
		}
		else if(attribute.type == TypeVarChar)
		{
			int varcharLen;
			memcpy(&varcharLen, key, sizeof(int));
			keyRidSize = sizeof(short) + varcharLen + sizeof(short) + 2 * sizeof(int);		//varchar len stored as short, varchar len, noOfRids, and new RId
		}

		//TODO : check if keyRidSize or just RID size we need to check depending upon the key is already there or no
		if(freeBytesLeft >= keyRidSize)		//have space in leaf node
		{
			// cout<<"FREE BYTES LEFT : "<<freeBytesLeft<<endl;
			if(attribute.type == TypeInt)
			{
				int intKey;
				short noOfRids;
			    memcpy(&intKey, key, sizeof(int));

			    bool isNewKey = false;

				int currentKey;
				while(readOffset < freeBytesStartAt)
				{
					memcpy(&currentKey, (char*)node + readOffset, sizeof(int));

					if(intKey < currentKey)
					{
						isNewKey = true;
						break;
					}

					if(intKey  == currentKey)
					{
						//int to finish key read and short to finish noOfRids reading
						readOffset = readOffset + sizeof(int) + sizeof(short);		//point to the place where RIDS start for the currentKey
						break;
					}
					//go to the next key
					readOffset = readOffset + sizeof(int);
					memcpy(&noOfRids, (char*)node + readOffset, sizeof(short));
					readOffset = readOffset + sizeof(short) + noOfRids * 2 * sizeof(int);	//each RID is always 8 bytes
				}

				if(readOffset < freeBytesStartAt)
				{
					int sizeofDataAfterKey = freeBytesStartAt - readOffset;

					void* dataToBeMoved = malloc(sizeofDataAfterKey);
					memcpy(dataToBeMoved, (char*)node + readOffset, sizeofDataAfterKey);

					if(isNewKey)
					{
						// cout<<"Read offset Before: "<<readOffset<<endl;
						insertNewKeyIntAtOffset(node, noOfKeys, intKey, readOffset, rid);	//readOffset gets modified in this method
						// cout<<"Read offset After: "<<readOffset<<endl;
						memcpy((char*)node + readOffset, dataToBeMoved, sizeofDataAfterKey);
					}
					else	//just insert RID
					{
						memcpy(&noOfRids, (char*)node + readOffset - sizeof(short), sizeof(short));
						memcpy((char*)node + readOffset, &rid.pageNum, sizeof(int));

						memcpy((char*)node + readOffset + sizeof(int), &rid.slotNum, sizeof(int));

						memcpy((char*)node + readOffset + 2*sizeof(int), dataToBeMoved, sizeofDataAfterKey);

						//update noOfRids for that key
						noOfRids = noOfRids + 1;
						memcpy((char*)node + readOffset - sizeof(short), &noOfRids, sizeof(short));
					}
					free(dataToBeMoved);
				}
				else	//
				{
					isNewKey = true;
					// cout<<"Read offset Before: "<<readOffset<<endl;
					//cout<<"Int Key : "<<intKey<<endl;
					insertNewKeyIntAtOffset(node, noOfKeys, intKey, readOffset, rid);
					// cout<<"Read offset After: "<<readOffset<<endl;
				}

				//Change meta data
				if(isNewKey)
				{
					freeBytesLeft = freeBytesLeft - keyRidSize;
				}
				else
				{
					freeBytesLeft = freeBytesLeft - (2 * sizeof(int));
				}
				memcpy((char*)node + sizeof(char) + sizeof(short), &freeBytesLeft, sizeof(short));

				ixfileHandle.fileHandle.writePage(pageNo, node);

				splitData.result = 0;
			}
			else if(attribute.type == TypeReal)
			{
				float floatKey;
				short noOfRids;
			    memcpy(&floatKey, key, sizeof(float));

			    bool isNewKey = false;

				float currentKey;
				while(readOffset < freeBytesStartAt)
				{
					memcpy(&currentKey, (char*)node + readOffset, sizeof(float));

					if(floatKey < currentKey)
					{
						isNewKey = true;
						break;
					}

					if(floatKey  == currentKey)
					{
						//int to finish key read and short to finish noOfRids reading
						readOffset = readOffset + sizeof(float) + sizeof(short);		//point to the place where RIDS start for the currentKey
						break;
					}
					//go to the next key
					readOffset = readOffset + sizeof(float);
					memcpy(&noOfRids, (char*)node + readOffset, sizeof(short));
					readOffset = readOffset + sizeof(short) + noOfRids * 2 * sizeof(int);	//each RID is always 8 bytes
				}

				if(readOffset < freeBytesStartAt)
				{
					int sizeofDataAfterKey = freeBytesStartAt - readOffset;

					void* dataToBeMoved = malloc(sizeofDataAfterKey);
					memcpy(dataToBeMoved, (char*)node + readOffset, sizeofDataAfterKey);

					if(isNewKey)
					{
						// cout<<"Read offset Before: "<<readOffset<<endl;
						insertNewKeyFloatAtOffset(node, noOfKeys, floatKey, readOffset, rid);	//readOffset gets modified in this method
						// cout<<"Read offset After: "<<readOffset<<endl;
						memcpy((char*)node + readOffset, dataToBeMoved, sizeofDataAfterKey);
					}
					else	//just insert RID
					{
						memcpy(&noOfRids, (char*)node + readOffset - sizeof(short), sizeof(short));
						memcpy((char*)node + readOffset, &rid.pageNum, sizeof(int));

						memcpy((char*)node + readOffset + sizeof(int), &rid.slotNum, sizeof(int));

						memcpy((char*)node + readOffset + 2*sizeof(int), dataToBeMoved, sizeofDataAfterKey);

						//update noOfRids for that key
						noOfRids = noOfRids + 1;
						memcpy((char*)node + readOffset - sizeof(short), &noOfRids, sizeof(short));
					}
					free(dataToBeMoved);
				}
				else	//
				{
					isNewKey = true;
					// cout<<"Read offset Before: "<<readOffset<<endl;
					//cout<<"Int Key : "<<intKey<<endl;
					insertNewKeyFloatAtOffset(node, noOfKeys, floatKey, readOffset, rid);
					// cout<<"Read offset After: "<<readOffset<<endl;
				}

				//Change meta data
				if(isNewKey)
				{
					freeBytesLeft = freeBytesLeft - keyRidSize;
				}
				else
				{
					freeBytesLeft = freeBytesLeft - (2 * sizeof(int));
				}
				memcpy((char*)node + sizeof(char) + sizeof(short), &freeBytesLeft, sizeof(short));

				ixfileHandle.fileHandle.writePage(pageNo, node);

				splitData.result = 0;
			}
			else
			{
				short noOfRids;
				int givenVarcharLen;
			    memcpy(&givenVarcharLen, key, sizeof(int));
			    short givenVarcharLenShort  = (short)givenVarcharLen;
				void *varcharKeyPtr =  malloc(givenVarcharLen);
			    memcpy(varcharKeyPtr, (char*)key + sizeof(int), givenVarcharLen);
				string varcharKey = string((char*)varcharKeyPtr, givenVarcharLen);

				// cout<<"KEY TO INSERT! : "<<varcharKey<<endl;
			    bool isNewKey = false;

			    // cout<<"ReadOffset before starting : "<<readOffset<<endl;
				while(readOffset < freeBytesStartAt)
				{
					short currentVarcharLength;
					memcpy(&currentVarcharLength, (char*)node + readOffset, sizeof(short));

					// cout<<"Current VC Len : "<<currentVarcharLength<<endl;
				    void* currentKeyPtr = malloc(currentVarcharLength);
					memcpy(currentKeyPtr, (char*)node + readOffset + sizeof(short), currentVarcharLength);

					string currentKey = string((char*)currentKeyPtr, currentVarcharLength);
					// cout<<"CURRENT KEY WHILE INSERTING : "<<currentKey<<endl;
					// cout<<"LEN OF CURRENT KEY WHILE INSERTING : "<<currentVarcharLength<<endl;

					// cout<<"During comparision! : Current Key : "<<currentKey<<" New Key : "<<varcharKey<<endl;
					if(varcharKey.compare(currentKey) < 0)
					{
						isNewKey = true;
						break;
					}

					if(varcharKey.compare(currentKey) == 0)
					{
						//length of varchar and varchar to finish key read and short to finish noOfRids reading
						readOffset = readOffset + sizeof(short) + currentVarcharLength + sizeof(short);		//point to the place where RIDS start for the currentKey
						break;
					}
					//go to the next key
					readOffset = readOffset + sizeof(short) + currentVarcharLength;
					memcpy(&noOfRids, (char*)node + readOffset, sizeof(short));
					// cout<<"No of RIds : "<<noOfRids<<endl;
					readOffset = readOffset + sizeof(short) + noOfRids * 2 * sizeof(int);	//each RID is always 8 bytes
				}

				if(readOffset < freeBytesStartAt)
				{
					int sizeofDataAfterKey = freeBytesStartAt - readOffset;

					void* dataToBeMoved = malloc(sizeofDataAfterKey);
					memcpy(dataToBeMoved, (char*)node + readOffset, sizeofDataAfterKey);

					if(isNewKey)
					{
						// cout<<"Less than key!"<<endl;
					    // cout<<"Key to insert  : "<<varcharKey<<endl;
					    // cout<<"RID : "<<rid.pageNum<<" : "<<rid.slotNum<<endl;
						// cout<<"Read offset Before: "<<readOffset<<endl;
						insertNewKeyVarcharAtOffset(node, noOfKeys, varcharKeyPtr, givenVarcharLenShort, readOffset, rid);	//readOffset gets modified in this method
						// cout<<"Read offset After: "<<readOffset<<endl;
						memcpy((char*)node + readOffset, dataToBeMoved, sizeofDataAfterKey);
					}
					else	//just insert RID
					{
						// cout<<"Key to insert  : "<<varcharKey<<endl;
						// cout<<"RID : "<<rid.pageNum<<" : "<<rid.slotNum<<endl;
						// cout<<"Read Offset  : "<<readOffset<<endl;
						memcpy(&noOfRids, (char*)node + readOffset - sizeof(short), sizeof(short));
						memcpy((char*)node + readOffset, &rid.pageNum, sizeof(int));

						memcpy((char*)node + readOffset + sizeof(int), &rid.slotNum, sizeof(int));

						memcpy((char*)node + readOffset + 2*sizeof(int), dataToBeMoved, sizeofDataAfterKey);

						//update noOfRids for that key
						noOfRids = noOfRids + 1;
						memcpy((char*)node + readOffset - sizeof(short), &noOfRids, sizeof(short));
					}
					free(dataToBeMoved);
				}
				else	//
				{
					isNewKey = true;
					// cout<<"Key to insert  : "<<varcharKey<<endl;
					// cout<<"RID : "<<rid.pageNum<<" : "<<rid.slotNum<<endl;
					insertNewKeyVarcharAtOffset(node, noOfKeys, varcharKeyPtr, givenVarcharLenShort, readOffset, rid);	//readOffset gets modified in this method
				}

				//Change meta data
				if(isNewKey)
				{
					freeBytesLeft = freeBytesLeft - keyRidSize;
				}
				else
				{
					freeBytesLeft = freeBytesLeft - (2 * sizeof(int));
				}
				memcpy((char*)node + sizeof(char) + sizeof(short), &freeBytesLeft, sizeof(short));

				ixfileHandle.fileHandle.writePage(pageNo, node);

				splitData.result = 0;
			}
		}
		else	//split page
		 {
		 	if(pageNo == 0)	//root splitting
		 	{
		 		void* rootPage = malloc(PAGE_SIZE);
		 		void* leftPage = malloc(PAGE_SIZE);
		 		void* rightPage = malloc(PAGE_SIZE);

		 		int leftWriteOffset = 0;
		 		int rightWriteOffset = 0;
		 		int rootWriteOffset = 0;

		 		char typeOfNewPages = '1';
		 		fillCommonMetaData(leftPage, leftWriteOffset, typeOfNewPages);
		 		fillCommonMetaData(rightPage, rightWriteOffset, typeOfNewPages);

		 		//put all info for root node except the key
		 		typeOfNewPages = '0';
		 		fillCommonMetaData(rootPage, rootWriteOffset, typeOfNewPages);
		 		int rootPageNoOfKeys = 1;
		 		memcpy((char*)rootPage + sizeof(char), &rootPageNoOfKeys, sizeof(short));
		 		int leftPageId = 1;
		 		int rightPageId = 2;
		 		memcpy((char*)rootPage + rootWriteOffset, &leftPageId, sizeof(int));

		 		if(attribute.type == TypeInt || attribute.type == TypeReal)
		 		{
					rootWriteOffset = rootWriteOffset + 2*sizeof(int);					//note! skipped key space here along with PageId
		 			memcpy((char*)rootPage + rootWriteOffset, &rightPageId, sizeof(int));
		 			rootWriteOffset = rootWriteOffset + sizeof(int);
		 		}
		 		else
		 		{
		 			rootWriteOffset = rootWriteOffset + sizeof(int);					//note! skipped only PageId for varchar
		 		}

		 		short leftPageNoOfKeys = 0;
		 		short rightPageNoOfKeys = 0;

		 		//You will get your first two leaf nodes ever -> hence Pg 2
		 		int pointerToNextPage = 2;
		 		memcpy((char*)leftPage + leftWriteOffset, &pointerToNextPage, sizeof(int));
		 		leftWriteOffset = leftWriteOffset + sizeof(int);

		 		pointerToNextPage = -1;
		 		memcpy((char*)rightPage + rightWriteOffset, &pointerToNextPage, sizeof(int));
		 		rightWriteOffset = rightWriteOffset + sizeof(int);

		 		if(attribute.type == TypeInt)
		 		{
		 			int oldReadOffset = readOffset;
		 			short keyCounter = 0;
		 			bool isNewKey = false;

		 			int intKey;
		 			memcpy(&intKey, key, sizeof(int));

		 			short splitKey;

		 			checkIfNewKeyInt(node, typeOfPage, intKey, readOffset, freeBytesStartAt, noOfKeys, isNewKey, splitKey, keyCounter);
		 			if(noOfKeys == 1)
		 			{
		 				splitData.result = -1;
		 				return;
		 			}
		 			short currentKeyPointer = 0;
	 				while(currentKeyPointer < noOfKeys)
	 				{
	 					//Root Node Key
	 					if(currentKeyPointer == splitKey)		//copy up to root
						{
	 						if(!isNewKey || currentKeyPointer != keyCounter)
	 						{
	 							int rootKey;
								memcpy(&rootKey, (char*)node + oldReadOffset, sizeof(int));
								memcpy((char*)rootPage + rootWriteOffset - 2*sizeof(int), &rootKey, sizeof(int));
	 						}
	 						else
	 						{
	 							memcpy((char*)rootPage + rootWriteOffset - 2*sizeof(int), &intKey, sizeof(int));
	 						}
						}

	 					//oldReadOffset points to the place where the first key starts from
	 					short noOfRids;
						memcpy(&noOfRids, (char*)node + oldReadOffset + sizeof(int), sizeof(short));

						int sizeofDataToBeWritten = noOfRids * 2 * sizeof(int) + sizeof(int) + sizeof(short);
	 					if(currentKeyPointer != keyCounter)		//write the already existing data, not the new data
	 					{
							if(currentKeyPointer < splitKey)
							{
								memcpy((char*)leftPage + leftWriteOffset, (char*)node + oldReadOffset, sizeofDataToBeWritten);
								leftWriteOffset = leftWriteOffset + sizeofDataToBeWritten;
								leftPageNoOfKeys = leftPageNoOfKeys + 1;
							}
							else
							{
								memcpy((char*)rightPage + rightWriteOffset, (char*)node + oldReadOffset, sizeofDataToBeWritten);
								rightWriteOffset = rightWriteOffset + sizeofDataToBeWritten;
								rightPageNoOfKeys = rightPageNoOfKeys + 1;
							}
							oldReadOffset = oldReadOffset + sizeofDataToBeWritten;
	 					}
	 					else		//this is the position the new key fits into the already existing keys
	 					{
	 						short noOfRids;
							memcpy(&noOfRids, (char*)node + oldReadOffset + sizeof(int), sizeof(short));

	 						if(!isNewKey)
	 						{
	 							//write the already existing key with its noOfRids and Rids and append the rid
								int sizeofDataToBeWritten = noOfRids * 2 * sizeof(int) + sizeof(int) + sizeof(short);

	 							if(currentKeyPointer < splitKey)
								{
	 								int tempLeftWriteOffset = leftWriteOffset;
									memcpy((char*)leftPage + leftWriteOffset, (char*)node + oldReadOffset, sizeofDataToBeWritten);

									leftWriteOffset = leftWriteOffset + sizeofDataToBeWritten;
									leftPageNoOfKeys = leftPageNoOfKeys + 1;

									memcpy((char*)leftPage + leftWriteOffset, &rid.pageNum, sizeof(int));
									leftWriteOffset = leftWriteOffset + sizeof(int);

									memcpy((char*)leftPage + leftWriteOffset, &rid.slotNum, sizeof(int));
									leftWriteOffset = leftWriteOffset + sizeof(int);

									noOfRids = noOfRids + 1;
									memcpy((char*)leftPage + tempLeftWriteOffset + sizeof(int), &noOfRids, sizeof(short));
								}
								else
								{
	 								int tempRightWriteOffset = rightWriteOffset;
									memcpy((char*)rightPage + rightWriteOffset, (char*)node + oldReadOffset, sizeofDataToBeWritten);
									rightWriteOffset = rightWriteOffset + sizeofDataToBeWritten;
									rightPageNoOfKeys = rightPageNoOfKeys + 1;

									memcpy((char*)rightPage + rightWriteOffset, &rid.pageNum, sizeof(int));
									rightWriteOffset = rightWriteOffset + sizeof(int);

									memcpy((char*)rightPage + rightWriteOffset, &rid.slotNum, sizeof(int));
									rightWriteOffset = rightWriteOffset + sizeof(int);

									noOfRids = noOfRids + 1;
									memcpy((char*)rightPage + tempRightWriteOffset + sizeof(int), &noOfRids, sizeof(short));
								}
	 							oldReadOffset = oldReadOffset + sizeofDataToBeWritten;
	 						}
	 						else
	 						{
		 						if(currentKeyPointer < splitKey)
								{
		 							insertNewKeyIntAtOffset(leftPage, leftPageNoOfKeys, intKey, leftWriteOffset, rid);
								}
								else
								{
		 							insertNewKeyIntAtOffset(rightPage, rightPageNoOfKeys, intKey, rightWriteOffset, rid);								}
	 							}

	 						}
						currentKeyPointer = currentKeyPointer + 1;
	 				}
		 		}
		 		else if(attribute.type == TypeReal)
		 		{
		 			int oldReadOffset = readOffset;
		 			short keyCounter = 0;
		 			bool isNewKey = false;

		 			float floatKey;
		 			memcpy(&floatKey, key, sizeof(float));

		 			short splitKey;

		 			checkIfNewKeyFloat(node, typeOfPage, floatKey, readOffset, freeBytesStartAt, noOfKeys, isNewKey, splitKey, keyCounter);
		 			if(noOfKeys == 1)
		 			{
		 				splitData.result = -1;
		 				return;
		 			}
		 			short currentKeyPointer = 0;
	 				while(currentKeyPointer < noOfKeys)
	 				{
	 					//Root Node Key
	 					if(currentKeyPointer == splitKey)		//copy up to root
						{
	 						if(!isNewKey || currentKeyPointer != keyCounter)
	 						{
	 							float rootKey;
								memcpy(&rootKey, (char*)node + oldReadOffset, sizeof(float));
								memcpy((char*)rootPage + rootWriteOffset - sizeof(int) - sizeof(float), &rootKey, sizeof(float));
	 						}
	 						else
	 						{
	 							memcpy((char*)rootPage + rootWriteOffset - sizeof(int) - sizeof(float), &floatKey, sizeof(float));
	 						}
						}

	 					//oldReadOffset points to the place where the first key starts from
	 					short noOfRids;
						memcpy(&noOfRids, (char*)node + oldReadOffset + sizeof(float), sizeof(short));

						int sizeofDataToBeWritten = noOfRids * 2 * sizeof(int) + sizeof(float) + sizeof(short);
	 					if(currentKeyPointer != keyCounter)		//write the already existing data, not the new data
	 					{
							if(currentKeyPointer < splitKey)
							{
								memcpy((char*)leftPage + leftWriteOffset, (char*)node + oldReadOffset, sizeofDataToBeWritten);
								leftWriteOffset = leftWriteOffset + sizeofDataToBeWritten;
								leftPageNoOfKeys = leftPageNoOfKeys + 1;
							}
							else
							{
								memcpy((char*)rightPage + rightWriteOffset, (char*)node + oldReadOffset, sizeofDataToBeWritten);
								rightWriteOffset = rightWriteOffset + sizeofDataToBeWritten;
								rightPageNoOfKeys = rightPageNoOfKeys + 1;
							}
							oldReadOffset = oldReadOffset + sizeofDataToBeWritten;
	 					}
	 					else		//this is the position the new key fits into the already existing keys
	 					{
	 						short noOfRids;
							memcpy(&noOfRids, (char*)node + oldReadOffset + sizeof(float), sizeof(short));

	 						if(!isNewKey)
	 						{
	 							//write the already existing key with its noOfRids and Rids and append the rid
								int sizeofDataToBeWritten = noOfRids * 2 * sizeof(int) + sizeof(float) + sizeof(short);

	 							if(currentKeyPointer < splitKey)
								{
	 								int tempLeftWriteOffset = leftWriteOffset;
									memcpy((char*)leftPage + leftWriteOffset, (char*)node + oldReadOffset, sizeofDataToBeWritten);

									leftWriteOffset = leftWriteOffset + sizeofDataToBeWritten;
									leftPageNoOfKeys = leftPageNoOfKeys + 1;

									memcpy((char*)leftPage + leftWriteOffset, &rid.pageNum, sizeof(int));
									leftWriteOffset = leftWriteOffset + sizeof(int);

									memcpy((char*)leftPage + leftWriteOffset, &rid.slotNum, sizeof(int));
									leftWriteOffset = leftWriteOffset + sizeof(int);

									noOfRids = noOfRids + 1;
									memcpy((char*)leftPage + tempLeftWriteOffset + sizeof(float), &noOfRids, sizeof(short));
								}
								else
								{
	 								int tempRightWriteOffset = rightWriteOffset;
									memcpy((char*)rightPage + rightWriteOffset, (char*)node + oldReadOffset, sizeofDataToBeWritten);
									rightWriteOffset = rightWriteOffset + sizeofDataToBeWritten;
									rightPageNoOfKeys = rightPageNoOfKeys + 1;

									memcpy((char*)rightPage + rightWriteOffset, &rid.pageNum, sizeof(int));
									rightWriteOffset = rightWriteOffset + sizeof(int);

									memcpy((char*)rightPage + rightWriteOffset, &rid.slotNum, sizeof(int));
									rightWriteOffset = rightWriteOffset + sizeof(int);

									noOfRids = noOfRids + 1;
									memcpy((char*)rightPage + tempRightWriteOffset + sizeof(float), &noOfRids, sizeof(short));
								}
	 							oldReadOffset = oldReadOffset + sizeofDataToBeWritten;
	 						}
	 						else
	 						{
		 						if(currentKeyPointer < splitKey)
								{
		 							insertNewKeyFloatAtOffset(leftPage, leftPageNoOfKeys, floatKey, leftWriteOffset, rid);
								}
								else
								{
		 							insertNewKeyFloatAtOffset(rightPage, rightPageNoOfKeys, floatKey, rightWriteOffset, rid);								}
	 							}

	 						}
						currentKeyPointer = currentKeyPointer + 1;
	 				}
	 			}
	 			else
		 		{
		 			int oldReadOffset = readOffset;
		 			short keyCounter = 0;
		 			bool isNewKey = false;

		 			int varcharLength;
		 			memcpy(&varcharLength, key, sizeof(int));
		 			short varcharLengthShort = (short)varcharLength;
		 			void *varcharKeyPtr = malloc(varcharLength);
		 			memcpy(varcharKeyPtr, (char*)key + sizeof(int), varcharLength);

		 			short splitKey;

		 			checkIfNewKeyVarchar(node, typeOfPage, varcharKeyPtr, varcharLength, readOffset, freeBytesStartAt, noOfKeys, isNewKey, splitKey, keyCounter);
		 			if(noOfKeys == 1)
		 			{
		 				splitData.result = -1;
		 				return;
		 			}
		 			short currentKeyPointer = 0;
	 				while(currentKeyPointer < noOfKeys)
	 				{
	 					//Root Node Key
	 					if(currentKeyPointer == splitKey)		//copy up to root
						{
	 						if(!isNewKey || currentKeyPointer != keyCounter)
	 						{
	 							short rootKeyLength;
	 							memcpy(&rootKeyLength, (char*)node + oldReadOffset, sizeof(short));
	 							void* rootKey = malloc(rootKeyLength);
								memcpy(rootKey, (char*)node + oldReadOffset + sizeof(short), rootKeyLength);

								//write into root page the varchar length and the varchar, Also the right page Id
								memcpy((char*)rootPage + rootWriteOffset, &rootKeyLength, sizeof(short));
								rootWriteOffset = rootWriteOffset + sizeof(short);
								memcpy((char*)rootPage + rootWriteOffset, rootKey, rootKeyLength);
								rootWriteOffset = rootWriteOffset + rootKeyLength;
	 						}
	 						else
	 						{
	 							memcpy((char*)rootPage + rootWriteOffset, &varcharLengthShort, sizeof(short));
	 							rootWriteOffset = rootWriteOffset + sizeof(short);
	 							memcpy((char*)rootPage + rootWriteOffset, varcharKeyPtr, varcharLength);
								rootWriteOffset = rootWriteOffset + varcharLength;
	 						}

	 						memcpy((char*)rootPage + rootWriteOffset, &rightPageId, sizeof(int));
							rootWriteOffset = rootWriteOffset + sizeof(int);
						}

	 					//oldReadOffset points to the place where the first key starts from
	 					short noOfRids;
	 					short currentKeyLength;
						memcpy(&currentKeyLength, (char*)node + oldReadOffset, sizeof(short));
						void* currentKey = malloc(currentKeyLength);
						memcpy(currentKey, (char*)node + oldReadOffset + sizeof(short), currentKeyLength);

						int sizeOfCurrentKey = sizeof(short) + currentKeyLength;

						memcpy(&noOfRids, (char*)node + oldReadOffset + sizeOfCurrentKey, sizeof(short));

						int sizeofDataToBeWritten = noOfRids * 2 * sizeof(int) +  sizeOfCurrentKey + sizeof(short);
	 					if(currentKeyPointer != keyCounter)		//write the already existing data, not the new data
	 					{
							if(currentKeyPointer < splitKey)
							{
								memcpy((char*)leftPage + leftWriteOffset, (char*)node + oldReadOffset, sizeofDataToBeWritten);
								leftWriteOffset = leftWriteOffset + sizeofDataToBeWritten;
								leftPageNoOfKeys = leftPageNoOfKeys + 1;
							}
							else
							{
								memcpy((char*)rightPage + rightWriteOffset, (char*)node + oldReadOffset, sizeofDataToBeWritten);
								rightWriteOffset = rightWriteOffset + sizeofDataToBeWritten;
								rightPageNoOfKeys = rightPageNoOfKeys + 1;
							}
							oldReadOffset = oldReadOffset + sizeofDataToBeWritten;
	 					}
	 					else		//this is the position the new key fits into the already existing keys
	 					{
	 						if(!isNewKey)
	 						{
	 							//write the already existing key with its noOfRids and Rids and append the rid
	 							if(currentKeyPointer < splitKey)
								{
	 								int tempLeftWriteOffset = leftWriteOffset;
									memcpy((char*)leftPage + leftWriteOffset, (char*)node + oldReadOffset, sizeofDataToBeWritten);

									leftWriteOffset = leftWriteOffset + sizeofDataToBeWritten;
									leftPageNoOfKeys = leftPageNoOfKeys + 1;

									memcpy((char*)leftPage + leftWriteOffset, &rid.pageNum, sizeof(int));
									leftWriteOffset = leftWriteOffset + sizeof(int);

									memcpy((char*)leftPage + leftWriteOffset, &rid.slotNum, sizeof(int));
									leftWriteOffset = leftWriteOffset + sizeof(int);

									noOfRids = noOfRids + 1;
									memcpy((char*)leftPage + tempLeftWriteOffset + sizeOfCurrentKey, &noOfRids, sizeof(short));	//write the noOfRids
								}
								else
								{
	 								int tempRightWriteOffset = rightWriteOffset;
									memcpy((char*)rightPage + rightWriteOffset, (char*)node + oldReadOffset, sizeofDataToBeWritten);

									rightWriteOffset = rightWriteOffset + sizeofDataToBeWritten;
									rightPageNoOfKeys = rightPageNoOfKeys + 1;

									memcpy((char*)rightPage + rightWriteOffset, &rid.pageNum, sizeof(int));
									rightWriteOffset = rightWriteOffset + sizeof(int);

									memcpy((char*)rightPage + rightWriteOffset, &rid.slotNum, sizeof(int));
									rightWriteOffset = rightWriteOffset + sizeof(int);

									noOfRids = noOfRids + 1;
									memcpy((char*)rightPage + tempRightWriteOffset + sizeOfCurrentKey, &noOfRids, sizeof(short));
								}
	 							oldReadOffset = oldReadOffset + sizeofDataToBeWritten;
	 						}
	 						else
	 						{
		 						if(currentKeyPointer < splitKey)
								{
		 							insertNewKeyVarcharAtOffset(leftPage, leftPageNoOfKeys, varcharKeyPtr, varcharLengthShort, leftWriteOffset, rid);
								}
								else
								{
		 							insertNewKeyVarcharAtOffset(rightPage, rightPageNoOfKeys, varcharKeyPtr, varcharLengthShort, rightWriteOffset, rid);								}
	 							}

	 						}
						currentKeyPointer = currentKeyPointer + 1;
	 				}
	 			}

	 			//Change meta data
 				//Root
				//update free bytes left
				short rootPageFreeBytesLeft = PAGE_SIZE - rootWriteOffset;
 				memcpy((char*)rootPage + sizeof(char) + sizeof(short), &rootPageFreeBytesLeft, sizeof(short));

 				//Left Page :
 				//update number of keys
 				memcpy((char*)leftPage + sizeof(char), &leftPageNoOfKeys, sizeof(short));
 				//update free bytes left
	 			short leftPageFreeBytesLeft = PAGE_SIZE - leftWriteOffset;
	 			memcpy((char*)leftPage + sizeof(char) + sizeof(short), &leftPageFreeBytesLeft, sizeof(short));

 				//Right Page :
 				//update number of keys
 				memcpy((char*)rightPage + sizeof(char), &rightPageNoOfKeys, sizeof(short));
 				//update free bytes left
	 			short rightPageFreeBytesLeft = PAGE_SIZE - rightWriteOffset;
	 			memcpy((char*)rightPage + sizeof(char) + sizeof(short), &rightPageFreeBytesLeft, sizeof(short));

	 			ixfileHandle.fileHandle.writePage(0, rootPage);
	 			ixfileHandle.fileHandle.appendPage(leftPage);
	 			ixfileHandle.fileHandle.appendPage(rightPage);

	 			free(rootPage);
	 			free(leftPage);
	 			free(rightPage);
	 			splitData.result = 0;
		 	}
		 	else
		 	{
		 		void* newLeafPage =  malloc(PAGE_SIZE);

		 		char newLeafTypeOfPage = '1';
		 		int newLeafWriteOffset = 0;
		 		fillCommonMetaData(newLeafPage, newLeafWriteOffset, newLeafTypeOfPage);

		 		memcpy((char*)newLeafPage + newLeafWriteOffset, &nextPageId, sizeof(int));	//set the next pageId to he nextPage Id of the original Page
		 		newLeafWriteOffset = newLeafWriteOffset + sizeof(int);

				short splitKey;
				int newLeafReadOffset = sizeof(char) + 2*sizeof(short) + sizeof(int);			//we set it to point to where key insertion shold start from

				int oldReadOffset = 0;			//needed to reset readOffset to beginning of leaf

		 		if(attribute.type == TypeInt)
				{
					short keyCounter = 0;
					bool isNewKey = false;

					int intKey;
					memcpy(&intKey, key, sizeof(int));

					oldReadOffset = readOffset;

					checkIfNewKeyInt(node, typeOfPage, intKey, readOffset, freeBytesStartAt, noOfKeys, isNewKey, splitKey, keyCounter);
					if(noOfKeys == 1)
		 			{
		 				splitData.result = -1;
		 				return;
		 			}

					int placeToInsert = 0;
					short tempKey = 0;

					while(tempKey < splitKey)
					{
						if(tempKey == keyCounter)				//since we do not have space in the page to insert a new key, we just find a place for it
						{
							placeToInsert = oldReadOffset;
							if(!isNewKey)
							{
								oldReadOffset = oldReadOffset + sizeof(int);	//skip key
								short noOfRids;
								memcpy(&noOfRids, (char*)node + oldReadOffset, sizeof(short));
								oldReadOffset = oldReadOffset + sizeof(short) + noOfRids * 2*sizeof(int);
							}
							// if(isNewKey)
							// {
							// 	oldReadOffset = oldReadOffset + sizeof(int) + sizeof(short) + sizeof(int); //make space for new key
							// }
							// else
							// {
							// 	oldReadOffset = oldReadOffset + sizeof(int);			//only RID
							// }
						}
						else
						{
							oldReadOffset = oldReadOffset + sizeof(int);	//skip key
							short noOfRids;
							memcpy(&noOfRids, (char*)node + oldReadOffset, sizeof(short));
							oldReadOffset = oldReadOffset + sizeof(short) + noOfRids * 2*sizeof(int);
						}

						tempKey = tempKey + 1;
					}

					int sizeOfDataToRemove = freeBytesStartAt - oldReadOffset;
					void* dataToBeRemoved = malloc(sizeOfDataToRemove);
					memcpy(dataToBeRemoved, (char*)node + oldReadOffset, sizeOfDataToRemove);

					memcpy((char*)newLeafPage + newLeafWriteOffset, dataToBeRemoved, sizeOfDataToRemove);
					newLeafWriteOffset = newLeafWriteOffset + sizeOfDataToRemove;
					free(dataToBeRemoved);

					tempKey = splitKey;

					if(isNewKey && tempKey == keyCounter)
					{
						int intKey;
						memcpy(&intKey, key, sizeof(int));
						memcpy((char*)splitData.key, &intKey, sizeof(int));
					}
					else
					{
						memcpy((char*)splitData.key, (char*)newLeafPage + newLeafReadOffset, sizeof(int));

					}

					if(keyCounter >= splitKey)
					{
						while(tempKey < noOfKeys)
						{
							if(tempKey == keyCounter)
							{
								if(!isNewKey)
								{
									newLeafReadOffset = newLeafReadOffset + sizeof(int) + sizeof(short);
								}

								int moveDataSize = newLeafWriteOffset - newLeafReadOffset;
								void* dataToBeMoved;
								if(moveDataSize != 0)
								{
									dataToBeMoved = malloc(moveDataSize);
									memcpy(dataToBeMoved, (char*)newLeafPage + newLeafReadOffset, moveDataSize);
								}

								insertKeyRidAtOffsetInt(intKey, rid, newLeafPage, newLeafReadOffset, isNewKey);

								if(moveDataSize != 0)
								{
									memcpy((char*)newLeafPage + newLeafReadOffset, dataToBeMoved, moveDataSize);
									newLeafReadOffset = newLeafReadOffset + moveDataSize;
									free(dataToBeMoved);
								}
								break;
							}
							else
							{
								newLeafReadOffset = newLeafReadOffset + sizeof(int);	//skip key
								short noOfRids;
								memcpy(&noOfRids, (char*)newLeafPage + newLeafReadOffset, sizeof(short));
								newLeafReadOffset = newLeafReadOffset + sizeof(short) + noOfRids * 2*sizeof(int);
							}
							tempKey = tempKey + 1;
						}
					}
					else
					{
						newLeafReadOffset = newLeafWriteOffset;
					}


					freeBytesStartAt = oldReadOffset;
					if(keyCounter < splitKey)			//now we can insert into the old leaf since it is halved
					{
						if(placeToInsert != 0)
						{
							if(!isNewKey)
							{
								placeToInsert = placeToInsert + sizeof(short) + sizeof(int);
							}
							int moveDataSize = freeBytesStartAt - placeToInsert;
					 		void* dataToBeMoved;
					 		if(moveDataSize > 0)
					 		{
								dataToBeMoved = malloc(moveDataSize);
					 			memcpy(dataToBeMoved, (char*)node + placeToInsert, moveDataSize);
					 		}

							insertKeyRidAtOffsetInt(intKey, rid, node, placeToInsert, isNewKey);

					 		if(moveDataSize > 0)
					 		{
					 			memcpy((char*)node + placeToInsert, dataToBeMoved, moveDataSize);

					 			int slotIdCheck;
					 			memcpy(&slotIdCheck, (char*)node + placeToInsert - sizeof(int), sizeof(int));
					 			placeToInsert = placeToInsert + moveDataSize;
					 			free(dataToBeMoved);
					 		}
					 		oldReadOffset = placeToInsert;
						}
					}
				}
				else if (attribute.type == TypeReal)
				{
					short keyCounter = 0;
					bool isNewKey = false;

					float floatKey;
					memcpy(&floatKey, key, sizeof(float));

					oldReadOffset = readOffset;

					checkIfNewKeyFloat(node, typeOfPage, floatKey, readOffset, freeBytesStartAt, noOfKeys, isNewKey, splitKey, keyCounter);

					if(noOfKeys == 1)
		 			{
		 				splitData.result = -1;
		 				return;
		 			}
					int placeToInsert = 0;
					short tempKey = 0;

					while(tempKey < splitKey)
					{
						if(tempKey == keyCounter)				//since we do not have space in the page to insert a new key, we just find a place for it
						{
							placeToInsert = oldReadOffset;
							if(!isNewKey)
							{
								oldReadOffset = oldReadOffset + sizeof(float);	//skip key
								short noOfRids;
								memcpy(&noOfRids, (char*)node + oldReadOffset, sizeof(short));
								oldReadOffset = oldReadOffset + sizeof(short) + noOfRids * 2*sizeof(int);
							}
						}
						else
						{
							oldReadOffset = oldReadOffset + sizeof(float);	//skip key
							short noOfRids;
							memcpy(&noOfRids, (char*)node + oldReadOffset, sizeof(short));
							oldReadOffset = oldReadOffset + sizeof(short) + noOfRids * 2*sizeof(int);
						}

						tempKey = tempKey + 1;
					}

					int sizeOfDataToRemove = freeBytesStartAt - oldReadOffset;
					void* dataToBeRemoved = malloc(sizeOfDataToRemove);
					memcpy(dataToBeRemoved, (char*)node + oldReadOffset, sizeOfDataToRemove);

					memcpy((char*)newLeafPage + newLeafWriteOffset, dataToBeRemoved, sizeOfDataToRemove);
					newLeafWriteOffset = newLeafWriteOffset + sizeOfDataToRemove;
					free(dataToBeRemoved);

					tempKey = splitKey;
					newLeafReadOffset = sizeof(char) + 2*sizeof(short) + sizeof(int);

					if(isNewKey && tempKey == keyCounter)
					{
						memcpy((char*)splitData.key, &floatKey, sizeof(float));
					}
					else
					{
						float check;
						memcpy(&check, (char*)newLeafPage + newLeafReadOffset, sizeof(float));
						// cout<<"Middle Key : "<<check<<endl;
						memcpy((char*)splitData.key, (char*)newLeafPage + newLeafReadOffset, sizeof(float));
						//int doubleCheck;
						//memcpy(&doubleCheck, (char*)splitData.key, sizeof(int));
						//cout<<"Double check  : "<<doubleCheck<<endl;
					}

					if(keyCounter >= splitKey)
					{
						while(tempKey < noOfKeys)
						{
							if(tempKey == keyCounter)
							{
								if(!isNewKey)
								{
									newLeafReadOffset = newLeafReadOffset + sizeof(float) + sizeof(short);
								}

								int moveDataSize = newLeafWriteOffset - newLeafReadOffset;
								void* dataToBeMoved;
								if(moveDataSize != 0)
								{
									dataToBeMoved = malloc(moveDataSize);
									memcpy(dataToBeMoved, (char*)newLeafPage + newLeafReadOffset, moveDataSize);
								}

								insertKeyRidAtOffsetFloat(floatKey, rid, newLeafPage, newLeafReadOffset, isNewKey);

								if(moveDataSize != 0)
								{
									memcpy((char*)newLeafPage + newLeafReadOffset, dataToBeMoved, moveDataSize);
									newLeafReadOffset = newLeafReadOffset + moveDataSize;
									free(dataToBeMoved);
								}
								break;
							}
							else
							{
								newLeafReadOffset = newLeafReadOffset + sizeof(float);	//skip key
								short noOfRids;
								memcpy(&noOfRids, (char*)newLeafPage + newLeafReadOffset, sizeof(short));
								newLeafReadOffset = newLeafReadOffset + sizeof(short) + noOfRids * 2*sizeof(int);
							}
							tempKey = tempKey + 1;
						}
					}
					else
					{
						newLeafReadOffset = newLeafWriteOffset;
					}


					freeBytesStartAt = oldReadOffset;
					if(keyCounter < splitKey)			//now we can insert into the old leaf since it is halved
					{
						if(placeToInsert != 0)
						{
							if(!isNewKey)
							{
								placeToInsert = placeToInsert + sizeof(short) + sizeof(float);
							}

							int moveDataSize = freeBytesStartAt - placeToInsert;
					 		void* dataToBeMoved;
					 		if(moveDataSize > 0)
					 		{
								dataToBeMoved = malloc(moveDataSize);
					 			memcpy(dataToBeMoved, (char*)node + placeToInsert, moveDataSize);
					 		}

							insertKeyRidAtOffsetFloat(floatKey, rid, node, placeToInsert, isNewKey);

					 		if(moveDataSize > 0)
					 		{
					 			memcpy((char*)node + placeToInsert, dataToBeMoved, moveDataSize);
					 			placeToInsert = placeToInsert + moveDataSize;
					 			free(dataToBeMoved);
					 		}
					 		oldReadOffset = placeToInsert;
						}
					}
				}
				else
				{
					short keyCounter = 0;
					bool isNewKey = false;

					int varcharLen;
					memcpy(&varcharLen, key, sizeof(int));

					short varcharLenShort = (short)varcharLen;

					void* varcharKeyPtr  = malloc(varcharLen);
					memcpy(varcharKeyPtr, (char*)key + sizeof(int), varcharLen);

					oldReadOffset = readOffset;

					checkIfNewKeyVarchar(node, typeOfPage, varcharKeyPtr, varcharLen, readOffset, freeBytesStartAt, noOfKeys, isNewKey, splitKey, keyCounter);
					if(noOfKeys == 1)
		 			{
		 				splitData.result = -1;
		 				return;
		 			}
					int placeToInsert = 0;
					short tempKey = 0;

					while(tempKey < splitKey)
					{
						if(tempKey == keyCounter)				//since we do not have space in the page to insert a new key, we just find a place for it
						{
							placeToInsert = oldReadOffset;
							if(!isNewKey)						//dont increment key couter when it is not a new key
							{
								short currentVarcharLength;
								memcpy(&currentVarcharLength, (char*)node + oldReadOffset, sizeof(short));

								oldReadOffset = oldReadOffset + sizeof(short) + currentVarcharLength;	//skip key
								short noOfRids;
								memcpy(&noOfRids, (char*)node + oldReadOffset, sizeof(short));
								oldReadOffset = oldReadOffset + sizeof(short) + noOfRids * 2*sizeof(int);
							}
						}
						else
						{
							short currentVarcharLength;
							memcpy(&currentVarcharLength, (char*)node + oldReadOffset, sizeof(short));

							// void* checkVarchar = malloc(currentVarcharLength);
							// memcpy(checkVarchar, (char*)node + oldReadOffset + sizeof(short), currentVarcharLength);
							// cout<<"CurrentVarchar :"<<string((char*)checkVarchar, currentVarcharLength)<<endl;

							oldReadOffset = oldReadOffset + sizeof(short) + currentVarcharLength;	//skip key
							short noOfRids;
							memcpy(&noOfRids, (char*)node + oldReadOffset, sizeof(short));
							oldReadOffset = oldReadOffset + sizeof(short) + noOfRids * 2*sizeof(int);
							// cout<<"NO of RIDS : "<<noOfRids<<endl;
						}

						tempKey = tempKey + 1;
					}

					int sizeOfDataToRemove = freeBytesStartAt - oldReadOffset;
					void* dataToBeRemoved = malloc(sizeOfDataToRemove);
					memcpy(dataToBeRemoved, (char*)node + oldReadOffset, sizeOfDataToRemove);

					memcpy((char*)newLeafPage + newLeafWriteOffset, dataToBeRemoved, sizeOfDataToRemove);
					newLeafWriteOffset = newLeafWriteOffset + sizeOfDataToRemove;
					free(dataToBeRemoved);

					tempKey = splitKey;

					if(isNewKey && tempKey == keyCounter)
					{
						void* keyToBeCopied = malloc(varcharLen + sizeof(short));
						memcpy(keyToBeCopied, &varcharLenShort, sizeof(short));

						memcpy((char*)keyToBeCopied + sizeof(short), varcharKeyPtr, varcharLen);

						memcpy((char*)splitData.key, keyToBeCopied, sizeof(short) + varcharLen);

						// cout<<"Checking if varchar was copied fine"<<endl;
						// void* checkKey = malloc(varcharLen);
						// memcpy(checkKey, (char*)splitData.key + sizeof(short), varcharLen);
						// cout<<string((char*)checkKey, varcharLen)<<endl;
					}
					else
					{
						//int check;
						//memcpy(&check, (char*)newLeafPage + newLeafReadOffset, sizeof(int));
						//cout<<"Middle Key : "<<check<<endl;
						short storedVarcharLen;
						memcpy(&storedVarcharLen, (char*)newLeafPage + newLeafReadOffset, sizeof(short));

						// void* checkVarchar = malloc(storedVarcharLen);
						// memcpy(checkVarchar, (char*)newLeafPage + newLeafReadOffset + sizeof(short), storedVarcharLen);
						// cout<<"CHECKING VARCHAR :"<<string((char*)checkVarchar, storedVarcharLen)<<endl;
						memcpy((char*)splitData.key, (char*)newLeafPage + newLeafReadOffset, sizeof(short) + storedVarcharLen);

						// void* checkKey = malloc(storedVarcharLen + sizeof(short));
						// memcpy(checkKey, splitData.key, storedVarcharLen + sizeof(short));
						// cout<<string((char*)checkKey + sizeof(short), storedVarcharLen)<<endl;

					}

					if(keyCounter >= splitKey)
					{
						while(tempKey < noOfKeys)
						{
							if(tempKey == keyCounter)
							{
								if(!isNewKey)
								{
									short currentVarcharLength;
									memcpy(&currentVarcharLength, (char*)newLeafPage + newLeafReadOffset, sizeof(short));

									newLeafReadOffset = newLeafReadOffset + currentVarcharLength + sizeof(short) + sizeof(short);
								}

								int moveDataSize = newLeafWriteOffset - newLeafReadOffset;
								void* dataToBeMoved;
								if(moveDataSize != 0)
								{
									dataToBeMoved = malloc(moveDataSize);
									memcpy(dataToBeMoved, (char*)newLeafPage + newLeafReadOffset, moveDataSize);
								}

								insertKeyRidAtOffsetVarchar(varcharKeyPtr, varcharLenShort, rid, newLeafPage, newLeafReadOffset, isNewKey);

								if(moveDataSize != 0)
								{
									memcpy((char*)newLeafPage + newLeafReadOffset, dataToBeMoved, moveDataSize);
									newLeafReadOffset = newLeafReadOffset + moveDataSize;
									free(dataToBeMoved);
								}
								break;
							}
							else
							{
								short currentVarcharLength;
								memcpy(&currentVarcharLength, (char*)newLeafPage + newLeafReadOffset, sizeof(short));
								newLeafReadOffset = newLeafReadOffset + currentVarcharLength + sizeof(short);	//skip the key

								short noOfRids;
								memcpy(&noOfRids, (char*)newLeafPage + newLeafReadOffset, sizeof(short));
								newLeafReadOffset = newLeafReadOffset + sizeof(short) + noOfRids * 2*sizeof(int);
							}
							tempKey = tempKey + 1;
						}
					}
					else
					{
						newLeafReadOffset = newLeafWriteOffset;
					}


					freeBytesStartAt = oldReadOffset;
					if(keyCounter < splitKey)			//now we can insert into the old leaf since it is halved
					{
						if(placeToInsert != 0)
						{
							short lengthOfVarchar;
							memcpy(&lengthOfVarchar, (char*)node + placeToInsert, sizeof(short));
							// void* varchar = malloc(lengthOfVarchar);
							// memcpy(varchar, (char*)node + placeToInsert + sizeof(short), lengthOfVarchar);

							// cout<<"PLACE TO INSERT VALUES  : "<<string((char*)varchar, lengthOfVarchar)<<endl;
							if(!isNewKey)
							{
								placeToInsert = placeToInsert + sizeof(short) + lengthOfVarchar + sizeof(short);
							}


							int moveDataSize = freeBytesStartAt - placeToInsert;
					 		void* dataToBeMoved;
					 		if(moveDataSize > 0)
					 		{
								dataToBeMoved = malloc(moveDataSize);
					 			memcpy(dataToBeMoved, (char*)node + placeToInsert, moveDataSize);
					 		}

							insertKeyRidAtOffsetVarchar(varcharKeyPtr, varcharLenShort, rid, node, placeToInsert, isNewKey);

					 		if(moveDataSize > 0)
					 		{
					 			memcpy((char*)node + placeToInsert, dataToBeMoved, moveDataSize);
					 			int checkSlotId;
					 			memcpy(&checkSlotId, (char*)node + placeToInsert - sizeof(int), sizeof(int));
					 			placeToInsert = placeToInsert + moveDataSize;
					 			free(dataToBeMoved);
					 		}
					 	oldReadOffset = placeToInsert;
						}
					}
				}

				//update meta data
				short newLeafNoOfKeys = noOfKeys - splitKey;
				memcpy((char*)newLeafPage + sizeof(char), &newLeafNoOfKeys, sizeof(short));

				short newLeafFreeBytesLeft = PAGE_SIZE - newLeafReadOffset;		//add the nextPageId size too here! Since in fillMeta data we fill page common meta data only
				memcpy((char*)newLeafPage + sizeof(char) + sizeof(short), &newLeafFreeBytesLeft, sizeof(short));

				//Update split page meta data
				memcpy((char*)node + sizeof(char), &splitKey, sizeof(short));

				freeBytesLeft = PAGE_SIZE - oldReadOffset;
				memcpy((char*)node + sizeof(char) + sizeof(short), &freeBytesLeft, sizeof(short));

				int newLeafPageId = ixfileHandle.fileHandle.getNumberOfPages();
				memcpy((char*)node + sizeof(char) + 2*sizeof(short), &newLeafPageId, sizeof(int));


				ixfileHandle.fileHandle.appendPage(newLeafPage);
				ixfileHandle.fileHandle.writePage(pageNo, node);

				free(newLeafPage);

				splitData.pageId = newLeafPageId;
				splitData.result = 0;

		 	}
		 }
	}
	else		//it is a non leaf page
	{
		int originalReadOffset = readOffset;	//we store this incase there is a root plit and we have to write to it
		int pageId = getPageIdForInsertionOrScan(node, readOffset, noOfKeys, attribute, key);
		//cout<<"PAGE ID :"<<pageId<<endl;
		//readOffset after this step indicates the place the new key is to be inserted
		insertEntryRecursive(ixfileHandle, pageId, attribute, key, rid, splitData);

		// cout<<"SPLIT DATA : "<<splitData.pageId<<":" <<splitData.result<<endl;
		short sizeOfKeyPgId;

		if(splitData.pageId != -1)		//means child was split
		{
			if(attribute.type == TypeInt)
			{
				sizeOfKeyPgId = 2 * sizeof(int);
			}
			else if (attribute.type == TypeReal)
			{
				sizeOfKeyPgId = sizeof(float) + sizeof(int);
			}
			else
			{
				short varcharLen;
				memcpy(&varcharLen, (char*)splitData.key, sizeof(short));
				sizeOfKeyPgId = sizeof(int) + varcharLen + sizeof(short);
			}


			if(freeBytesLeft >= sizeOfKeyPgId) //Key and pageId
			{
				int sizeOfDataToBeMoved = freeBytesStartAt - readOffset;
				void* dataToBeMoved;

				if(sizeOfDataToBeMoved != 0 )
				{
					dataToBeMoved = malloc(sizeOfDataToBeMoved);
					memcpy(dataToBeMoved, (char*)node + readOffset, sizeOfDataToBeMoved);
				}

				if(attribute.type == TypeInt)
				{
					memcpy((char*)node + readOffset, (char*)splitData.key, sizeof(int));
					readOffset = readOffset + sizeof(int);
				}
				else if(attribute.type == TypeReal)
				{
					float check;
					memcpy(&check, (char*)splitData.key, sizeof(float));
					memcpy((char*)node + readOffset, (char*)splitData.key, sizeof(float));
					readOffset = readOffset + sizeof(float);
				}
				else
				{
					memcpy((char*)node + readOffset, (char*)splitData.key, sizeOfKeyPgId - sizeof(int));
					readOffset = readOffset + sizeOfKeyPgId - sizeof(int);
				}

				memcpy((char*)node + readOffset, &splitData.pageId, sizeof(int));
				readOffset = readOffset + sizeof(int);

				if(sizeOfDataToBeMoved != 0)
				{
					memcpy((char*)node + readOffset, dataToBeMoved, sizeOfDataToBeMoved);
					readOffset = readOffset + sizeOfDataToBeMoved;
					free(dataToBeMoved);
				}
				noOfKeys = noOfKeys + 1;
				memcpy((char*)node + sizeof(char), &noOfKeys, sizeof(short));

				freeBytesLeft = PAGE_SIZE - readOffset;
				memcpy((char*)node + sizeof(char) + sizeof(short), &freeBytesLeft, sizeof(short));

				ixfileHandle.fileHandle.writePage(pageNo, node);

				splitData.result = 0;
				splitData.pageId = -1;
			}
			else
			{
				readOffset = originalReadOffset;
				if(pageNo  == 0)	// if it is the root page which is to be split
				{
					void* rootPage = malloc(PAGE_SIZE);
			 		void* leftPage = malloc(PAGE_SIZE);
			 		void* rightPage = malloc(PAGE_SIZE);

			 		int leftWriteOffset = 0;
			 		int rightWriteOffset = 0;
			 		int rootWriteOffset = 0;

			 		char typeOfNewPages = '0';
			 		fillCommonMetaData(leftPage, leftWriteOffset, typeOfNewPages);
			 		fillCommonMetaData(rightPage, rightWriteOffset, typeOfNewPages);

			 		//put all info for root node except the key
			 		fillCommonMetaData(rootPage, rootWriteOffset, typeOfNewPages);
			 		int rootPageNoOfKeys = 1;
			 		memcpy((char*)rootPage + sizeof(char), &rootPageNoOfKeys, sizeof(short));

			 		int leftPageId = ixfileHandle.fileHandle.getNumberOfPages();
			 		int rightPageId = ixfileHandle.fileHandle.getNumberOfPages() + 1;

			 		memcpy((char*)rootPage + rootWriteOffset, &leftPageId, sizeof(int));		//skipped page Id
			 		rootWriteOffset = rootWriteOffset + sizeof(int);

			 		if(attribute.type == TypeInt || attribute.type == TypeReal)
			 		{
			 			rootWriteOffset = rootWriteOffset + sizeof(int);				//note! skipped key space here along with PageId
			 			memcpy((char*)rootPage + rootWriteOffset, &rightPageId, sizeof(int));
			 			rootWriteOffset = rootWriteOffset + sizeof(int);
			 		}


			 		short leftPageNoOfKeys = 0;
			 		short rightPageNoOfKeys = 0;

			 		int oldReadOffset = readOffset;

			 		short splitKey;

			 		if(attribute.type == TypeInt)
			 		{
			 			short keyCounter = 0;
			 			bool isNewKey = false;

			 			int returnedKey;
			 			memcpy(&returnedKey, (char*)splitData.key, sizeof(int));

			 			checkIfNewKeyInt(node, typeOfPage, returnedKey, readOffset, freeBytesStartAt, noOfKeys, isNewKey, splitKey, keyCounter);
			 			short currentKeyPointer = 0;

			 			memcpy((char*)leftPage + leftWriteOffset, (char*)node + oldReadOffset, sizeof(int));
			 			leftWriteOffset = leftWriteOffset + sizeof(int);		//write first pageId

			 			oldReadOffset = oldReadOffset + sizeof(int);

		 				while(currentKeyPointer < noOfKeys)
		 				{
		 					//Root Node Key
		 					if(currentKeyPointer == splitKey)		//copy up to root
							{
		 						if(currentKeyPointer != keyCounter)
		 						{
		 							int rootKey;
									memcpy(&rootKey, (char*)node + oldReadOffset, sizeof(int));				//read the key and not the pageId to write into root
									memcpy((char*)rootPage + rootWriteOffset - 2*sizeof(int), &rootKey, sizeof(int));

									//write only the pageId corresponding to this key on the non root index node
									int checkPageId;
									memcpy(&checkPageId, (char*)node + oldReadOffset + sizeof(int), sizeof(int));
									memcpy((char*)rightPage + rightWriteOffset, (char*)node + oldReadOffset + sizeof(int), sizeof(int));
									rightWriteOffset = rightWriteOffset + sizeof(int);
									oldReadOffset = oldReadOffset + 2*sizeof(int);											//skip the page and key both

		 						}
		 						else
		 						{
		 							memcpy((char*)rootPage + rootWriteOffset - 2*sizeof(int), (char*)splitData.key, sizeof(int));

		 							memcpy((char*)rightPage + rightWriteOffset, &splitData.pageId, sizeof(int));
									rightWriteOffset = rightWriteOffset + sizeof(int);

		 						}
		 						currentKeyPointer = currentKeyPointer + 1;
		 						continue;

							}

		 					//oldReadOffset points to the place where the first key starts from

		 					if(currentKeyPointer != keyCounter)		//write the already existing data, not the new data
		 					{
								if(currentKeyPointer < splitKey)
								{
									int leftKey;
									memcpy(&leftKey, (char*)node + oldReadOffset, sizeof(int));
									// cout<<"Left Key : "<<leftKey<<endl;
									memcpy((char*)leftPage + leftWriteOffset, (char*)node + oldReadOffset, 2 * sizeof(int));		//write the key and the pageId
									leftWriteOffset = leftWriteOffset + 2*sizeof(int);
									leftPageNoOfKeys = leftPageNoOfKeys + 1;
								}
								else
								{
									int rightKey;
									memcpy(&rightKey, (char*)node + oldReadOffset, sizeof(int));
									// cout<<"Right Key : "<<rightKey<<endl;

									memcpy((char*)rightPage + rightWriteOffset, (char*)node + oldReadOffset, 2 * sizeof(int));
									rightWriteOffset = rightWriteOffset + 2*sizeof(int);
									rightPageNoOfKeys = rightPageNoOfKeys + 1;
								}

								oldReadOffset = oldReadOffset + 2*sizeof(int);
		 					}
		 					else		//this is the position the new key fits into the already existing keys
		 					{

		 						int checkKey;
		 						memcpy(&checkKey, (char*)splitData.key, sizeof(int));

		 						if(currentKeyPointer < splitKey)
								{
									memcpy((char*)leftPage + leftWriteOffset, (char*)splitData.key, sizeof(int));
									leftWriteOffset = leftWriteOffset + sizeof(int);
									memcpy((char*)leftPage + leftWriteOffset, &splitData.pageId, sizeof(int));
									leftWriteOffset = leftWriteOffset + sizeof(int);
									leftPageNoOfKeys = leftPageNoOfKeys + 1;
								}
								else
								{
									memcpy((char*)rightPage + rightWriteOffset, (char*)splitData.key, sizeof(int));
									rightWriteOffset = rightWriteOffset + sizeof(int);
									memcpy((char*)rightPage + rightWriteOffset, &splitData.pageId, sizeof(int));
									rightWriteOffset = rightWriteOffset + sizeof(int);
									rightPageNoOfKeys = rightPageNoOfKeys + 1;
								}
	 						}

							currentKeyPointer = currentKeyPointer + 1;
		 				}

			 			//Change meta data
		 				//Root
						//update free bytes left
						short rootPageFreeBytesLeft = PAGE_SIZE - rootWriteOffset;
		 				memcpy((char*)rootPage + sizeof(char) + sizeof(short), &rootPageFreeBytesLeft, sizeof(short));

		 				//Left Page :
		 				//update number of keys
		 				memcpy((char*)leftPage + sizeof(char), &leftPageNoOfKeys, sizeof(short));
		 				//update free bytes left
			 			short leftPageFreeBytesLeft = PAGE_SIZE - leftWriteOffset;
			 			memcpy((char*)leftPage + sizeof(char) + sizeof(short), &leftPageFreeBytesLeft, sizeof(short));

		 				//Right Page :
		 				//update number of keys
		 				memcpy((char*)rightPage + sizeof(char), &rightPageNoOfKeys, sizeof(short));
		 				//update free bytes left
			 			short rightPageFreeBytesLeft = PAGE_SIZE - rightWriteOffset;
			 			memcpy((char*)rightPage + sizeof(char) + sizeof(short), &rightPageFreeBytesLeft, sizeof(short));

			 			ixfileHandle.fileHandle.writePage(0, rootPage);
			 			ixfileHandle.fileHandle.appendPage(leftPage);
			 			ixfileHandle.fileHandle.appendPage(rightPage);

			 			free(rootPage);
			 			free(leftPage);
			 			free(rightPage);
			 			splitData.result = 0;
			 			// splitData.pageId = -1;
			 		}
			 		else if(attribute.type == TypeReal)
			 		{
			 			short keyCounter = 0;
			 			bool isNewKey = false;

			 			float returnedKey;
			 			memcpy(&returnedKey, (char*)splitData.key, sizeof(float));

			 			checkIfNewKeyFloat(node, typeOfPage, returnedKey, readOffset, freeBytesStartAt, noOfKeys, isNewKey, splitKey, keyCounter);
			 			short currentKeyPointer = 0;

			 			memcpy((char*)leftPage + leftWriteOffset, (char*)node + oldReadOffset, sizeof(int));
			 			leftWriteOffset = leftWriteOffset + sizeof(int);		//write first pageId

			 			oldReadOffset = oldReadOffset + sizeof(int);

		 				while(currentKeyPointer < noOfKeys)
		 				{
		 					//Root Node Key
		 					if(currentKeyPointer == splitKey)		//copy up to root
							{
		 						if(currentKeyPointer != keyCounter)
		 						{
		 							float rootKey;
									memcpy(&rootKey, (char*)node + oldReadOffset, sizeof(float));				//read the key and not the pageId to write into root
									memcpy((char*)rootPage + rootWriteOffset - 2*sizeof(int), &rootKey, sizeof(float));

									//write only the pageId corresponding to this key on the non root index node
									memcpy((char*)rightPage + rightWriteOffset, (char*)node + oldReadOffset + sizeof(float), sizeof(int));
									rightWriteOffset = rightWriteOffset + sizeof(int);
									oldReadOffset = oldReadOffset + sizeof(float) + sizeof(int);											//skip the page and key both

		 						}
		 						else
		 						{
		 							memcpy((char*)rootPage + rootWriteOffset - sizeof(float) - sizeof(int), (char*)splitData.key, sizeof(float));

		 							memcpy((char*)rightPage + rightWriteOffset, &splitData.pageId, sizeof(int));
									rightWriteOffset = rightWriteOffset + sizeof(int);
		 						}
		 						currentKeyPointer = currentKeyPointer + 1;
		 						continue;

							}

		 					//oldReadOffset points to the place where the first key starts from

		 					if(currentKeyPointer != keyCounter)		//write the already existing data, not the new data
		 					{
								if(currentKeyPointer < splitKey)
								{
									memcpy((char*)leftPage + leftWriteOffset, (char*)node + oldReadOffset, 2 * sizeof(int));		//write the key and the pageId
									leftWriteOffset = leftWriteOffset + 2*sizeof(int);
									leftPageNoOfKeys = leftPageNoOfKeys + 1;
								}
								else
								{
									memcpy((char*)rightPage + rightWriteOffset, (char*)node + oldReadOffset, 2 * sizeof(int));
									rightWriteOffset = rightWriteOffset + 2*sizeof(int);
									rightPageNoOfKeys = rightPageNoOfKeys + 1;
								}

								oldReadOffset = oldReadOffset + 2*sizeof(int);
		 					}
		 					else		//this is the position the new key fits into the already existing keys
		 					{
		 						if(currentKeyPointer < splitKey)
								{
									memcpy((char*)leftPage + leftWriteOffset, (char*)splitData.key, sizeof(float));
									leftWriteOffset = leftWriteOffset + sizeof(float);
									memcpy((char*)leftPage + leftWriteOffset, &splitData.pageId, sizeof(int));
									leftWriteOffset = leftWriteOffset + sizeof(int);
									leftPageNoOfKeys = leftPageNoOfKeys + 1;
								}
								else
								{
									memcpy((char*)rightPage + rightWriteOffset, (char*)splitData.key, sizeof(float));
									rightWriteOffset = rightWriteOffset + sizeof(float);
									memcpy((char*)rightPage + rightWriteOffset, &splitData.pageId, sizeof(int));
									rightWriteOffset = rightWriteOffset + sizeof(int);
									rightPageNoOfKeys = rightPageNoOfKeys + 1;
								}
	 						}

							currentKeyPointer = currentKeyPointer + 1;
		 				}

			 			//Change meta data
		 				//Root
						//update free bytes left
						short rootPageFreeBytesLeft = PAGE_SIZE - rootWriteOffset;
		 				memcpy((char*)rootPage + sizeof(char) + sizeof(short), &rootPageFreeBytesLeft, sizeof(short));

		 				//Left Page :
		 				//update number of keys
		 				memcpy((char*)leftPage + sizeof(char), &leftPageNoOfKeys, sizeof(short));
		 				//update free bytes left
			 			short leftPageFreeBytesLeft = PAGE_SIZE - leftWriteOffset;
			 			memcpy((char*)leftPage + sizeof(char) + sizeof(short), &leftPageFreeBytesLeft, sizeof(short));

		 				//Right Page :
		 				//update number of keys
		 				memcpy((char*)rightPage + sizeof(char), &rightPageNoOfKeys, sizeof(short));
		 				//update free bytes left
			 			short rightPageFreeBytesLeft = PAGE_SIZE - rightWriteOffset;
			 			memcpy((char*)rightPage + sizeof(char) + sizeof(short), &rightPageFreeBytesLeft, sizeof(short));

			 			ixfileHandle.fileHandle.writePage(0, rootPage);
			 			ixfileHandle.fileHandle.appendPage(leftPage);
			 			ixfileHandle.fileHandle.appendPage(rightPage);

			 			free(rootPage);
			 			free(leftPage);
			 			free(rightPage);
			 			splitData.result = 0;
			 			// splitData.pageId = -1;
			 		}
			 		else
			 		{
			 			int oldReadOffset = readOffset;
			 			short keyCounter = 0;
			 			bool isNewKey = false;

						short varcharLengthShort;
						memcpy(&varcharLengthShort, (char*)splitData.key, sizeof(short));
						int varcharLength = (int)varcharLengthShort;
			 			void *varcharKeyPtr = malloc(varcharLength);
			 			memcpy(varcharKeyPtr, (char*)splitData.key + sizeof(short), varcharLengthShort);

			 			short splitKey;

			 			checkIfNewKeyVarchar(node, typeOfPage, varcharKeyPtr, varcharLength, readOffset, freeBytesStartAt, noOfKeys, isNewKey, splitKey, keyCounter);
			 			short currentKeyPointer = 0;

			 			memcpy((char*)leftPage + leftWriteOffset, (char*)node + oldReadOffset, sizeof(int));
			 			leftWriteOffset = leftWriteOffset + sizeof(int);		//write first pageId

			 			oldReadOffset = oldReadOffset + sizeof(int);

		 				while(currentKeyPointer < noOfKeys)
		 				{
		 					//Root Node Key
		 					if(currentKeyPointer == splitKey)		//copy up to root
							{
		 						if(currentKeyPointer != keyCounter)
		 						{
		 							short currentVarcharLength;
		 							memcpy(&currentVarcharLength, (char*)node + oldReadOffset, sizeof(short));
									memcpy((char*)rootPage + rootWriteOffset, (char*)node + oldReadOffset, sizeof(short) + currentVarcharLength);
									rootWriteOffset = rootWriteOffset + sizeof(short) + currentVarcharLength;
									oldReadOffset = oldReadOffset + sizeof(short) + currentVarcharLength;

									//write only the pageId corresponding to this key on the non root index node
									memcpy((char*)rightPage + rightWriteOffset, (char*)node + oldReadOffset, sizeof(int));
									rightWriteOffset = rightWriteOffset + sizeof(int);
									oldReadOffset = oldReadOffset + sizeof(int);

													//skip the page and key both
		 						}
		 						else
		 						{
		 							short currentVarcharLength;
		 							memcpy(&currentVarcharLength, (char*)splitData.key, sizeof(short));
		 							memcpy((char*)rootPage + rootWriteOffset, (char*)splitData.key, sizeof(short) + currentVarcharLength);
		 							rootWriteOffset = rootWriteOffset + sizeof(short) + currentVarcharLength;


		 							memcpy((char*)rightPage + rightWriteOffset, &splitData.pageId, sizeof(int));
									rightWriteOffset = rightWriteOffset + sizeof(int);

		 						}

		 						memcpy((char*)rootPage + rootWriteOffset, &rightPageId, sizeof(int));
			 					rootWriteOffset = rootWriteOffset + sizeof(int);

		 						currentKeyPointer = currentKeyPointer + 1;
		 						continue;

							}

		 					//oldReadOffset points to the place where the first key starts from

		 					if(currentKeyPointer != keyCounter)		//write the already existing data, not the new data
		 					{
								if(currentKeyPointer < splitKey)
								{
									short leftKeyLength;
									memcpy(&leftKeyLength, (char*)node + oldReadOffset, sizeof(short));

									// cout<<"Left Key : "<<leftKey<<endl;
									memcpy((char*)leftPage + leftWriteOffset, (char*)node + oldReadOffset, sizeof(short) + leftKeyLength + sizeof(int));		//write the key and the pageId
									leftWriteOffset = leftWriteOffset + sizeof(short) + leftKeyLength + sizeof(int);
									leftPageNoOfKeys = leftPageNoOfKeys + 1;
									oldReadOffset = oldReadOffset + sizeof(short) + leftKeyLength + sizeof(int);

								}
								else
								{
									short rightKeyLength;
									memcpy(&rightKeyLength, (char*)node + oldReadOffset, sizeof(short));
									// cout<<"Right Key : "<<rightKey<<endl;

									memcpy((char*)rightPage + rightWriteOffset, (char*)node + oldReadOffset, sizeof(short) + rightKeyLength + sizeof(int));
									rightWriteOffset = rightWriteOffset + sizeof(short) + rightKeyLength + sizeof(int);
									rightPageNoOfKeys = rightPageNoOfKeys + 1;
									oldReadOffset = oldReadOffset + sizeof(short) + rightKeyLength + sizeof(int);

								}

		 					}
		 					else		//this is the position the new key fits into the already existing keys
		 					{

		 						if(currentKeyPointer < splitKey)
								{
									memcpy((char*)leftPage + leftWriteOffset, (char*)splitData.key, sizeof(short) + varcharLength);
									leftWriteOffset = leftWriteOffset + sizeof(short) + varcharLength;
									memcpy((char*)leftPage + leftWriteOffset, &splitData.pageId, sizeof(int));
									leftWriteOffset = leftWriteOffset + sizeof(int);
									leftPageNoOfKeys = leftPageNoOfKeys + 1;
								}
								else
								{
									memcpy((char*)rightPage + rightWriteOffset, (char*)splitData.key, sizeof(short) + varcharLength);
									rightWriteOffset = rightWriteOffset + sizeof(short) + varcharLength;
									memcpy((char*)rightPage + rightWriteOffset, &splitData.pageId, sizeof(int));
									rightWriteOffset = rightWriteOffset + sizeof(int);
									rightPageNoOfKeys = rightPageNoOfKeys + 1;
								}
	 						}

							currentKeyPointer = currentKeyPointer + 1;
		 				}

			 			//Change meta data
		 				//Root
						//update free bytes left
						short rootPageFreeBytesLeft = PAGE_SIZE - rootWriteOffset;
		 				memcpy((char*)rootPage + sizeof(char) + sizeof(short), &rootPageFreeBytesLeft, sizeof(short));

		 				//Left Page :
		 				//update number of keys
		 				memcpy((char*)leftPage + sizeof(char), &leftPageNoOfKeys, sizeof(short));
		 				//update free bytes left
			 			short leftPageFreeBytesLeft = PAGE_SIZE - leftWriteOffset;
			 			memcpy((char*)leftPage + sizeof(char) + sizeof(short), &leftPageFreeBytesLeft, sizeof(short));

		 				//Right Page :
		 				//update number of keys
		 				memcpy((char*)rightPage + sizeof(char), &rightPageNoOfKeys, sizeof(short));
		 				//update free bytes left
			 			short rightPageFreeBytesLeft = PAGE_SIZE - rightWriteOffset;
			 			memcpy((char*)rightPage + sizeof(char) + sizeof(short), &rightPageFreeBytesLeft, sizeof(short));

			 			ixfileHandle.fileHandle.writePage(0, rootPage);
			 			ixfileHandle.fileHandle.appendPage(leftPage);
			 			ixfileHandle.fileHandle.appendPage(rightPage);

			 			free(rootPage);
			 			free(leftPage);
			 			free(rightPage);
			 			splitData.result = 0;
			 			// splitData.pageId = -1;
			 		}
				}
				else 				//the page to split is a non root index page
				{
			 		void* newIndexPage =  malloc(PAGE_SIZE);

			 		char newIndexPageType = '0';
			 		int newIndexPageWriteOffset = 0;
			 		fillCommonMetaData(newIndexPage, newIndexPageWriteOffset, newIndexPageType);

			 		if(attribute.type == TypeInt)
					{
						short keyCounter = 0;
						bool isNewKey = false;

						int intKey;
						memcpy(&intKey, (char*)splitData.key, sizeof(int));		//we have to insert the splitData.key

						short splitKey;
						int oldReadOffset = readOffset;

						checkIfNewKeyInt(node, typeOfPage, intKey, readOffset, freeBytesStartAt, noOfKeys, isNewKey, splitKey, keyCounter);

						int placeToInsert = 0;
						short tempKey = 0;

						oldReadOffset = oldReadOffset + sizeof(int);			//skip first pageId
						while(tempKey < splitKey)
						{
							if(tempKey == keyCounter)				//since we do not have space in the page to insert a new key, we just find a place for it and keep
							{
								placeToInsert = oldReadOffset;
							}
							else
							{
								oldReadOffset = oldReadOffset + 2*sizeof(int);
							}

							tempKey = tempKey + 1;
						}

						tempKey = splitKey;

						if(tempKey == keyCounter)
						{
							memcpy((char*)splitData.key, &intKey, sizeof(int));

							//copy the splitData.pageId into the new non index leaf
							memcpy((char*)newIndexPage + newIndexPageWriteOffset, &splitData.pageId, sizeof(int));
							newIndexPageWriteOffset = newIndexPageWriteOffset + sizeof(int);

						}
						else
						{
							memcpy((char*)splitData.key, (char*)node + oldReadOffset, sizeof(int));
							oldReadOffset = oldReadOffset + sizeof(int);
						}

						tempKey = tempKey + 1;					//we do not want this key to appear in the orginal and the new index pages since it will be pushed up

						int sizeOfDataToRemove = freeBytesStartAt - oldReadOffset;		//this helps us copy the pg id for the key pushed up
						void* dataToBeRemoved = malloc(sizeOfDataToRemove);
						memcpy(dataToBeRemoved, (char*)node + oldReadOffset, sizeOfDataToRemove);

						memcpy((char*)newIndexPage + newIndexPageWriteOffset, dataToBeRemoved, sizeOfDataToRemove);
						newIndexPageWriteOffset = newIndexPageWriteOffset + sizeOfDataToRemove;
						free(dataToBeRemoved);

						int newIndexPageReadOffset = sizeof(char) + 2*sizeof(short) + sizeof(int);	//skip Page Id too
						if(keyCounter >= splitKey)
						{
							while(tempKey < noOfKeys)
							{
								if(tempKey == keyCounter)
								{
									int moveDataSize = newIndexPageWriteOffset - newIndexPageReadOffset;
									void* dataToBeMoved;
									if(moveDataSize != 0)
									{
										dataToBeMoved = malloc(moveDataSize);
										memcpy(dataToBeMoved, (char*)newIndexPage + newIndexPageReadOffset, moveDataSize);
									}

									memcpy((char*)newIndexPage + newIndexPageReadOffset, &intKey, sizeof(int));
									newIndexPageReadOffset = newIndexPageReadOffset + sizeof(int);
									memcpy((char*)newIndexPage + newIndexPageReadOffset, &splitData.pageId, sizeof(int));
									newIndexPageReadOffset = newIndexPageReadOffset + sizeof(int);

									if(moveDataSize != 0)
									{
										memcpy((char*)newIndexPage + newIndexPageReadOffset, dataToBeMoved, moveDataSize);
										newIndexPageReadOffset = newIndexPageReadOffset + moveDataSize;
										free(dataToBeMoved);
									}
									break;
								}
								else
								{
									newIndexPageReadOffset = newIndexPageReadOffset + 2*sizeof(int);	//skip key and PageID
								}
								tempKey = tempKey + 1;
							}
						}
						else
						{
							newIndexPageReadOffset = newIndexPageWriteOffset;
						}

						freeBytesStartAt = oldReadOffset;
						if(keyCounter < splitKey)			//now we can insert into the old leaf since it is halved
						{
							if(placeToInsert != 0)
							{
								int moveDataSize = freeBytesStartAt - placeToInsert;
						 		void* dataToBeMoved;
						 		if(moveDataSize > 0)
						 		{
									dataToBeMoved = malloc(moveDataSize);
						 			memcpy(dataToBeMoved, (char*)node + placeToInsert, moveDataSize);
						 		}

								memcpy((char*)node + placeToInsert, &intKey, sizeof(int));
								placeToInsert = placeToInsert + sizeof(int);
								memcpy((char*)node + placeToInsert, &splitData.pageId, sizeof(int));
								placeToInsert = placeToInsert + sizeof(int);

						 		if(moveDataSize > 0)
						 		{
						 			memcpy((char*)node + placeToInsert, dataToBeMoved, moveDataSize);
						 			placeToInsert = placeToInsert + moveDataSize;
						 			free(dataToBeMoved);
						 		}

						 		oldReadOffset = placeToInsert;
							}
						}

						//update meta data
						short newIndexPageNoOfKeys = noOfKeys - splitKey - 1;				//since the spilt key is copied to the root by setting splitData.key
						memcpy((char*)newIndexPage + sizeof(char), &newIndexPageNoOfKeys, sizeof(short));

						short newIndexPageFreeBytesLeft = PAGE_SIZE - newIndexPageReadOffset;		//add the nextPageId size too here! Since in fillMeta data we fill page common meta data only
						memcpy((char*)newIndexPage + sizeof(char) + sizeof(short), &newIndexPageFreeBytesLeft, sizeof(short));

						//Update split page meta data
						memcpy((char*)node + sizeof(char), &splitKey, sizeof(short));

						freeBytesLeft = PAGE_SIZE - oldReadOffset;
						memcpy((char*)node + sizeof(char) + sizeof(short), &freeBytesLeft, sizeof(short));

						int newIndexPageId = ixfileHandle.fileHandle.getNumberOfPages();

						ixfileHandle.fileHandle.appendPage(newIndexPage);
						ixfileHandle.fileHandle.writePage(pageNo, node);

						free(newIndexPage);

						splitData.pageId = newIndexPageId;			//splitData.key is already set up
						splitData.result = 0;
					}
					else if (attribute.type == TypeReal)
					{
						short keyCounter = 0;
						bool isNewKey = false;

						float floatKey;
						memcpy(&floatKey, (char*)splitData.key, sizeof(float));		//we have to insert the splitData.key

						short splitKey;
						int oldReadOffset = readOffset;

						checkIfNewKeyFloat(node, typeOfPage, floatKey, readOffset, freeBytesStartAt, noOfKeys, isNewKey, splitKey, keyCounter);

						int placeToInsert = 0;
						short tempKey = 0;

						oldReadOffset = oldReadOffset + sizeof(int);			//skip first pageId
						while(tempKey < splitKey)
						{
							if(tempKey == keyCounter)				//since we do not have space in the page to insert a new key, we just find a place for it and keep
							{
								placeToInsert = oldReadOffset;
							}
							else
							{
								oldReadOffset = oldReadOffset + sizeof(float) + sizeof(int);
							}

							tempKey = tempKey + 1;
						}

						tempKey = splitKey;

						if(tempKey == keyCounter)
						{
							memcpy((char*)splitData.key, &floatKey, sizeof(float));

							memcpy((char*)newIndexPage + newIndexPageWriteOffset, &splitData.pageId, sizeof(int));
							newIndexPageWriteOffset = newIndexPageWriteOffset + sizeof(int);
						}
						else
						{
							memcpy((char*)splitData.key, (char*)node + oldReadOffset, sizeof(float));
							oldReadOffset = oldReadOffset + sizeof(float);
						}

						tempKey = tempKey + 1;					//we do not want this key to appear in the orginal and the new index pages since it will be pushed up

						int sizeOfDataToRemove = freeBytesStartAt - oldReadOffset;		//this helps us copy the pg id for the key pushed up
						void* dataToBeRemoved = malloc(sizeOfDataToRemove);
						memcpy(dataToBeRemoved, (char*)node + oldReadOffset, sizeOfDataToRemove);

						memcpy((char*)newIndexPage + newIndexPageWriteOffset, dataToBeRemoved, sizeOfDataToRemove);
						newIndexPageWriteOffset = newIndexPageWriteOffset + sizeOfDataToRemove;
						free(dataToBeRemoved);

						int newIndexPageReadOffset = sizeof(char) + 2*sizeof(short) + sizeof(int);	//skip Page Id too
						if(keyCounter >= splitKey)
						{
							while(tempKey < noOfKeys)
							{
								if(tempKey == keyCounter)
								{
									int moveDataSize = newIndexPageWriteOffset - newIndexPageReadOffset;
									void* dataToBeMoved;
									if(moveDataSize != 0)
									{
										dataToBeMoved = malloc(moveDataSize);
										memcpy(dataToBeMoved, (char*)newIndexPage + newIndexPageReadOffset, moveDataSize);
									}

									memcpy((char*)newIndexPage + newIndexPageReadOffset, &floatKey, sizeof(float));
									newIndexPageReadOffset = newIndexPageReadOffset + sizeof(float);
									memcpy((char*)newIndexPage + newIndexPageReadOffset, &splitData.pageId, sizeof(int));
									newIndexPageReadOffset = newIndexPageReadOffset + sizeof(int);

									if(moveDataSize != 0)
									{
										memcpy((char*)newIndexPage + newIndexPageReadOffset, dataToBeMoved, moveDataSize);
										newIndexPageReadOffset = newIndexPageReadOffset + moveDataSize;
										free(dataToBeMoved);
									}
									break;
								}
								else
								{
									newIndexPageReadOffset = newIndexPageReadOffset + sizeof(float) + sizeof(int);	//skip key and PageID
								}
								tempKey = tempKey + 1;
							}
						}
						else
						{
							newIndexPageReadOffset = newIndexPageWriteOffset;
						}

						freeBytesStartAt = oldReadOffset;
						if(keyCounter < splitKey)			//now we can insert into the old leaf since it is halved
						{
							if(placeToInsert != 0)
							{
								int moveDataSize = freeBytesStartAt - placeToInsert;
						 		void* dataToBeMoved;
						 		if(moveDataSize > 0)
						 		{
									dataToBeMoved = malloc(moveDataSize);
						 			memcpy(dataToBeMoved, (char*)node + placeToInsert, moveDataSize);
						 		}

								memcpy((char*)node + placeToInsert, &floatKey, sizeof(float));
								placeToInsert = placeToInsert + sizeof(float);
								memcpy((char*)node + placeToInsert, &splitData.pageId, sizeof(int));
								placeToInsert = placeToInsert + sizeof(int);

						 		if(moveDataSize > 0)
						 		{
						 			memcpy((char*)node + placeToInsert, dataToBeMoved, moveDataSize);
						 			placeToInsert = placeToInsert + moveDataSize;
						 			free(dataToBeMoved);
						 		}

						 		oldReadOffset = placeToInsert;
							}
						}

						//update meta data
						short newIndexPageNoOfKeys = noOfKeys - splitKey - 1;				//since the spilt key is copied to the root by setting splitData.key
						memcpy((char*)newIndexPage + sizeof(char), &newIndexPageNoOfKeys, sizeof(short));

						short newIndexPageFreeBytesLeft = PAGE_SIZE - newIndexPageReadOffset;		//add the nextPageId size too here! Since in fillMeta data we fill page common meta data only
						memcpy((char*)newIndexPage + sizeof(char) + sizeof(short), &newIndexPageFreeBytesLeft, sizeof(short));

						//Update split page meta data
						memcpy((char*)node + sizeof(char), &splitKey, sizeof(short));

						freeBytesLeft = PAGE_SIZE - oldReadOffset;
						memcpy((char*)node + sizeof(char) + sizeof(short), &freeBytesLeft, sizeof(short));

						int newIndexPageId = ixfileHandle.fileHandle.getNumberOfPages();

						ixfileHandle.fileHandle.appendPage(newIndexPage);
						ixfileHandle.fileHandle.writePage(pageNo, node);

						free(newIndexPage);

						splitData.pageId = newIndexPageId;			//splitData.key is already set up
						splitData.result = 0;
					}
					else
					{
						short keyCounter = 0;
						bool isNewKey = false;

			 			short varcharLengthShort;
						memcpy(&varcharLengthShort, (char*)splitData.key, sizeof(short));
						int varcharLength = (int)varcharLengthShort;

			 			void *varcharKeyPtr = malloc(varcharLength);
			 			memcpy(varcharKeyPtr, (char*)splitData.key + sizeof(short), varcharLength);

						short splitKey;
						int oldReadOffset = readOffset;

						checkIfNewKeyVarchar(node, typeOfPage, varcharKeyPtr, varcharLength, readOffset, freeBytesStartAt, noOfKeys, isNewKey, splitKey, keyCounter);

						int placeToInsert = 0;
						short tempKey = 0;

						oldReadOffset = oldReadOffset + sizeof(int);			//skip first pageId
						while(tempKey < splitKey)
						{
							if(tempKey == keyCounter)				//since we do not have space in the page to insert a new key, we just find a place for it and keep
							{
								placeToInsert = oldReadOffset;
							}
							else
							{
								short currentVarcharLength;
								memcpy(&currentVarcharLength, (char*)node + oldReadOffset, sizeof(short));
								oldReadOffset = oldReadOffset + sizeof(short) + currentVarcharLength + sizeof(int);		//skip key and its corresponding pageId
							}

							tempKey = tempKey + 1;
						}

						tempKey = splitKey;

						if(tempKey == keyCounter)
						{

							//splitData Key is same as earlier split data key
							//Redundant code

							// cout<<"Middle Key : "<<string((char*)varcharKeyPtr, varcharLength)<<endl;
							// memcpy((char*)splitData.key, &varcharLengthShort, sizeof(short));
							// memcpy((char*)splitData.key + sizeof(short), varcharKeyPtr, varcharLength);


							memcpy((char*)newIndexPage + newIndexPageWriteOffset, &splitData.pageId, sizeof(int));
							newIndexPageWriteOffset = newIndexPageWriteOffset + sizeof(int);
						}
						else
						{
							short currentVarcharLength;
							memcpy(&currentVarcharLength, (char*)node + oldReadOffset, sizeof(short));
							memcpy((char*)splitData.key, (char*)node + oldReadOffset, sizeof(short) + currentVarcharLength);
							oldReadOffset = oldReadOffset + sizeof(short) + currentVarcharLength;
						}

						tempKey = tempKey + 1;					//we do not want this key to appear in the orginal and the new index pages since it will be pushed up

						int sizeOfDataToRemove = freeBytesStartAt - oldReadOffset;		//this helps us copy the pg id for the key pushed up
						void* dataToBeRemoved = malloc(sizeOfDataToRemove);
						memcpy(dataToBeRemoved, (char*)node + oldReadOffset, sizeOfDataToRemove);

						memcpy((char*)newIndexPage + newIndexPageWriteOffset, dataToBeRemoved, sizeOfDataToRemove);
						newIndexPageWriteOffset = newIndexPageWriteOffset + sizeOfDataToRemove;
						free(dataToBeRemoved);

						int newIndexPageReadOffset = sizeof(char) + 2*sizeof(short) + sizeof(int);	//skip Page Id too
						if(keyCounter >= splitKey)
						{
							while(tempKey < noOfKeys)
							{
								if(tempKey == keyCounter)
								{
									int moveDataSize = newIndexPageWriteOffset - newIndexPageReadOffset;
									void* dataToBeMoved;
									if(moveDataSize != 0)
									{
										dataToBeMoved = malloc(moveDataSize);
										memcpy(dataToBeMoved, (char*)newIndexPage + newIndexPageReadOffset, moveDataSize);
									}

									memcpy((char*)newIndexPage + newIndexPageReadOffset, (char*)splitData.key, sizeof(short) + varcharLength);
									newIndexPageReadOffset = newIndexPageReadOffset + sizeof(short) + varcharLength;

									memcpy((char*)newIndexPage + newIndexPageReadOffset, &splitData.pageId, sizeof(int));
									newIndexPageReadOffset = newIndexPageReadOffset + sizeof(int);

									if(moveDataSize != 0)
									{
										memcpy((char*)newIndexPage + newIndexPageReadOffset, dataToBeMoved, moveDataSize);
										newIndexPageReadOffset = newIndexPageReadOffset + moveDataSize;
										free(dataToBeMoved);
									}
									break;
								}
								else
								{
									short currentVarcharLength;
									memcpy(&currentVarcharLength, (char*)newIndexPage + newIndexPageReadOffset, sizeof(short));

									newIndexPageReadOffset = newIndexPageReadOffset + sizeof(short) + currentVarcharLength + sizeof(int);	//skip key and PageID
								}
								tempKey = tempKey + 1;
							}
						}
						else
						{
							newIndexPageReadOffset = newIndexPageWriteOffset;
						}

						freeBytesStartAt = oldReadOffset;
						if(keyCounter < splitKey)			//now we can insert into the old leaf since it is halved
						{
							if(placeToInsert != 0)
							{
								int moveDataSize = freeBytesStartAt - placeToInsert;
						 		void* dataToBeMoved;
						 		if(moveDataSize > 0)
						 		{
									dataToBeMoved = malloc(moveDataSize);
						 			memcpy(dataToBeMoved, (char*)node + placeToInsert, moveDataSize);
						 		}

								memcpy((char*)node + placeToInsert, (char*)splitData.key, sizeof(short) + varcharLength);
								placeToInsert = placeToInsert + sizeof(short) + varcharLength;

								memcpy((char*)node + placeToInsert, &splitData.pageId, sizeof(int));
								placeToInsert = placeToInsert + sizeof(int);

						 		if(moveDataSize > 0)
						 		{
						 			memcpy((char*)node + placeToInsert, dataToBeMoved, moveDataSize);
						 			placeToInsert = placeToInsert + moveDataSize;
						 			free(dataToBeMoved);
						 		}

						 		oldReadOffset = placeToInsert;
							}
						}

						//update meta data
						short newIndexPageNoOfKeys = noOfKeys - splitKey - 1;				//since the spilt key is copied to the root by setting splitData.key
						memcpy((char*)newIndexPage + sizeof(char), &newIndexPageNoOfKeys, sizeof(short));

						short newIndexPageFreeBytesLeft = PAGE_SIZE - newIndexPageReadOffset;		//add the nextPageId size too here! Since in fillMeta data we fill page common meta data only
						memcpy((char*)newIndexPage + sizeof(char) + sizeof(short), &newIndexPageFreeBytesLeft, sizeof(short));

						//Update split page meta data
						memcpy((char*)node + sizeof(char), &splitKey, sizeof(short));

						freeBytesLeft = PAGE_SIZE - oldReadOffset;
						memcpy((char*)node + sizeof(char) + sizeof(short), &freeBytesLeft, sizeof(short));

						int newIndexPageId = ixfileHandle.fileHandle.getNumberOfPages();

						ixfileHandle.fileHandle.appendPage(newIndexPage);
						ixfileHandle.fileHandle.writePage(pageNo, node);

						free(newIndexPage);

						splitData.pageId = newIndexPageId;			//splitData.key is already set up
						splitData.result = 0;
					}
			 	}
			}
		}

	}
	free(node);
}

int IndexManager::getPageIdForInsertionOrScan(void* node, int &readOffset, short &noOfKeys, const Attribute &attribute, const void* key)
{
	short tempNoOfKeys = 1;
	int pageId;

	while(tempNoOfKeys <= noOfKeys)
	{
		memcpy(&pageId, (char*)node + readOffset, sizeof(int));
		readOffset = readOffset + sizeof(int);	//pageId

		if(key == NULL)
		{
			break;
		}

		if(attribute.type == TypeInt)
		{
			int intKey;
			memcpy(&intKey, key, sizeof(int));

			int storedKey;
			memcpy(&storedKey, (char*)node + readOffset, sizeof(int));

			if(intKey < storedKey)
			{
				break;
			}
			readOffset = readOffset + sizeof(int);
		}
		else if(attribute.type == TypeReal)
		{
			float floatKey;
			memcpy(&floatKey, key, sizeof(float));

			float storedKey;
			memcpy(&storedKey, (char*)node + readOffset, sizeof(float));

			if(floatKey < storedKey)
			{
				break;
			}
			readOffset = readOffset + sizeof(float);

		}
		else if(attribute.type == TypeVarChar)
		{
			int insertVarcharLen;
			memcpy(&insertVarcharLen, (char*)key, sizeof(int));

			void* insertKey = malloc(insertVarcharLen);
			memcpy(insertKey, (char*)key + sizeof(int), insertVarcharLen);
			string insertKeyStr = string((char*)insertKey, insertVarcharLen);
			// cout<<"INSERT STR IN SCAN : "<<insertKeyStr<<endl;

			short storedVarcharLen;
			memcpy(&storedVarcharLen, (char*)node + readOffset, sizeof(short));

			void* storedKey = malloc(storedVarcharLen);
			memcpy(storedKey, (char*)node + readOffset + sizeof(short), storedVarcharLen);
			string storedKeyStr = string((char*)storedKey, storedVarcharLen);
			// cout<<"COMPARING WITH  :"<<sstoredKeyStr<<" IN SCAN"<<endl;
			if(insertKeyStr.compare(storedKeyStr) < 0)
			{
				break;
			}
			readOffset = readOffset + sizeof(short) + storedVarcharLen;
		}

		tempNoOfKeys = tempNoOfKeys + 1;
	}

	if(tempNoOfKeys > noOfKeys)	//key to be inserted is not less than any of the stored Keys
	{
		memcpy(&pageId, (char*)node + readOffset, sizeof(int));
		readOffset = readOffset + sizeof(int);	//pageId
	}

	return pageId;
}


RC IndexManager::checkIfValidFileHandle(IXFileHandle &ixfileHandle)
{
	return pagedFileManager->checkIfFileExists(ixfileHandle.fileName);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
//	char typeOfKey = static_cast <char>(attribute.type);
//	cout<<"Casted Enum"<<typeOfKey<<endl;

	if(checkIfValidFileHandle(ixfileHandle) != 0)
	{
		return -1;
	}

	RC insertResult = -1;
	if(ixfileHandle.fileHandle.getNumberOfPages() == 0)		//empty tree
	{
		return createTree(ixfileHandle, attribute, key, rid);
	}
	else
	{
		SplitData splitData;
		splitData.pageId = -1;
		splitData.key = malloc(PAGE_SIZE);
		splitData.result = -1;
		insertEntryRecursive(ixfileHandle, 0, attribute, key, rid, splitData);
		free(splitData.key);
		insertResult = splitData.result;
		//get no of pages.
		// if only 1 pg
		////check if free space available
		////
	}
    return insertResult;
}


RC IndexManager::getMoveOffset(short &startingPoint,short &endingPoint,const void *key, void *pageData, const Attribute &attribute, short &noOfRid)
{
	char typeOfPage;
	short noOfKeys;
	memcpy(&typeOfPage, pageData, sizeof(char));
	memcpy(&noOfKeys, (char*)pageData + sizeof(char), sizeof(short));
	short counter =1; //potential prob
	if(typeOfPage=='1') //leaf
	{

		short offset = 9* sizeof(char); //5 for header and 4 for pointer to next page
		while(counter <= noOfKeys) {

			if (attribute.type == TypeInt) {

				int intKey;
				memcpy(&intKey, key, sizeof(int));

				int storedKey;
                startingPoint = offset;
				memcpy(&storedKey, (char *) pageData + offset, sizeof(int));
                offset = offset+ sizeof(int);


                memcpy(&noOfRid, (char *) pageData + offset, sizeof(short));
                offset += sizeof(short);
                offset = offset + (noOfRid*2* sizeof(int));
                if(storedKey == intKey)
				{

                    endingPoint = offset;
                    return 0;
				}




			} else if (attribute.type == TypeReal) {
                float floatKey;
                memcpy(&floatKey,key, sizeof(float));

                float storedKey;
                startingPoint = offset;
				memcpy(&storedKey, (char *) pageData + offset, sizeof(float));
                offset = offset + sizeof(float);

                memcpy(&noOfRid, (char *) pageData + offset, sizeof(short));
                offset = offset + sizeof(short);
                offset = offset + (noOfRid*2* sizeof(int));
                if(storedKey == floatKey)
                {
                    endingPoint = offset;
                    return 0;

				}

			} else {
				short varcharLen;
				startingPoint = offset;

                memcpy(&varcharLen, (char *) pageData + offset, sizeof(short));
                offset = offset + sizeof(short);

                void *storedKey = malloc(varcharLen);
                memcpy(storedKey, (char *) pageData + offset, varcharLen);

                offset = offset+ varcharLen;


                memcpy(&noOfRid, (char *) pageData + offset, sizeof(short));

                int keyLength;
                memcpy(&keyLength, key, sizeof(int));

                void *extractedkey = malloc(keyLength);
                memcpy(extractedkey, (char *)key + sizeof(int),keyLength);


                offset = offset + sizeof(short);
                offset = offset + (noOfRid*2* sizeof(int));

                if(string((char*)storedKey, varcharLen) == string((char*)extractedkey, keyLength))
				{

                    endingPoint = offset;
					return 0;
				}

			}
			counter++;
		}// while loop

		return -1;
	} //if loop for leaf page ends
	else
	{
		short offset = 5* sizeof(char); //5 for header

		while(counter <= noOfKeys)
		{
			if (attribute.type == TypeInt)
			{
                startingPoint = offset;
                offset = offset + sizeof(int); //the first page pointer

				int storedKey;
				memcpy(&storedKey, (char *) pageData + offset, sizeof(int));
                offset = offset + sizeof(int);

				int intKey;
                memcpy(&intKey, key, sizeof(int));


                if (storedKey > intKey) //potential problem
				{

					endingPoint = offset;
					return 0;
				}

			}
			else if (attribute.type == TypeReal) {
                startingPoint = offset;
                offset = offset + sizeof(float);
                float storedKey;
				memcpy(&storedKey, (char *) pageData + offset, sizeof(float));
                offset = offset + sizeof(float);

                float floatKey;
                memcpy(&floatKey, key, sizeof(float));
                if (storedKey > floatKey)
				{

					endingPoint = offset;
					return 0;
				}

			}

			else {
                startingPoint = offset;
                offset = offset + sizeof(int);

                short varcharLen;
                memcpy(&varcharLen, (char *) pageData + offset, sizeof(short));
                offset = offset + sizeof(short);

                void *storedKey = malloc(varcharLen);
				memcpy(storedKey, (char *) pageData + offset, varcharLen);
                offset = offset + varcharLen;

                int keyLength;
                memcpy(&keyLength, key, sizeof(int));

                void *extractedkey = malloc(keyLength);
                memcpy(extractedkey, (char *)key + sizeof(int),keyLength);



                if(string((char*)storedKey, varcharLen).compare(string((char*)extractedkey, keyLength))>0) //potential problem
                {
                	endingPoint = offset;
                	return 0;
				}


			}
			counter++;
		}
		// while loop iterating through all slots ends here
		// if nothing has been returned we know
		// Final check to see if key will correspond to the last page. Which means that it will be greater than the last
		//storedKey
		if(attribute.type == TypeInt)
		{
			int storedKey;
			offset = startingPoint +sizeof(int);
			memcpy(&storedKey, (char *) pageData + offset, sizeof(int));
			offset = offset + sizeof(int);
			int intKey;
			memcpy(&intKey, key, sizeof(int));


			if (storedKey <= intKey) //potential problem
			{

				endingPoint = offset;
				return -2;
			}


		}
		else if(attribute.type == TypeReal)
		{
			float storedKey;
			offset = startingPoint + sizeof(int);
			memcpy(&storedKey, (char *) pageData + offset, sizeof(float));
			offset = offset +sizeof(float);

			float intKey;
			memcpy(&intKey, key, sizeof(float));


			if (storedKey <= intKey) //potential problem
			{

				endingPoint = offset;
				return -2;
			}

		}
		else
		{
			int keyLength;
			memcpy(&keyLength, key, sizeof(int));

			void *extractedkey  = malloc(keyLength);
			memcpy(extractedkey, (char *)key + sizeof(int),keyLength);

			short varcharLen;

			offset = startingPoint + sizeof(int);
			memcpy(&varcharLen, (char *) pageData + offset, sizeof(short));
			offset = offset + sizeof(short);

			void *storedKey = malloc(varcharLen);
			memcpy(storedKey, (char *) pageData + offset, varcharLen);
			offset = offset + varcharLen;


            if(string((char*)storedKey, varcharLen).compare(string((char*)extractedkey, keyLength))<=0) //potential problem
			{

				endingPoint = offset;
				return -2;
			}




		}
		return 0;

	}

}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid) {

	if(checkIfValidFileHandle(ixfileHandle) != 0)
	{
		return -1;
	}

	int pageNo = 0;
	int siblingPageNo = -1;
	int result;

	if (ixfileHandle.fileHandle.getNumberOfPages() == 1) // if root is the only node
	{
		void* rootPage = malloc(PAGE_SIZE);
		ixfileHandle.fileHandle.readPage(0, rootPage);
		short startingPoint;

		short endingPoint;
		short freeBytesLeft;
		short noOfRid;
		int offset;

		//for debugging
		char status;
		short noOfKeys;
		memcpy(&status,rootPage, sizeof(char));
		//cout<<status;
		memcpy(&noOfKeys,(char *)rootPage + sizeof(char), sizeof(short));
		//cout<<noOfKeys;


		memcpy(&freeBytesLeft, (char *) rootPage + (3 * sizeof(char)), sizeof(short));
		short freeBytesStartAt = PAGE_SIZE - freeBytesLeft;

		int result = getMoveOffset(startingPoint,endingPoint,key, rootPage, attribute, noOfRid);
		if(result == -1)
		{
			return -1;
		}
		short lengthToMove = freeBytesStartAt- endingPoint;

		if (freeBytesStartAt == 5 * sizeof(char)) //if root is empty
		{
			free(rootPage);
			return -1; //returns to test case

		}


		if(noOfRid==1)
		{

			memmove((char *)rootPage + startingPoint, (char *)rootPage+ endingPoint, lengthToMove);

			freeBytesLeft = freeBytesLeft + (endingPoint - startingPoint);
			noOfKeys = noOfKeys -1;

			offset = sizeof(char);
			memcpy((char*)rootPage+offset,&noOfKeys,sizeof(short));

			offset = offset + sizeof(short);
			memcpy((char*)rootPage+offset,&freeBytesLeft,sizeof(short));


			ixfileHandle.fileHandle.writePage(pageNo, rootPage);
			free(rootPage);

			return 0;
		}
		else
		{
			short startOfRid= endingPoint - (2* sizeof(int)*noOfRid);
			offset = startOfRid;
			int counterOfRid = 1;
			int ridPageNo,ridSlotNo;

			while(counterOfRid<=noOfRid) //looking up the RID
			{
				memcpy(&ridPageNo, (char *) rootPage + offset, sizeof(int));
				offset = offset + sizeof(int);
				memcpy(&ridSlotNo, (char *) rootPage + offset, sizeof(int));
				offset = offset + sizeof(int);
				if(rid.slotNum==ridSlotNo && rid.pageNum == ridPageNo)
				{
					break;

				}
				counterOfRid++;

			}
			if(counterOfRid>noOfRid)
			{
				return -1;

			}

			int destination = startOfRid + (counterOfRid-1)*(2*sizeof(int));
			int source = destination + (2* sizeof(int));

			memmove((char *) rootPage + destination, (char *) rootPage + source, freeBytesStartAt - source); //removing one RID
			noOfRid = noOfRid -1;


			// updating the no of RIDS
			if(attribute.type == TypeInt)
			{
				offset = startingPoint+ sizeof(int);
				memcpy((char *)rootPage+offset,&noOfRid, sizeof(short));
			}
			else if(attribute.type == TypeReal)
			{
				offset = startingPoint+ sizeof(float);
				memcpy((char *)rootPage+offset,&noOfRid, sizeof(short));
			}
			else
			{
				short varcharLen;

				offset = startingPoint;
				memcpy(&varcharLen, (char *) rootPage + offset, sizeof(short));
				offset = offset + sizeof(short)+ varcharLen;
				memcpy((char *)rootPage,&noOfRid, sizeof(short));

			}

			//updating freeBytesLeft
			freeBytesLeft = freeBytesLeft+2* sizeof(int);
			memcpy((char *)rootPage+ 3* sizeof(char),&freeBytesLeft, sizeof(short));

			ixfileHandle.fileHandle.writePage(pageNo, rootPage);
			free(rootPage);

			return 0;



		}

	}

	else // when there are more than one node
	{
        void* rootPage = malloc(PAGE_SIZE);
		result = deleteRecursive(ixfileHandle, attribute, key, rid, pageNo, siblingPageNo,rootPage,pageNo,0,0);
        free(rootPage);

	}

	if (result == -1)
	{
		return -1;

	}
	else
	{
		return 0;
	}
}

void IX_ScanIterator::scanRecursive(int pageNo, const void* key, const Attribute &attribute)
{
	ixfileHandle->fileHandle.readPage(pageNo, pageInMemory);
	char typeOfPage;
	short freeBytesLeft;
	int localReadOffset = 0;

	IndexManager *im = IndexManager::instance();
	im->getCommonMetaData(pageInMemory, typeOfPage, noOfKeys, freeBytesLeft, localReadOffset);

	//once you get a leaf page
	if(typeOfPage == '1')
	{
		memcpy(&nextPageId, (char*)pageInMemory + localReadOffset, sizeof(int));
		localReadOffset = localReadOffset + sizeof(int);
		readOffset = localReadOffset;						//set the readOffset
	}
	else
	{
		int searchedPageId  = im->getPageIdForInsertionOrScan(pageInMemory, localReadOffset, noOfKeys, attribute, key);
		scanRecursive(searchedPageId, key, attribute);
	}
}

RC IndexManager::deleteRecursive(IXFileHandle ixfileHandle, const Attribute &attribute, const void *key,const RID &rid, int pageNo, int siblingPageNo, void *rootPage, int rootPageNo, short startPoint, short endpoint)
{
	int readOffset = 0;
	char typeOfPage;
	short noOfKeys;
	short freeBytesLeft;


    void* nodePage = malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(pageNo, nodePage);

	getCommonMetaData(nodePage, typeOfPage, noOfKeys, freeBytesLeft, readOffset);

	if(typeOfPage=='0') //root node
	{
		short offset = 5*sizeof(char);
		int newPageNo;
		short startingPoint;
		short endingPoint;
		short noOfRid;
		int resultFromRecursion;

		int result = getMoveOffset(startingPoint,endingPoint,key, nodePage, attribute, noOfRid);

		if(result == -2) // if result corresponds to last pageNo
		{	int lastPageNo;
			memcpy(&lastPageNo,(char *)nodePage + endingPoint,sizeof(int));
            int siblingPageNo;
            memcpy(&siblingPageNo, (char *) nodePage + startingPoint, sizeof(int));
            resultFromRecursion = deleteRecursive(ixfileHandle, attribute, key, rid, lastPageNo, siblingPageNo, nodePage, pageNo, startingPoint, endingPoint);//calling for child node

		}
		else
		{
			memcpy(&newPageNo, (char *) nodePage + startingPoint, sizeof(int));
			int siblingPageNo;
			memcpy(&siblingPageNo, (char *) nodePage + endingPoint, sizeof(int));
			resultFromRecursion = deleteRecursive(ixfileHandle, attribute, key, rid, newPageNo, siblingPageNo, nodePage, pageNo, startingPoint, endingPoint);//calling for child node
		}

		if(resultFromRecursion == -1)
		{
			return -1; //this is returned to deleteEntry function

		}
		return 1;
		//return 1;



	}
	else //delete logic goes here....since this is child node. recursion call returns to same function's if loop
	{


		short offset = 5*sizeof(char);
		short startingPoint;
		short endingPoint;
		short noOfRid;


        int result = getMoveOffset(startingPoint,endingPoint,key, nodePage, attribute, noOfRid);

		if(result == -1) //no such leaf node exists.
		{

			return -1;

		}
		else //deletion of node
		{

			int freeBytesStartAt = PAGE_SIZE - freeBytesLeft;

			short lengthToMove = freeBytesStartAt - endingPoint;

			if(noOfRid==1) //if we have just one RID
			{
				memmove((char *)nodePage+startingPoint, (char *)nodePage + endingPoint, lengthToMove);

				freeBytesLeft = freeBytesLeft + (endingPoint - startingPoint);
				noOfKeys = noOfKeys -1;

				offset = sizeof(char);
				memcpy((char*)nodePage+offset,&noOfKeys,sizeof(short));

				offset = offset + sizeof(short);
				memcpy((char*)nodePage+offset,&freeBytesLeft,sizeof(short));


		}

			else // when we have a list of RIDS !
			{

				short startOfRid=  endingPoint - (2* sizeof(int)*noOfRid);
				offset = startOfRid;
				int counterOfRid = 1;
				int ridPageNo,ridSlotNo;

				while(counterOfRid<=noOfRid) //looking up the RID
				{
					memcpy(&ridPageNo, (char *) nodePage + offset, sizeof(int));
					offset = offset + sizeof(int);
					memcpy(&ridSlotNo, (char *) nodePage + offset, sizeof(int));
					offset = offset + sizeof(int);
					if(rid.slotNum==ridSlotNo && rid.pageNum == ridPageNo)
					{
						break;

					}
					counterOfRid++;

				}

				if(counterOfRid>noOfRid)
				{
					return -1;

				}

				int destination = startOfRid + (counterOfRid-1)*(2*sizeof(int));
				int source = destination + (2* sizeof(int));

				memmove((char *) nodePage + destination, (char *) nodePage + source, freeBytesStartAt - source); //removing one RID
				noOfRid = noOfRid -1;


				// updating the no of RIDS
				if(attribute.type == TypeInt)
				{
					offset = startingPoint+ sizeof(int);
					memcpy((char *)nodePage+offset,&noOfRid, sizeof(short));
				}
				else if(attribute.type == TypeReal)
				{
					offset = startingPoint+ sizeof(float);
					memcpy((char *)nodePage+offset,&noOfRid, sizeof(short));
				}
				else
				{
					short varcharLen;
                    offset = startingPoint;
					memcpy(&varcharLen, (char *) nodePage + offset, sizeof(short));
					offset = offset + sizeof(short)+ varcharLen;
					memcpy((char *)nodePage+offset,&noOfRid, sizeof(short));

				}

				//updating freeBytesLeft
				freeBytesLeft = freeBytesLeft+2* sizeof(int);
				memcpy((char *)nodePage+ 3* sizeof(char),&freeBytesLeft, sizeof(short));


            }



        }


	} //deletion over

	  //logic for redistribution
//      		if(siblingPageNo==-1)
//			{
//				return 0;
//			}
//            void *siblingData = malloc(PAGE_SIZE);
//            short siblingBytesLeft;
//            ixfileHandle.fileHandle.readPage(siblingPageNo, siblingData);//reading sibling page
//            memcpy(&siblingBytesLeft, (char *) siblingData + 3 * sizeof(char), sizeof(short));
//
//			memcpy(&freeBytesLeft, (char *) nodePage + 3 * sizeof(char), sizeof(short));
//
//
//			short freeSpacePtr = PAGE_SIZE - freeBytesLeft;
//            short siblingFreeSpacePtr = PAGE_SIZE - siblingBytesLeft;
//            short sizeOfNodeData = PAGE_SIZE - freeBytesLeft - (9* sizeof(char));;
//            short sizeOfSiblingData = PAGE_SIZE - siblingBytesLeft - (9* sizeof(char));
//
//
//    if(sizeOfNodeData < ((PAGE_SIZE-9)/2))
//            {
//
//                //if(abs(sizeOfNodeData+sizeOfSiblingData)<(PAGE_SIZE-9))
//                //if(siblingBytesLeft<(PAGE_SIZE-(5* sizeof(char)))/2)
//                if(siblingBytesLeft>=sizeOfNodeData)
//                {
//
//                    if (abs(sizeOfNodeData + sizeOfSiblingData) > (PAGE_SIZE - 9)) {
//                        RC resultFromRedistribution = redistribute(ixfileHandle, attribute, pageNo, siblingPageNo,
//                                                                   siblingData, nodePage, rootPage, rootPageNo,
//                                                                   startPoint);
//
//                    } else {
//                        //startpoint is starting point of root nodes pointer+key that got us here.
//                        //endpoint is offset to next key of node.
//                        RC resultFromMerge = merge(ixfileHandle, attribute, pageNo, siblingPageNo, rootPage,
//                                                   siblingData, nodePage, rootPageNo, startPoint, endpoint);
////                    cout<<endl<<"each merge";
////                    IndexManager *indexManager;
////                    indexManager->printBtree(ixfileHandle, attribute);
//
////                    cout<<"we need to merge";//MERGE
////
//                    }
//                }
//
//
//
//            }

	ixfileHandle.fileHandle.writePage(pageNo, nodePage);
    //free(nodePage);

	return 0;
}

// RC IndexManager :: merge(IXFileHandle ixfileHandle,const Attribute  &attribute, int pageNo, int siblingPageNo,void* rootPage, void* siblingData ,void* nodePage,  int rootPageNo , short startingPoint, short endingPoint)
// {
//     int readOffset = 0;
// 	int rootReadOffset = 0;
//     int siblingReadOffset = 0;

//     int sizeOfNodeData;
//     int sizeOfSiblingData;
//     int ptrOfRootPage;
//     int sizeOfRootData;

// 	char typeOfPage;
// 	short noOfKeys;
// 	short freeBytesLeft;
// 	getCommonMetaData(nodePage, typeOfPage, noOfKeys, freeBytesLeft, readOffset);

// 	char siblingTypeOfPage;
// 	short siblingNoOfKeys;
// 	short siblingFreeBytesLeft;

// 	char rootTypeOfPage;
// 	short rootNoOfKeys;
// 	short rootFreeBytesLeft;



//     if(typeOfPage == '1')			//leaf page - >  use leaf page format
//     {



// 		getCommonMetaData(siblingData, siblingTypeOfPage, siblingNoOfKeys, siblingFreeBytesLeft, siblingReadOffset);
// 		getCommonMetaData(rootPage, rootTypeOfPage, rootNoOfKeys, rootFreeBytesLeft, rootReadOffset);

// 		sizeOfNodeData = PAGE_SIZE - freeBytesLeft - (9* sizeof(char));
//         sizeOfSiblingData = PAGE_SIZE - siblingFreeBytesLeft - (9* sizeof(char));
//         ptrOfRootPage = PAGE_SIZE - rootFreeBytesLeft;

//         if(attribute.type ==TypeInt) {
//             int offset = 9 * sizeof(char);
//             int firstKey;
//             int siblingFirstKey;
//             memcpy(&firstKey, (char *) nodePage + offset, sizeof(int));
//             memcpy(&siblingFirstKey, (char *) siblingData + offset, sizeof(int));

//             if (siblingFirstKey > firstKey) //merge it with the node on the right, data will move to the right
//             {
//                 void *dataToMove = malloc(sizeOfNodeData);
//                 memcpy(dataToMove, (char *) nodePage + offset, sizeOfNodeData); //copying the node data
//                 memmove((char *) siblingData + offset + sizeOfNodeData, (char *) siblingData + offset,
//                         sizeOfSiblingData); //moving the sibling data
//                 memcpy((char *) siblingData + offset, dataToMove, sizeOfNodeData); //copying into sibling data

//             } else //merge to left node data will move to the left
//             {
//                 //changes
//                 void *dataToMove = malloc(sizeOfNodeData);
//                 memcpy(dataToMove, (char *) nodePage + offset, sizeOfNodeData);
//                 memcpy((char *) siblingData + (PAGE_SIZE - freeBytesLeft), dataToMove, sizeOfNodeData);
//             }
//         }
//         else if(attribute.type ==TypeReal) {
//             int offset = 9 * sizeof(char);
//             float firstKey;
//             float siblingFirstKey;
//             memcpy(&firstKey, (char *) nodePage + offset, sizeof(float));
//             memcpy(&siblingFirstKey, (char *) siblingData + offset, sizeof(float));

//             if (siblingFirstKey > firstKey) //merge it with the node on the right, data will move to the right
//             {
//                 void *dataToMove = malloc(sizeOfNodeData);
//                 memcpy(dataToMove, (char *) nodePage + offset, sizeOfNodeData); //copying the node data
//                 memmove((char *) siblingData + offset + sizeOfNodeData, (char *) siblingData + offset,
//                         sizeOfSiblingData); //moving the sibling data
//                 memcpy((char *) siblingData + offset, dataToMove, sizeOfNodeData); //copying into sibling data

//             } else //merge to left node data will move to the left
//             {
//                 //changes
//                 void *dataToMove = malloc(sizeOfNodeData);
//                 memcpy(dataToMove, (char *) nodePage + offset, sizeOfNodeData);
//                 memcpy((char *) siblingData + (PAGE_SIZE - freeBytesLeft), dataToMove, sizeOfNodeData);
//             }
//          }
//         else // for varchar
//         {
//             int offset = 9 * sizeof(char);
//             short firstKeyLength;
//             short siblingFirstKeyLength;


// //          string currentKey = string((char*)currentKeyPtr, currentVarcharLength);
//             memcpy(&firstKeyLength, (char *) nodePage + offset, sizeof(short));
//             void *firstKey = malloc(firstKeyLength);
//             memcpy(&firstKey, (char *) nodePage + offset + sizeof(short), firstKeyLength);

//             memcpy(&siblingFirstKeyLength, (char *) siblingData + offset, sizeof(short));
//             void *siblingFirstKey = malloc(siblingFirstKeyLength);
//             memcpy(&siblingFirstKey, (char *) siblingData + offset + sizeof(short), siblingFirstKeyLength);

//             string currentFirstKey = string((char *) firstKey, firstKeyLength);
//             string currentSiblingFirstKey = string((char *) siblingFirstKey, siblingFirstKeyLength);

//             if (currentSiblingFirstKey.compare(currentFirstKey) >
//                 0) //merge it with the node on the right, data will move to the right
//             {
//                 void *dataToMove = malloc(sizeOfNodeData);
//                 memcpy(dataToMove, (char *) nodePage + offset, sizeOfNodeData); //copying the node data
//                 memmove((char *) siblingData + offset + sizeOfNodeData, (char *) siblingData + offset,
//                         sizeOfSiblingData); //moving the sibling data
//                 memcpy((char *) siblingData + offset, dataToMove, sizeOfNodeData); //copying into sibling data

//             } else //merge to left node data will move to the left
//             {
//                 //changes
//                 void *dataToMove = malloc(sizeOfNodeData);
//                 memcpy(dataToMove, (char *) nodePage + offset, sizeOfNodeData);
//                 memcpy((char *) siblingData + (PAGE_SIZE - freeBytesLeft), dataToMove, sizeOfNodeData);
//             }
//         }

//             //updating siblingData
//             siblingFreeBytesLeft = siblingFreeBytesLeft - sizeOfNodeData;
//             memcpy((char *)siblingData+ 3* sizeof(char),&siblingFreeBytesLeft, sizeof(short));
//             siblingNoOfKeys = siblingNoOfKeys +noOfKeys;
//             memcpy((char*)siblingData+ sizeof(char),&siblingNoOfKeys,sizeof(short));


//             //updating nodePage
//             freeBytesLeft = freeBytesLeft + sizeOfNodeData;
//             memcpy((char *)nodePage+ 3* sizeof(char),&freeBytesLeft, sizeof(short));
//             noOfKeys = -1;
//             memcpy((char*)nodePage+ sizeof(char),&noOfKeys,sizeof(short));




//             //updating rootPage doesn't change

//         if(rootNoOfKeys!=1) {



//             //updating root page
//             memmove((char *) rootPage + startingPoint, (char *) rootPage + endingPoint, ptrOfRootPage - endingPoint);
//             rootNoOfKeys = rootNoOfKeys - 1;
//             memcpy((char *) rootPage + sizeof(char), &rootNoOfKeys, sizeof(short));
//             rootFreeBytesLeft = rootFreeBytesLeft + (endingPoint - startingPoint);
//             memcpy((char *) rootPage + 3 * sizeof(char), &rootFreeBytesLeft, sizeof(short));


//             ixfileHandle.fileHandle.writePage(siblingPageNo,siblingData);
//             ixfileHandle.fileHandle.writePage(pageNo,nodePage);
//             ixfileHandle.fileHandle.writePage(rootPageNo,rootPage);
//         } else
//         {
//             ixfileHandle.fileHandle.writePage(rootPageNo,siblingData);

//             siblingFreeBytesLeft = siblingFreeBytesLeft + sizeOfSiblingData;
//             memcpy((char *)siblingData+ 3* sizeof(char),&siblingFreeBytesLeft, sizeof(short));
//             siblingNoOfKeys = -1;
//             memcpy((char*)siblingData+ sizeof(char),&siblingNoOfKeys,sizeof(short));

//             ixfileHandle.fileHandle.writePage(siblingPageNo,siblingData);
//             ixfileHandle.fileHandle.writePage(pageNo,nodePage);


// 		}
//             return 0;


//     } else //type of page = '0'
//     {

// 		ixfileHandle.fileHandle.readPage(rootPageNo,rootPage);
// 		ixfileHandle.fileHandle.readPage(pageNo,nodePage);
// 		ixfileHandle.fileHandle.readPage(siblingPageNo,siblingData);

// 		getCommonMetaData(nodePage, typeOfPage, noOfKeys, freeBytesLeft, readOffset);
// 		getCommonMetaData(siblingData, siblingTypeOfPage, siblingNoOfKeys, siblingFreeBytesLeft, siblingReadOffset);
// 		getCommonMetaData(rootPage, rootTypeOfPage, rootNoOfKeys, rootFreeBytesLeft, rootReadOffset);

// //		cout<<"Lets ook at this"<<endl<<endl;
// //		IndexManager *indexManager;
// //		indexManager->printBtree(ixfileHandle, attribute);
// //   	exit(EXIT_SUCCESS);

//         sizeOfNodeData = PAGE_SIZE - freeBytesLeft - (5* sizeof(char));
//         sizeOfSiblingData = PAGE_SIZE - siblingFreeBytesLeft - (5* sizeof(char));
//         ptrOfRootPage = PAGE_SIZE - rootFreeBytesLeft;
//         sizeOfRootData = ptrOfRootPage - 5* sizeof(char);
//         void *dataToCopy = malloc(PAGE_SIZE);

//         if(rootNoOfKeys == 1 ) {
//             if (attribute.type == TypeInt) {
//                 int offset = 5 * sizeof(char);
//                 int firstKey;
//                 int siblingFirstKey;
//                 memcpy(&firstKey, (char *) nodePage + offset, sizeof(int));
//                 memcpy(&siblingFirstKey, (char *) siblingData + offset, sizeof(int));


//                 if (siblingFirstKey > firstKey) //merge it with the node on the right, data will move to the right
//                 {
//                     memcpy(dataToCopy, rootPage, offset);
//                     memcpy((char *) dataToCopy + offset, (char *) nodePage + offset, sizeOfNodeData);
//                     memcpy((char *) dataToCopy + offset + sizeOfNodeData, (char *) rootPage + offset, sizeOfRootData);
//                     memcpy((char *) dataToCopy + offset + sizeOfRootData, (char *) siblingData + offset,
//                            sizeOfSiblingData);


//                 } else {
//                     memcpy(dataToCopy, rootPage, offset);
//                     memcpy((char *) dataToCopy + offset, (char *) siblingData + offset, sizeOfSiblingData);
//                     memcpy((char *) dataToCopy + offset + sizeOfSiblingData, (char *) rootPage + offset,
//                            sizeOfRootData);
//                     memcpy((char *) dataToCopy + offset + sizeOfRootData, (char *) nodePage + offset, sizeOfNodeData);

//                 }


//             } else if (attribute.type == TypeReal) {
//                 int offset = 5 * sizeof(char);
//                 float firstKey;
//                 float siblingFirstKey;
//                 memcpy(&firstKey, (char *) nodePage + offset, sizeof(float));
//                 memcpy(&siblingFirstKey, (char *) siblingData + offset, sizeof(float));


//                 if (siblingFirstKey > firstKey) //merge it with the node on the right, data will move to the right
//                 {
//                     memcpy(dataToCopy, rootPage, offset);
//                     memcpy((char *) dataToCopy + offset, (char *) nodePage + offset, sizeOfNodeData);
//                     memcpy((char *) dataToCopy + offset + sizeOfNodeData, (char *) rootPage + offset, sizeOfRootData);
//                     memcpy((char *) dataToCopy + offset + sizeOfRootData, (char *) siblingData + offset,
//                            sizeOfSiblingData);


//                 } else {
//                     memcpy(dataToCopy, rootPage, offset);
//                     memcpy((char *) dataToCopy + offset, (char *) siblingData + offset, sizeOfSiblingData);
//                     memcpy((char *) dataToCopy + offset + sizeOfSiblingData, (char *) rootPage + offset,
//                            sizeOfRootData);
//                     memcpy((char *) dataToCopy + offset + sizeOfRootData, (char *) nodePage + offset, sizeOfNodeData);

//                 }


//             } else {
//                 int offset = 5 * sizeof(char);
//                 short firstKeyLength;
//                 short siblingFirstKeyLength;

//                 memcpy(&firstKeyLength, (char *) nodePage + offset, sizeof(short));
//                 void *firstKey = malloc(firstKeyLength);
//                 memcpy(&firstKey, (char *) nodePage + offset + sizeof(short), firstKeyLength);

//                 memcpy(&siblingFirstKeyLength, (char *) siblingData + offset, sizeof(short));
//                 void *siblingFirstKey = malloc(siblingFirstKeyLength);
//                 memcpy(&siblingFirstKey, (char *) siblingData + offset + sizeof(short), siblingFirstKeyLength);

//                 string currentFirstKey = string((char *) firstKey, firstKeyLength);
//                 string currentSiblingFirstKey = string((char *) siblingFirstKey, siblingFirstKeyLength);

//                 if (currentSiblingFirstKey.compare(currentFirstKey) > 0) //merge it with the node on the right,
//                     // data will move to the right
//                 {


//                     memcpy(dataToCopy, rootPage, offset);
//                     memcpy((char *) dataToCopy + offset, (char *) nodePage + offset, sizeOfNodeData);
//                     memcpy((char *) dataToCopy + offset + sizeOfNodeData, (char *) rootPage + offset, sizeOfRootData);
//                     memcpy((char *) dataToCopy + offset + sizeOfRootData, (char *) siblingData + offset, sizeOfSiblingData);


//                 }
//                 else
//                 {
//                     memcpy(dataToCopy, rootPage, offset);
//                     memcpy((char *) dataToCopy + offset, (char *) siblingData + offset, sizeOfSiblingData);
//                     memcpy((char *) dataToCopy + offset + sizeOfSiblingData, (char *) rootPage + offset, sizeOfRootData);
//                     memcpy((char *) dataToCopy + offset + sizeOfRootData, (char *) nodePage + offset, sizeOfNodeData);

//                 }


//             }


//             //updating rootPage doesn't change
//             rootNoOfKeys = rootNoOfKeys+siblingNoOfKeys+noOfKeys;
//             memcpy((char*)dataToCopy+ sizeof(char),&rootNoOfKeys,sizeof(short));
//             rootFreeBytesLeft = rootFreeBytesLeft -(sizeOfNodeData+sizeOfSiblingData);
//             memcpy((char *)dataToCopy+ 3* sizeof(char),&rootFreeBytesLeft, sizeof(short));


//             //updating siblingData
//             siblingFreeBytesLeft = siblingFreeBytesLeft - sizeOfSiblingData;
//             memcpy((char *)siblingData+ 3* sizeof(char),&siblingFreeBytesLeft, sizeof(short));
//             siblingNoOfKeys = -1;
//             memcpy((char*)siblingData+ sizeof(char),&siblingNoOfKeys,sizeof(short));

//             //updating nodePage
//             freeBytesLeft = freeBytesLeft + sizeOfNodeData;
//             memcpy((char *)nodePage+ 3* sizeof(char),&freeBytesLeft, sizeof(short));
//             noOfKeys = -1;
//             memcpy((char*)nodePage+ sizeof(char),&noOfKeys,sizeof(short));




//             //writing pages
//             ixfileHandle.fileHandle.writePage(0, dataToCopy);
//             ixfileHandle.fileHandle.writePage(siblingPageNo,siblingData);
//             ixfileHandle.fileHandle.writePage(pageNo,nodePage);

//             free(dataToCopy);


//             }

//         else if(rootNoOfKeys>1)
//         {   //if parent is not root

//             if(attribute.type == TypeInt)
//             {}
//             else if(attribute.type == TypeReal)
//             {}
//             else
//             {}


//         }

//     }



// }


// RC IndexManager :: redistribute(IXFileHandle ixfileHandle,const Attribute  &attribute, int pageNo, int siblingPageNo, void* siblingData ,void* nodePage, void* rootPage, int rootPageNo , short startingPoint)
// {
//     short siblingBytesLeft;
//     memcpy(&siblingBytesLeft,(char *)siblingData+ 3* sizeof(char), sizeof(short));
//     short siblingNoOfkeys;
//     memcpy(&siblingNoOfkeys,(char *)siblingData+ 1* sizeof(char), sizeof(short));


//     short siblingFreeBytesStartAt = PAGE_SIZE- siblingBytesLeft;
//     short counter=0;

//     short bytesToOffer = (PAGE_SIZE - siblingBytesLeft) - (PAGE_SIZE-(6* sizeof(char)))/2 ;


//     short freeBytesLeft;
//     memcpy(&freeBytesLeft,(char *)nodePage+ 3* sizeof(char), sizeof(short));

//     short noOfkeys;
//     memcpy(&noOfkeys,(char *)nodePage+ 1* sizeof(char), sizeof(short));


//     short offsetToNodePage = PAGE_SIZE -freeBytesLeft;

//     short offset =9* sizeof(char);
//     short noOfRid;
//     while((offset-9)<=bytesToOffer)
//     {
//         if (attribute.type == TypeInt) {

//             int storedKey;
//             memcpy(&storedKey, (char *) siblingData + offset, sizeof(int));
//             offset = offset+ sizeof(int);
//             memcpy(&noOfRid, (char *) siblingData + offset, sizeof(short));
//             offset += sizeof(short);
//             offset = offset + (noOfRid*2* sizeof(int));
//             counter = counter+1;

//         }
//         else if (attribute.type == TypeReal) {

//             offset = offset + sizeof(float);
//             memcpy(&noOfRid, (char *) nodePage + offset, sizeof(short));
//             offset = offset + sizeof(short);
//             offset = offset + (noOfRid*2* sizeof(int));
//             ++counter;

//         } else {

//             short varcharLen;
//             memcpy(&varcharLen, (char *) nodePage + offset, sizeof(short));
//             offset = offset + sizeof(short);
//             offset = offset+ varcharLen;

//             memcpy(&noOfRid, (char *) nodePage + offset, sizeof(short));
//             offset = offset + sizeof(short);
//             offset = offset + (noOfRid*2* sizeof(int));
//             counter++;


//         }


//     }
//      memcpy((char *)nodePage+offsetToNodePage,(char *)siblingData+(9* sizeof(char)),(offset-9));
//      memmove((char *)siblingData+(9* sizeof(char)),(char *)siblingData+offset,siblingFreeBytesStartAt-offset);
// //
// //    //updating meta data of nodePage
//       noOfkeys = noOfkeys+counter;
//       memcpy((char*)nodePage + sizeof(char), &noOfkeys, sizeof(short));
// //
//       freeBytesLeft = freeBytesLeft - (offset -9);
//       memcpy((char*)nodePage + 3*sizeof(char),&freeBytesLeft,  sizeof(short));
// //
// //    //updating metadata of siblingPage

//     siblingNoOfkeys = siblingNoOfkeys-counter;
//     memcpy((char*)siblingData + sizeof(char), &siblingNoOfkeys, sizeof(short));

//     siblingBytesLeft = siblingBytesLeft + (offset -9);
//     memcpy((char*)siblingData + 3*sizeof(char),&siblingBytesLeft,  sizeof(short));


//     //get first key
//     offset = 9* sizeof(char);

//     //updation of root node begins here

//     if(attribute.type == TypeInt)
//     {
//         int firstKey;
//         memcpy(&firstKey,(char *)siblingData+offset, sizeof(int));
//         memcpy((char *)rootPage+startingPoint+ sizeof(int),&firstKey,sizeof(int));




//     }
//     else if(attribute.type == TypeReal)
//     {
//         float firstKey;
//         memcpy(&firstKey,(char *)siblingData+offset, sizeof(float));
//         memcpy((char *)rootPage+startingPoint+ sizeof(int),&firstKey,sizeof(float));

//     }
//     else
//     {
//         short varcharLen;
//         memcpy(&varcharLen, (char*)siblingData + offset, sizeof(short));
//         offset = offset + sizeof(short);

//         void*storedKey = malloc(varcharLen);
//         memcpy(&storedKey, (char*)siblingData + offset, varcharLen);


//         short rootVarcharLen;
//         memcpy(&rootVarcharLen, (char*)rootPage + startingPoint+ sizeof(int), sizeof(short));
//         offset = offset + sizeof(short);

//         if(rootVarcharLen>varcharLen)
//         {


//             memcpy((char*)rootPage + startingPoint+ sizeof(int),&varcharLen, sizeof(short));
//             memcpy((char*)rootPage + startingPoint+ sizeof(int)+ sizeof(short),&storedKey, varcharLen);
//             short moveLeftSize = rootVarcharLen-varcharLen;
//             memmove((char*)rootPage + startingPoint+ sizeof(int)+ sizeof(short)+ varcharLen,
//                     (char*)rootPage + startingPoint+ sizeof(int)+ sizeof(short)+rootVarcharLen, moveLeftSize);

//         }

//         else
//         {

//             short moveRightSize = varcharLen - rootVarcharLen;
//             memmove((char*)rootPage + startingPoint+ sizeof(int)+ sizeof(short)+ varcharLen,
//                     (char*)rootPage + startingPoint+ sizeof(int)+ sizeof(short)+rootVarcharLen, moveRightSize);

//             memcpy((char*)rootPage + startingPoint+ sizeof(int),&varcharLen, sizeof(short));
//             memcpy((char*)rootPage + startingPoint+ sizeof(int)+ sizeof(short),&storedKey, varcharLen);

//         }

//     }
//     ixfileHandle.fileHandle.writePage(siblingPageNo,siblingData);
//     ixfileHandle.fileHandle.writePage(pageNo,nodePage);
//     ixfileHandle.fileHandle.writePage(rootPageNo,rootPage);

//     return 0;

// }

RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
	if(checkIfValidFileHandle(ixfileHandle) != 0)
	{
		return -1;
	}
	ix_ScanIterator.ixfileHandle = &ixfileHandle;
    ix_ScanIterator.storedAttribute = attribute;
    ix_ScanIterator.lowKey = lowKey;
    ix_ScanIterator.highKey = highKey;
    ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
    ix_ScanIterator.highKeyInclusive = highKeyInclusive;
	ix_ScanIterator.pageInMemory = malloc(PAGE_SIZE);
	ix_ScanIterator.currentKey = malloc(PAGE_SIZE);
    ix_ScanIterator.scanRecursive(0, lowKey, attribute);

    // cout<<"While setting : "<<ix_ScanIterator.pageInMemory<<endl;
    // //DEBUG
    // float checker;
    // memcpy(&checker, (char*)(ix_ScanIterator.pageInMemory) + 9, sizeof(float));
    // short checkNoOfRids;
    // memcpy(&checkNoOfRids, (char*)ix_ScanIterator.pageInMemory + 13, sizeof(short));
    // int pageNum;
    // int slotId;
    // memcpy(&pageNum, (char*)ix_ScanIterator.pageInMemory + 15, sizeof(int));
    // memcpy(&slotId, (char*)ix_ScanIterator.pageInMemory + 19, sizeof(int));
    // cout<<"RID : "<<pageNum<<":"<<slotId<<endl;
    //printBtree(ixfileHandle, attribute);

    return 0;
}

void IndexManager::getCommonMetaData(void* nodeData, char &typeOfPage, short &noOfKeys, short &freeBytesLeft, int &readOffset) const
{
	memcpy(&typeOfPage, nodeData, sizeof(char));
	readOffset =  readOffset + sizeof(char);

	memcpy(&noOfKeys, (char*)nodeData + readOffset, sizeof(short));
	// cout<<"Get meta data : No of keys : "<<noOfKeys<<endl;
	readOffset = readOffset + sizeof(short);

	memcpy(&freeBytesLeft, (char*)nodeData + readOffset, sizeof(short));
	// cout<<"Get meta data : No of free bytes : "<<freeBytesLeft<<endl;
	readOffset = readOffset + sizeof(short);
}

void IndexManager::printNonLeafNode(void* nodeData, short &noOfKeys, short &freeBytesLeft, int &readOffset, const Attribute &attribute,int &printLevel) const
{
	int originalReadOffset = readOffset;
	// short leftPageId;
	// short rightPageId;

	// memcpy(&leftPageId, (char*)nodeData + readOffset, sizeof(int));

	short tempNoOfKeys = 1;
	for(int i=0;i<printLevel;i++)
	{
		cout<<"\t";
	}
	cout<<"{\"keys\": [";
	while(tempNoOfKeys <= noOfKeys)
	{
		if(tempNoOfKeys!=1)
		{cout<<","; }

		int checkPageId;
		memcpy(&checkPageId, (char*)nodeData + readOffset, sizeof(int));
		readOffset = readOffset + sizeof(int);	//page Id

		if(attribute.type == TypeInt)
		{
			int storedKey;
			memcpy(&storedKey, (char*)nodeData + readOffset, sizeof(int));
			readOffset = readOffset + sizeof(int);

			cout<<"\""<<storedKey<<"\"";
		}
		else if(attribute.type == TypeReal)
		{
			float storedKey;
			memcpy(&storedKey, (char*)nodeData + readOffset, sizeof(float));
			readOffset = readOffset + sizeof(float);

			cout<<"\""<<storedKey<<"\"";
		}
		else if(attribute.type == TypeVarChar)
		{
			short varcharLen;
			memcpy(&varcharLen, (char*)nodeData + readOffset, sizeof(short));
			readOffset = readOffset + sizeof(short);

			void* storedKey = malloc(varcharLen);
			memcpy(storedKey, (char*)nodeData + readOffset, varcharLen);
			readOffset = readOffset + varcharLen;

			cout<<"\""<<string((char*)storedKey, varcharLen)<<"\"";
		}

		tempNoOfKeys = tempNoOfKeys + 1;
	}
	cout<<"]," <<endl;
    for(int i=0;i<printLevel;i++)
    {
        cout<<"\t";
    }
	cout<<" \"children\": [" <<endl;

	readOffset = originalReadOffset;					//set itto point to the beginning of the pageID Key section for printing its children
}

void IndexManager::printLeafNode(void* nodeData, short &noOfKeys, short &freeBytesLeft, int &readOffset, const Attribute &attribute, int &printLevel) const
{
	//	int nextpageNo;
	// memcpy(&nextpageNo, (char*)nodeData + readOffset, sizeof(int));
	readOffset = readOffset + sizeof(int);

	short tempNoOfKeys = 1;
	for(int i=0;i<printLevel;i++)
	{
		cout<<"\t";
	}
	cout<<"{ \"keys\" :[";
	while(tempNoOfKeys <= noOfKeys)
	{
		if(tempNoOfKeys!=1)
		{
			cout<<","; //after each key

		}

		if(attribute.type == TypeInt)
		{
			int storedKey;
			memcpy(&storedKey, (char*)nodeData + readOffset, sizeof(int));
			readOffset = readOffset + sizeof(int);

			short noOfRids;
			memcpy(&noOfRids, (char*)nodeData + readOffset, sizeof(short));
			readOffset = readOffset + sizeof(short);

			short tempIdPointer = 1;

			cout<<"\""<<storedKey<<":[";

			while(tempIdPointer <= noOfRids)
			{
				if(tempIdPointer!=1)
				{
					cout<<",";
				}
				int storedPgId;
				memcpy(&storedPgId, (char*)nodeData + readOffset, sizeof(int));
				readOffset = readOffset + sizeof(int);

				int storedSlotNo;
				memcpy(&storedSlotNo, (char*)nodeData + readOffset, sizeof(int));
				readOffset = readOffset + sizeof(int);


				cout<<"("<<storedPgId<<","<<storedSlotNo<<")";
				tempIdPointer = tempIdPointer + 1;
			}
			cout<<"]\""; //after each key
		}
		else if(attribute.type == TypeReal)
		{
			float storedKey;
			memcpy(&storedKey, (char*)nodeData + readOffset, sizeof(float));
			readOffset = readOffset + sizeof(float);

			short noOfRids;
			memcpy(&noOfRids, (char*)nodeData + readOffset, sizeof(short));
			readOffset = readOffset + sizeof(short);


			cout<<"\""<<storedKey<<":[";

			short tempIdPointer = 1;

			while(tempIdPointer <= noOfRids)
			{
				if(tempIdPointer!=1)
				{
					cout<<","; //after each rid

				}

				int storedPgId;
				memcpy(&storedPgId, (char*)nodeData + readOffset, sizeof(int));
				readOffset = readOffset + sizeof(int);

				int storedSlotNo;
				memcpy(&storedSlotNo, (char*)nodeData + readOffset, sizeof(int));
				readOffset = readOffset + sizeof(int);
				cout << "(" << storedPgId << "," << storedSlotNo << ")";
				tempIdPointer = tempIdPointer + 1;



			}
			cout<<"]\""; //after each key
		}
		else
		{
			short varcharLen;
			memcpy(&varcharLen, (char*)nodeData + readOffset, sizeof(short));
			readOffset = readOffset + sizeof(short);

			void* storedKey = malloc(varcharLen);
			memcpy(storedKey, (char*)nodeData + readOffset, varcharLen);
			readOffset = readOffset + varcharLen;

			short noOfRids;
			memcpy(&noOfRids, (char*)nodeData + readOffset, sizeof(short));
			readOffset = readOffset + sizeof(short);

			short tempIdPointer = 1;

			cout<<"\""<<string((char*)storedKey, varcharLen)<<":[";

			while(tempIdPointer <= noOfRids)
			{
				if (tempIdPointer != 1) {
					cout << ","; ///after each rid

				}

				int storedPgId;
				memcpy(&storedPgId, (char *) nodeData + readOffset, sizeof(int));
				readOffset = readOffset + sizeof(int);

				int storedSlotNo;
				memcpy(&storedSlotNo, (char *) nodeData + readOffset, sizeof(int));
				readOffset = readOffset + sizeof(int);


				cout << "(" << storedPgId << "," << storedSlotNo << ")";
				tempIdPointer = tempIdPointer + 1;

			}
			cout<<"]\""; //after each key
		}

		tempNoOfKeys = tempNoOfKeys + 1;

	}
	cout<<"]}"; //after all leaf nodes have been printed from same page
}


void IndexManager::printBTreeInternal(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageId, int &printLevel)const
{
	void* nodeData = malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(pageId, nodeData);

	int readOffset = 0;
	char typeOfPage;
	short noOfKeys;
	short freeBytesLeft;
	short rootNodeCounter = 0;

	getCommonMetaData(nodeData, typeOfPage, noOfKeys, freeBytesLeft, readOffset);

	if(typeOfPage == '1')			//leaf page - >  use leaf page format
	{

		printLeafNode(nodeData, noOfKeys, freeBytesLeft, readOffset, attribute, printLevel);

		printLevel = printLevel -1;


	}
	else //if its a root node
	{
		rootNodeCounter++;
		printNonLeafNode(nodeData, noOfKeys, freeBytesLeft, readOffset, attribute, printLevel); //printed the nodes

		short freeBytesStartAt = PAGE_SIZE - freeBytesLeft;
		int originalReadOffset = readOffset;

		//COMMON for int and float
		while(readOffset < freeBytesStartAt) //to go through each key in the non leaf node
		{
			if(readOffset!=originalReadOffset) //to put the comma at the end of { "keys" :["0:[(0,0)]","1:[(1,3)]"," ...
			{

				cout<<","<<endl;
			}

			int childPageId;
			memcpy(&childPageId, (char*)nodeData + readOffset, sizeof(int));
			readOffset = readOffset + sizeof(int);

			printLevel = printLevel +1;
			printBTreeInternal(ixfileHandle, attribute, childPageId, printLevel);

			int sizeOfKey = sizeof(int);				//default
			if(attribute.type == TypeVarChar && readOffset < freeBytesStartAt)
			{
				short varcharLength;
				memcpy(&varcharLength, (char*)nodeData + readOffset, sizeof(short));
				sizeOfKey = sizeof(short) + varcharLength;

			}
			readOffset = readOffset + sizeOfKey;		//move for the child Page id processed and the key itself [Both int and float]
		}

        if(printLevel !=0)
		{printLevel = printLevel -1;}
        cout<<endl;
		for(int i=0;i<printLevel;i++)
		{
			cout<<"\t";
		}

		if(printLevel!=0)
		{	//cout<<endl<<printLevel<<endl;
			cout<<"]"<<"}"; //last of node
			cout<<","<<endl;
		}
		else
		{
			cout<<endl<<"]"<<"}";//<<endl; //last of node
		}
	}

}

// void IndexManager::printNonLeafNode(void* nodeData, short &noOfKeys, short &freeBytesLeft, int &readOffset, const Attribute &attribute) const
// {
// 	cout<<"Printing NON LEAF NODE"<<endl;
// 	int originalReadOffset = readOffset;
// //	short leftPageId;
// //	short rightPageId;

// //	memcpy(&leftPageId, (char*)nodeData + readOffset, sizeof(int));

// 	short tempNoOfKeys = 1;
// 	while(tempNoOfKeys <= noOfKeys)
// 	{
// 		int checkPageId;
// 		memcpy(&checkPageId, (char*)nodeData + readOffset, sizeof(int));
// 		cout<<"Checking non leaf pg : "<<checkPageId<<endl;
// 		readOffset = readOffset + sizeof(int);	//page Id
// 		if(attribute.type == TypeInt)
// 		{
// 			int storedKey;
// 			memcpy(&storedKey, (char*)nodeData + readOffset, sizeof(int));
// 			readOffset = readOffset + sizeof(int);

// 			cout<<"KEY : "<<storedKey<<endl;
// 		}
// 		else if(attribute.type == TypeReal)
// 		{
// 			float storedKey;
// 			memcpy(&storedKey, (char*)nodeData + readOffset, sizeof(float));
// 			readOffset = readOffset + sizeof(float);

// 			cout<<"KEY : "<<storedKey<<endl;
// 		}
// 		else if(attribute.type == TypeVarChar)
// 		{
// 			short varcharLen;
// 			memcpy(&varcharLen, (char*)nodeData + readOffset, sizeof(short));
// 			readOffset = readOffset + sizeof(short);

// 			void* storedKey = malloc(varcharLen);
// 			memcpy(&storedKey, (char*)nodeData + readOffset, varcharLen);
// 			readOffset = readOffset + varcharLen;

// 			cout<<"KEY : "<<string((char*)storedKey, varcharLen)<<endl;
// 		}

// 		tempNoOfKeys = tempNoOfKeys + 1;
// 	}
// 	readOffset = originalReadOffset;					//set itto point to the beginning of the pageID Key section for printing its children
// }

// void IndexManager::printLeafNode(void* nodeData, short &noOfKeys, short &freeBytesLeft, int &readOffset, const Attribute &attribute) const
// {
// 	cout<<"PRINTING LEAF NODE"<<endl;
// //	int nextpageNo;
// //	memcpy(&nextpageNo, (char*)nodeData + readOffset, sizeof(int));
// 	readOffset = readOffset + sizeof(int);

// 	short tempNoOfKeys = 1;
// 	while(tempNoOfKeys <= noOfKeys)
// 	{
// 		if(attribute.type == TypeInt)
// 		{
// 			int storedKey;
// 			memcpy(&storedKey, (char*)nodeData + readOffset, sizeof(int));
// 			readOffset = readOffset + sizeof(int);

// 			short noOfRids;
// 			memcpy(&noOfRids, (char*)nodeData + readOffset, sizeof(short));
// 			readOffset = readOffset + sizeof(short);

// 			short tempIdPointer = 1;

// 			while(tempIdPointer <= noOfRids)
// 			{
// 				int storedPgId;
// 				memcpy(&storedPgId, (char*)nodeData + readOffset, sizeof(int));
// 				readOffset = readOffset + sizeof(int);

// 				int storedSlotNo;
// 				memcpy(&storedSlotNo, (char*)nodeData + readOffset, sizeof(int));
// 				readOffset = readOffset + sizeof(int);

// 				cout<<"KEY : "<<storedKey<<endl;
// 				cout<<"("<<storedPgId<<","<<storedSlotNo<<")"<<endl;
// 				tempIdPointer = tempIdPointer + 1;
// 			}
// 		}
// 		else if(attribute.type == TypeReal)
// 		{
// 			float storedKey;
// 			memcpy(&storedKey, (char*)nodeData + readOffset, sizeof(float));
// 			readOffset = readOffset + sizeof(float);

// 			short noOfRids;
// 			memcpy(&noOfRids, (char*)nodeData + readOffset, sizeof(short));
// 			readOffset = readOffset + sizeof(short);

// 			short tempIdPointer = 1;

// 			while(tempIdPointer <= noOfRids)
// 			{
// 				int storedPgId;
// 				memcpy(&storedPgId, (char*)nodeData + readOffset, sizeof(int));
// 				readOffset = readOffset + sizeof(int);

// 				int storedSlotNo;
// 				memcpy(&storedSlotNo, (char*)nodeData + readOffset, sizeof(int));
// 				readOffset = readOffset + sizeof(int);

// 				cout<<"KEY : "<<storedKey<<endl;
// 				cout<<"("<<storedPgId<<","<<storedSlotNo<<")"<<endl;
// 				tempIdPointer = tempIdPointer + 1;
// 			}
// 		}
// 		else
// 		{
// 			short varcharLen;
// 			memcpy(&varcharLen, (char*)nodeData + readOffset, sizeof(short));
// 			readOffset = readOffset + sizeof(short);

// 			void* storedKey = malloc(varcharLen);
// 			memcpy(&storedKey, (char*)nodeData + readOffset, varcharLen);
// 			readOffset = readOffset + varcharLen;

// 			short noOfRids;
// 			memcpy(&noOfRids, (char*)nodeData + readOffset, sizeof(short));
// 			readOffset = readOffset + sizeof(short);

// 			short tempIdPointer = 1;

// 			while(tempIdPointer <= noOfRids)
// 			{
// 				int storedPgId;
// 				memcpy(&storedPgId, (char*)nodeData + readOffset, sizeof(int));
// 				readOffset = readOffset + sizeof(int);

// 				int storedSlotNo;
// 				memcpy(&storedSlotNo, (char*)nodeData + readOffset, sizeof(int));
// 				readOffset = readOffset + sizeof(int);

// 				cout<<"KEY : "<<storedKey<<endl;
// 				cout<<"("<<storedPgId<<","<<storedSlotNo<<")"<<endl;
// 				tempIdPointer = tempIdPointer + 1;
// 			}
// 		}
// 		tempNoOfKeys = tempNoOfKeys + 1;
// 	}
// }

// void IndexManager::printBTreeInternal(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageId) const
// {
// 	void* nodeData = malloc(PAGE_SIZE);
// 	ixfileHandle.fileHandle.readPage(pageId, nodeData);

// 	int readOffset = 0;
// 	char typeOfPage;
// 	short noOfKeys;
// 	short freeBytesLeft;

// 	getCommonMetaData(nodeData, typeOfPage, noOfKeys, freeBytesLeft, readOffset);

// 	if(typeOfPage == '1')			//leaf page - >  use leaf page format
// 	{
// 		printLeafNode(nodeData, noOfKeys, freeBytesLeft, readOffset, attribute);
// 	}
// 	else
// 	{
// 		printNonLeafNode(nodeData, noOfKeys, freeBytesLeft, readOffset, attribute);

// 		short freeBytesStartAt = PAGE_SIZE - freeBytesLeft;

// 		if(attribute.type == TypeInt)
// 		{
// 			while(readOffset < freeBytesStartAt)
// 			{
// 				int childPageId;
// 				memcpy(&childPageId, (char*)nodeData + readOffset, sizeof(int));
// 				printBTreeInternal(ixfileHandle, attribute, childPageId);
// 				readOffset = readOffset + 2*sizeof(int);		//move for the child Page id processed and the key itself
// 			}
// 		}
// 		else if(attribute.type == TypeReal)
// 		{
// 			while(readOffset < freeBytesStartAt)
// 			{
// 				int childPageId;
// 				memcpy(&childPageId, (char*)nodeData + readOffset, sizeof(int));
// 				cout<<"Child pg Id : "<<endl;
// 				printBTreeInternal(ixfileHandle, attribute, childPageId);
// 				readOffset = readOffset + sizeof(int) + sizeof(float);		//move for the child Page id processed and the key itself
// 			}
// 		}

// 	}
// }


void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const
{
	if(pagedFileManager->checkIfFileExists(ixfileHandle.fileName) != 0)
	{
		return;
	}
	int printLevel = 0;
	printBTreeInternal(ixfileHandle, attribute, 0, printLevel);
	// printBTreeInternal(ixfileHandle, attribute, 0);

    cout<<endl;

}

IX_ScanIterator::IX_ScanIterator()
{
	readOffset = 0;
	noOfKeys = 0;
	currentKeyPointer = 0;
	currentRId = 0;
	currentNoOfRids = 0;
	nextPageId = 0;
	lowKey = 0;
	highKey = 0;
	lowKeyInclusive = false;
	highKeyInclusive = false;
	ixfileHandle = 0;
}

IX_ScanIterator::~IX_ScanIterator()
{

}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	if(currentNoOfRids != 0)
	{
		if(currentRId == currentNoOfRids)
		{
			currentKeyPointer = currentKeyPointer + 1;
			currentRId = 0;
			currentNoOfRids = 0;
		}
		else
		{
			if(storedAttribute.type == TypeInt || storedAttribute.type == TypeReal)
			{
				memcpy(key, currentKey, sizeof(int));
			}
			else
			{
				int varcharLength;
				memcpy(&varcharLength, currentKey, sizeof(int));
				memcpy(key, currentKey, sizeof(int) + varcharLength);
			}

			memcpy(&rid.pageNum, (char*)pageInMemory + readOffset, sizeof(int));
			readOffset = readOffset + sizeof(int);
			memcpy(&rid.slotNum, (char*)pageInMemory + readOffset, sizeof(int));
			readOffset = readOffset + sizeof(int);
			currentRId = currentRId + 1;
			return 0;
		}
	}

	while(currentKeyPointer < noOfKeys)
	{
		if(storedAttribute.type == TypeInt)
		{
			int storedKey;
			memcpy(&storedKey, (char*)pageInMemory + readOffset, sizeof(int));
			readOffset = readOffset + sizeof(int);

			memcpy(&currentNoOfRids, (char*)pageInMemory + readOffset, sizeof(short));

			readOffset = readOffset + sizeof(short);

			if(( lowKey == NULL || (storedKey > *((int*)lowKey)) || (lowKeyInclusive && storedKey == *((int*)lowKey)))
					&& (highKey == NULL || (storedKey < *((int*)highKey)) || (highKeyInclusive && storedKey == *((int*)highKey))))
			{
				memcpy(key, &storedKey, sizeof(int));
				memcpy(currentKey, &storedKey, sizeof(int));
				memcpy(&rid.pageNum, (char*)pageInMemory + readOffset, sizeof(int));
				readOffset = readOffset + sizeof(int);
				memcpy(&rid.slotNum, (char*)pageInMemory + readOffset, sizeof(int));
				readOffset = readOffset + sizeof(int);
				currentRId = currentRId + 1;
				break;
			}
			else
			{
				readOffset = readOffset + (currentNoOfRids * 2* sizeof(int));
				currentKeyPointer = currentKeyPointer + 1;
				currentRId = 0;
				currentNoOfRids = 0;
			}
		}
		else if(storedAttribute.type == TypeReal)
		{
			float storedKey;
			memcpy(&storedKey, (char*)pageInMemory + readOffset, sizeof(float));
			readOffset = readOffset + sizeof(float);


			memcpy(&currentNoOfRids, (char*)pageInMemory + readOffset, sizeof(short));
			readOffset = readOffset + sizeof(short);

			if((lowKey == NULL || (storedKey > *(float*)lowKey) || (lowKeyInclusive && storedKey == *(float*)lowKey))
					&& (highKey == NULL || (storedKey < *(float*)highKey) || (highKeyInclusive && storedKey == *(float*)highKey)))
			{
				memcpy(key, &storedKey, sizeof(float));
				memcpy(currentKey, &storedKey, sizeof(float));

				memcpy(&rid.pageNum, (char*)pageInMemory + readOffset, sizeof(int));
				readOffset = readOffset + sizeof(int);
				memcpy(&rid.slotNum, (char*)pageInMemory + readOffset, sizeof(int));
				readOffset = readOffset + sizeof(int);
				currentRId = currentRId + 1;
				break;
			}
			else
			{
				readOffset = readOffset + (currentNoOfRids * 2* sizeof(int));
				currentKeyPointer = currentKeyPointer + 1;
				currentRId = 0;
				currentNoOfRids = 0;
			}
		}
		else
		{
			short lengthOfVarchar;
			memcpy(&lengthOfVarchar, (char*)pageInMemory + readOffset, sizeof(short));
			readOffset = readOffset + sizeof(short);

			void* storedKeyPtr = malloc(lengthOfVarchar);
			memcpy(storedKeyPtr, (char*)pageInMemory + readOffset, lengthOfVarchar);
			readOffset = readOffset + lengthOfVarchar;
			string storedKeyStr = string((char*)storedKeyPtr, lengthOfVarchar);
			// cout<<"STORED KEY : "<<storedKeyStr<<endl;

			memcpy(&currentNoOfRids, (char*)pageInMemory + readOffset, sizeof(short));
			readOffset = readOffset + sizeof(short);

			int lowKeyLength;
			int highKeyLength;

			void* lowKeyPtr;
			string lowKeyStr;

			void* highKeyPtr;
			string highKeyStr;

			if(lowKey != NULL)
			{
				memcpy(&lowKeyLength, (char*)lowKey, sizeof(int));
				lowKeyPtr = malloc(lowKeyLength);
				memcpy(lowKeyPtr, (char*)lowKey + sizeof(int), lowKeyLength);
				lowKeyStr = string((char*)lowKeyPtr, lowKeyLength);
				// cout<<"Low Key str : "<<lowKeyStr<<endl;
				free(lowKeyPtr);
			}
			if(highKey != NULL)
			{
				memcpy(&highKeyLength, (char*)highKey, sizeof(int));
				highKeyPtr = malloc(highKeyLength);
				memcpy(highKeyPtr, (char*)highKey + sizeof(int), highKeyLength);
				highKeyStr = string((char*)highKeyPtr, highKeyLength);
				// cout<<"High key str : "<<highKeyStr<<endl;
				free(highKeyPtr);
			}

			if((lowKey == NULL || storedKeyStr.compare(lowKeyStr) > 0 || (lowKeyInclusive && storedKeyStr.compare(lowKeyStr) == 0))
					&& (highKey == NULL || storedKeyStr.compare(highKeyStr) < 0 || (highKeyInclusive && storedKeyStr.compare(highKeyStr) == 0)))
			{
				int intLengthOfVarchar = (int)lengthOfVarchar;
				memcpy(key, &intLengthOfVarchar, sizeof(int));
				memcpy((char*)key + sizeof(int), storedKeyPtr, lengthOfVarchar);

				memcpy(currentKey, &intLengthOfVarchar, sizeof(int));
				memcpy((char*)currentKey + sizeof(int), storedKeyPtr, lengthOfVarchar);

				memcpy(&rid.pageNum, (char*)pageInMemory + readOffset, sizeof(int));
				readOffset = readOffset + sizeof(int);
				memcpy(&rid.slotNum, (char*)pageInMemory + readOffset, sizeof(int));
				readOffset = readOffset + sizeof(int);
				currentRId = currentRId + 1;
				break;
			}
			else
			{
				readOffset = readOffset + (currentNoOfRids * 2* sizeof(int));
				currentKeyPointer = currentKeyPointer + 1;
				currentRId = 0;
				currentNoOfRids = 0;
			}
		}
	}

	if(currentKeyPointer == noOfKeys)
	{
		if(highKey != NULL)
		{
			if(storedAttribute.type == TypeInt)
			{
				int currentKeyInt;
				memcpy(&currentKeyInt, currentKey, sizeof(int));
				int highKeyInt;
				memcpy(&highKeyInt, highKey, sizeof(int));

				if(highKeyInt == currentKeyInt)
				{
					return IX_EOF;
				}

			}
			else if(storedAttribute.type == TypeReal)
			{
				float currentKeyFloat;
				memcpy(&currentKeyFloat, currentKey, sizeof(float));
				float highKeyFloat;
				memcpy(&highKeyFloat, highKey, sizeof(float));

				if(highKeyFloat == currentKeyFloat)
				{
					return IX_EOF;
				}
			}
			else
			{
				int highKeyLength;
				memcpy(&highKeyLength, (char*)highKey, sizeof(int));
				void* highKeyPtr = malloc(highKeyLength);
				memcpy(highKeyPtr, (char*)highKey + sizeof(int), highKeyLength);
				string highKeyStr = string((char*)highKeyPtr, highKeyLength);
				// cout<<"High key str : "<<highKeyStr<<endl;
				free(highKeyPtr);

				int varcharLength;
				memcpy(&varcharLength, currentKey, sizeof(int));
				void* currentKeyPtr = malloc(varcharLength);
				memcpy(currentKeyPtr, (char*)currentKey + sizeof(int),varcharLength);
				string currentKeyStr = string((char*)currentKeyPtr, varcharLength);

				if(highKeyStr.compare(currentKeyStr) == 0)
				{
					return IX_EOF;
				}
			}

		}

		if(nextPageId == -1)		//no more page to scan
		{
			return IX_EOF;
		}
		ixfileHandle->fileHandle.readPage(nextPageId, pageInMemory);
		char typeOfPage;
		short freeBytesLeft;
		readOffset = 0;
		currentRId = 0;
		currentKeyPointer = 0;
		currentNoOfRids = 0;
		IndexManager *im = IndexManager::instance();
		im->getCommonMetaData(pageInMemory, typeOfPage, noOfKeys, freeBytesLeft, readOffset);
		memcpy(&nextPageId, (char*)pageInMemory + readOffset, sizeof(int));
		readOffset = readOffset + sizeof(int);
		return getNextEntry(rid, key);
	}
	return 0;
}

RC IX_ScanIterator::close()
{
	readOffset = 0;
	noOfKeys = 0;
	currentKeyPointer = 0;
	currentRId = 0;
	currentNoOfRids = 0;
	nextPageId = 0;
	ixfileHandle = 0;
	free(pageInMemory);
	free(currentKey);
	return 0;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	appendPageCount = fileHandle.appendPageCounter;
	writePageCount = fileHandle.writePageCounter;
	readPageCount = fileHandle.readPageCounter;
    return 0;
}

