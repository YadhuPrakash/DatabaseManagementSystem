
#include "qe.h"
#include <bitset>

void Iterator::compareStrings(const string &givenVarcharStr, const string &storedVarcharStr, CompOp compOp, bool &satisfied)
{
	switch(compOp)
	{
		case EQ_OP:
		if(storedVarcharStr.compare(givenVarcharStr) == 0)
		{
			satisfied = true;
		}
		break;

		case LT_OP:
		if(storedVarcharStr.compare(givenVarcharStr) < 0)
		 {
			 satisfied = true;
		 }
		break;

		case LE_OP:
	    if(storedVarcharStr.compare(givenVarcharStr) <= 0 )
		 {
			 satisfied = true;
		 }
		break;
		case GT_OP:
		 if(storedVarcharStr.compare(givenVarcharStr) > 0)
		 {
			 satisfied = true;
		 }
		break;
		case GE_OP:
		if(storedVarcharStr.compare(givenVarcharStr) >= 0)
		{
			satisfied = true;
		}
		break;
		case NE_OP:
		 if(storedVarcharStr.compare(givenVarcharStr) != 0)
		 {
			 satisfied = true;
		 }
		break;
		case NO_OP:
			 satisfied = true;
			break;
	}
}


void Iterator::compareInts(const int &givenInt, const int &storedInt, CompOp compOp, bool &satisfied)
{
	switch(compOp)
	{
		case EQ_OP:
		if(storedInt == givenInt)
		{
			satisfied = true;
		}
		break;

		case LT_OP:
		if(storedInt < givenInt)
		{
			satisfied = true;
		}
		break;

		case LE_OP:
	    if(storedInt <= givenInt)
		{
			satisfied = true;
		}
		break;

		case GT_OP:
		if(storedInt > givenInt)
		{
			satisfied = true;
		}
		break;

		case GE_OP:
		if(storedInt >= givenInt)
		{
			satisfied = true;
		}
		break;

		case NE_OP:
		if(storedInt != givenInt)
		{
			satisfied = true;
		}
		break;

		case NO_OP:
			 satisfied = true;
		break;
	}
}

void Iterator::compareFloats(const float &givenFloat, const float &storedFloat, CompOp compOp, bool &satisfied)
{

	switch(compOp)
	{
		case EQ_OP:
		if(storedFloat == givenFloat)
		{
			satisfied = true;
		}
		break;

		case LT_OP:
		if(storedFloat < givenFloat)
		{
			satisfied = true;
		}
		break;

		case LE_OP:
	    if(storedFloat <= givenFloat)
		{
			satisfied = true;
		}
		break;

		case GT_OP:
		if(storedFloat > givenFloat)
		{
			satisfied = true;
		}
		break;

		case GE_OP:
		if(storedFloat >= givenFloat)
		{
			satisfied = true;
		}
		break;

		case NE_OP:
		if(storedFloat != givenFloat)
		{
			satisfied = true;
		}
		break;

		case NO_OP:
		satisfied = true;
		break;
	}
}


Filter::Filter(Iterator* input, const Condition &condition)
{
	storedIterator = input;
	storedCondition = condition;
	attributesInitialised = false;
}

RC Filter::getNextTuple(void *data)
{
	if(!attributesInitialised)
	{
		storedIterator->getAttributes(storedAttrs);
		attributesInitialised = true;
	}

	int nullBitIndicatorSize = ceil(float(storedAttrs.size())/CHAR_BIT);

	CompOp compOp = storedCondition.op;

	void* storedTuple = malloc(PAGE_SIZE);

	while(storedIterator->getNextTuple(storedTuple) != RM_EOF)
	{
		//parse the data
		int readOffset = 0;
		unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullBitIndicatorSize);
		memcpy(nullFieldsIndicator, storedTuple, nullBitIndicatorSize);
		readOffset = readOffset + nullBitIndicatorSize;

		short i  = 0;
		bool nullBit = false;
		bool satisfied = false;

		for (vector<Attribute>::const_iterator  it = storedAttrs.begin() ; it != storedAttrs.end(); ++it, i++)
		{
			nullBit = nullFieldsIndicator[i/CHAR_BIT] & (1 << (7 - (i%CHAR_BIT)));		//nullBitIndicatorSize in bytes, so multiply by 8 for bits

			if(nullBit && storedCondition.lhsAttr.compare(it->name) == 0)
			{
				break;
			}

			if(!nullBit)
			{
				if(it->type == TypeVarChar)
				{
					string storedVarcharStr;
					string givenVarcharStr;

					int storedVarcharLen;
					memcpy(&storedVarcharLen, (char *)storedTuple + readOffset, sizeof(int));
					void* storedVarchar = malloc(storedVarcharLen);
					memcpy(storedVarchar, (char*)storedTuple + readOffset + sizeof(int), storedVarcharLen);
					readOffset = readOffset + sizeof(int) + storedVarcharLen;

					if(storedCondition.lhsAttr.compare(it->name) == 0)
					{
						storedVarcharStr = string((char*)storedVarchar, storedVarcharLen);
						int givenVarcharLen;
						memcpy(&givenVarcharLen, (char *)storedCondition.rhsValue.data, sizeof(int));
						void* givenVarchar = malloc(givenVarcharLen);
						memcpy(givenVarchar, (char*)storedCondition.rhsValue.data + sizeof(int), givenVarcharLen);
						givenVarcharStr = string((char*)givenVarchar, givenVarcharLen);
						free(givenVarchar);

						compareStrings(givenVarcharStr, storedVarcharStr, compOp, satisfied);
					}

					free(storedVarchar);
				}
				else if (it->type == TypeInt)
				{
					if(storedCondition.lhsAttr.compare(it->name) == 0)
					{
						int givenInt;
						memcpy(&givenInt, (char*)storedCondition.rhsValue.data, sizeof(int));

						int storedInt;
						memcpy(&storedInt, (char*)storedTuple + readOffset, sizeof(int));

						compareInts(givenInt, storedInt, compOp, satisfied);
					}
					readOffset  = readOffset + sizeof(int);
				}
				else if (it->type == TypeReal)
				{
					if(storedCondition.lhsAttr.compare(it->name) == 0)
					{
						float givenFloat;
						memcpy(&givenFloat, (char*)storedCondition.rhsValue.data, sizeof(float));

						float storedFloat;
						memcpy(&storedFloat, (char*)storedTuple + readOffset, sizeof(float));
						compareFloats(givenFloat, storedFloat, compOp, satisfied);
					}
					readOffset  = readOffset + sizeof(float);
				}
				// Cannot break since we need to parse the entire tuple to get the readOffset which is the length of the data
				// to be copied in data*
				// if(satisfied)
				// {
				// 	break;
				// }
			}
		}
		if(satisfied)
		{
			memcpy(data, storedTuple, readOffset);
			free(storedTuple);
			return 0;
		}
	}
	free(storedTuple);
	return QE_EOF;
}

void Filter::getAttributes(vector<Attribute> &attrs) const
{
	storedIterator->getAttributes(attrs);
	// for(int i = 0; i < attrs.size(); i++)
	// {
	// 	cout<<"ATTR : "<<attrs[i].name<<" ";
	// }
	// cout<<endl;
}

//Filter Ends here

Project::Project(Iterator *input, const vector<string> &attrNames)
{
	storedIterator = input;
	storedAttrNames = attrNames;
	attributesInitialised = false;
	returnAttributesInitialised = false;
};

RC Project::getNextTuple(void *data)
{
	if(!attributesInitialised)
	{
		storedIterator->getAttributes(storedAttrs);
		attributesInitialised = true;
	}

	int writeNullBitIndicatorSize = ceil(float(storedAttrNames.size())/CHAR_BIT);
	int readNullBitIndicatorSize = ceil(float(storedAttrs.size())/CHAR_BIT);

	void* storedTuple = malloc(PAGE_SIZE);

	if(storedIterator->getNextTuple(storedTuple)!= RM_EOF)
	{
		short j = 0;

		unsigned char *nullFieldsIndicator = (unsigned char *) malloc(readNullBitIndicatorSize);
		memcpy(nullFieldsIndicator, storedTuple, readNullBitIndicatorSize);

		unsigned char *writeNullFieldsIndicator = (unsigned char *) malloc(writeNullBitIndicatorSize);
		memset(writeNullFieldsIndicator, 0, writeNullBitIndicatorSize);
		int writeOffset = writeNullBitIndicatorSize;

		for (vector<string>::const_iterator itName = storedAttrNames.begin() ; itName != storedAttrNames.end(); ++itName, j++)
		{
			string attrName = *itName;
			int readOffset = readNullBitIndicatorSize;

			short i  = 0;
			bool nullBit = false;

			for (vector<Attribute>::const_iterator  it = storedAttrs.begin() ; it != storedAttrs.end(); ++it, i++)
			{
				nullBit = nullFieldsIndicator[i/CHAR_BIT] & (1 << (7 - (i%CHAR_BIT)));		//nullBitIndicatorSize in bytes, so multiply by 8 for bits

				if(it->name.compare(attrName) == 0)
				{
					if(!returnAttributesInitialised)
					{
						returnAttrs.push_back(*it);
					}
				}
				if(!nullBit)
				{
					if(it->type == TypeVarChar)
					{
						int storedVarcharLen;
						memcpy(&storedVarcharLen, (char *)storedTuple + readOffset, sizeof(int));

						if(it->name.compare(attrName) == 0)
						{
							memcpy((char*)data + writeOffset, (char*)storedTuple + readOffset, sizeof(int) + storedVarcharLen);
							writeOffset = writeOffset + sizeof(int) + storedVarcharLen;
							break;
						}

						readOffset = readOffset + sizeof(int) + storedVarcharLen;
					}
					else if (it->type == TypeInt)
					{
						if(it->name.compare(attrName) == 0)
						{
							// int storedInt;
							// memcpy(&storedInt, (char*)storedTuple + readOffset, sizeof(int));
							memcpy((char*)data + writeOffset, (char*)storedTuple + readOffset, sizeof(int));
							writeOffset = writeOffset + sizeof(int);
							break;
						}
						readOffset  = readOffset + sizeof(int);
					}
					else if (it->type == TypeReal)
					{
						if(it->name.compare(attrName) == 0)
						{
							// float givenFloat;
							// memcpy(&givenFloat, (char*)storedCondition.rhsValue.data, sizeof(float));

							memcpy((char*)data + writeOffset, (char*)storedTuple + readOffset, sizeof(float));
							writeOffset = writeOffset + sizeof(float);
							break;
						}
						readOffset  = readOffset + sizeof(float);
					}
				}
				else
				{
					if(it->name.compare(attrName) == 0)
					{
						writeNullFieldsIndicator[j/CHAR_BIT] = writeNullFieldsIndicator[j/CHAR_BIT] | 1 << (CHAR_BIT - j);		//set null bit indicator for that field
					}
				}

			}
		}
		returnAttributesInitialised = true;
		memcpy(data, writeNullFieldsIndicator, writeNullBitIndicatorSize);
		free(storedTuple);
		return 0;
	}
	free(storedTuple);
	return QE_EOF;
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
	vector<Attribute> receievedAttrs;
	storedIterator->getAttributes(receievedAttrs);

	for(unsigned int i  = 0; i < storedAttrNames.size(); i++)
	{
		string attrName = storedAttrNames[i];

		for(unsigned int j = 0;j < receievedAttrs.size(); j++)
		{
			if(receievedAttrs[j].name.compare(attrName) == 0)
			{
				// cout<<receievedAttrs[j].name<<endl;
				attrs.push_back(receievedAttrs[j]);
			}
		}
	}
	// for(unsigned int i = 0; i < attrs.size(); i++)
	// {
	// 	cout<<"PROJ ATTR : "<<attrs[i].name<<" ";
	// }
	// cout<<endl;
}

//Project ends here


INLJoin::INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        )
{
	storedLeftIterator = leftIn;
	storedIndexScanIterator = rightIn;
	storedCondition = condition;
	attributesInitialised = false;
	finishedScanOfRightIn = true;
	storedLeftTuple = malloc(PAGE_SIZE);
	searchKey = malloc(PAGE_SIZE);
	nullFieldsIndicatorLeftIn = 0;
	leftInSize = 0;
	rightInSize = 0;
}

BNLJoin::BNLJoin(Iterator *leftIn,            // Iterator of input R
TableScan *rightIn,           // TableScan Iterator of input S
const Condition &condition,   // Join condition
const unsigned numPages       // # of pages that can be loaded into memory,
//   i.e., memory block size (decided by the optimizer)
)
	{
		storedRightTableScan = rightIn;
		storedLeftIterator = leftIn;
		storedCondition = condition;
		this->numPages = numPages;
		outputBuffer = malloc(PAGE_SIZE);
		buffer = malloc(PAGE_SIZE*numPages);


	}

void INLJoin::fuseNullBitIndicatorsNWrite(unsigned char* nullFieldsIndicatorLeftIn, unsigned char* nullFieldsIndicatorRightIn, void* data, int &writeOffset)
{
	int finalNullFieldsIndicatorSize = 	ceil(float((leftInAttributes.size() + rightInAttributes.size()))/CHAR_BIT);
	unsigned char *finalNullFieldsIndicator = (unsigned char *) malloc(finalNullFieldsIndicatorSize);
	memset(finalNullFieldsIndicator, 0, finalNullFieldsIndicatorSize);

	int j = 0;
	bool nullBit = false;

	for(unsigned int i = 0; i < leftInAttributes.size(); i++)
	{
		nullBit = nullFieldsIndicatorLeftIn[i/CHAR_BIT] & (1 << (7 - (i%CHAR_BIT)));		//nullBitIndicatorSize in bytes, so multiply by 8 for bits

		if(nullBit)
		{
			finalNullFieldsIndicator[j/CHAR_BIT] = finalNullFieldsIndicator[j/CHAR_BIT] | 1 << (CHAR_BIT - j);		//set null bit indicator for that field
		}
		j++;
	}

	nullBit = false;

	for(unsigned int i = 0; i < rightInAttributes.size(); i++)
	{
		nullBit = nullFieldsIndicatorRightIn[i/CHAR_BIT] & (1 << (7 - (i%CHAR_BIT)));		//nullBitIndicatorSize in bytes, so multiply by 8 for bits

		if(nullBit)
		{
			finalNullFieldsIndicator[j/CHAR_BIT] = finalNullFieldsIndicator[j/CHAR_BIT] | 1 << (CHAR_BIT - j);		//set null bit indicator for that field
		}
		j++;
	}
	// cout<<"FINAL NULL FIELDS INDICATOR  : "<<bitset<8>(*finalNullFieldsIndicator)<<endl;

	memcpy((char*)data + writeOffset, finalNullFieldsIndicator, finalNullFieldsIndicatorSize);
	writeOffset = writeOffset + finalNullFieldsIndicatorSize;
}


int INLJoin::getLengthOfTuple(void* tuple, vector<Attribute> &attributes)
{
	int readOffset = ceil(float(attributes.size())/CHAR_BIT);

	if(tuple == NULL)
	{
		for (vector<Attribute>::const_iterator  it = attributes.begin() ; it != attributes.end(); ++it)
		{
			if(it->type == TypeVarChar)
			{
				readOffset = readOffset + sizeof(int) + it->length;
			}
			else if (it->type == TypeInt)
			{
				readOffset  = readOffset + sizeof(int);
			}
			else if (it->type == TypeReal)
			{
				readOffset  = readOffset + sizeof(float);
			}
		}
		return readOffset;
	}
	else
	{
		unsigned char *nullFieldsIndicator = (unsigned char *) malloc(readOffset);
		memcpy(nullFieldsIndicator, tuple, readOffset);

		short i  = 0;
		bool nullBit = false;

		for (vector<Attribute>::const_iterator  it = attributes.begin() ; it != attributes.end(); ++it, i++)
		{
			nullBit = nullFieldsIndicator[i/CHAR_BIT] & (1 << (7 - (i%CHAR_BIT)));		//nullBitIndicatorSize in bytes, so multiply by 8 for bits

			if(!nullBit)
			{
				if(it->type == TypeVarChar)
				{
					int storedVarcharLen;
					memcpy(&storedVarcharLen, (char *)tuple + readOffset, sizeof(int));
					readOffset = readOffset + sizeof(int) + storedVarcharLen;
				}
				else if (it->type == TypeInt)
				{
					readOffset  = readOffset + sizeof(int);
				}
				else if (it->type == TypeReal)
				{
					readOffset  = readOffset + sizeof(float);
				}
			}
		}

		free(nullFieldsIndicator);
	}

	return readOffset;
}


RC INLJoin::getNextTuple(void *data)
{
	if(!attributesInitialised)
	{
		storedLeftIterator->getAttributes(leftInAttributes);
		storedIndexScanIterator->getAttributes(rightInAttributes);
		rightInSize = getLengthOfTuple(NULL, rightInAttributes);
		attributesInitialised = true;
	}

	int nullBitIndicatorSizeLeftIn = ceil(float(leftInAttributes.size())/CHAR_BIT);
	int nullBitIndicatorSizeRightIn = ceil(float(rightInAttributes.size())/CHAR_BIT);

	if(finishedScanOfRightIn)
	{
		if(storedLeftIterator->getNextTuple(storedLeftTuple) != RM_EOF)
		{
			//parse the data
			if(nullFieldsIndicatorLeftIn != 0)
			{
				free(nullFieldsIndicatorLeftIn);
			}
			int readOffset = 0;
			nullFieldsIndicatorLeftIn = (unsigned char *) malloc(nullBitIndicatorSizeLeftIn);
			memcpy(nullFieldsIndicatorLeftIn, storedLeftTuple, nullBitIndicatorSizeLeftIn);
			readOffset = readOffset + nullBitIndicatorSizeLeftIn;

			short i  = 0;
			bool nullBit = false;

			for (vector<Attribute>::const_iterator  it = leftInAttributes.begin() ; it != leftInAttributes.end(); ++it, i++)
			{
				nullBit = nullFieldsIndicatorLeftIn[i/CHAR_BIT] & (1 << (7 - (i%CHAR_BIT)));		//nullBitIndicatorSizeLeftIn in bytes, so multiply by 8 for bits

				if(nullBit && storedCondition.lhsAttr.compare(it->name) == 0)
				{
					break;
				}

				if(!nullBit)
				{
					if(it->type == TypeVarChar)
					{
						int storedVarcharLen;
						memcpy(&storedVarcharLen, (char *)storedLeftTuple + readOffset, sizeof(int));

						if(storedCondition.lhsAttr.compare(it->name) == 0)
						{
							memcpy(searchKey, (char*)storedLeftTuple + readOffset, sizeof(int) + storedVarcharLen);
						}
						readOffset = readOffset + sizeof(int) + storedVarcharLen;
					}
					else if (it->type == TypeInt)
					{
						if(storedCondition.lhsAttr.compare(it->name) == 0)
						{
							memcpy(searchKey, (char*)storedLeftTuple + readOffset, sizeof(int));
						}
						readOffset  = readOffset + sizeof(int);
					}
					else if (it->type == TypeReal)
					{
						if(storedCondition.lhsAttr.compare(it->name) == 0)
						{
							memcpy(searchKey, (char*)storedLeftTuple + readOffset, sizeof(float));
						}
						readOffset  = readOffset + sizeof(float);
					}
				}
			}
			leftInSize = readOffset;
		}
		else
		{
			free(nullFieldsIndicatorLeftIn);
			return QE_EOF;
		}
		storedIndexScanIterator->setIterator(searchKey, searchKey, true, true);

//		float checker;
//		memcpy(&checker, (char*)searchKey,sizeof(float));
//		cout<<"Search Key : "<<checker<<endl;
	}

	void* storedRightTuple = malloc(PAGE_SIZE);

	if(storedIndexScanIterator->getNextTuple(storedRightTuple) != IX_EOF)
	{
		finishedScanOfRightIn = false;
		unsigned char *nullFieldsIndicatorRightIn = (unsigned char *) malloc(nullBitIndicatorSizeRightIn);
		memcpy(nullFieldsIndicatorRightIn, storedRightTuple, nullBitIndicatorSizeRightIn);
		int writeOffset = 0;
		fuseNullBitIndicatorsNWrite(nullFieldsIndicatorLeftIn, nullFieldsIndicatorRightIn, data, writeOffset);

		memcpy((char*)data + writeOffset, (char*)storedLeftTuple + nullBitIndicatorSizeLeftIn, leftInSize - nullBitIndicatorSizeLeftIn);
		writeOffset = writeOffset + leftInSize - nullBitIndicatorSizeLeftIn;
		int sizeOfRightTuple = getLengthOfTuple(storedRightTuple, rightInAttributes);
		memcpy((char*)data + writeOffset, (char*)storedRightTuple + nullBitIndicatorSizeRightIn, sizeOfRightTuple - nullBitIndicatorSizeRightIn);
		writeOffset = writeOffset + sizeOfRightTuple - nullBitIndicatorSizeRightIn;
	}
	else
	{
		finishedScanOfRightIn = true;
	}
	free(storedRightTuple);
	if(finishedScanOfRightIn)
	{
		return getNextTuple(data);
	}
	return 0;
}

// For attribute in vector<Attribute>, name it as rel.attr
void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
	vector<Attribute> leftInAttrs;
	storedLeftIterator->getAttributes(leftInAttrs);

	vector<Attribute> rightInAttrs;
	storedIndexScanIterator->getAttributes(rightInAttrs);

	for(unsigned int i = 0; i < leftInAttrs.size(); i++)
	{
		attrs.push_back(leftInAttrs[i]);
	}

	for(unsigned int i = 0; i < rightInAttrs.size(); i++)
	{
		attrs.push_back(rightInAttrs[i]);
	}
}
//INL JOIN ENDS HERE
// BNL starts HERE

RC BNLJoin::buildBuffer() //and adding to hash map.
{
	vector<Attribute> leftRecordDescriptor;
	Attribute currentAttribute;
	storedLeftIterator->getAttributes(leftRecordDescriptor);
	int offsetToThisRecord;
	writeOffsetForBuffer = 0;
	//loading bufferPage and adding to hash map
	int sizeOfNextTuple =  getLengthOfTuple(NULL, leftRecordDescriptor);
	while(writeOffsetForBuffer+sizeOfNextTuple < PAGE_SIZE*numPages) {
		void *data = malloc(1000);

		if (storedLeftIterator->getNextTuple(data) != RBFM_EOF) { //potential problem
			int size = getLengthOfTuple(data, leftRecordDescriptor);
			//cout<<size;
			//writing to buffer
			offsetToThisRecord = writeOffsetForBuffer;
			memcpy((char *) buffer + writeOffsetForBuffer, &size, sizeof(int));
			writeOffsetForBuffer = writeOffsetForBuffer + sizeof(int);
			memcpy((char *) buffer + writeOffsetForBuffer, data, size);
			writeOffsetForBuffer = writeOffsetForBuffer + size;

			//get the key we need to add to hash map.
			for (unsigned int i = 0; i < leftRecordDescriptor.size(); i++) {
				if (storedCondition.lhsAttr == leftRecordDescriptor[i].name) {
					currentAttribute = leftRecordDescriptor[i];
					void *key = malloc(1000);
					relationManager->extractedKey(key, leftRecordDescriptor, i, data);

					if (currentAttribute.type == TypeInt) {
						int keyForHashTable;
						memcpy(&keyForHashTable, key, sizeof(int));
						mapForLeftInt.insert(std::make_pair(keyForHashTable,offsetToThisRecord));

					} else if (currentAttribute.type == TypeReal) {
						float keyForHashTable;
						memcpy(&keyForHashTable, key, sizeof(float));
						mapForLeftFloat.insert(std::make_pair(keyForHashTable,offsetToThisRecord));
					} else {
						int varcharLen;
						memcpy(&varcharLen, key, sizeof(int));
						void *keyForHashTable = malloc(varcharLen);
						memcpy(keyForHashTable, (char *) key + sizeof(int), varcharLen);
						string currentKey = string((char *) keyForHashTable, varcharLen);
						mapForLeftString.insert(std::make_pair(currentKey,offsetToThisRecord));

					}
					free(key);
				}
			} //hash map addition over
			getNextTupleFinalOffset = writeOffsetForBuffer;
		} else
		{
			bufferCompleted = true;
			return 1;
		}
	free(data);
	}
	return 1;

}



RC BNLJoin::buildOutputBuffer() {
	//adding data to output buffer.
	//1) extract the key.
	//2) check if it exists in the hash table.
	//3) if true get data from buffer page.
	//4) fuse both tuples.
	//5) generate fused nullbitindicator.
	//6) insert into output buffer.

	vector<Attribute> rightRecordDescriptor;
	Attribute currentAttribute;
	storedRightTableScan->getAttributes(rightRecordDescriptor);
	vector<Attribute> leftRecordDescriptor;
	storedLeftIterator->getAttributes(leftRecordDescriptor);
	int offsetToThisRecord;
	void *rightData = malloc(1000);
	int sizeOfNextTuple = getLengthOfTuple(NULL, rightRecordDescriptor);
	int sizeOfNextLeftTuple = getLengthOfTuple(NULL, leftRecordDescriptor);

	writeOffsetForOutputBuffer = 0;
	while (writeOffsetForOutputBuffer + sizeOfNextTuple + sizeOfNextLeftTuple < PAGE_SIZE) {

		if (storedRightTableScan->getNextTuple(rightData) != RBFM_EOF)
		{

			int rightNullBitIndicatorSize = ceil(float(rightRecordDescriptor.size())/CHAR_BIT);
			unsigned char* rightNullBitIndicator = (unsigned char*)malloc(rightNullBitIndicatorSize);
			memcpy(rightNullBitIndicator,rightData,rightNullBitIndicatorSize);

			int sizeOfRightData = getLengthOfTuple(rightData, rightRecordDescriptor);
			sizeOfRightData = sizeOfRightData - rightNullBitIndicatorSize;

			for (unsigned int i = 0; i < rightRecordDescriptor.size(); i++) { //finding out which attribute join is on
				if (storedCondition.rhsAttr == rightRecordDescriptor[i].name) {
					currentAttribute = rightRecordDescriptor[i];
					void *key = malloc(1000);
					relationManager->extractedKey(key, rightRecordDescriptor, i, rightData);

					if (currentAttribute.type == TypeInt) //depending on type, choose the hashmap to look in
					{
						int keyForHashTable;
						memcpy(&keyForHashTable, key, sizeof(int));

						int countOfKey =  (int)mapForLeftInt.count(keyForHashTable);
						//testing
//						cout<<endl<<"countOfkey"<<countOfKey<<endl;

						std::multimap<int,int>::iterator it = mapForLeftInt.find(keyForHashTable);
						for(int i=0;i<countOfKey;i++)
						{


							int offsetToBuffer = it->second;

							//testing
//							cout<<endl<<"Offset To built Buffer's key"<<offsetToBuffer<<endl;

							void *leftData = malloc(1000); //potential problem
							int sizeOfBufferData;
							memcpy(&sizeOfBufferData, (char *) buffer + offsetToBuffer, sizeof(int));
							memcpy(leftData, (char *) buffer + offsetToBuffer + sizeof(int), sizeOfBufferData);

							//testing OK WORKING
//							int leftInt;
//							memcpy(&leftInt,(char *)leftData+1, sizeof(int));
//							cout<<endl<<leftInt<<endl;
//							memcpy(&leftInt,(char *)leftData+5, sizeof(int));
//							cout<<endl<<leftInt<<endl;
//							float LeftFloat;
//							memcpy(&LeftFloat,(char *)leftData+9, sizeof(int));
//							cout<<endl<<LeftFloat<<endl;


							int leftNullBitIndicatorSize = ceil(float(leftRecordDescriptor.size())/CHAR_BIT);
							unsigned char *leftNullBitIndicator= (unsigned char *) malloc(leftNullBitIndicatorSize);
							memcpy(leftNullBitIndicator,leftData,leftNullBitIndicatorSize);

							void* finalNullBitIndicator= malloc(100);
							int writeOffset =0;
							int finalNullFieldsIndicatorSize = 	ceil(float((leftRecordDescriptor.size() + rightRecordDescriptor.size()))/CHAR_BIT);
							fuseNullBitIndicatorsNWrite(leftNullBitIndicator,rightNullBitIndicator,finalNullBitIndicator,writeOffset);

							void *finalJoinData = malloc(PAGE_SIZE);
							int finalwriteoffset=0;
							memcpy((char *)finalJoinData+finalwriteoffset,finalNullBitIndicator,finalNullFieldsIndicatorSize);
							finalwriteoffset = finalwriteoffset +finalNullFieldsIndicatorSize;

							//problem
							sizeOfBufferData =sizeOfBufferData - leftNullBitIndicatorSize;
							memcpy((char *)finalJoinData+finalwriteoffset,(char *)leftData + leftNullBitIndicatorSize,sizeOfBufferData);
							finalwriteoffset = finalwriteoffset +sizeOfBufferData;

							//problem
							memcpy((char *)finalJoinData+finalwriteoffset,(char *)rightData+rightNullBitIndicatorSize,sizeOfRightData);
							finalwriteoffset = finalwriteoffset +sizeOfRightData;

							//testing(size of both data(12+12)+nullbit(1))
							// THIS Is the size of final data.
//							cout<<endl<<"sizeOfJoined Data"<<finalwriteoffset<<endl;
//							int testOffset = writeOffsetForOutputBuffer;
//							cout<<endl<<"test Offset"<<testOffset<<endl;


							memcpy((char *)outputBuffer+writeOffsetForOutputBuffer,&finalwriteoffset, sizeof(int));
							writeOffsetForOutputBuffer = writeOffsetForOutputBuffer + sizeof(int);
							memcpy((char *)outputBuffer+writeOffsetForOutputBuffer,finalJoinData, finalwriteoffset);
							writeOffsetForOutputBuffer = writeOffsetForOutputBuffer + finalwriteoffset;

							//testing WORKING OK
//							int leftInt;
//							memcpy(&leftInt,(char *)outputBuffer+testOffset+ sizeof(int)+finalNullFieldsIndicatorSize, sizeof(int));
//							cout<<endl<<"left first attr"<<leftInt<<endl;
//							memcpy(&leftInt,(char *)outputBuffer+testOffset+sizeof(int)+finalNullFieldsIndicatorSize+4, sizeof(int));
//							cout<<endl<<"left second attr"<<leftInt<<endl;
//							float LeftFloat;
//							memcpy(&LeftFloat,(char *)outputBuffer+testOffset+sizeof(int)+finalNullFieldsIndicatorSize+8, sizeof(int));
//							cout<<endl<<"left third attr"<<LeftFloat<<endl;
//							memcpy(&leftInt,(char *)outputBuffer+testOffset+sizeof(int)+finalNullFieldsIndicatorSize+12, sizeof(int));
//							cout<<endl<<"right first attr"<<leftInt<<endl;
//							memcpy(&LeftFloat,(char *)outputBuffer+testOffset+sizeof(int)+finalNullFieldsIndicatorSize+16, sizeof(int));
//							cout<<endl<<"right second attr"<<LeftFloat<<endl;
//							memcpy(&leftInt,(char *)outputBuffer+testOffset+sizeof(int)+finalNullFieldsIndicatorSize+20, sizeof(int));
//							cout<<endl<<"right third attr"<<leftInt<<endl;






							free(leftData);
							free(leftNullBitIndicator);
							free(finalNullBitIndicator);
							free(finalJoinData);
							it++;
						}

					} else if (currentAttribute.type == TypeReal) {
						float keyForHashTable;
						memcpy(&keyForHashTable, key, sizeof(float));


						int countOfKey =  (int)mapForLeftFloat.count(keyForHashTable);
						std::multimap<float,int>::iterator it = mapForLeftFloat.find(keyForHashTable);
						for(int i=0;i<countOfKey;i++)
						{

							int offsetToBuffer = it->second;
							void *leftData = malloc(1000); //potential problem
							int sizeOfBufferData;
							memcpy(&sizeOfBufferData, (char *) buffer + offsetToBuffer, sizeof(int));
							memcpy(leftData, (char *) buffer + offsetToBuffer + sizeof(int), sizeOfBufferData);

							int leftNullBitIndicatorSize = ceil(float(leftRecordDescriptor.size())/CHAR_BIT);
							unsigned char *leftNullBitIndicator= (unsigned char *) malloc(leftNullBitIndicatorSize);
							memcpy(leftNullBitIndicator,leftData,leftNullBitIndicatorSize);


							void* finalNullBitIndicator= malloc(100);
							int writeOffset =0;
							int finalNullFieldsIndicatorSize = 	ceil(float((leftRecordDescriptor.size() + rightRecordDescriptor.size()))/CHAR_BIT);


							fuseNullBitIndicatorsNWrite(leftNullBitIndicator,rightNullBitIndicator,finalNullBitIndicator,writeOffset);

							void *finalJoinData = malloc(PAGE_SIZE);
							int finalwriteoffset=0;
							memcpy((char *)finalJoinData+finalwriteoffset,finalNullBitIndicator,finalNullFieldsIndicatorSize);
							finalwriteoffset = finalwriteoffset +finalNullFieldsIndicatorSize;

							//problem
							sizeOfBufferData =sizeOfBufferData - leftNullBitIndicatorSize;
							memcpy((char *)finalJoinData+finalwriteoffset,(char *)leftData + leftNullBitIndicatorSize,sizeOfBufferData);
							finalwriteoffset = finalwriteoffset +sizeOfBufferData;

							//problem
						//	sizeOfRightData = sizeOfRightData - rightNullBitIndicatorSize;
							memcpy((char *)finalJoinData+finalwriteoffset,(char *)rightData+rightNullBitIndicatorSize,sizeOfRightData);
							finalwriteoffset = finalwriteoffset +sizeOfRightData;


							memcpy((char *)outputBuffer+writeOffsetForOutputBuffer,&finalwriteoffset, sizeof(int));
							writeOffsetForOutputBuffer = writeOffsetForOutputBuffer + sizeof(int);
							memcpy((char *)outputBuffer+writeOffsetForOutputBuffer,finalJoinData, finalwriteoffset);
							writeOffsetForOutputBuffer = writeOffsetForOutputBuffer + finalwriteoffset;

							it++;
							free(leftData);
							free(leftNullBitIndicator);
							free(finalNullBitIndicator);
							free(finalJoinData);
						}

					} else {
						int varcharLen;
						memcpy(&varcharLen, key, sizeof(int));
						void *keyForHashTable = malloc(varcharLen);
						memcpy(keyForHashTable, (char *) key + sizeof(int), varcharLen);
						string currentKey = string((char *) keyForHashTable, varcharLen);

						int countOfKey =  (int)mapForLeftString.count(currentKey);
						std::multimap<string,int>::iterator it = mapForLeftString.find(currentKey);
						for(int i=0;i<countOfKey;i++)
						{
							int offsetToBuffer = it->second;
							void *leftData = malloc(1000); //potential problem
							int sizeOfBufferData;
							memcpy(&sizeOfBufferData, (char *) buffer + offsetToBuffer, sizeof(int));
							memcpy(leftData, (char *) buffer + offsetToBuffer + sizeof(int), sizeOfBufferData);

							int leftNullBitIndicatorSize = ceil(float(leftRecordDescriptor.size())/CHAR_BIT);
							unsigned char *leftNullBitIndicator= (unsigned char *) malloc(leftNullBitIndicatorSize);
							memcpy(leftNullBitIndicator,leftData,leftNullBitIndicatorSize);

							void* finalNullBitIndicator= malloc(100);
							int writeOffset =0;
							int finalNullFieldsIndicatorSize = 	ceil(float((leftRecordDescriptor.size() + rightRecordDescriptor.size()))/CHAR_BIT);
							fuseNullBitIndicatorsNWrite(leftNullBitIndicator,rightNullBitIndicator,finalNullBitIndicator,writeOffset);

							void *finalJoinData = malloc(PAGE_SIZE);
							int finalwriteoffset=0;
							memcpy((char *)finalJoinData+finalwriteoffset,finalNullBitIndicator,finalNullFieldsIndicatorSize);
							finalwriteoffset = finalwriteoffset +finalNullFieldsIndicatorSize;

							//problem
							sizeOfBufferData =sizeOfBufferData - leftNullBitIndicatorSize;
							memcpy((char *)finalJoinData+finalwriteoffset,(char *)leftData + leftNullBitIndicatorSize,sizeOfBufferData);
							finalwriteoffset = finalwriteoffset +sizeOfBufferData;

							//problem
							//ssizeOfRightData = sizeOfRightData - rightNullBitIndicatorSize;
							memcpy((char *)finalJoinData+finalwriteoffset,(char *)rightData+rightNullBitIndicatorSize,sizeOfRightData);
							finalwriteoffset = finalwriteoffset +sizeOfRightData;

							memcpy((char *)outputBuffer+writeOffsetForOutputBuffer,&finalwriteoffset, sizeof(int));
							writeOffsetForOutputBuffer = writeOffsetForOutputBuffer + sizeof(int);
							memcpy((char *)outputBuffer+writeOffsetForOutputBuffer,finalJoinData, finalwriteoffset);
							writeOffsetForOutputBuffer = writeOffsetForOutputBuffer + finalwriteoffset;


							it++;
							free(leftData);
							free(leftNullBitIndicator);
							free(finalNullBitIndicator);
							free(finalJoinData);
						}


					}
					free(key);
				}
			}

			free(rightNullBitIndicator);
		}
		else
		{
			outputBufferCompleted = true;
			getNextTupleFinalOffset =writeOffsetForOutputBuffer;
			return 	1;

		}

	}
	free(rightData);
	getNextTupleFinalOffset = writeOffsetForOutputBuffer;
	return 1;
}

RC BNLJoin::getNextTuple(void *data)
{
	if(getNextTupleFinalOffset==0) //No data has been loaded to the output page. getNextTupleFinalOffset is freeBytesStartAt
		                           // equivalent
	{
		// Done by build Buffer.
		//1) load data into the buffer page
		//2) process them and add them to the hash map
		//Done by buildOutputBuffer
		//1) Do join and fill outputBuffer page
		buildBuffer();
		buildOutputBuffer();


		void* tuple = malloc(1000);
		int sizeoftuple;
		sizeoftuple = giveNextTuple(tuple);
		memcpy(data,tuple,sizeoftuple);
		free(tuple);
		return 1;
	}
	else if (getNextTupleFinalOffset<=getNextTupleReadOffset) //output page has been read completely
	{

		if(outputBufferCompleted != true)
		{
			getNextTupleFinalOffset = 0;
			getNextTupleReadOffset = 0;
			buildOutputBuffer();
			//give Next Tuple
			void* tuple = malloc(1000);
			int sizeoftuple;
			sizeoftuple = giveNextTuple(tuple);
			memcpy(data,tuple,sizeoftuple);
			free(tuple);
			return 1;

		}
		else //output buffer is complete
		{
			if(bufferCompleted !=true)
			{

				storedRightTableScan->setIterator();
				getNextTupleFinalOffset = 0; //freeBytesStartAt for buffer
				getNextTupleReadOffset = 0; //offset for output buffer
				buildBuffer();
				buildOutputBuffer();
				//give Next tuple
				void* tuple = malloc(1000);
				int sizeoftuple;
				sizeoftuple = giveNextTuple(tuple);
				memcpy(data,tuple,sizeoftuple);
				free(tuple);
				return 1;
			}
			else //buffer also complete
			{
				return QE_EOF;
			}

		}

	}
	else // give the next tuple in the output buffer page.
	{
		void* tuple = malloc(1000);
		int sizeoftuple;
		sizeoftuple = giveNextTuple(tuple);
		memcpy(data,tuple,sizeoftuple);
		free(tuple);
		return 1;


	}

	return QE_EOF;

}

RC BNLJoin::giveNextTuple(void* tuple)
{

	int size;
	memcpy(&size,(char *)outputBuffer+getNextTupleReadOffset, sizeof(int));
	getNextTupleReadOffset += sizeof(int);
	memcpy(tuple,(char *)outputBuffer+getNextTupleReadOffset, size);
	getNextTupleReadOffset += size;
	return size;

}

void  BNLJoin::getAttributes(vector<Attribute> &attrs) const
{


	vector<Attribute> leftRecordDescriptor;
	storedLeftIterator->getAttributes(leftRecordDescriptor);

	vector<Attribute> rightRecordDescriptor;
	storedRightTableScan->getAttributes(rightRecordDescriptor);

	for(unsigned int i = 0; i < leftRecordDescriptor.size(); i++)
	{
		attrs.push_back(leftRecordDescriptor[i]);
	}

	for(unsigned int i = 0; i < rightRecordDescriptor.size(); i++)
	{
		attrs.push_back(rightRecordDescriptor[i]);
	}

}

RC BNLJoin::getLengthOfTuple(void* tuple, vector<Attribute> &attributes)
{
	int readOffset = ceil(float(attributes.size())/CHAR_BIT);

	if(tuple == NULL)
	{
		for (vector<Attribute>::const_iterator  it = attributes.begin() ; it != attributes.end(); ++it)
		{
			if(it->type == TypeVarChar)
			{
				readOffset = readOffset + sizeof(int) + it->length;
			}
			else if (it->type == TypeInt)
			{
				readOffset  = readOffset + sizeof(int);
			}
			else if (it->type == TypeReal)
			{
				readOffset  = readOffset + sizeof(float);
			}
		}
		return readOffset;
	}
	else
	{
		unsigned char *nullFieldsIndicator = (unsigned char *) malloc(readOffset);
		memcpy(nullFieldsIndicator, tuple, readOffset);

		short i  = 0;
		bool nullBit = false;

		for (vector<Attribute>::const_iterator  it = attributes.begin() ; it != attributes.end(); ++it, i++)
		{
			nullBit = nullFieldsIndicator[i/CHAR_BIT] & (1 << (7 - (i%CHAR_BIT)));		//nullBitIndicatorSize in bytes, so multiply by 8 for bits

			if(!nullBit)
			{
				if(it->type == TypeVarChar)
				{
					int storedVarcharLen;
					memcpy(&storedVarcharLen, (char *)tuple + readOffset, sizeof(int));
					readOffset = readOffset + sizeof(int) + storedVarcharLen;
				}
				else if (it->type == TypeInt)
				{
					readOffset  = readOffset + sizeof(int);
				}
				else if (it->type == TypeReal)
				{
					readOffset  = readOffset + sizeof(float);
				}
			}
		}

		free(nullFieldsIndicator);
	}

	return readOffset;
}

void BNLJoin::fuseNullBitIndicatorsNWrite(unsigned char* nullFieldsIndicatorLeftIn, unsigned char* nullFieldsIndicatorRightIn, void* data, int &writeOffset)
{
	vector<Attribute> leftInAttributes;
	storedLeftIterator->getAttributes(leftInAttributes);
	vector<Attribute> rightInAttributes;
	storedRightTableScan->getAttributes(rightInAttributes);
	int finalNullFieldsIndicatorSize = 	ceil(float((leftInAttributes.size() + rightInAttributes.size()))/CHAR_BIT);
	unsigned char *finalNullFieldsIndicator = (unsigned char *) malloc(finalNullFieldsIndicatorSize);
	memset(finalNullFieldsIndicator, 0, finalNullFieldsIndicatorSize);

	int j = 0;
	bool nullBit = false;



	for(unsigned int i = 0; i < leftInAttributes.size(); i++)
	{
		nullBit = nullFieldsIndicatorLeftIn[i/CHAR_BIT] & (1 << (7 - (i%CHAR_BIT)));		//nullBitIndicatorSize in bytes, so multiply by 8 for bits

		if(nullBit)
		{
			finalNullFieldsIndicator[j/CHAR_BIT] = finalNullFieldsIndicator[j/CHAR_BIT] | 1 << (CHAR_BIT - j);		//set null bit indicator for that field
		}
		j++;
	}

	nullBit = false;

	for(unsigned int i = 0; i < rightInAttributes.size(); i++)
	{
		nullBit = nullFieldsIndicatorRightIn[i/CHAR_BIT] & (1 << (7 - (i%CHAR_BIT)));		//nullBitIndicatorSize in bytes, so multiply by 8 for bits

		if(nullBit)
		{
			finalNullFieldsIndicator[j/CHAR_BIT] = finalNullFieldsIndicator[j/CHAR_BIT] | 1 << (CHAR_BIT - j);		//set null bit indicator for that field
		}
		j++;
	}
	// cout<<"FINAL NULL FIELDS INDICATOR  : "<<bitset<8>(*finalNullFieldsIndicator)<<endl;

	memcpy((char*)data + writeOffset, finalNullFieldsIndicator, finalNullFieldsIndicatorSize);
	writeOffset = writeOffset + finalNullFieldsIndicatorSize;
}

// BNL ends HERE
Aggregate::Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        )
{
	storedInput = input;
	storedAggAttr = aggAttr;
	storedOp = op;
	doGroupBy = false;
	aggDone = false;
};

Aggregate::~Aggregate()
{
	storedInput = 0;
};

RC Aggregate::getNextTuple(void *data)
{
	if(doGroupBy)
	{
		return QE_EOF;
	}
	if(aggDone)
	{
		return QE_EOF;
	}

	vector<Attribute> receivedAttributes;
	storedInput->getAttributes(receivedAttributes);

	int nullBitIndicatorSize = ceil(float(receivedAttributes.size())/CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullBitIndicatorSize);

	void* storedTuple = malloc(PAGE_SIZE);
	bool valInitialised = false;
	int numOfTuples = 0;
	float aggValue;
	if(storedOp == SUM || storedOp == AVG || storedOp == COUNT)
	{
		valInitialised = true;
		aggValue = 0;
	}

	while(storedInput->getNextTuple(storedTuple) != RM_EOF)
	{
		memcpy(nullFieldsIndicator, storedTuple, nullBitIndicatorSize);
		numOfTuples = numOfTuples + 1;
		bool nullBit = false;
		int readOffset = nullBitIndicatorSize;
		short i = 0;
		for (vector<Attribute>::const_iterator  it = receivedAttributes.begin() ; it != receivedAttributes.end(); ++it, i++)
		{
			nullBit = nullFieldsIndicator[i/CHAR_BIT] & (1 << (7 - (i%CHAR_BIT)));		//nullBitIndicatorSizeLeftIn in bytes, so multiply by 8 for bits

			if(nullBit && storedAggAttr.name.compare(it->name) == 0)			//do not consider null values for agg attr
			{
				numOfTuples = numOfTuples - 1;
				break;
			}

			if(!nullBit)
			{
				if(it->type == TypeVarChar)
				{
					int storedVarcharLen;
					memcpy(&storedVarcharLen, (char *)storedTuple + readOffset, sizeof(int));
					readOffset = readOffset + sizeof(int) + storedVarcharLen;
				}
				else if (it->type == TypeReal)
				{
					if(storedAggAttr.name.compare(it->name) == 0)
					{
						float storedVal;
						memcpy(&storedVal, (char*)storedTuple + readOffset, sizeof(float));

						if(storedOp == MAX)
						{
							if(valInitialised)
							{
								if(aggValue < storedVal)
								{
									aggValue = storedVal;
								}
							}
							else
							{
								aggValue = storedVal;
								valInitialised = true;
							}
						}
						else if(storedOp == MIN)
						{
							if(valInitialised)
							{
								if(aggValue > storedVal)
								{
									aggValue = storedVal;
								}
							}
							else
							{
								aggValue = storedVal;
								valInitialised = true;
							}
						}
						else if (storedOp == COUNT)
						{
							if(!valInitialised)
							{
								valInitialised = true;
							}
							aggValue = aggValue + 1;
						}
						else if (storedOp == AVG || storedOp == SUM)
						{
							if(!valInitialised)
							{
								valInitialised = true;
							}
							aggValue = aggValue + storedVal;
						}
					}
					readOffset  = readOffset + sizeof(float);
				}
				else if (it->type == TypeInt)
				{
					if(storedAggAttr.name.compare(it->name) == 0)
					{
						int storedVal;
						memcpy(&storedVal, (char*)storedTuple + readOffset, sizeof(int));

						if(storedOp == MAX)
						{
							if(valInitialised)
							{
								if(aggValue < storedVal)
								{
									aggValue = (float)storedVal;
									// cout<<"CURRENT AGG VALUE : "<<aggValue<<"     ";
									// cout<<"CURRENT STORED VALUE : "<<storedVal<<endl;
								}
							}
							else
							{
								aggValue = (float)storedVal;
								valInitialised = true;
							}
						}
						else if(storedOp == MIN)
						{
							if(valInitialised)
							{
								if(aggValue > storedVal)
								{
									aggValue = (float)storedVal;
								}
							}
							else
							{
								aggValue = (float)storedVal;
								valInitialised = true;
							}
						}
						else if (storedOp == COUNT)
						{
							if(!valInitialised)
							{
								valInitialised = true;
							}
							aggValue = aggValue + 1;
						}
						else if (storedOp == AVG || storedOp == SUM)
						{
							if(!valInitialised)
							{
								valInitialised = true;
							}
							aggValue = aggValue + storedVal;
						}
					}
					readOffset  = readOffset + sizeof(int);
				}
			}
		}
	}
	if(storedOp == AVG)
	{
		aggValue = aggValue/numOfTuples;
	}
	aggDone = true;
	unsigned char* writeNullBitIndicator  = (unsigned char *) malloc(sizeof(char));			//since only one value is returned
	memset(writeNullBitIndicator, 0, sizeof(char));
	if(!valInitialised)
	{
		writeNullBitIndicator[0] = 128;
		memcpy(data, writeNullBitIndicator, sizeof(char));
		return 0;
	}
	memcpy(data, writeNullBitIndicator, sizeof(char));
	memcpy((char*)data + sizeof(char), &aggValue, sizeof(float));
	return 0;
};


// Please name the output attribute as aggregateOp(aggAttr)
// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
// output attrname = "MAX(rel.attr)"
void Aggregate::getAttributes(vector<Attribute> &attrs) const
{
	Attribute attr;
	attr.name = storedOp + "(" + storedAggAttr.name + ")";
	attr.type = storedAggAttr.type;
	attr.length = storedAggAttr.length;
	attrs.push_back(attr);
};


Aggregate::Aggregate(Iterator *input,             // Iterator of input R
          Attribute aggAttr,           // The attribute over which we are computing an aggregate
          Attribute groupAttr,         // The attribute over which we are grouping the tuples
          AggregateOp op              // Aggregate operation
)
{
	storedInput = input;
	storedAggAttr = aggAttr;
	storedOp = op;
	storedGroupAttr = groupAttr;
	aggDone = false;
	doGroupBy = true;

}
