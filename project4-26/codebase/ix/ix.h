#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan


//static void* MyMalloc(size_t size)
//{
//    // cout<<"MALLOCED : "<<size<<endl;
//    return malloc(size);
//}
//
//static void MyFree(void* ptr)
//{
//    // cout<<"FREE"<<endl;
//    free(ptr);
//}

struct SplitData
{
	int pageId;
	void* key;
	int result;
};

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;
        int getPageIdForInsertionOrScan(void* node, int &readOffset, short &noOfKeys, const Attribute &attribute, const void* key);
        void getCommonMetaData(void* nodeData, char &typeOfPage, short &noOfKeys, short &freeBytesLeft, int &readOffset) const;

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        void fillCommonMetaData(void* newNode, int &writeOffset, char &typeOfPage);

		void printLeafNode(void* nodeData, short &noOfKeys, short &freeBytesLeft, int &readOffset, const Attribute &attribute, int &printLevel) const;
		void printNonLeafNode(void* nodeData, short &noOfKeys, short &freeBytesLeft, int &readOffset, const Attribute &attribute, int &printLevel) const;
        void printBTreeInternal(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageId, int &printLevel)const;
        // void printLeafNode(void* nodeData, short &noOfKeys, short &freeBytesLeft, int &readOffset, const Attribute &attribute) const;
        // void printNonLeafNode(void* nodeData, short &noOfKeys, short &freeBytesLeft, int &readOffset, const Attribute &attribute) const;
        // void printBTreeInternal(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageId) const;

        RC createTree(IXFileHandle &ixfileHandle, const Attribute &attribute, const void* key, const RID &rid);
        void insertEntryRecursive(IXFileHandle &fileHandle, int pageNo, const Attribute &attribute, const void* key, const RID &rid, SplitData &splitData);

        void insertNewKeyIntAtOffset(void* node, short &noOfKeys, int &intKey, int &writeOffset, const RID &rid);
        void insertNewKeyVarcharAtOffset(void* node, short &noOfKeys, void *varcharKey, short varcharLength, int &writeOffset, const RID &rid);
        void insertNewKeyFloatAtOffset(void* node, short &noOfKeys, float &floatKey, int &writeOffset, const RID &rid);

        void checkIfNewKeyInt(void* node, char typeOfPage, int intKey, int &readOffset, short freeBytesStartAt, short &noOfKeys, bool &isNewKey, short &splitKey, short &keyCounter);
        void checkIfNewKeyFloat(void* node, char typeOfPage, float floatKey, int &readOffset, short freeBytesStartAt, short &noOfKeys, bool &isNewKey, short &splitKey, short &keyCounter);
        void checkIfNewKeyVarchar(void* node, char typeOfPage, void *varcharKeyPtr, int varcharLength, int &readOffset, short freeBytesStartAt, short &noOfKeys, bool &isNewKey, short &splitKey, short &keyCounter);
        
        void insertKeyRidAtOffsetInt(int intKey, const RID &rid, void* node, int &offset, bool isNewKey);
        void insertKeyRidAtOffsetFloat(float floatKey, const RID &rid, void* node, int &offset, bool isNewKey);
        void insertKeyRidAtOffsetVarchar(void* varcharKey, short varcharLength, const RID &rid, void* node, int &offset, bool isNewKey);

        RC deleteRecursive(IXFileHandle ixfileHandle, const Attribute &attribute, const void *key,const RID &rid, int pageNo, int siblingPageNo, void* rootPage, int rootPageNo, short startingPoint, short endingPoint);
        RC getMoveOffset(short &startingPoint,short &endingPoint,const void *key, void* pageData,const Attribute &attribute, short &noOfRid);
        RC checkIfValidFileHandle(IXFileHandle &ixfileHandle);
        RC redistribute(IXFileHandle ixfileHandle,const Attribute &attribute, int pageNo, int siblingPageNo, void *siblingData, void *nodePage, void* rootPage, int rootPageNo, short startingPoint);
        RC   merge(IXFileHandle ixfileHandle,const Attribute  &attribute, int pageNo, int siblingPageNo, void* siblingData ,void* nodePage, void* rootPage, int rootPageNo , short startingPoint, short endingPoint);

};


class IXFileHandle {
    public:

	FileHandle fileHandle;
	string fileName;
    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};

class IX_ScanIterator {
    public:

		IXFileHandle *ixfileHandle;
		Attribute storedAttribute;
		const void *lowKey;
		const void *highKey;
		bool lowKeyInclusive;
		bool highKeyInclusive;		// Constructor
		void* pageInMemory;
		int readOffset;
		short noOfKeys;
		int nextPageId;
		int currentKeyPointer;
		int currentRId;
		int currentNoOfRids;
		void* currentKey;

        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();

        void scanRecursive(int pageNo, const void* key, const Attribute &attribute);
};


#endif
