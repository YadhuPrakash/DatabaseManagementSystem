#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <map>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ MIN=0, MAX, COUNT, SUM, AVG } AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};


struct Condition {
    string  lhsAttr;        // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};


class Iterator {
    // All the relational operators and access methods are iterators.
    public:
		void compareStrings(const string &givenVarcharStr, const string &storedVarcharStr, CompOp compOp, bool &satisfied);
		void compareInts(const int &givenInt, const int &storedInt, CompOp compOp,bool &satisfied);
		void compareFloats(const float &givenFloat, const float &storedFloat, CompOp compOp, bool &satisfied);

		virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual ~Iterator() {};
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RelationManager &rm;
        RM_ScanIterator *iter;
        string tableName;
        vector<Attribute> attrs;
        vector<string> attrNames;
        RID rid;

        TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL):rm(rm)
        {
        	//Set members
        	this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs.at(i).name);
            }

            // Call RM scan to get an iterator
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator()   //use
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
        };

        RC getNextTuple(void *data)
        {
            return iter->getNextTuple(rid, data);
            // int result =  iter->getNextTuple(rid, data);
            //Change back to return iter->...
            // cout<<"RID : "<<rid.pageNum<<":"<<rid.slotNum<<endl;
            // return result;
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~TableScan()
        {
        	iter->close();
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RelationManager &rm;
        RM_IndexScanIterator *iter;
        string tableName;
        string attrName;
        vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;

        IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL):rm(rm)
        {
        	// Set members
        	this->tableName = tableName;
        	this->attrName = attrName;


            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void* lowKey,
                         void* highKey,
                         bool lowKeyInclusive,
                         bool highKeyInclusive)
        {
            iter->close();
            delete iter;
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                           highKeyInclusive, *iter);
        };

        RC getNextTuple(void *data)
        {
            int rc = iter->getNextEntry(rid, key);
             // if(rc != -1)
             // {
             //     cout<<"INDEX SCAN  : RID : "<<rid.pageNum<<":"<<rid.slotNum<<endl;
             // }
            if(rc == 0)
            {
                rc = rm.readTuple(tableName.c_str(), rid, data);
            }
            return rc;
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~IndexScan()
        {
            iter->close();
        };
};


class Filter : public Iterator {
    // Filter operator
    public:
    vector<Attribute> storedAttrs;
    bool attributesInitialised;
	Iterator *storedIterator;
	Condition storedCondition;

        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );
        ~Filter(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
};


class Project : public Iterator {
    // Projection operator
    public:
	vector<string> storedAttrNames;
	bool attributesInitialised;
	bool returnAttributesInitialised;
	Iterator *storedIterator;
	vector<Attribute> storedAttrs;
	vector<Attribute> returnAttrs;

        Project(Iterator *input,                    // Iterator of input R
              const vector<string> &attrNames);   // vector containing attribute names
        ~Project()
        {
        	storedIterator = 0;
        };

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
};

class BNLJoin : public Iterator {
    // Block nested-loop join operator
    public:
        Iterator *storedLeftIterator;
        TableScan *storedRightTableScan;
        Condition storedCondition;
        RelationManager* relationManager = 0;
        unsigned numPages=0;
        int getNextTupleReadOffset = 0;
        int getNextTupleFinalOffset = 0;
        int writeOffsetForBuffer = 0;
        int writeOffsetForOutputBuffer = 0;
        void *buffer;
        void *outputBuffer;
        bool outputBufferCompleted;
        bool bufferCompleted;
        std::multimap<int, int> mapForLeftInt;
        std::multimap<float, int> mapForLeftFloat;
        std::multimap<std::string, int> mapForLeftString;


    BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numPages       // # of pages that can be loaded into memory,
			                                 //   i.e., memory block size (decided by the optimizer)
        );

        ~BNLJoin(){};

        RC getNextTuple(void *data);//{return QE_EOF;};
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
        RC buildBuffer();
        RC buildOutputBuffer();
        RC getLengthOfTuple(void* tuple, vector<Attribute> &attributes);
        void fuseNullBitIndicatorsNWrite(unsigned char* nullFieldsIndicatorLeftIn, unsigned char* nullFieldsIndicatorRightIn, void* data, int &writeOffset);
        RC giveNextTuple(void* tuple);


};


class INLJoin : public Iterator {
    // Index nested-loop join operator
	Iterator *storedLeftIterator;
	IndexScan *storedIndexScanIterator;
	Condition storedCondition;
	vector<Attribute> leftInAttributes;
	vector<Attribute> rightInAttributes;
	bool attributesInitialised;
    bool finishedScanOfRightIn;
    void* storedLeftTuple;
    void* searchKey;
    int leftInSize;
    int rightInSize;
    unsigned char *nullFieldsIndicatorLeftIn;


    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        );
        ~INLJoin()
        {
        	storedLeftIterator = 0;
        	storedIndexScanIterator = 0;
            free(storedLeftTuple);
            free(searchKey);
        };

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;

    private:
        int getLengthOfTuple(void* data, vector<Attribute> &attributes);
        void fuseNullBitIndicatorsNWrite(unsigned char* nullFieldsIndicatorLeftIn, unsigned char* nullFieldsIndicatorRightIn, void* data, int &writeOffset);
};

// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
    // Grace hash join operator
    public:
      GHJoin(Iterator *leftIn,               // Iterator of input R
            Iterator *rightIn,               // Iterator of input S
            const Condition &condition,      // Join condition (CompOp is always EQ)
            const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
      ){};
      ~GHJoin(){};

      RC getNextTuple(void *data){return QE_EOF;};
      // For attribute in vector<Attribute>, name it as rel.attr
      void getAttributes(vector<Attribute> &attrs) const{};
};

// class HashMapHandler
// {
// 	public:
//  		map<int, float> intAggValMap;
//  		map<float, float> floatAggValMap;
//  		map<string, float> varcharAggValMap;

//  		float getAggValue(void * key, Attribute groupAttr);

//  		HashMapHandler(){};
//  		~HashMapHandler(){};
// };

class Aggregate : public Iterator {
    // Aggregation operator
	public:
	Iterator *storedInput;
	Attribute storedAggAttr;
    AggregateOp storedOp;
    bool aggDone;
    Attribute storedGroupAttr;
    bool doGroupBy;
    // HashMapHandler hashMapHandler;

        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  Attribute aggAttr,           // The attribute over which we are computing an aggregate
                  Attribute groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        );
        ~Aggregate();

        RC getNextTuple(void *data);
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(vector<Attribute> &attrs) const;
};



#endif
