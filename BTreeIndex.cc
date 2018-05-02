
#include "BTreeIndex.h"
#include "BTreeNode.h"

using namespace std;


/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid = -1;
}

// Our helper functions to find stored variable information
RC BTreeIndex::readInfo() {
	if (pf.read(0, buffer) < 0)
		return RC_FILE_READ_FAILED;

	PageId* getroot = (PageId*) buffer;
	rootPid = *getroot;

	int* getheight = (int*) (buffer + sizeof(PageId));
	treeHeight = *getheight;

	return 0;
}

RC BTreeIndex::writeInfo() {
	PageId* getroot = (PageId*) buffer;
	*getroot = rootPid;

	int* getheight = (int*) (buffer + sizeof(PageId));
	*getheight = treeHeight;

	return pf.write(0, buffer);
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
	return pf.open(indexname, mode);
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
    return pf.close();
}

RC BTreeIndex::recInsert(int key, const RecordId& rid, PageId pid, int& midKey,
                   int& currheight, PageId& leftChild, PageId& rightChild)
{
    // Check if the node is a leafNode
    if (treeHeight == currheight) {
        BTLeafNode newleaf;
        if(newleaf.read(pid, pf) < 0)
        	return RC_FILE_READ_FAILED;


        // Check if the insert will cause overflow
        if (newleaf.getKeyCount() < BTLeafNode::max_key_count) 
        {
            if (newleaf.insert(key, rid) < 0)
                fprintf(stdout, "leaf insert error %d\n", pid);
            newleaf.write(pid, pf);
            midKey = 0;
        } 
        else 
        {
            BTLeafNode sibling;
            PageId prevNext = newleaf.getNextNodePtr();
            if (newleaf.insertAndSplit(key, rid, sibling, midKey) < 0)
                fprintf(stdout, "leaf split error %d\n", pid);

            PageId sibPid = pf.endPid();
            sibling.setNextNodePtr(prevNext);
            sibling.write(sibPid, pf);

            newleaf.setNextNodePtr(sibPid);
            newleaf.write(pid, pf);

            leftChild = pid;
            rightChild = sibPid;
        }
        return 0;
    } 

    else {
        // Find the proper pointer to child

        BTNonLeafNode newNonleaf;
        newNonleaf.read(pid, pf);

        PageId newpid = pid;

        newNonleaf.locateChildPtr(key, newpid);

        currheight++;

        recInsert(key, rid, newpid, midKey, currheight, leftChild, rightChild);
        // Check if insert causes overflow in a child node
        if (midKey == 0) {
            return 0;
        }
        if (newNonleaf.getKeyCount() < BTLeafNode::max_key_count) 
        {
            if (newNonleaf.insert(midKey, rightChild) < 0)
                fprintf(stdout, "Nonleaf insert error %d\n", pid);

            newNonleaf.write(pid, pf);
            midKey = 0;
        } 
        else 
        {
            BTNonLeafNode sib;
            PageId sibPid = pf.endPid();
            
            if (newNonleaf.insertAndSplit(key, rightChild, sib, midKey) < 0)
                fprintf(stdout, "nonleaf split error %d\n", midKey);

            sib.write(sibPid, pf);
            newNonleaf.write(pid, pf);

            leftChild = pid;
            rightChild = sibPid;
        }
    }
    currheight++;
    return 0;
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
	if (rootPid < 1) {
        rootPid = 1;

        BTLeafNode newRoot;
        
        if (newRoot.insert(key, rid) < 0)
            fprintf(stderr, "root error %d\n", rootPid);
        
        newRoot.write(1, pf);
        treeHeight = 0;
    } 

    else {
        int currheight = 0;
        PageId leftChild = -1;
        PageId rightChild = -1;
        int midKey = 0;
        recInsert(key, rid, rootPid, midKey, currheight, leftChild, rightChild);



        // Check if theres a split at root node
        if (midKey != 0) {
            BTNonLeafNode newNonRoot;
            rootPid = pf.endPid();
            
            if (newNonRoot.initializeRoot(leftChild, midKey, rightChild) < 0)
                fprintf(stderr, "initialize Root error %d\n", midKey);
            newNonRoot.write(rootPid, pf);
            treeHeight++;
        }

        //treeHeight++;
    }

    writeInfo();
    //treeHeight++;
    return 0;

}

/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
	// pid 0 is saved for variable storage. rootPid cannot be 0
	if (rootPid < 1)
		return RC_NO_SUCH_RECORD;

	cursor.pid = rootPid;

	// at root
	int currHeight = 0;

	while (currHeight < treeHeight)
	{
		BTNonLeafNode nonLeaf;
		if (nonLeaf.read(cursor.pid, pf) < 0)
        {
			return RC_FILE_READ_FAILED;
        }

		RC childPtr = nonLeaf.locateChildPtr(searchKey, cursor.pid);
		if (childPtr < 0)
        {
			return childPtr;
        }

		currHeight++;
	}

	//BTLeafNode leafNode;

	if (cacheLeaf.read(cursor.pid, pf) < 0)
    {
		return RC_FILE_READ_FAILED;
    }


	cacheLeaf.locate(searchKey, cursor.eid);


    return 0;
}


RC BTreeIndex::readLeafEntry(int eid, int& key, RecordId& rid, IndexCursor& cursor)
{
    BTLeafNode leafNode;
    if (leafNode.read(cursor.pid, pf) < 0)
      return RC_FILE_READ_FAILED;


    leafNode.readEntry(cursor.eid, key, rid);

    return 0;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{

	if (cacheLeaf.readEntry(cursor.eid, key, rid) < 0)
		return RC_NO_SUCH_RECORD;

	cursor.eid++;

    return 0;
}
