#include "BTreeNode.h"

using namespace std;

/*
______________________________________
|	  |	    |	  |			  |	     |
|#keys|entry|entry|... ... ...|pageID|
|_____|_____|_____|___________|______|
1024 B in a page
first 4B for int for #keys
last 4B for PageId of sibling
-->
(1024 - 4 - 4) / entry_size = number of entries in a page
entry = RecordID & int = 12B
number of entries = floor 1016/12 = 84
one for overflow insert --> 83
*/

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{
	currPid = pid;
	return pf.read(pid, buffer);
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{ 
	return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{ 
	int key_count = 0;
	memcpy(&key_count, buffer, sizeof(int));

	return key_count;
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{ 
	int key_count = getKeyCount();

	if (key_count >= (max_key_count + 1)) // + 1 for one overflow insert
		return RC_NODE_FULL;

	leaf_entry* key_start = (leaf_entry *) (buffer + sizeof(int));
	// keys start after the #keys int in the beginning of buffer

	int i;
	for (i = 0; i < key_count; i++)
	{
		if (key_start->ent_key > key)
			break;
		key_start++;
	}

	// need to move remaining entries one spot to the right to make space for insert
	if (i < key_count)
	{
		int rest = key_count - i;

		memmove(key_start+1, key_start, entry_size * rest);
	}

	leaf_entry new_entry;
	new_entry.rec_id = rid;
	new_entry.ent_key = key;

	*key_start = new_entry;

	key_count++; // after insert succeed, should increment;
	memcpy(buffer,&key_count, sizeof(int)); // update

	return 0; 
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{ 
	if (insert(key, rid) < 0)
		return RC_NODE_FULL;

	int key_count = getKeyCount();

	int front_half = (key_count) / 2;
	int back_half = key_count / 2;

	leaf_entry* key_start = (leaf_entry *) (buffer + sizeof(int));
	leaf_entry* split_point = key_start + front_half;

	leaf_entry* sib_key_start = (leaf_entry *) (sibling.buffer + sizeof(int));

	// copy backhalf into siblings buffer
	memcpy(sib_key_start, split_point, sizeof(leaf_entry) * back_half);
	memcpy(&siblingKey, sib_key_start, sizeof(int)); // copy the first key
	
	/* UPDATE KEY COUNTS */
	memcpy(sibling.buffer, &back_half, sizeof(int));
	memcpy(buffer, &front_half, sizeof(int));

	PageId sibNextptr = getNextNodePtr();

	sibling.setNextNodePtr(sibNextptr);

	memset(split_point, '\0', sizeof(leaf_entry) * back_half); // clear out the back half of the buffer

	return 0; 
}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{ 
	int key_count = getKeyCount();


	leaf_entry* key_start = (leaf_entry *) (buffer + sizeof(int));

	int i;
	for (i = 0; i < key_count; i++)
	{
		if (key_start->ent_key == searchKey)
		{
			eid = i;
			return 0;
		}

		if (key_start->ent_key > searchKey)
		{
			eid = i;
			return RC_NO_SUCH_RECORD;
		}

		key_start++;
	}

	// if out of loop, iterated all entries & no match found
	eid = i;

	return RC_NO_SUCH_RECORD;
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{ 
	int key_count = getKeyCount();

	leaf_entry* key_start = (leaf_entry*) (buffer + sizeof(int));

	if (eid >= key_count)
	{
		return RC_NO_SUCH_RECORD;
	}

	leaf_entry* read_entry = key_start + eid;

	key = read_entry->ent_key;
	rid = read_entry->rec_id;

	return 0;
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{ 
	// stored at end of buffer 
	// location buffer + PAGE_SIZE - sizeof(PageId)
	int next_Id = PageFile::PAGE_SIZE - sizeof(PageId);
	PageId siblingPg;
	memcpy(&siblingPg, buffer + next_Id, sizeof(PageId));

	return siblingPg; 
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{ 
	int next_Id = PageFile::PAGE_SIZE - sizeof(PageId);
	memcpy(buffer + next_Id, &pid, sizeof(PageId));

	// any possible errors?
	return 0; 
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{ 
	return pf.read(pid, buffer); 
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{ 
	return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{ 
	int key_count = 0;
	memcpy(&key_count, &buffer, sizeof(int));

	return key_count; }


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{ 
	int key_count = getKeyCount();

	if (key_count >= max_key_count + 1)
		return RC_NODE_FULL;

	entry_node* key_start = (entry_node *) (buffer + sizeof(int) + sizeof(PageId));
	// keys start after the #keys int in the beginning of buffer

	int i;
	for (i = 0; i < key_count; i++)
	{
		if (key_start->ent_key > key)
			break;
		key_start++;
	}

	// need to move remaining entries one spot to the right to make space for insert
	if (i < key_count)
	{
		int rest = key_count - i;
		memmove(key_start+1, key_start, entry_size * rest);
	}

	entry_node new_entry;
	new_entry.pag_id = pid;
	new_entry.ent_key = key;

	*key_start = new_entry;

	key_count++; // after insert succeed, should increment;
	memcpy(buffer,&key_count, sizeof(int)); // update

	return 0; 
 }

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{  
	if (insert(key, pid) < 0)
		return RC_NODE_FULL;

	int key_count = getKeyCount();

	int front_half = (key_count) / 2;
	int back_half = (key_count)/ 2 - 1;

	entry_node* key_start = (entry_node*) (buffer + sizeof(int) + sizeof(PageId));
	entry_node* split_point = key_start + front_half;

	entry_node* sib_key_start = (entry_node*) (sibling.buffer + sizeof(int) + sizeof(PageId));
	midKey = split_point->ent_key;

	PageId* sib_ptr_start = (PageId*) (sibling.buffer + sizeof(int));
	PageId sibFirstPtr = split_point->pag_id;

	*sib_ptr_start = sibFirstPtr;
	
	// copy backhalf into siblings buffer
	memcpy(sib_key_start, ++split_point, sizeof(entry_node) * back_half );
	memcpy(sibling.buffer, &back_half, sizeof(int)); // update key_count in sibling buffer
	memcpy(buffer, &front_half, sizeof(int));

	memset(split_point, '\0', sizeof(entry_node) * (back_half + 1)); // clear out the back half of the buffer

	return 0;  }

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ 
	int key_count = getKeyCount();

	entry_node * key_start = (entry_node*) (buffer + sizeof(int) + sizeof(PageId));
	PageId* firstptr = (PageId *) (buffer + sizeof(int));

	if (key_start->ent_key > searchKey) {
		pid = *firstptr;
		return 0;
	}

	int i;
	for (i = 0; i < key_count; i++){
		if (key_start->ent_key == searchKey)
		{
			pid = key_start->pag_id;
			return 0;
		}

		if (key_start->ent_key < searchKey)
		{
			if (i == key_count-1)
			{
				pid = key_start->pag_id;
				return 0;
			}
		}
		if ((key_start+1)->ent_key > searchKey)
		{
			pid = key_start->pag_id;
			return 0;
		}


   		key_start++;
   	}
   	
   	return RC_NO_SUCH_RECORD;
 }

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{ 
	int key_count = 0;

	entry_node* key_start = (entry_node *) (buffer + sizeof(int)+ sizeof(PageId));
	// keys start after the #keys int in the beginning of buffer
	PageId * first = (PageId*) (buffer + sizeof(int));
	entry_node new_entry;
	new_entry.pag_id = pid2;
	new_entry.ent_key = key;

	*key_start = new_entry;

	key_count++; // after insert succeed, should increment;
	memcpy(buffer,&key_count, sizeof(int)); // update

	*first = pid1;
	

	

	return 0; }
