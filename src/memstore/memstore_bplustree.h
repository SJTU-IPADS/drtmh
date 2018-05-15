#ifndef MEMSTOREBPLUSTREE_H
#define MEMSTOREBPLUSTREE_H

#include <stdlib.h>
#include <iostream>
#include <assert.h>
//#include "util/rtmScope.h"
#include "util/rtm.h"
//#include "util/rtm_arena.h"
//#include "util/mutexlock.h"
#include "port/port_posix.h"
//#include "memstore.h"

#include "memstore.h"

#define MEM_BTREE_M  15
#define NENT  15

#define BTREE_PROF 0
#define BTREE_LOCK 0
#define BTPREFETCH 0
#define DUMMY 1

#define ENABLE_RTM 1


//using namespace leveldb;

class MemstoreBPlusTree  : public Memstore {

  //Test purpose
 public:
  struct LeafNode {
  LeafNode() : num_keys(0){}//, writes(0), reads(0) {}
    //		uint64_t padding[4];
    unsigned num_keys;
    uint64_t keys[MEM_BTREE_M];
    MemNode *values[MEM_BTREE_M];
    LeafNode *left;
    LeafNode *right;
    uint64_t seq;
    //		uint64_t writes;
    //		uint64_t reads;
    //		uint64_t padding1[4];
  };

  struct InnerNode {
  InnerNode() : num_keys(0) {}//, writes(0), reads(0) {}
    //		uint64_t padding[8];
    //		unsigned padding;
    unsigned num_keys;
    uint64_t 	 keys[NENT];
    void*	 children[NENT+1];
    //		uint64_t writes;
    //		uint64_t reads;
    //		uint64_t padding1[8];
  };

  //The result object of the delete function
  struct DeleteResult {
  DeleteResult(): value(0), freeNode(false), upKey(-1){}
    MemNode* value;  //The value of the record deleted
    bool freeNode;	//if the children node need to be free
    uint64_t upKey; //the key need to be updated -1: default value
  };

  class Iterator : public Memstore::Iterator {
  public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    Iterator(){};
    Iterator(MemstoreBPlusTree* tree);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid();

    // Returns the key at the current position.
    // REQUIRES: Valid()
    MemNode* CurNode();


    uint64_t Key();

    // Advances to the next position.
    // REQUIRES: Valid()
    bool Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    bool Prev();

    // Advance to the first entry with a key >= target
    void Seek(uint64_t key);

    void SeekPrev(uint64_t key);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

    uint64_t* GetLink();

    uint64_t GetLinkTarget();

  private:
    MemstoreBPlusTree* tree_;
    LeafNode* node_;
    uint64_t seq_;
    int leaf_index;
    uint64_t *link_;
    uint64_t target_;
    uint64_t key_;
    MemNode* value_;
    uint64_t snapshot_;
    // Intentionally copyable
  };

 public:
  MemstoreBPlusTree() {
    root = new LeafNode();
    reinterpret_cast<LeafNode*>(root)->left = NULL;
    reinterpret_cast<LeafNode*>(root)->right = NULL;
    reinterpret_cast<LeafNode*>(root)->seq = 0;
    depth = 0;
#if BTREE_PROF
    writes = 0;
    reads = 0;
    calls = 0;
#endif
  }



  ~MemstoreBPlusTree() {

#if BTREE_PROF
    printf("calls %ld avg %f writes %f\n", calls, (float)(reads + writes)/(float)calls,(float)(writes)/(float)calls );
#endif
    prof.reportAbortStatus();
  }

  inline void ThreadLocalInit(){
    if(false == localinit_) {
      //      arena_ = new RTMArena();

      dummyval_ = GetMemNode();
      dummyval_->value = NULL;

      dummyleaf_ = new LeafNode();
      localinit_ = true;
    }

  }

  inline LeafNode* new_leaf_node() {

#if DUMMY
    LeafNode* result = dummyleaf_;
    dummyleaf_ = NULL;
#else
    LeafNode* result = new LeafNode();
#endif
    //LeafNode* result = (LeafNode *)(arena_->AllocateAligned(sizeof(LeafNode)));
    return result;
  }

  inline InnerNode* new_inner_node() {
    InnerNode* result = new InnerNode();
    //InnerNode* result = (InnerNode *)(arena_->AllocateAligned(sizeof(InnerNode)));
    return result;
  }

  inline LeafNode* FindLeaf(uint64_t key) {
    InnerNode* inner;
    register void* node= root;
    register unsigned d= depth;
    unsigned index = 0;
    while( d-- != 0 ) {
      index = 0;
      inner= reinterpret_cast<InnerNode*>(node);
      while((index < inner->num_keys) && (key >= inner->keys[index])) {
        ++index;
      }
      node= inner->children[index];
    }
    return reinterpret_cast<LeafNode*>(node);
  }

  inline bool CompareKey(uint64_t k0,uint64_t k1) { return k0 == k1;}
  inline MemNode* Get(uint64_t key)
  {
    register int nested = _xtest();
    if(0 == nested)
      RTMTX::Begin(&rtmlock);
    else if(rtmlock.IsLocked())
      _xabort(0xff);
    //    RTMScope begtx(&prof, depth * 2, 1, &rtmlock);

    InnerNode* inner;
    register void* node= root;
    register unsigned d= depth;
    unsigned index = 0;
    while( d-- != 0 ) {
      index = 0;
      inner= reinterpret_cast<InnerNode*>(node);
      //			reads++;
      while((index < inner->num_keys) && (key >= inner->keys[index])) {
	++index;
      }
      node= inner->children[index];
    }

    LeafNode* leaf= reinterpret_cast<LeafNode*>(node);
    //		reads++;
    if (leaf->num_keys == 0) return NULL;
    unsigned k = 0;
    while((k < leaf->num_keys) && (leaf->keys[k]<key)) {
      ++k;
    }
    register MemNode* res = NULL;
    if (k == leaf->num_keys) {
      goto GET_END;
    }

    if( leaf->keys[k] == key ) {
      res =  leaf->values[k];
    }
  GET_END:
    if(0 == nested)
      RTMTX::End(&rtmlock);

    return res;
  }


  inline MemNode* Put(uint64_t k, uint64_t* val)
  {
    ThreadLocalInit();
    MemNode *node = _GetWithInsert(k,NULL);
    node->value = val;
    node->seq = 0;

#if BTREE_PROF
    reads = 0;
    writes = 0;
    calls = 0;
#endif
    return node;
  }

  inline int slotAtLeaf(uint64_t key, LeafNode* cur) {

    int slot = 0;

    while((slot < cur->num_keys) && (cur->keys[slot] < key)) {
      slot++;
    }

    return slot;
  }

  inline MemNode* removeLeafEntry(LeafNode* cur, int slot) {

    assert(slot < cur->num_keys);
    cur->seq = cur->seq + 1;

    MemNode* value = cur->values[slot];

    cur->num_keys--;

    //The key deleted is the last one
    if (slot == cur->num_keys)
      return value;

    //Re-arrange the entries in the leaf
    for(int i = slot + 1; i <= cur->num_keys; i++) {
      cur->keys[i - 1] = cur->keys[i];
      cur->values[i - 1] = cur->values[i];
    }

    return value;

  }


  inline DeleteResult* LeafDelete(uint64_t key, LeafNode* cur) {

    //step 1. find the slot of the key
    int slot = slotAtLeaf(key, cur);

    //the record of the key doesn't exist, just return
    if(slot == cur->num_keys) {
      return NULL;
    }


    //		 assert(cur->values[slot]->value == (uint64_t *)2);

    //	printf("delete node\n");
    DeleteResult *res = new DeleteResult();

    //step 2. remove the entry of the key, and get the deleted value
    res->value = removeLeafEntry(cur, slot);

    //step 3. if node is empty, remove the node from the list
    if(cur->num_keys == 0) {
      if(cur->left != NULL)
	cur->left->right = cur->right;
      if(cur->right != NULL)
	cur->right->left = cur->left;

      //Parent is responsible for the node deletion
      res->freeNode = true;

      return res;
    }

    //The smallest key in the leaf node has been changed, update the parent key
    if(slot == 0) {
      res->upKey = cur->keys[0];
    }

    return res;
  }

  inline int slotAtInner(uint64_t key, InnerNode* cur) {

    int slot = 0;

    while((slot < cur->num_keys) && (cur->keys[slot] <= key)) {
      slot++;
    }

    return slot;
  }

  inline void removeInnerEntry(InnerNode* cur, int slot, DeleteResult* res) {

    assert(slot <= cur->num_keys);

    //If there is only one available entry
    if(cur->num_keys == 0) {
      assert(slot == 0);
      res->freeNode = true;
      return;
    }


    //The key deleted is the last one
    if (slot == cur->num_keys) {
      cur->num_keys--;
      return;
    }

    //replace the children slot
    for(int i = slot + 1; i <= cur->num_keys; i++)
      cur->children[i - 1] = cur->children[i];

    //delete the first entry, upkey is needed
    if (slot == 0) {

      //record the first key as the upkey
      res->upKey = cur->keys[slot];

      //delete the first key
      for(int i = slot; i < cur->num_keys - 1; i++) {
	cur->keys[i] = cur->keys[i + 1];
      }

    } else {
      //delete the previous key
      for(int i = slot; i < cur->num_keys; i++) {
	cur->keys[i - 1] = cur->keys[i];
      }
    }

    cur->num_keys--;

  }

  inline DeleteResult* InnerDelete(uint64_t key, InnerNode* cur ,int depth)
  {

    DeleteResult* res = NULL;

    //step 1. find the slot of the key
    int slot = slotAtInner(key, cur);

    //step 2. remove the record recursively
    //This is the last level of the inner nodes
    if(depth == 1) {
      res = LeafDelete(key, (LeafNode *)cur->children[slot]);
    } else {
      //printf("Delete Inner Node  %d\n", depth);
      //printInner((InnerNode *)cur->children[slot], depth - 1);
      res = InnerDelete(key, (InnerNode *)cur->children[slot], (depth - 1));
    }

    //The record is not found
    if(res == NULL) {
      return res;
    }

    //step 3. Remove the entry if the total children node has been removed
    if(res->freeNode) {
      //FIXME: Should free the children node here

      //remove the node from the parent node
      res->freeNode = false;
      removeInnerEntry(cur, slot, res);
      return res;
    }

    //step 4. update the key if needed
    if(res->upKey != -1) {
      if (slot != 0) {
	cur->keys[slot - 1] = res->upKey;
	res->upKey = -1;
      }
    }

    return res;

  }



  inline MemNode* Delete_rtm(uint64_t key) {
    // NOT implemented!
    fprintf(stdout,"delete rtm btree no implemented!\n");
    exit(-1);

    DeleteResult* res = NULL;
    if (depth == 0) {
      //Just delete the record from the root
      res = LeafDelete(key, (LeafNode*)root);
    }
    else {
      res = InnerDelete(key, (InnerNode*)root, depth);
    }

    if (res == NULL)
      return NULL;

    if(res->freeNode)
      root = NULL;

    return res->value;
  }



  inline MemNode* GetWithDelete(uint64_t key) {

    ThreadLocalInit();

    MemNode* value = Delete_rtm(key);

#if DUMMY
    if(dummyval_ == NULL) {
      dummyval_ = GetMemNode();
    }

    if(dummyleaf_ == NULL) {
      dummyleaf_ = new LeafNode();
    }
#endif
    return value;
  }

  inline MemNode* _GetWithInsert(uint64_t key,char *val) {

    ThreadLocalInit();

    dummyval_->value = (uint64_t *)val;
    MemNode* value = Insert_rtm(key);

#if DUMMY
    if(dummyval_ == NULL) {
      dummyval_ = GetMemNode();
    } else {
      dummyval_->value = NULL;
    }

    if(dummyleaf_ == NULL) {
      dummyleaf_ = new LeafNode();
    }
#endif
    //    value->seq = 0;
    return value;

  }

  inline MemNode* Insert_rtm(uint64_t key) {
#if ENABLE_RTM
    register char nested = _xtest();

    if(0 == nested) {
#if 0
      // using profile
      RTMTX::Begin(&rtmlock,&prof);
#else
      RTMTX::Begin(&rtmlock);
#endif
    }
    else if(rtmlock.IsLocked())
      _xabort(0xff);
#endif
#if BTREE_PROF
    calls++;
#endif

    if (root == NULL) {
      root = new_leaf_node();
      reinterpret_cast<LeafNode*>(root)->left = NULL;
      reinterpret_cast<LeafNode*>(root)->right = NULL;
      reinterpret_cast<LeafNode*>(root)->seq = 0;
      depth = 0;
    }

    MemNode* val = NULL;
    if (depth == 0) {
      LeafNode *new_leaf = LeafInsert(key, reinterpret_cast<LeafNode*>(root), &val);
      if (new_leaf != NULL) {
	InnerNode *inner = new_inner_node();
	inner->num_keys = 1;
	inner->keys[0] = new_leaf->keys[0];
	inner->children[0] = root;
	inner->children[1] = new_leaf;
	depth++;
	root = inner;
	//				checkConflict(inner, 1);
	//				checkConflict(&root, 1);
#if BTREE_PROF
	writes++;
#endif
	//				inner->writes++;
      }
    }
    else {

#if BTPREFETCH
      for(int i = 0; i <= 64; i+= 64)
	prefetch(reinterpret_cast<char*>(root) + i);
#endif

      InnerInsert(key, reinterpret_cast<InnerNode*>(root), depth, &val);

    }
#if ENABLE_RTM
    if(0 == nested)
      RTMTX::End(&rtmlock);
#endif
    return val;
  }

  inline InnerNode* InnerInsert(uint64_t key, InnerNode *inner, int d, MemNode** val) {



    unsigned k = 0;
    uint64_t upKey;
    InnerNode *new_sibling = NULL;

    while((k < inner->num_keys) && (key >= inner->keys[k])) {
      ++k;
    }


    void *child = inner->children[k];

#if BTPREFETCH
    for(int i = 0; i <= 64; i+= 64)
      prefetch(reinterpret_cast<char*>(child) + i);
#endif


    if (d == 1) {

      LeafNode *new_leaf = LeafInsert(key, reinterpret_cast<LeafNode*>(child), val);

      if (new_leaf != NULL) {

	InnerNode *toInsert = inner;

	if (inner->num_keys == NENT) {

	  new_sibling = new_inner_node();

	  if (new_leaf->num_keys == 1) {
	    new_sibling->num_keys = 0;
	    upKey = new_leaf->keys[0];
	    toInsert = new_sibling;
	    k = -1;
	  }
	  else {
	    unsigned treshold= (NENT+1)/2;
	    new_sibling->num_keys= inner->num_keys -treshold;

	    for(unsigned i=0; i < new_sibling->num_keys; ++i) {
	      new_sibling->keys[i]= inner->keys[treshold+i];
	      new_sibling->children[i]= inner->children[treshold+i];
	    }

	    new_sibling->children[new_sibling->num_keys] = inner->children[inner->num_keys];
	    inner->num_keys= treshold-1;

	    upKey = inner->keys[treshold-1];

	    if (new_leaf->keys[0] >= upKey) {
	      toInsert = new_sibling;
	      if (k >= treshold) k = k - treshold;
	      else k = 0;
	    }
	  }
	  //					inner->keys[NENT-1] = upKey;
	  new_sibling->keys[NENT-1] = upKey;
	  //					checkConflict(new_sibling, 1);
#if BTREE_PROF
	  writes++;
#endif
	  //					new_sibling->writes++;

	}

	if (k != -1) {
	  for (int i=toInsert->num_keys; i>k; i--) {
	    toInsert->keys[i] = toInsert->keys[i-1];
	    toInsert->children[i+1] = toInsert->children[i];
	  }
	  toInsert->num_keys++;
	  toInsert->keys[k] = new_leaf->keys[0];
	}

	toInsert->children[k+1] = new_leaf;
	//				checkConflict(inner, 1);
#if BTREE_PROF
	writes++;
#endif
	//				inner->writes++;
      }
      else {
#if BTREE_PROF
	reads++;
#endif
	//				inner->reads++;
	//				checkConflict(inner, 0);
      }

      //			if (new_sibling!=NULL && new_sibling->num_keys == 0) printf("sibling\n");
    }
    else {


      bool s = true;
      InnerNode *new_inner =
	InnerInsert(key, reinterpret_cast<InnerNode*>(child), d - 1, val);


      if (new_inner != NULL) {
	InnerNode *toInsert = inner;
	InnerNode *child_sibling = new_inner;


	unsigned treshold= (NENT+1)/2;
	if (inner->num_keys == NENT) {
	  new_sibling = new_inner_node();

	  if (child_sibling->num_keys == 0) {
	    new_sibling->num_keys = 0;
	    upKey = child_sibling->keys[NENT-1];
	    toInsert = new_sibling;
	    k = -1;
	  }

	  else  {
	    new_sibling->num_keys= inner->num_keys -treshold;

	    for(unsigned i=0; i < new_sibling->num_keys; ++i) {
	      new_sibling->keys[i]= inner->keys[treshold+i];
	      new_sibling->children[i]= inner->children[treshold+i];
	    }
	    new_sibling->children[new_sibling->num_keys]=
	      inner->children[inner->num_keys];

	    //XXX: should threshold ???
	    inner->num_keys= treshold-1;

	    upKey = inner->keys[treshold-1];
	    //printf("UP %lx\n",upKey);
	    if (key >= upKey) {
	      toInsert = new_sibling;
	      if (k >= treshold) k = k - treshold;
	      else k = 0;
	    }
	  }
	  //XXX: what is this used for???
	  //inner->keys[NENT-1] = upKey;
	  new_sibling->keys[NENT-1] = upKey;

#if BTREE_PROF
	  writes++;
#endif
	  //					new_sibling->writes++;
	  //					checkConflict(new_sibling, 1);
	}
	if (k != -1) {
	  for (int i=toInsert->num_keys; i>k; i--) {
	    toInsert->keys[i] = toInsert->keys[i-1];
	    toInsert->children[i+1] = toInsert->children[i];
	  }

	  toInsert->num_keys++;
	  toInsert->keys[k] = reinterpret_cast<InnerNode*>(child_sibling)->keys[NENT-1];
	}
	toInsert->children[k+1] = child_sibling;

#if BTREE_PROF
	writes++;
#endif
	//				inner->writes++;
	//				checkConflict(inner, 1);
      }
      else {
#if BTREE_PROF
	reads++;
#endif
	//				inner->reads++;
	//				checkConflict(inner, 0);
      }


    }

    if (d==depth && new_sibling != NULL) {
      InnerNode *new_root = new_inner_node();
      new_root->num_keys = 1;
      new_root->keys[0]= upKey;
      new_root->children[0] = root;
      new_root->children[1] = new_sibling;
      root = new_root;
      depth++;

#if BTREE_PROF
      writes++;
#endif
      //			new_root->writes++;
      //			checkConflict(new_root, 1);
      //			checkConflict(&root, 1);
    }
    //		else if (d == depth) checkConflict(&root, 0);
    //		if (inner->num_keys == 0) printf("inner\n");
    //if (new_sibling->num_keys == 0) printf("sibling\n");
    return new_sibling;
  }

  inline LeafNode* LeafInsert(uint64_t key, LeafNode *leaf, MemNode** val) {

    LeafNode *new_sibling = NULL;
    unsigned k = 0;
    while((k < leaf->num_keys) && (leaf->keys[k]<key)) {
      ++k;
    }

    if((k < leaf->num_keys) && (leaf->keys[k] == key)) {

#if BTPREFETCH
      prefetch(reinterpret_cast<char*>(leaf->values[k]));
#endif
      *val = leaf->values[k];

#if BTREE_PROF
      reads++;
#endif
      assert(*val != NULL);
      return NULL;
    }


    LeafNode *toInsert = leaf;
    if (leaf->num_keys == MEM_BTREE_M) {
      new_sibling = new_leaf_node();

      if (leaf->right == NULL && k == leaf->num_keys) {
	new_sibling->num_keys = 0;
	toInsert = new_sibling;
	k = 0;
      }
      else {
	unsigned threshold= (MEM_BTREE_M+1)/2;
	new_sibling->num_keys= leaf->num_keys -threshold;
	for(unsigned j=0; j < new_sibling->num_keys; ++j) {
	  new_sibling->keys[j]= leaf->keys[threshold+j];
	  new_sibling->values[j]= leaf->values[threshold+j];
	}
	leaf->num_keys= threshold;


	if (k>=threshold) {
	  k = k - threshold;
	  toInsert = new_sibling;
	}
      }
      if (leaf->right != NULL) leaf->right->left = new_sibling;
      new_sibling->right = leaf->right;
      new_sibling->left = leaf;
      leaf->right = new_sibling;
      new_sibling->seq = 0;
#if BTREE_PROF
      writes++;
#endif
      //			new_sibling->writes++;
      //			checkConflict(new_sibling, 1);
    }


    //printf("IN LEAF1 %d\n",toInsert->num_keys);
    //printTree();

#if BTPREFETCH
    prefetch(reinterpret_cast<char*>(dummyval_));
#endif

    for (int j=toInsert->num_keys; j>k; j--) {
      toInsert->keys[j] = toInsert->keys[j-1];
      toInsert->values[j] = toInsert->values[j-1];
    }

    toInsert->num_keys = toInsert->num_keys + 1;
    toInsert->keys[k] = key;

#if DUMMY
    toInsert->values[k] = dummyval_;
    *val = dummyval_;
#else
    toInsert->values[k] = GetMemNode();
    *val = toInsert->values[k];
#endif

    assert(*val != NULL);
    dummyval_ = NULL;


#if BTREE_PROF
    writes++;
#endif
    //		leaf->writes++;
    //		checkConflict(leaf, 1);
    //printf("IN LEAF2");
    //printTree();
    leaf->seq = leaf->seq + 1;
    return new_sibling;
  }


  Iterator* GetIterator() {
    return new MemstoreBPlusTree::Iterator(this);
  }
  void printLeaf(LeafNode *n);
  void printInner(InnerNode *n, unsigned depth);
  void PrintStore();
  void PrintList();

  //YCSB TREE COMPARE Test Purpose
  void TPut(uint64_t key, uint64_t *value){
#if BTREE_LOCK
    MutexSpinLock lock(&slock);
#else
    //RTMArenaScope begtx(&rtmlock, &prof, arena_);
    RTMScope begtx(&prof, depth * 2, 1, &rtmlock);
#endif

    if (root == NULL) {
      root = new_leaf_node();
      reinterpret_cast<LeafNode*>(root)->left = NULL;
      reinterpret_cast<LeafNode*>(root)->right = NULL;
      reinterpret_cast<LeafNode*>(root)->seq = 0;
      depth = 0;
    }

    if (depth == 0) {
      LeafNode *new_leaf = TLeafInsert(key, reinterpret_cast<LeafNode*>(root), value);
      if (new_leaf != NULL) {
	InnerNode *inner = new_inner_node();
	inner->num_keys = 1;
	inner->keys[0] = new_leaf->keys[0];
	inner->children[0] = root;
	inner->children[1] = new_leaf;
	depth++;
	root = inner;
	//				checkConflict(inner, 1);
	//				checkConflict(&root, 1);
#if BTREE_PROF
	writes++;
#endif
	//				inner->writes++;
      }
    }
    else {

#if BTPREFETCH
      for(int i = 0; i <= 64; i+= 64)
	prefetch(reinterpret_cast<char*>(root) + i);
#endif

      TInnerInsert(key, reinterpret_cast<InnerNode*>(root), depth, value);

    }

  }



  inline LeafNode* TLeafInsert(uint64_t key, LeafNode *leaf, uint64_t *value) {

    LeafNode *new_sibling = NULL;
    unsigned k = 0;
    while((k < leaf->num_keys) && (leaf->keys[k]<key)) {
      ++k;
    }

    if((k < leaf->num_keys) && (leaf->keys[k] == key)) {

      leaf->values[k] = (MemNode *)value;

      return NULL;
    }


    LeafNode *toInsert = leaf;
    if (leaf->num_keys == MEM_BTREE_M) {
      new_sibling = new_leaf_node();

      if (leaf->right == NULL && k == leaf->num_keys) {
	new_sibling->num_keys = 0;
	toInsert = new_sibling;
	k = 0;
      }
      else {
	unsigned threshold= (MEM_BTREE_M+1)/2;
	new_sibling->num_keys= leaf->num_keys -threshold;
	for(unsigned j=0; j < new_sibling->num_keys; ++j) {
	  new_sibling->keys[j]= leaf->keys[threshold+j];
	  new_sibling->values[j]= leaf->values[threshold+j];
	}
	leaf->num_keys= threshold;


	if (k>=threshold) {
	  k = k - threshold;
	  toInsert = new_sibling;
	}
      }
      if (leaf->right != NULL) leaf->right->left = new_sibling;
      new_sibling->right = leaf->right;
      new_sibling->left = leaf;
      leaf->right = new_sibling;
      new_sibling->seq = 0;
#if BTREE_PROF
      writes++;
#endif
      //			new_sibling->writes++;
      //			checkConflict(new_sibling, 1);
    }


    //printf("IN LEAF1 %d\n",toInsert->num_keys);
    //printTree();



    for (int j=toInsert->num_keys; j>k; j--) {
      toInsert->keys[j] = toInsert->keys[j-1];
      toInsert->values[j] = toInsert->values[j-1];
    }

    toInsert->num_keys = toInsert->num_keys + 1;
    toInsert->keys[k] = key;
    toInsert->values[k] = (MemNode *)value;


    return new_sibling;
  }


  inline InnerNode* TInnerInsert(uint64_t key, InnerNode *inner, int d, uint64_t* value) {



    unsigned k = 0;
    uint64_t upKey;
    InnerNode *new_sibling = NULL;

    while((k < inner->num_keys) && (key >= inner->keys[k])) {
      ++k;
    }


    void *child = inner->children[k];

#if BTPREFETCH
    for(int i = 0; i <= 64; i+= 64)
      prefetch(reinterpret_cast<char*>(child) + i);
#endif


    if (d == 1) {

      LeafNode *new_leaf = TLeafInsert(key, reinterpret_cast<LeafNode*>(child), value);

      if (new_leaf != NULL) {

	InnerNode *toInsert = inner;

	if (inner->num_keys == NENT) {

	  new_sibling = new_inner_node();

	  if (new_leaf->num_keys == 1) {
	    new_sibling->num_keys = 0;
	    upKey = new_leaf->keys[0];
	    toInsert = new_sibling;
	    k = -1;
	  }
	  else {
	    unsigned treshold= (NENT+1)/2;
	    new_sibling->num_keys= inner->num_keys -treshold;

	    for(unsigned i=0; i < new_sibling->num_keys; ++i) {
	      new_sibling->keys[i]= inner->keys[treshold+i];
	      new_sibling->children[i]= inner->children[treshold+i];
	    }

	    new_sibling->children[new_sibling->num_keys] = inner->children[inner->num_keys];
	    inner->num_keys= treshold-1;

	    upKey = inner->keys[treshold-1];

	    if (new_leaf->keys[0] >= upKey) {
	      toInsert = new_sibling;
	      if (k >= treshold) k = k - treshold;
	      else k = 0;
	    }
	  }
	  //					inner->keys[NENT-1] = upKey;
	  new_sibling->keys[NENT-1] = upKey;


	}

	if (k != -1) {
	  for (int i=toInsert->num_keys; i>k; i--) {
	    toInsert->keys[i] = toInsert->keys[i-1];
	    toInsert->children[i+1] = toInsert->children[i];
	  }
	  toInsert->num_keys++;
	  toInsert->keys[k] = new_leaf->keys[0];
	}

	toInsert->children[k+1] = new_leaf;

      }


      //			if (new_sibling!=NENTULL && new_sibling->num_keys == 0) printf("sibling\n");
    }
    else {


      bool s = true;
      InnerNode *new_inner =
	TInnerInsert(key, reinterpret_cast<InnerNode*>(child), d - 1, value);


      if (new_inner != NULL) {
	InnerNode *toInsert = inner;
	InnerNode *child_sibling = new_inner;


	unsigned treshold= (NENT+1)/2;
	if (inner->num_keys == NENT) {
	  new_sibling = new_inner_node();

	  if (child_sibling->num_keys == 0) {
	    new_sibling->num_keys = 0;
	    upKey = child_sibling->keys[NENT-1];
	    toInsert = new_sibling;
	    k = -1;
	  }

	  else  {
	    new_sibling->num_keys= inner->num_keys -treshold;

	    for(unsigned i=0; i < new_sibling->num_keys; ++i) {
	      new_sibling->keys[i]= inner->keys[treshold+i];
	      new_sibling->children[i]= inner->children[treshold+i];
	    }
	    new_sibling->children[new_sibling->num_keys]=
	      inner->children[inner->num_keys];

	    //XXX: should threshold ???
	    inner->num_keys= treshold-1;

	    upKey = inner->keys[treshold-1];
	    //printf("UP %lx\n",upKey);
	    if (key >= upKey) {
	      toInsert = new_sibling;
	      if (k >= treshold) k = k - treshold;
	      else k = 0;
	    }
	  }
	  //XXX: what is this used for???
	  //inner->keys[NENT-1] = upKey;
	  new_sibling->keys[NENT-1] = upKey;


	}
	if (k != -1) {
	  for (int i=toInsert->num_keys; i>k; i--) {
	    toInsert->keys[i] = toInsert->keys[i-1];
	    toInsert->children[i+1] = toInsert->children[i];
	  }

	  toInsert->num_keys++;
	  toInsert->keys[k] = reinterpret_cast<InnerNode*>(child_sibling)->keys[NENT-1];
	}
	toInsert->children[k+1] = child_sibling;


      }



    }

    if (d==depth && new_sibling != NULL) {
      InnerNode *new_root = new_inner_node();
      new_root->num_keys = 1;
      new_root->keys[0]= upKey;
      new_root->children[0] = root;
      new_root->children[1] = new_sibling;
      root = new_root;
      depth++;


    }
    //		else if (d == depth) checkConflict(&root, 0);
    //		if (inner->num_keys == 0) printf("inner\n");
    //if (new_sibling->num_keys == 0) printf("sibling\n");
    return new_sibling;
  }







 public:

  //  static __thread RTMArena* arena_;	  // Arena used for allocations of nodes
  static __thread bool localinit_;
  static __thread MemNode *dummyval_;
  static __thread LeafNode *dummyleaf_;

  char padding1[64];
  void *root;
  int depth;

  char padding2[64];
  RTMProfile delprof;
  char padding3[64];

  RTMProfile prof;
  char padding6[64];
  //  port::SpinLock slock;
  //  SpinLock rtmlock;
#if BTREE_PROF
 public:
  uint64_t reads;
  uint64_t writes;
  uint64_t calls;
#endif
  char padding4[64];
  SpinLock rtmlock;
  char padding5[64];

  /*
		int current_tid;
		void *waccess[4][30];
		void *raccess[4][30];
		int windex[4];
		int rindex[4];*/
  static MemNode *GetMemNode() {
    //    MemNode *mem = new MemNode;

    return new MemNode;
  }
};

//__thread RTMArena* MemstoreBPlusTree::arena_ = NULL;
//__thread bool MemstoreBPlusTree::localinit_ = false;



#endif
