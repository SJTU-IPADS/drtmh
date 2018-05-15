#include "memstore/memstore_uint64bplustree.h"

#define unlikely(x) __builtin_expect(!!(x), 0)
	
//__thread RTMArena* MemstoreUint64BPlusTree::arena_ = NULL;
__thread bool MemstoreUint64BPlusTree::localinit_ = false;
__thread MemNode* MemstoreUint64BPlusTree::dummyval_ = NULL;

#if 0
void MemstoreUint64BPlusTree::printLeaf(LeafNode *n) {
  printf("Leaf Key num %d\n", n->num_keys);
  for (int i=0; i<n->num_keys;i++)
    printf("key  %s value %lx \t ",n->keys[i], n->values[i]);
  printf("\n");
}
	

void MemstoreUint64BPlusTree::printInner(InnerNode *n, unsigned depth) {
  printf("Inner %d Key num %d\n", depth, n->num_keys);
  for (int i=0; i<n->num_keys;i++)
    printf("\t%lx	",n->keys[i]);
  printf("\n");
  for (int i=0; i<=n->num_keys; i++)
    if (depth>1) printInner(reinterpret_cast<InnerNode*>(n->children[i]), depth-1);
    else printLeaf(reinterpret_cast<LeafNode*>(n->children[i]));
}
#endif
void MemstoreUint64BPlusTree::PrintStore() {
  printf("===============B+ Tree=========================\n");
#if 0		 
  if(root == NULL) {
    printf("Empty Tree\n");
    return;
  }
  if (depth == 0) printLeaf(reinterpret_cast<LeafNode*>(root));
  else {
    printInner(reinterpret_cast<InnerNode*>(root), depth);
  }
#endif		 
  printf("========================================\n");
} 

void MemstoreUint64BPlusTree::PrintList() {
#if 0		
  void* min = root;
  int d = depth;
  while (d > 0) {
    min = reinterpret_cast<InnerNode*>(min)->children[0]; 
    d--;
  }
  LeafNode *leaf = reinterpret_cast<LeafNode*>(min);
  while (leaf != NULL) {
    printLeaf(leaf);
    if (leaf->right != NULL)
      assert(leaf->right->left == leaf);
    leaf = leaf->right;
  }
#endif			
}
/*

  void MemstoreBPlusTree::topLeaf(LeafNode *n) {
  total_nodes++;
  if (n->writes > 40) printf("Leaf %lx , w %ld , r %ld\n", n, n->writes, n->reads);
		
  }

  void MemstoreBPlusTree::topInner(InnerNode *n, unsigned depth){
  total_nodes++;
  if (n->writes > 40) printf("Inner %lx depth %d , w %ld , r %ld\n", n, depth, n->writes, n->reads);
  for (int i=0; i<=n->num_keys;i++)
  if (depth > 1) topInner(reinterpret_cast<InnerNode*>(n->children[i]), depth-1);
  else topLeaf(reinterpret_cast<LeafNode*>(n->children[i]));
  }
	
  void MemstoreBPlusTree::top(){
  if (depth == 0) topLeaf(reinterpret_cast<LeafNode*>(root));
  else topInner(reinterpret_cast<InnerNode*>(root), depth);
  printf("TOTAL NODES %d\n", total_nodes);
  }

  void MemstoreBPlusTree::checkConflict(void *node, int mode) {
  if (mode == 1) {
  waccess[current_tid][windex[current_tid]] = node;
  windex[current_tid]++;
  for (int i= 0; i<4; i++) {
  if (i==current_tid) continue;
  for (int j=0; j<windex[i]; j++)
  if (node == waccess[i][j]) wconflict++;
  for (int j=0; j<rindex[i]; j++)
  if (node == raccess[i][j]) rconflict++;
  }
  }
  else {
  raccess[current_tid][rindex[current_tid]] = node;
  rindex[current_tid]++;
  for (int i= 0; i<4; i++) {
  if (i==current_tid) continue;
  for (int j=0; j<windex[i]; j++)
  if (node == waccess[i][j]) rconflict++;
  }
  }
		
  }*/
		
MemstoreUint64BPlusTree::Iterator::Iterator(MemstoreUint64BPlusTree* tree)
{
  tree_ = tree;
  node_ = NULL;
}
	
uint64_t* MemstoreUint64BPlusTree::Iterator::GetLink()
{
  return link_;
}
	
uint64_t MemstoreUint64BPlusTree::Iterator::GetLinkTarget()
{
  return target_;
}
	
	
// Returns true iff the iterator is positioned at a valid node.
bool MemstoreUint64BPlusTree::Iterator::Valid()
{
  bool b = (node_ != NULL) && (node_->num_keys > 0);
  return b;
}
	
// Advances to the next position.
// REQUIRES: Valid()
bool MemstoreUint64BPlusTree::Iterator::Next()
{
  //get next different key
  assert(Valid());
  bool b = true;
  RTMScope bgtx(&tree_->prof,1,1,&tree_->rtmlock);
  assert(node_ != NULL);
  if (node_->seq != seq_) {
    b = false;
    while (node_ != NULL) {
      int k = 0; 
      int num = node_->num_keys;
      while (k < num)  {
        int tmp = tree_->Compare(key_, node_->keys[k]);
        if (tmp < 0) break;
        ++k;
      }
      if (k == num) {
        node_ = node_->right;
        if (node_ == NULL) return b;
      }
      else {
        leaf_index = k;
        break;
      }
    }
			
  }
  else leaf_index++;
  if (leaf_index >= node_->num_keys) {
    node_ = node_->right;
    leaf_index = 0;		
    if (node_ != NULL){
      link_ = (uint64_t *)(&node_->seq);
      target_ = node_->seq;		
    }
  }
  if (node_ != NULL) {
    tree_->ArrayAssign(key_ , node_->keys[leaf_index]);
    value_ = node_->values[leaf_index];
    seq_ = node_->seq;
  }
  return b;
}
	
// Advances to the previous position.
// REQUIRES: Valid()
bool MemstoreUint64BPlusTree::Iterator::Prev()
{
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  //assert(Valid());
  if(!Valid())
    return false;
  
  //FIXME: This function doesn't support link information
  //  printf("PREV\n");
  bool b = true;
  RTMScope bgtx(&tree_->prof,1,1,&tree_->rtmlock);
  //  RTMArenaScope begtx(&tree_->rtmlock, &tree_->prof, tree_->arena_);
  if (node_->seq != seq_) {
    b = false;
    while (node_ != NULL) {
      int k = 0; 
      int num = node_->num_keys;
      while (k < num)  {
	int tmp = tree_->Compare(key_ , node_->keys[k]);
	if (tmp <= 0) break;
	++k;
      }
      if (k == num) {
	if (node_->right == NULL) break;
	node_ = node_->right;
      }
      else {
	leaf_index = k;
	break;
      }
    }
  }
  // printf("id %d\n",leaf_index);
  leaf_index--;
  if (leaf_index < 0) {
    node_ = node_->left;	
    //if (node_ != NULL) printf("NOTNULL\n");
    if (node_ != NULL) {
      leaf_index = node_->num_keys - 1;
      link_ = (uint64_t *)(&node_->seq);
      target_ = node_->seq;		
    }
  }
	  
  if (node_ != NULL) {
    tree_->ArrayAssign(key_ , node_->keys[leaf_index]);
    value_ = node_->values[leaf_index];
    seq_ = node_->seq;
  }
  return b;
}
	
uint64_t MemstoreUint64BPlusTree::Iterator::Key()
{
  return (uint64_t)key_;
  //  return (uint64_t) (node_->keys[leaf_index]);
}
	
MemNode* MemstoreUint64BPlusTree::Iterator::CurNode()
{
  if (!Valid()) return NULL;
  return value_;
}
	
// Advance to the first entry with a key >= target
void MemstoreUint64BPlusTree::Iterator::Seek(uint64_t key)
{
  //  RTMScope bgtx(&tree_->prof,1,1,&tree_->rtmlock);
  RTMScope bgtx(NULL,1,1,&tree_->rtmlock);  
  //  RTMArenaScope begtx(&tree_->rtmlock, &tree_->prof, tree_->arena_);
  LeafNode *leaf = tree_->FindLeaf((uint64_t *)key);
  link_ = (uint64_t *)(&leaf->seq);
  target_ = leaf->seq;		
  int num = leaf->num_keys;
  //fprintf(stdout,"num keys %d\n",num);
  //  assert(num > 0);
  int k = 0; 
  while (k < num)  {
    //printf("a %s\n",key +4);
    //printf("b %s\n",leaf->keys[k] +4);
    int tmp = tree_->Compare((uint64_t *)key, leaf->keys[k]);

    if (tmp <= 0) break;
    ++k;
  }
  if (k == num) {
    //    fprintf(stdout,"Get k %d %p\n",k,leaf);
    node_ = leaf->right;
    leaf_index = 0;

  }
  else {
    leaf_index = k;
    node_ = leaf;
  }		
  seq_ = node_->seq;
  tree_->ArrayAssign(key_ , node_->keys[leaf_index]);
  value_ = node_->values[leaf_index];
}
	
void MemstoreUint64BPlusTree::Iterator::SeekPrev(uint64_t key)
{
  LeafNode *leaf = tree_->FindLeaf((uint64_t *)key);
  link_ = (uint64_t *)(&leaf->seq);
  target_ = leaf->seq;		
		
  int k = 0; 
  int num = leaf->num_keys;
  while (k < num)  {
    int tmp = tree_->Compare((uint64_t *)key, leaf->keys[k]);
    if (tmp <= 0) break;
    ++k;
  }
  if (k == 0) {
    node_ = leaf->left;			
    link_ = (uint64_t *)(&node_->seq);
    target_ = node_->seq;		
    leaf_index = node_->num_keys - 1;						
  }
  else {
    k = k - 1;
    node_ = leaf;
  }
}
	
	
// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
void MemstoreUint64BPlusTree::Iterator::SeekToFirst()
{
  void* min = tree_->root;
  int d = tree_->depth;
  while (d > 0) {
    min = reinterpret_cast<InnerNode*>(min)->children[0]; 
    d--;
  }
  node_ = reinterpret_cast<LeafNode*>(min);
  link_ = (uint64_t *)(&node_->seq);
  target_ = node_->seq;		
  leaf_index = 0;
  RTMScope bgtx(&tree_->prof,1,1,&tree_->rtmlock);
  //  RTMArenaScope begtx(&tree_->rtmlock, &tree_->prof, tree_->arena_);
  tree_->ArrayAssign(key_ , node_->keys[0]);
  value_ = node_->values[0];
  seq_ = node_->seq;
}
	
// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
void MemstoreUint64BPlusTree::Iterator::SeekToLast()
{
  //TODO
  assert(0);
}




