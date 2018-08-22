#pragma once

namespace nocc {

namespace rtx {

class RTXIterator {
 public:
  RTXIterator(OCC *tx,int tableid,bool sec = false):
      tx_(tx){
    if(sec) {
      iter_ = (tx_->db_->_indexs[tableid])->GetIterator();
    } else {
      iter_ = (tx_->db_->stores_[tableid])->GetIterator();
    }
  }

  bool valid() {
    return cur_ != NULL && cur_->value != NULL;
  }

  uint64_t key() {
    return iter_->Key();
  }

  char *value() {
    if(valid())
      return (char *)val_;
    return NULL;
  }

  MemNode *node() {
    return (MemNode *)cur_;
  }

  void next() {
    bool r = iter_->Next();

    while(iter_->Valid()) {
      cur_ = iter_->CurNode();
      { // RTM scope
        RTMScope rtm(NULL);
        val_ = cur_->value;

        if(prev_link_ != iter_->GetLink()) {
          prev_link_ = iter_->GetLink();
        }

        if(val_ != NULL) {
          return;
        }
      }
      iter_->Next();
    }
    cur_ = NULL;
  }

  void prev() {
    assert(false);
  }

  void seek(uint64_t key) {

    iter_->Seek(key);
    cur_ = iter_->CurNode();

    //No keys is equal or larger than key
    if(!iter_->Valid()){
      assert(cur_ == NULL);
      return;
    }

    //Second, find the first key which value is not NULL
    while(iter_->Valid()) {
      {
        RTMScope rtm(NULL);
        val_ = cur_->value;

        if(val_ != NULL) {
          return;
        }
      }

      iter_->Next();
      cur_ = iter_->CurNode();
    }
    cur_ = NULL;
  }

 private:
  OCC *tx_;
  Memstore::Iterator *iter_;

  MemNode* cur_ = NULL;
  uint64_t *val_ = NULL;
  uint64_t *prev_link_ = NULL;
};

} // namespace rtx

} // namespace nocc
