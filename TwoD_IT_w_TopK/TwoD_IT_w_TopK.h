#ifndef TWOD_IT_W_TOPK_H
#define TWOD_IT_W_TOPK_H

#include <algorithm>
#include <inttypes.h>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>



class TwoDITNode;
class TopKIterator;

// 1d-interval in interval_dimension-time space
class TwoDInterval {
public:
  TwoDInterval() {};
  TwoDInterval(const std::string &id, const std::string &low, const std::string &high, const uint64_t &timestamp) :
    _id(id), _low(low), _high(high), _timestamp(timestamp), tree_node(nullptr) {};
  
  std::string GetId() const {return _id;};
  std::string GetLowPoint() const {return _low;};
  std::string GetHighPoint() const {return _high;};
  uint64_t GetTimeStamp() const {return _timestamp;};
  
  bool operator == (const TwoDInterval& otherInterval)
    const {return (_id == otherInterval._id);}
  bool operator > (const TwoDInterval& otherInterval)
    const {return (_timestamp > otherInterval._timestamp);}
  
  // overlap operator
  bool operator * (const TwoDInterval& otherInterval) const {
    // point intersections are considered intersections
    if (_low < otherInterval._low) return (_high >= otherInterval._low);
    return (otherInterval._high >= _low);
    }

  TwoDITNode* tree_node;

protected:
  std::string _id;
  std::string _low;
  std::string _high;
  uint64_t _timestamp;
};


// Interval tree node
class TwoDITNode {
public:
  TwoDITNode() : is_red(false) {};

  //TwoDInterval *interval;
  std::string interval_id;
  bool is_red;
  std::string max_high;
  uint64_t max_timestamp;
  TwoDITNode *left, *right, *parent;
};


// Storage and index for intervals
class TwoDITwTopK {
public:
  TwoDITwTopK();
  TwoDITwTopK(const std::string &filename, const bool &sync_from_file);
  ~TwoDITwTopK();

  void insertInterval(const std::string &id, const std::string &minKey, const std::string &maxKey, const uint64_t &maxTimestamp);
  
  void deleteInterval(const std::string &id);
  void deleteAllIntervals(const std::string &id_prefix);
  
  void getInterval(TwoDInterval &ret_interval, const std::string &id) const;
  void topK(std::vector<TwoDInterval> &ret_value, const std::string &minKey, const std::string &maxKey, const uint32_t &k) const;
  
  void sync() const;

  void setSyncFile(const std::string &filename);
  void getSyncFile(std::string &filename) const;
  void setSyncThreshold(const uint32_t &threshold);
  void getSyncThreshold(uint32_t &threshold) const;
  
  void setIdDelimiter(const char &delim);
  void getIdDelimiter(char &delim) const;

  void storagePrint() const;
  void treePrintLevelOrder() const;
  void treePrintInOrder() const;
  
private:
  
  void setDefaults();
  
  void treePrintInOrderRecursive(TwoDITNode* x, const int &depth) const;
  int treeHeight();
  void treeInsert(TwoDITNode* z);
  void treeInsertFixup(TwoDITNode* z);
  void treeDelete(TwoDITNode* z);
  void treeDeleteFixup(TwoDITNode* x);
  TwoDITNode* treeMinimum(TwoDITNode* x) const;
  TwoDITNode* treeSuccessor(TwoDITNode* x) const;
  void treeLeftRotate(TwoDITNode* x);
  void treeRightRotate(TwoDITNode* x);
  void treeTransplant(TwoDITNode* u, TwoDITNode* v);
  void treeMaxFieldsFixup(TwoDITNode* x);
  void treeSetMaxFields(TwoDITNode* x);
  void treeDestroy(TwoDITNode* x);
  
  TwoDITNode *root, nil;
  std::unordered_map<std::string, TwoDInterval> storage;
  
  std::unordered_map<std::string, std::unordered_set<std::string> > ids;
  char id_delim;
  
  std::string sync_file;
  uint32_t sync_threshold;
  mutable uint32_t sync_counter;
  
  bool iterator_in_use;
  TopKIterator *iterator;
  
friend class TopKIterator;
};


class TopKIterator {
public:
  
  TopKIterator(TwoDITwTopK &it, TwoDInterval &ret_int, const std::string &min, const std::string &max);
  ~TopKIterator();
  
  bool next();
  void restart(const std::string &min, const std::string &max);
  void stop(const bool &release=true);

private:
  
  bool start(const std::string &min, const std::string &max);
  
  TwoDITwTopK *_it;
  TwoDInterval *_ret_int, search_int;
  
  bool iterator_in_use;
  std::vector<std::pair<TwoDITNode*, uint64_t>> nodes;
  std::unordered_set<TwoDITNode*> explored;

};


#endif
