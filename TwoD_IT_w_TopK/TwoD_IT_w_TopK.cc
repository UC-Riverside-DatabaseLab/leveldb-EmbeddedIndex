
#include "TwoD_IT_w_TopK.h"
#include <deque>
#include <exception>
#include <functional>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <list>
#include <sstream>
#include <utility>


//
static void split(std::list<std::string> &elems, const std::string &s, const char &delim) {
//based on http://stackoverflow.com/a/236803

std::stringstream ss(s);
std::string item1, item2;

std::getline(ss, item1, delim);
elems.push_back(item1);

std::getline(ss, item2);
elems.push_back(item2);
};


//
template <typename T>
static T max2(const T &a, const T &b) {
if (a>b) {
  return a;
}

return b;
};


//
template <typename T>
static T max3(const T &a, const T &b, const T &c) {
if (a>b) {
  if (a>c) {
    return a;
  }
}
else {
  if (b>c) {
    return b;
  }
}

return c;
};


//
void TwoDITwTopK::setDefaults() {

// set default values
id_delim = '+';

sync_threshold = 10000;
sync_counter = 0;
sync_file = "interval.str";

root = &nil;
nil.is_red = false;

iterator_in_use = false;
iterator = nullptr;

storage.reserve(1000000);

//std::ofstream o1("perf.log");
};


//
TwoDITwTopK::TwoDITwTopK() {

setDefaults();
};


//
TwoDITwTopK::TwoDITwTopK(const std::string &filename, const bool &sync_from_file) {

setDefaults();
sync_file = filename;

if (sync_from_file) {
  std::ifstream ifile(filename.c_str());

  if (ifile.is_open()) {
  
  std::list<TwoDInterval> l;
  
  ifile.read((char *)&l, sizeof(l));
  
  for (std::list<TwoDInterval>::const_iterator it = l.begin(); it != l.end(); it++) {
    insertInterval(it->GetId(), it->GetLowPoint(), it->GetHighPoint(), it->GetTimeStamp());
  }
    //std::string id, minKey, maxKey;
    //uint64_t timestamp;
    /*TwoDInterval i;

    //while (ifile>>id and ifile>>minKey and ifile>>maxKey and ifile>>timestamp) {
      //insertInterval(id, minKey, maxKey, timestamp);
    while (true) {
      try {
        ifile.read((char *)&i, sizeof(i));
        insertInterval(i.GetId(), i.GetLowPoint(), i.GetHighPoint(), i.GetTimeStamp());
      }
      catch (...) {
        break;
      }
    }*/
    
    ifile.close();
  }
}
};


//
TwoDITwTopK::~TwoDITwTopK() {

sync();
treeDestroy(root);
};


//
void TwoDITwTopK::insertInterval(const std::string &id, const std::string &minKey, const std::string &maxKey, const uint64_t &maxTimestamp) {

try {
  if (iterator_in_use)
    iterator->stop();
  
  if (id == "")
    throw std::runtime_error("Empty interval ID string");
  
  TwoDITNode *z = new TwoDITNode;
  
  std::list<std::string> r;
  split(r, id, id_delim);
  
  if (ids.find(r.front()) == ids.end()) {
    // create empty unordered_set for new key
    ids[r.front()] = std::unordered_set<std::string>();
  }
  else if (ids[r.front()].find(r.back()) != ids[r.front()].end()) {
    // existing id is being rewritten, so delete the old interval from storage
    deleteInterval(id);
  }
  
  ids[r.front()].insert(r.back());
  
  storage[id] = z;
  
  z->interval = TwoDInterval(id, minKey, maxKey, maxTimestamp);
  treeInsert(z);
    
  if (++sync_counter > sync_threshold) { sync(); }
}
catch(std::exception &e) {
  std::cerr<<std::endl<<"Insert failure: "<<e.what()<<std::endl;
}
}


//
void TwoDITwTopK::deleteInterval(const std::string &id) {

if(iterator_in_use)
  iterator->stop();

if (storage.find(id) != storage.end()) {
  
  std::list<std::string> r;
  split(r, id, id_delim);
  
  ids[r.front()].erase(r.back());
  if (ids[r.front()].empty())
    ids.erase(r.front());
  
  treeDelete(storage[id]);

  storage.erase(id);

  if (++sync_counter > sync_threshold) { sync(); }
}
};


//
void TwoDITwTopK::deleteAllIntervals(const std::string &id_prefix) {
if (ids.find(id_prefix) != ids.end()) {
  std::list<std::string> intervals_to_delete;
  
  for (std::unordered_set<std::string>::iterator it = ids[id_prefix].begin(); it != ids[id_prefix].end(); it++) {
    intervals_to_delete.push_back((*it == "") ? id_prefix : id_prefix + id_delim + *it);
  }
  
  for (std::list<std::string>::iterator it = intervals_to_delete.begin(); it != intervals_to_delete.end(); it++) {
    deleteInterval(*it);
  }
}
}


//
void TwoDITwTopK::getInterval(TwoDInterval &ret_interval, const std::string &id) const {

std::list<std::string> r;
split(r, id, id_delim);

if (ids.find(r.front()) != ids.end() and ids.at(r.front()).find(r.back()) != ids.at(r.front()).end())
  ret_interval = storage.at(id)->interval;
else
  ret_interval = TwoDInterval("", "", "", 0LL);

}


//
/*
void TwoDITwTopK::topK(std::vector<TwoDInterval> &ret_value, const std::string &minKey, const std::string &maxKey) {

TwoDInterval test("", minKey, maxKey, 0LL);
TwoDITNode *x;
std::unordered_set<TwoDITNode*> found;

while(treeIntervalSearch(test, found, x))
  ret_value.push_back(x->interval);

std::sort(ret_value.begin(), ret_value.end(), std::greater<TwoDInterval>());
};
*/

void TwoDITwTopK::topK(std::vector<TwoDInterval> &ret_value, const std::string &minKey, const std::string &maxKey) {

TwoDInterval test("", minKey, maxKey, 0LL);
TwoDITNode *x;
std::unordered_set<TwoDITNode*> found;
x = root;
treeIntervalSearch(test, ret_value, x);

//treeIntervalSearch(test, found, x);
//std::unordered_set<TwoDITNode*>
/*

 for (std::unordered_set< TwoDITNode*>::const_iterator it = found.begin(); it != found.end(); it++) {
	ret_value.push_back((*it)->interval);
  }
*/

	std::sort(ret_value.begin(), ret_value.end(), std::greater<TwoDInterval>());

}


//
void TwoDITwTopK::sync() const {

std::ofstream ofile(sync_file.c_str());

if (ofile.is_open()) {
  std::list<TwoDInterval> l;
  
  for (std::unordered_map<std::string, TwoDITNode*>::const_iterator it = storage.begin(); it != storage.end(); it++) {
    l.push_back(it->second->interval);
  }

  ofile.write((char *)&l, sizeof(l));
  ofile.close();
}

sync_counter = 0;
}


//
void TwoDITwTopK::setSyncFile(const std::string &filename) { sync_file = filename; }
void TwoDITwTopK::getSyncFile(std::string &filename) const { filename = sync_file; }

void TwoDITwTopK::setSyncThreshold(const uint32_t &threshold) { sync_threshold = threshold; }
void TwoDITwTopK::getSyncThreshold(uint32_t &threshold) const { threshold = sync_threshold; }

void TwoDITwTopK::setIdDelimiter(const char &delim) { id_delim = delim; }
void TwoDITwTopK::getIdDelimiter(char &delim) const { delim = id_delim; }


//
void TwoDITwTopK::storagePrint() const {

for (std::unordered_map<std::string, TwoDITNode*>::const_iterator it = storage.begin(); it != storage.end(); it++) {
  std::cout<<"("<<it->second->interval.GetId()<<","<<it->second->interval.GetLowPoint()<<","<<it->second->interval.GetHighPoint()
        <<","<<it->second->interval.GetTimeStamp()<<")"<<"\n";
}
}


//
void TwoDITwTopK::treePrintLevelOrder() const {
int depth, level=0;
TwoDITNode* x;
std::deque<std::pair<TwoDITNode*, int>> nodes;
std::stringstream line1, line2, line3, buffer;

if (root != &nil) {
  nodes.push_back(std::make_pair(root, 0));
}

while (!nodes.empty()) {
  x = nodes.front().first;
  depth = nodes.front().second;
  nodes.pop_front();
  
  if (depth != level) {
    std::cout<<line1.str()<<std::endl<<line2.str()<<std::endl<<line3.str()<<std::endl<<std::endl;
    line1.str(std::string());
    line2.str(std::string());
    line3.str(std::string());
    level++;
  }
  
  buffer<<"("<<x->interval.GetId()<<","<<x->interval.GetLowPoint()<<","<<x->interval.GetHighPoint()
        <<","<<x->interval.GetTimeStamp()<<")";
  line1<<std::setw(13)<<buffer.str();
  buffer.str(std::string());

  buffer<<"("<<x->max_high<<","<<x->max_timestamp<<","<<(x->is_red ? 'R' : 'B')<<")";
  line2<<std::setw(13)<<buffer.str();
  buffer.str(std::string());
  
  if (x->left != &nil) {
    nodes.push_back(std::make_pair(x->left, depth+1));
    buffer<<'/'<<x->left->interval.GetId();
  }
  buffer<<"    ";
  
  if (x->right != &nil) {
    nodes.push_back(std::make_pair(x->right, depth+1));
    buffer<<'\\'<<x->right->interval.GetId();
  }
  line3<<std::setw(13)<<buffer.str();
  buffer.str(std::string());
}

std::cout<<line1.str()<<std::endl<<line2.str()<<std::endl;
}


//
void TwoDITwTopK::treePrintInOrder() const {

treePrintInOrderRecursive(root, 0);
std::cout<<std::endl;
}


//
int TwoDITwTopK::treeHeight() const {

return treeHeightRecursive(root);
}


//
void TwoDITwTopK::treePrintInOrderRecursive(TwoDITNode* x, const int &depth) const {

if (x != &nil) {
  treePrintInOrderRecursive(x->left, depth + 1);
  std::cout<<" ("<<x->interval.GetId()<<","<<x->interval.GetLowPoint()<<","<<x->interval.GetHighPoint()
           <<","<<x->interval.GetTimeStamp()<<"):("<<x->max_high<<","<<x->max_timestamp
           <<","<<(x->is_red ? 'R' : 'B')<<","<<depth<<")";
  treePrintInOrderRecursive(x->right, depth + 1);
}
};


//
int TwoDITwTopK::treeHeightRecursive(TwoDITNode* x) const {

if (x == &nil)
  return 0;

int hl = treeHeightRecursive(x->left), hr = treeHeightRecursive(x->right);

if (hl > hr)
  return hl + 1;

return hr + 1;
}


//
bool TwoDITwTopK::treeIntervalSearch(const TwoDInterval &test_interval, std::unordered_set<TwoDITNode*> &found, TwoDITNode *&x) const {
  
  x = root;
  
  while (x != &nil) {
  
  if (x->interval * test_interval and found.find(x) == found.end()) {

    found.insert(x);
    //return true;
  }
  if (x->left == &nil)
    x = x->right;
  else if (x->left->max_high < test_interval.GetLowPoint())
    x = x->right;
  else
    x = x->left;
  }
  
  //return false;
}
void TwoDITwTopK::treeIntervalSearch(const TwoDInterval &test_interval, std::vector<TwoDInterval> &ret_value, TwoDITNode *x) const {

	if(x == &nil)
	{
		//std::cout<<"return1\n";
		return;
	}
	if(test_interval.GetLowPoint().compare(x->max_high) >0)
	{
		//std::cout<<"return2\n";

		return;
	}
	if(x->left!= &nil)
		treeIntervalSearch(test_interval, ret_value, x->left);

	if (x->interval * test_interval) {
		ret_value.push_back(x->interval);
	}

	if(test_interval.GetHighPoint().compare(x->interval.GetLowPoint()) <0)
	{
		//std::cout<<"return3\n";
		return;
	}
	if(x->right!= &nil)
		treeIntervalSearch(test_interval, ret_value, x->right);

}


//
void TwoDITwTopK::treeInsert(TwoDITNode* z) {
TwoDITNode *y = &nil, *x = root;

z->max_high = z->interval.GetHighPoint();
z->max_timestamp = z->interval.GetTimeStamp();

while (x != &nil) {
  y = x;
  
  if (y->max_high < z->max_high)
    y->max_high = z->max_high;
  if (y->max_timestamp < z->max_timestamp)
    y->max_timestamp = z->max_timestamp;
  
  if (z->interval.GetLowPoint() < x->interval.GetLowPoint())
    x = x->left;
  else
    x = x->right;
}

z->parent = y;
if (y == &nil)
  root = z;
else
  if (z->interval.GetLowPoint() < y->interval.GetLowPoint())
    y->left = z;
  else
    y->right = z;

z->left = &nil;
z->right = &nil;
z->is_red = true;

treeInsertFixup(z);
};


//
void TwoDITwTopK::treeInsertFixup(TwoDITNode* z) {
TwoDITNode *y;

while (z->parent->is_red) {
  if (z->parent == z->parent->parent->left) {
    y = z->parent->parent->right;
    if (y->is_red) {
      z->parent->is_red = false;
      y->is_red = false;
      z->parent->parent->is_red = true;
      z = z->parent->parent;
    }
    else {
      if (z == z->parent->right) {
        z = z->parent;
        treeLeftRotate(z);
      }
      z->parent->is_red = false;
      z->parent->parent->is_red = true;
      treeRightRotate(z->parent->parent);
    }
  }
  else {
    y = z->parent->parent->left;
    if (y->is_red) {
      z->parent->is_red = false;
      y->is_red = false;
      z->parent->parent->is_red = true;
      z = z->parent->parent;
    }
    else {
      if (z == z->parent->left) {
        z = z->parent;
        treeRightRotate(z);
      }
      z->parent->is_red = false;
      z->parent->parent->is_red = true;
      treeLeftRotate(z->parent->parent);
    }
  }
}

root->is_red = false;
};


//
void TwoDITwTopK::treeDelete(TwoDITNode* z) {
TwoDITNode *y = z, *x;
bool y_orig_is_red = y->is_red;


if (z->left == &nil) {
  x = z->right;
  treeTransplant(z, x);
}
else if (z->right == &nil) {
  x = z->left;
  treeTransplant(z, x);
}
else {
  y = treeMinimum(z->right);
  y_orig_is_red = y->is_red;
  x = y->right;
  if (y->parent == z) {
    x->parent = y;
  }
  else {
    treeTransplant(y, x);
    y->right = z->right;
    y->right->parent = y;
  }
  treeTransplant(z, y);
  y->left = z->left;
  y->left->parent = y;
  y->is_red = z->is_red;
}

treeMaxFieldsFixup(x->parent);

if (!y_orig_is_red)
  treeDeleteFixup(x);

delete z;
};


//
void TwoDITwTopK::treeDeleteFixup(TwoDITNode* x) {
TwoDITNode *w;

while (x != root and !x->is_red) {
  if (x == x->parent->left) {
    w = x->parent->right;
    if (w->is_red) {
      w->is_red = false;
      x->parent->is_red = true;
      treeLeftRotate(x->parent);
      w = x->parent->right;
    }
    if (!w->left->is_red and !w->right->is_red) {
      w->is_red = true;
      x = x->parent;
    }
    else {
      if (!w->right->is_red) {
        w->left->is_red = false;
        w->is_red = true;
        treeRightRotate(w);
        w = x->parent->right;
      }
      w->is_red = x->parent->is_red;
      x->parent->is_red = false;
      w->right->is_red = false;
      treeLeftRotate(x->parent);
      x = root;
    }
  }
  else {
    w = x->parent->left;
    if (w->is_red) {
      w->is_red = false;
      x->parent->is_red = true;
      treeRightRotate(x->parent);
      w = x->parent->left;
    }
    if (!w->left->is_red and !w->right->is_red) {
      w->is_red = true;
      x = x->parent;
    }
    else {
      if (!w->left->is_red) {
        w->right->is_red = false;
        w->is_red = true;
        treeLeftRotate(w);
        w = x->parent->left;
      }
      w->is_red = x->parent->is_red;
      x->parent->is_red = false;
      w->left->is_red = false;
      treeRightRotate(x->parent);
      x = root;
    }
  }
}

x->is_red = false;
};


//
TwoDITNode* TwoDITwTopK::treeMinimum(TwoDITNode* x) const {

while (x->left != &nil)
  x = x->left;

return x;
};


//
TwoDITNode* TwoDITwTopK::treeSuccessor(TwoDITNode* x) const {

if (x->right != &nil)
  return treeMinimum(x->right);

TwoDITNode* y = x->parent;

while (y != &nil and x == y->right) {
  x = y;
  y = y->parent;
}

return y;
};


//
void TwoDITwTopK::treeLeftRotate(TwoDITNode* x) {

TwoDITNode* y = x->right;
x->right = y->left;
if (y->left != &nil)
  y->left->parent = x;
y->parent = x->parent;

if (x->parent == &nil)
  root = y;
else
  if (x == x->parent->left)
    x->parent->left = y;
  else
    x->parent->right = y;

y->left= x;
x->parent = y;

y->max_high = x->max_high;
y->max_timestamp = x->max_timestamp;
treeSetMaxFields(x);
};


//
void TwoDITwTopK::treeRightRotate(TwoDITNode* x) {

TwoDITNode* y = x->left;
x->left = y->right;
if (y->right != &nil)
  y->right->parent = x;
y->parent = x->parent;

if (x->parent == &nil)
  root = y;
else
  if (x == x->parent->right)
    x->parent->right = y;
  else
    x->parent->left = y;

y->right= x;
x->parent = y;

y->max_high = x->max_high;
y->max_timestamp = x->max_timestamp;
treeSetMaxFields(x);
};


//
void TwoDITwTopK::treeTransplant(TwoDITNode* u, TwoDITNode* v) {

if (u->parent == &nil)
  root = v;
else if (u == u->parent->left)
  u->parent->left = v;
else
  u->parent->right = v;

v->parent = u->parent;
};


//
void TwoDITwTopK::treeMaxFieldsFixup(TwoDITNode* x) {

std::string old_high;
uint64_t old_timestamp;

while (x != &nil) {
  
  old_high = x->max_high;
  old_timestamp = x->max_timestamp;
  treeSetMaxFields(x);
  
  // early exemption
  if (x->max_high == old_high and x->max_timestamp == old_timestamp)
    break;
  
  x = x->parent;
}
};


//
void TwoDITwTopK::treeSetMaxFields(TwoDITNode* x) {

if (x->left != &nil)
  if (x->right != &nil) {
    x->max_high = max3<std::string>(x->interval.GetHighPoint(), x->left->max_high, x->right->max_high);
    x->max_timestamp = max3<uint64_t>(x->interval.GetTimeStamp(), x->left->max_timestamp, x->right->max_timestamp);
  }
  else {
    x->max_high = max2<std::string>(x->interval.GetHighPoint(), x->left->max_high);
    x->max_timestamp = max2<uint64_t>(x->interval.GetTimeStamp(), x->left->max_timestamp);
  }
else
  if (x->right != &nil) {
    x->max_high = max2<std::string>(x->interval.GetHighPoint(), x->right->max_high);
    x->max_timestamp = max2<uint64_t>(x->interval.GetTimeStamp(), x->right->max_timestamp);
  }
  else {
    x->max_high = x->interval.GetHighPoint();
    x->max_timestamp = x->interval.GetTimeStamp();
  }
};


//
void TwoDITwTopK::treeDestroy(TwoDITNode* x) {

if (x->left != &nil)
  treeDestroy(x->left);

if (x->right != &nil)
  treeDestroy(x->right);

delete x;
};


//
static bool heapCompare(const std::pair<TwoDITNode*, uint64_t> &a, const std::pair<TwoDITNode*, uint64_t> &b) {

if (a.second < b.second)
  return true;

return false;
};


//
TopKIterator::TopKIterator(TwoDITwTopK &it, TwoDInterval &ret_int, const std::string &min, const std::string &max) {

_it = &it;
_ret_int = &ret_int;

if(!start(min, max))
  std::cerr<<std::endl<<"Start failure: Interval tree is either empty or locked by another iterator."<<std::endl;
};


//
TopKIterator::~TopKIterator() {

if (iterator_in_use) {
  
  _it->iterator = nullptr;
  _it->iterator_in_use = false;
}
};


//
bool TopKIterator::next() {

if (iterator_in_use) {
  
  TwoDITNode *x;
  uint64_t p, t;
  
  while (!nodes.empty()) {
    
    std::pop_heap(nodes.begin(), nodes.end(), heapCompare);
    x = nodes.back().first;
    p = nodes.back().second;
    nodes.pop_back();
    
    if (explored.find(x) == explored.end()) {
    
      // branch by exploring children and bound from untenable sub-trees
      if ((x->left != &(_it->nil)) and (x->left->max_high >= search_int.GetLowPoint())) {
        
        nodes.push_back(std::make_pair(x->left, x->left->max_timestamp));
        std::push_heap(nodes.begin(), nodes.end(), heapCompare);
      }
      if ((x->right != &(_it->nil)) and (x->right->max_high >= search_int.GetLowPoint())) {
        
        nodes.push_back(std::make_pair(x->right, x->right->max_timestamp));
        std::push_heap(nodes.begin(), nodes.end(), heapCompare);
      }
    }
    
    if (x->interval * search_int) { // x intersects query interval
      
      t = x->interval.GetTimeStamp();
      if (t < p) {
        
        // reinsert older intersecting interval into heap with correct timestamp
        nodes.push_back(std::make_pair(x, t));
        std::push_heap(nodes.begin(), nodes.end(), heapCompare);
        
        // mark as explored
        explored.insert(x);

      }
      else {
        
        *_ret_int = x->interval;
        return true;
      }
    }
    
  }
}

return false;
};


//
void TopKIterator::restart(const std::string &min, const std::string &max) {

stop(false);
start(min, max);
};


//
void TopKIterator::stop(const bool &release) {

if (iterator_in_use) {
  
  if (release) {
    _it->iterator = nullptr;
    _it->iterator_in_use = false;
  }
  
  nodes.clear();
  explored.clear();
  iterator_in_use = false;
}
};


//
bool TopKIterator::start(const std::string &min, const std::string &max) {

if (_it->root != &(_it->nil) and !(_it->iterator_in_use)) {
  
  _it->iterator_in_use = true;
  _it->iterator = this;
  
  search_int = TwoDInterval("", min, max, 0);
  iterator_in_use = true;
  
  nodes.push_back(std::make_pair(_it->root, _it->root->max_timestamp));
  
  return true;
}

return false;
};


