
#include "TwoD_IT_w_TopK.h"
#include <algorithm>
#include <functional>
#include <sstream>



//
static void split(std::list<std::string> *elems, const std::string s, const char delim) {
//based on http://stackoverflow.com/a/236803

std::stringstream ss(s);
std::string item1, item2;

std::getline(ss, item1, delim);
elems->push_back(item1);

std::getline(ss, item2);
elems->push_back(item2);

};


//
TwoD_IT_w_TopK::TwoD_IT_w_TopK() {
// default id delimiter
id_delim = '+';

};


//
TwoD_IT_w_TopK::TwoD_IT_w_TopK(const std::string & filename) {};


//
TwoD_IT_w_TopK::~TwoD_IT_w_TopK() {};


//
void TwoD_IT_w_TopK::insertInterval(const std::string id, const std::string minKey, const std::string maxKey, const long long maxTimestamp) {

std::list<std::string> r;
split(&r, id, id_delim);

if (ids.find(r.front()) != ids.end() && ids[r.front()].find(r.back()) != ids[r.front()].end()) {
  // existing id is being rewritten, so delete the old interval from storage
  deleteInterval(id);
}

if (ids.find(r.front()) == ids.end()) {
  // create empty unordered_set for new key
  ids[r.front()] = std::unordered_set<std::string>();
}

ids[r.front()].insert(r.back());

storage.push_back(TwoD_Interval(id, minKey, maxKey, maxTimestamp));

};


//
void TwoD_IT_w_TopK::deleteInterval(const std::string id) {
storage.remove(TwoD_Interval(id, "", "", 0LL));

std::list<std::string> r;
split(&r, id, id_delim);

ids[r.front()].erase(r.back());

if (ids[r.front()].empty()) {
  ids.erase(r.front());
}
};


//
void TwoD_IT_w_TopK::deleteAllIntervals(const std::string id_prefix) {

std::list<std::string> intervals_to_delete;

if (ids.find(id_prefix) != ids.end()) {
  for (std::unordered_set<std::string>::iterator it = ids[id_prefix].begin(); it != ids[id_prefix].end(); it++) {
    if (*it == "") {
      intervals_to_delete.push_back(id_prefix);
    }
    else {
      intervals_to_delete.push_back(id_prefix + id_delim + *it);
    }
  }
  
  for (std::list<std::string>::iterator it = intervals_to_delete.begin(); it != intervals_to_delete.end(); it++) {
    deleteInterval(*it);
  }
}

};


//
void TwoD_IT_w_TopK::getInterval(TwoD_Interval *ret_interval, const std::string id) const {

std::list<std::string> r;
split(&r, id, id_delim);

if (ids.find(r.front()) != ids.end()) {
  for (std::list<TwoD_Interval>::const_iterator it = storage.begin(); it != storage.end(); it++) {
    if (it->GetId() == id) {
      *ret_interval = *it;
    }
  }
}
else {
  *ret_interval = TwoD_Interval("", "", "", 0LL);
}

};


//
void TwoD_IT_w_TopK::topK(std::vector<TwoD_Interval> *ret_value, const std::string minKey, const std::string maxKey, const int k) const {

TwoD_Interval test("", minKey, maxKey, 0LL);

for (std::list<TwoD_Interval>::const_iterator it = storage.begin(); it != storage.end(); it++) {
  if (*it * test) {
    ret_value->push_back(*it);
  }
}

std::sort(ret_value->begin(), ret_value->end(), std::greater<TwoD_Interval>());

if (ret_value->size() > k) {
  ret_value->erase(ret_value->begin() + k, ret_value->end());
}

};


//
void TwoD_IT_w_TopK::sync(const std::string & filename) const {};


//
void TwoD_IT_w_TopK::changeIdDelimiter(const char new_delim) {
id_delim = new_delim;

};


