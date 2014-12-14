
#include "TwoD_IT_w_TopK.h"
#include <algorithm>
#include <functional>
#include <fstream>
#include <sstream>



//
static void split(std::list<std::string> *elems, const std::string &s, const char &delim) {
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

// set default values
id_delim = '+';
sync_threshold = 10000;
sync_counter = 0;
sync_file = "interval.str";
};


//
TwoD_IT_w_TopK::TwoD_IT_w_TopK(const std::string &filename, const bool &sync_from_file) {

// set default values
id_delim = '+';
sync_threshold = 10000;
sync_counter = 0;

sync_file = filename;

if (sync_from_file) {
  std::ifstream ifile(filename.c_str());

  if (ifile.is_open()) {
    std::string id, minKey, maxKey;
    long long maxTimestamp;

    while (ifile>>id && ifile>>minKey && ifile>>maxKey && ifile>>maxTimestamp) {
      insertInterval(id, minKey, maxKey, maxTimestamp);
    }

    ifile.close();
  }
}
};


//
TwoD_IT_w_TopK::~TwoD_IT_w_TopK() { sync(); };


//
void TwoD_IT_w_TopK::insertInterval(const std::string &id, const std::string &minKey, const std::string &maxKey, const long long &maxTimestamp) {

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

incrementSyncCounter(1);
};


//
void TwoD_IT_w_TopK::deleteInterval(const std::string &id) {

storage.remove(TwoD_Interval(id, "", "", 0LL));

std::list<std::string> r;
split(&r, id, id_delim);

ids[r.front()].erase(r.back());

if (ids[r.front()].empty()) {
  ids.erase(r.front());
}

incrementSyncCounter(1);
};


//
void TwoD_IT_w_TopK::deleteAllIntervals(const std::string &id_prefix) {

std::list<std::string> intervals_to_delete;

if (ids.find(id_prefix) != ids.end()) {
  for (std::unordered_set<std::string>::iterator it = ids[id_prefix].begin(); it != ids[id_prefix].end(); it++) {
    intervals_to_delete.push_back((*it == "") ? id_prefix : id_prefix + id_delim + *it);
  }
  
  for (std::list<std::string>::iterator it = intervals_to_delete.begin(); it != intervals_to_delete.end(); it++) {
    deleteInterval(*it);
  }
}
};


//
void TwoD_IT_w_TopK::getInterval(TwoD_Interval *ret_interval, const std::string &id) const {

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

incrementSyncCounter(1);
};


//
void TwoD_IT_w_TopK::topK(std::vector<TwoD_Interval> *ret_value, const std::string &minKey, const std::string &maxKey, const int &k) const {

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

incrementSyncCounter(k);
};


//
void TwoD_IT_w_TopK::sync() const {

std::ofstream ofile(sync_file.c_str());

if (ofile.is_open()) {
  std::string out;
  for (std::list<TwoD_Interval>::const_iterator it = storage.begin(); it != storage.end(); it++) {
    ofile << it->GetId() << std::endl
          << it->GetLowPoint() << std::endl
          << it->GetHighPoint() << std::endl
          << it->GetMaxTimeStamp() << std::endl;
  }

ofile.close();
}
};


//
void TwoD_IT_w_TopK::incrementSyncCounter(const long &increment) const {

sync_counter += increment;

if (sync_counter >= sync_threshold) {
  sync();
  sync_counter = 0;
}
};


//

void TwoD_IT_w_TopK::setSyncFile(const std::string &filename) { sync_file = filename; };
void TwoD_IT_w_TopK::getSyncFile(std::string *filename) const { *filename = sync_file; };

void TwoD_IT_w_TopK::setSyncThreshold(const long &threshold) { sync_threshold = threshold; };
void TwoD_IT_w_TopK::getSyncThreshold(long *threshold) const { *threshold = sync_threshold; };

void TwoD_IT_w_TopK::setIdDelimiter(const char &delim) { id_delim = delim; };
void TwoD_IT_w_TopK::getIdDelimiter(char *delim) const { *delim = id_delim; };

