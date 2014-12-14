#ifndef TWOD_IT_W_TOPK_H
#define TWOD_IT_W_TOPK_H

#include <list>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>



// 1d-interval in interval_dimension-time space
class TwoD_Interval {
public:
  TwoD_Interval() {};
  TwoD_Interval(const std::string id, const std::string low, const std::string high, const long long max_timestamp)
    : _id(id), _low(low), _high(high), _max_timestamp(max_timestamp) {};
  
  std::string GetId() const {return _id;};
  std::string GetLowPoint() const {return _low;};
  std::string GetHighPoint() const {return _high;};
  long long GetMaxTimeStamp() const {return _max_timestamp;};
  
  bool operator == (const TwoD_Interval& otherInterval)
    const {return (_id == otherInterval._id);}
  bool operator > (const TwoD_Interval& otherInterval)
    const {return (_max_timestamp > otherInterval._max_timestamp);}
  
  // overlap operator
  bool operator * (const TwoD_Interval& otherInterval) const {
    // point intersections are considered intersections
    if (_low < otherInterval._low) return (_high >= otherInterval._low);
    return (otherInterval._high >= _low);}

protected:
  std::string _id;
  std::string _low;
  std::string _high;
  long long _max_timestamp;
};


// Storage and index for intervals
class TwoD_IT_w_TopK {
public:
  TwoD_IT_w_TopK();
  TwoD_IT_w_TopK(const std::string &filename, const bool &sync_from_file);
  ~TwoD_IT_w_TopK();

  void insertInterval(const std::string &id, const std::string &minKey, const std::string &maxKey, const long long &maxTimestamp);
  
  void deleteInterval(const std::string &id);
  void deleteAllIntervals(const std::string &id_prefix);
  
  void getInterval(TwoD_Interval *ret_interval, const std::string &id) const;
  void topK(std::vector<TwoD_Interval> *ret_value, const std::string &minKey, const std::string &maxKey, const int &k) const;
  
  void sync() const;
  void setSyncFile(const std::string &filename);
  void getSyncFile(std::string *filename) const;
  void setSyncThreshold(const long &threshold);
  void getSyncThreshold(long *threshold) const;
  
  void setIdDelimiter(const char &delim);
  void getIdDelimiter(char *delim) const;

private:
  std::list<TwoD_Interval> storage;
  
  std::unordered_map<std::string, std::unordered_set<std::string> > ids;
  char id_delim;
  
  std::string sync_file;
  long sync_threshold;
  mutable long sync_counter;
  void incrementSyncCounter(const long &increment) const;
  
};


#endif
