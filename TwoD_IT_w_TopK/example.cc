
#include "TwoD_IT_w_TopK.h"
#include <iostream>

int main() {

// Create object a
std::cout<<std::endl<<"> Creating new interval store A with unparameterized constructor."<<std::endl;
TwoD_IT_w_TopK a;

// Set delimiter
std::cout<<std::endl<<"> Setting delimiter for two-level IDs in A to + (default is also +)."<<std::endl;
a.setIdDelimiter('+');

// Set sync file name
std::cout<<std::endl<<"> Setting sync file for A to test.txt."<<std::endl;
a.setSyncFile("test.txt");

// Insert intervals (id, minKey, maxKey, maxTimestamp)
std::cout<<std::endl<<"> Inserting intervals (id, minKey, maxKey, maxTimestamp) into A:"<<std::endl
         <<"(0+2, a, m, 1)"<<std::endl
         <<"(0+4, d, e, 3)"<<std::endl
         <<"(1, b, d, 4)"<<std::endl
         <<"(2, l, s, 5)"<<std::endl
         <<"(3+2, g, n, 8)"<<std::endl
         <<"(4, n, w, 12)"<<std::endl
         <<"(5, i, z, 16)"<<std::endl
         <<"(6, q, x, 21)"<<std::endl
         <<"(7, h, i, 25)"<<std::endl
         <<"(0+2, b, n, 26) <- rewrite"<<std::endl
         <<"(5, h, j, 27) <- rewrite"<<std::endl
         <<"(0+4, c, o, 28) <- rewrite"<<std::endl
         <<"(8, b, t, 30)"<<std::endl;
a.insertInterval("0+2", "a", "m", 1);
a.insertInterval("0+4", "d", "e", 3);
a.insertInterval("1", "b", "d", 4);
a.insertInterval("2", "l", "s", 5);
a.insertInterval("3+2", "g", "n", 8);
a.insertInterval("4", "n", "w", 12);
a.insertInterval("5", "i", "z", 16);
a.insertInterval("6", "q", "x", 21);
a.insertInterval("7", "h", "i", 25);
a.insertInterval("0+2", "b", "n", 26);
a.insertInterval("5", "h", "j", 27);
a.insertInterval("0+4", "c", "o", 28);
a.insertInterval("8", "b", "t", 30);

// Query an interval (id)
std::cout<<std::endl<<"> Querying intervals with ids 1 and 10 in A:"<<std::endl;
TwoD_Interval i;
a.getInterval(&i, "1");
std::cout<<"("<<i.GetId()<<", "<<i.GetLowPoint()<<", "<<i.GetHighPoint()<<", "<<i.GetMaxTimeStamp()<<")"<<std::endl;
a.getInterval(&i, "10");
std::cout<<"("<<i.GetId()<<", "<<i.GetLowPoint()<<", "<<i.GetHighPoint()<<", "<<i.GetMaxTimeStamp()<<")"
         <<" <- return value when id is absent, id is an empty string"<<std::endl;

// Call top-k (minKey, maxKey, k)
std::cout<<std::endl<<"> Top-5 intervals that overlap with (n,o) in A:"<<std::endl;
std::vector<TwoD_Interval> r;
a.topK(&r, "n", "o", 5);
for(std::vector<TwoD_Interval>::const_iterator it = r.begin(); it != r.end(); it++) {
  std::cout<<"("<<it->GetId()<<", "<<it->GetLowPoint()<<", "<<it->GetHighPoint()<<", "<<it->GetMaxTimeStamp()<<")"<<std::endl;
}

// Delete interval (id)
std::cout<<std::endl<<"> Deleting interval with id 4 (i.e. (n,w)) in A."<<std::endl;
a.deleteInterval("4");

// Save state to sync file
std::cout<<std::endl<<"> Syncing A's current state to file."<<std::endl;
a.sync();

// Construct new object from saved state
std::cout<<std::endl<<"> Creating a new interval store B with state stored in file test.txt."<<std::endl;
TwoD_IT_w_TopK b("test.txt", true);

// Delete all intervals with common prefix (id_prefix)
std::cout<<std::endl<<">> Deleting all intervals with ids starting with 0 (i.e. (b,n) and (c,o)) in A."<<std::endl;
a.deleteAllIntervals("0");

std::cout<<std::endl<<"> Top-5 intervals that overlap with (n,o) in A:"<<std::endl;
r.clear();
a.topK(&r, "n", "o", 5);
for(std::vector<TwoD_Interval>::const_iterator it = r.begin(); it != r.end(); it++) {
  std::cout<<"("<<it->GetId()<<", "<<it->GetLowPoint()<<", "<<it->GetHighPoint()<<", "<<it->GetMaxTimeStamp()<<")"<<std::endl;
}

std::cout<<std::endl<<"> Top-5 intervals that overlap with (n,o) in B:"<<std::endl;
r.clear();
b.topK(&r, "n", "o", 5);
for(std::vector<TwoD_Interval>::const_iterator it = r.begin(); it != r.end(); it++) {
  std::cout<<"("<<it->GetId()<<", "<<it->GetLowPoint()<<", "<<it->GetHighPoint()<<", "<<it->GetMaxTimeStamp()<<")"<<std::endl;
}

std::cout<<std::endl;

return 0;
};
