
#include "TwoD_IT_w_TopK.h"
#include <iostream>

int main() {

TwoD_IT_w_TopK a;
a.changeIdDelimiter('+');

// Insert intervals (id, minKey, maxKey, maxTimestamp)
std::cout<<std::endl<<"Inserting intervals (id, minKey, maxKey, maxTimestamp):"<<std::endl
         <<"(0+2, a, m, 1)"<<std::endl
         <<"(0+4, d, e, 3)"<<std::endl
         <<"(1, b, d, 4)"<<std::endl
         <<"(2, l, s, 5)"<<std::endl
         <<"(3, g, n, 8)"<<std::endl
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
a.insertInterval("3", "g", "n", 8);
a.insertInterval("4", "n", "w", 12);
a.insertInterval("5", "i", "z", 16);
a.insertInterval("6", "q", "x", 21);
a.insertInterval("7", "h", "i", 25);
a.insertInterval("0+2", "b", "n", 26);
a.insertInterval("5", "h", "j", 27);
a.insertInterval("0+4", "c", "o", 28);
a.insertInterval("8", "b", "t", 30);


// Call top-k (minKey, maxKey, k)
std::cout<<std::endl<<"Top-5 intervals that overlap with (n,o):"<<std::endl;
std::vector<TwoD_Interval> r;
a.topK(&r, "n", "o", 5);
for(std::vector<TwoD_Interval>::const_iterator it = r.begin(); it != r.end(); it++) {
  std::cout<<"("<<it->GetId()<<", "<<it->GetLowPoint()<<", "<<it->GetHighPoint()<<", "<<it->GetMaxTimeStamp()<<")"<<std::endl;
  }

// Delete interval (id)
std::cout<<std::endl<<"Deleting interval with id 4 (i.e. (n,w))"<<std::endl;
a.deleteInterval("4");

// Delete all intervals with common prefix (id_prefix)
std::cout<<std::endl<<"Deleting all intervals with ids starting with 0 (i.e. (b,n) and (c,o))"<<std::endl;
a.deleteAllIntervals("0");

std::cout<<std::endl<<"Top-5 intervals that overlap with (n,o):"<<std::endl;
r.clear();
a.topK(&r, "n", "o", 5);
for(std::vector<TwoD_Interval>::const_iterator it = r.begin(); it != r.end(); it++) {
  std::cout<<"("<<it->GetId()<<", "<<it->GetLowPoint()<<", "<<it->GetHighPoint()<<", "<<it->GetMaxTimeStamp()<<")"<<std::endl;
  }

std::cout<<std::endl<<"Querying interval with id 1:"<<std::endl;
TwoD_Interval i;
a.getInterval(&i, "1");
std::cout<<"("<<i.GetId()<<", "<<i.GetLowPoint()<<", "<<i.GetHighPoint()<<", "<<i.GetMaxTimeStamp()<<")"<<std::endl;

std::cout<<std::endl;

return 0;
};
