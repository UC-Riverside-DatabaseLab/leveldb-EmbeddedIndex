#include <cstdlib>
#include <sstream>
#include <stdlib.h>
#include <cassert>
#include <algorithm>
#include <leveldb/db.h>
#include <leveldb/table.h>
#include <leveldb/slice.h>
#include <leveldb/filter_policy.h>
#include "leveldb/cache.h"
#include <fstream>
#include <iostream>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include <sys/time.h>
#include <unistd.h>

using namespace std;
using namespace leveldb;

//NYTweetsUserIDRangerw19
//NYTweetsTimerw19

//static int workloadtype = 2; //static =1, dynamic =2



//static string benchmark_file = "/home/mohiuddin/Desktop/LevelDB_Correctness_Testing/Benchmarks/NYTweetsUserIDRangerw19";

//static string database = "/home/mohiuddin/Desktop/LevelDB_Correctness_Testing/DB/Embedded_Static_UserID";
//static string static_dataset = "/home/mohiuddin/Desktop/LevelDB_Correctness_Testing/Datasets/StaticDatasetLarge";
//static string static_query = "/home/mohiuddin/Desktop/LevelDB_Correctness_Testing/Datasets/StaticQueryUser";
//static string static_pr = "/home/mohiuddin/Desktop/LevelDB_Correctness_Testing/Datasets/StaticQueryID";
//
//static string result_file = "/home/mohiuddin/Desktop/LevelDB_Correctness_Testing/Results/Embedded_Static_UserID.csv";
//static string iostat_file = "/home/mohiuddin/Desktop/LevelDB_Correctness_Testing/Results/Embedded_Static_UserID_iostat.csv";

//static uint64_t MAX_WRITE = 2000000;
//static uint64_t LOG_POINT =  MAX_WRITE/20;
//
//static uint64_t MAX_QUERY =  MAX_WRITE/1000;
//static uint64_t READ_LOG_POINT = MAX_QUERY/5;

//static int topkset[] = {10, 100, 1000};
//static int topksetlen = 3;

static string benchmark_file = "../Dataset/NYTweetsUserIDRangerw19";

static string database = "../DB/Embedded_Static_UserID";
static string static_dataset = "../Datasets/StaticDatasetSmall";
static string static_query = "../Datasets/StaticQueryUser";
static string static_pr = "../Datasets/StaticQueryID";

static string result_file = "../Results/Embedded_Static_UserID.csv";
static string iostat_file = "../Results/Embedded_Static_UserID_iostat.csv";

static uint64_t MAX_WRITE = 150000000;
static uint64_t LOG_POINT =  MAX_WRITE/150;

static uint64_t MAX_QUERY =  MAX_WRITE/10;
static uint64_t READ_LOG_POINT = MAX_QUERY/20;

static int topkset[] = {5, 10, 50, 100, 1000, 10000};
static int topksetlen = 6;


static std::string PrimaryAtt = "ID";
//static std::string SecondaryAtt = "CreationTime";
static std::string SecondaryAtt = "UserID";


static int isintervaltree = false;
static int numberofselectivity = 6;
static int topk = 5;


std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems) {
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}

std::vector<std::string> split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    split(s, delim, elems);
    return elems;
}

void ParseTweetJson(std::string& tweet, std::string& pkey)
{

	  if(PrimaryAtt.empty())
	      pkey = "";
	  rapidjson::Document docToParse;

	  docToParse.Parse<0>(tweet.c_str());

	  const char* pKeyAtt = PrimaryAtt.c_str();

	  if(!docToParse.IsObject()||!docToParse.HasMember(pKeyAtt)||docToParse[pKeyAtt].IsNull())
		  pkey= "";

	  std::ostringstream pKey;
	  if(docToParse[pKeyAtt].IsNumber())
	  {
	        if(docToParse[pKeyAtt].IsUint64())
	        {
	            unsigned long long int tid = docToParse[pKeyAtt].GetUint64();
	            pKey<<tid;

	        }
	        else if (docToParse[pKeyAtt].IsInt64())
	        {
	            long long int tid = docToParse[pKeyAtt].GetInt64();
	            pKey<<tid;
	        }
	        else if (docToParse[pKeyAtt].IsDouble())
	        {
	            double tid = docToParse[pKeyAtt].GetDouble();
	            pKey<<tid;
	        }

	        else if (docToParse[pKeyAtt].IsUint())
	        {
	            unsigned int tid = docToParse[pKeyAtt].GetUint();
	            pKey<<tid;
	        }
	        else if (docToParse[pKeyAtt].IsInt())
	        {
	            int tid = docToParse[pKeyAtt].GetInt();
	            pKey<<tid;
	        }
	  }
	  else if (docToParse[pKeyAtt].IsString())
	  {
	        const char* tid = docToParse[pKeyAtt].GetString();
	        pKey<<tid;
	  }
	  else if(docToParse[pKeyAtt].IsBool())
	  {
	        bool tid = docToParse[pKeyAtt].GetBool();
	        pKey<<tid;
	  }

	  //Slice key = pKey.str();
	  docToParse.RemoveMember(PrimaryAtt.c_str());
	  rapidjson::StringBuffer strbuf;
	  rapidjson::Writer<rapidjson::StringBuffer> writer(strbuf);
	  docToParse.Accept(writer);

	  tweet = strbuf.GetString();
	  //cout<<pKey.str()<<endl<<endl;
	  pkey = pKey.str();

}


void testWithBenchMark()
{
    leveldb::DB *db;
    leveldb::Options options;

    leveldb::IOStat lookup_iostat;
    leveldb::IOStat range_lookup_iostat;
    leveldb::IOStat get_iostat;

    w_iostat.clear();
    pr_iostat.clear();
    sr_iostat.clear();
    sr_range_iostat.clear();

    ofstream ofile_iostat(iostat_file.c_str(),  std::ofstream::out | std::ofstream::app );

	int bloomfilter = 100;
	string sbloomfilter = "100";

    options.filter_policy = leveldb::NewBloomFilterPolicy(bloomfilter);
    options.PrimaryAtt = PrimaryAtt;
    //options.secondaryAtt = "UserID";
    options.secondaryAtt = SecondaryAtt;
    options.create_if_missing = true;

    if(isintervaltree)
    	options.IntervalTreeFileName = "./interval_tree";
    else
    	options.IntervalTreeFileName ="";

    leveldb::Status status = leveldb::DB::Open(options, database, &db);
    assert(status.ok());

    ifstream ifile(benchmark_file.c_str());
    vector<leveldb::SKeyReturnVal> svalues;
    if (!ifile) { cerr << "Can't open input file " << endl; return; }
    string line;
    uint64_t i=0;
    rapidjson::Document d;

    leveldb::ReadOptions roptions;
    //roptions.iostat.clear();

    double w=0, rs=0, rp=0, rsrange = 0;
    long rscount = 0, rsrangecount = 0;
    double durationW=0,durationRS=0,durationRP=0, durationRSRange=0;
	ofstream ofile(result_file.c_str(),  std::ofstream::out | std::ofstream::app );

	//ofstream ofile1(result_file1.c_str(),  std::ofstream::out | std::ofstream::app );
    while(getline(ifile, line)) {
		i++;

        std::vector<std::string> x = split(line, '\t');
        leveldb::WriteOptions woptions;
        //iostat.clear();
		struct timeval start, end;
        if(x.size()>=3) {

            leveldb::Slice key = x[2];
//            if(x[1]=="w")
//            	continue;
//            cout<<x[1]<<endl;
//            cout<<x[2]<<endl;
            gettimeofday(&start, NULL);
            if(x[1]=="w") {

            	std::string pkey;
            	ParseTweetJson(x[2], pkey);
            	//cout<<pkey<<endl<<endl;
            	leveldb::Status s = db->Put(woptions, pkey, x[2]);
//            	leveldb::Status s = db->Put(woptions,  x[2]);
            	gettimeofday(&end, NULL);
                durationW+= ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
				w++;
            } else if(x[1]=="rs") {

            	int isrange = rand() % 2;
            	//if(!isrange)
            	{
//            		leveldb::DB::iostat.clear();
            		roptions.type = ReadType::SRead;

            		gettimeofday(&start, NULL);
					leveldb::Status s = db->Get(roptions, key , &svalues, topk);
					gettimeofday(&end, NULL);
//					printiostat(iostat, 1);
					//iostat.print();
					//lookup_iostat.update(iostat);

					durationRS+= ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
					int size = svalues.size();
					rscount+=size;
					if(svalues.size()>0) {
						svalues.clear();
					}
					rs++;
            	}
            //	else
            	{
            		x[3] = x[3].substr(0, x[3].length()-1);
            		leveldb::Slice ekey =x[3];
            		roptions.type = ReadType::SRRead;
//            		leveldb::DB::iostat.clear();
            		gettimeofday(&start, NULL);

            		leveldb::Status s = db->RangeLookUp(roptions, x[2] , x[3] , &svalues, topk);

					gettimeofday(&end, NULL);
					//range_lookup_iostat.update(leveldb::iostat);


					durationRSRange+= ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
					int size = svalues.size();
					rsrangecount+=size;
					if(svalues.size()>0) {
						svalues.clear();
					}

					//sleep(2);
					rsrange++;
            	}
            	{

//            		leveldb::Slice ekey = x[3];
//            		gettimeofday(&start, NULL);
//            		leveldb::Status s = db->RangeLookUp(roptions, x[2] ,x[2] , &svalues, topk);
//					gettimeofday(&end, NULL);
//					durationRST+= ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
//					int size = svalues.size();
//					rstcount+=size;
//					if(svalues.size()>0) {
//						svalues.clear();
//					}
					//rst++;
            	}
                //ofile1<<rs<<", "<<size<<endl;
            } else if(x[1]=="rp") {
                std::string pvalue;
                roptions.type = ReadType::PRead;
                leveldb::Status s = db->Get(roptions, key , &pvalue);
                gettimeofday(&end, NULL);
                durationRP+= ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
                //get_iostat.update(leveldb::iostat);
                rp++;
            }
        }
		if (i%LOG_POINT == 0)
		{
			if(i/LOG_POINT==20)
				break;

			if(i/LOG_POINT==1)
			{
				ofile << "No of Op (Millions)" <<"," << "Time Per Op." <<"," << "Time Per Write" <<"," << "Time Per Read" <<"," << "Time Per Lookup" <<"," << "Results Per Lookup" <<"," << "Time Per RangeLookup" <<"," << "Results Per RangeLookup" <<endl;

				cout << "No of Op (Millions)"  <<"," << "Time Per Op."  <<"," << "Time Per Write" <<"," << "Time Per Read" <<"," << "Time Per Lookup" <<"," << "Results Per Lookup" <<"," << "Time Per RangeLookup" <<"," << "Results Per RangeLookup" <<endl;

				sr_iostat.print(0, ofile_iostat, "");

				//sr_range_iostat.print(rsrange);

				//pr_iostat.print(rp);

				//w_iostat.print(w);
			}
			cout<< fixed;
			cout.precision(3);
			ofile << i/LOG_POINT <<"," << (durationW+durationRP+durationRS+durationRSRange)/i <<"," << durationW/w  <<"," << durationRP/rp <<"," << durationRS/rs <<"," << rscount/rs <<"," << durationRSRange/rsrange <<"," << rsrangecount/rsrange <<endl;
			cout << i/LOG_POINT <<",\t" << (durationW+durationRP+durationRS+durationRSRange)/i <<",\t" << durationW/w  <<",\t" << durationRP/rp <<",\t" << durationRS/rs <<",\t" << rscount/rs <<",\t" << durationRSRange/rsrange <<",\t" << rsrangecount/rsrange <<endl;

			cout<< "\nLookup IOStat\n\n";
			//printiostat(lookup_iostat, rs);
			sr_iostat.print(rs, ofile_iostat, "Lookup");
			sr_iostat.print(rs);

			//cout<< "\nRange Lookup IOStat\n\n";
			sr_range_iostat.print(rsrange, ofile_iostat, "RangeLookup");
			//printiostat(range_lookup_iostat, rsrange);

			//cout<< "\nGET IOStat\n\n";
			pr_iostat.print(rp, ofile_iostat,"Get");
			//printiostat(get_iostat, rp);

			//cout<< "\nWrite IOStat\n\n";
			w_iostat.print(w, ofile_iostat, "Put");



		}
    }

	ofile << i/LOG_POINT <<"," << (durationW+durationRP+durationRS+durationRSRange)/i <<"," << durationW/w  <<"," << durationRP/rp <<"," << durationRS/rs <<"," << rscount/rs <<"," << durationRSRange/rsrange <<"," << rsrangecount/rsrange <<endl;
	cout << i/LOG_POINT <<",\t" << (durationW+durationRP+durationRS+durationRSRange)/i <<",\t" << durationW/w  <<",\t" << durationRP/rp <<",\t" << durationRS/rs <<",\t" << rscount/rs <<",\t" << durationRSRange/rsrange <<",\t" << rsrangecount/rsrange <<endl;

	cout<< "\nLookup IOStat\n\n";
	//printiostat(lookup_iostat, rs);
	sr_iostat.print(rs, ofile_iostat, "Lookup");
	sr_iostat.print(rs);

	//cout<< "\nRange Lookup IOStat\n\n";
	sr_range_iostat.print(rsrange, ofile_iostat, "RangeLookup");
	//printiostat(range_lookup_iostat, rsrange);

	//cout<< "\nGET IOStat\n\n";
	pr_iostat.print(rp, ofile_iostat,"Get");
	//printiostat(get_iostat, rp);

	//cout<< "\nWrite IOStat\n\n";
	w_iostat.print(w, ofile_iostat, "Put");


	delete db;
    delete options.filter_policy;
    ofile.close();
    ofile_iostat.close();

}
void PerformStaticWorkload()
{
		leveldb::DB *db;
	    leveldb::Options options;

	    w_iostat.clear();
	    pr_iostat.clear();
	    sr_iostat.clear();
	    sr_range_iostat.clear();

	    ofstream ofile_iostat(iostat_file.c_str(),  std::ofstream::out | std::ofstream::app );

		int bloomfilter = 100;

	    options.filter_policy = leveldb::NewBloomFilterPolicy(bloomfilter);
	    options.max_open_files = 2000;
	    options.block_cache = leveldb::NewLRUCache(200 * 1048576);  // 100MB cache

	    options.PrimaryAtt = PrimaryAtt;
	    options.secondaryAtt = SecondaryAtt;
	    options.create_if_missing = true;
	    if(isintervaltree)
	    	options.IntervalTreeFileName = "./interval_tree";
	    else
	    	options.IntervalTreeFileName ="";

	    leveldb::Status status = leveldb::DB::Open(options, database, &db);
	    assert(status.ok());

	    string line;
	    uint64_t i=0;
	    rapidjson::Document d;

	    leveldb::ReadOptions roptions;
	    //roptions.iostat.clear();

	    uint64_t w=0, rs=0, rp=0;
	    double rscount =0;
	    double durationW=0,durationRS=0,durationRP=0;

	    struct timeval start, end;

		ofstream ofile(result_file.c_str(),  std::ofstream::out | std::ofstream::app );
		ifstream ifile(static_dataset.c_str());
		if (!ifile) { cerr << "Can't open input file " << endl; return; }


		ofile << "Op Type"  <<"," << "No of Op (Millions)" <<"," << "Time Per Op." << "," << "Results Per Op" <<endl;

		w_iostat.print(0, ofile_iostat, "");

		//write all records

		std::string tweet;
		while(getline(ifile, tweet)) {
			i++;
			leveldb::WriteOptions woptions;

			std::string pkey;
			ParseTweetJson(tweet, pkey);

			gettimeofday(&start, NULL);
			leveldb::Status s = db->Put(woptions, pkey, tweet);
			gettimeofday(&end, NULL);

			durationW+= ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
			w++;


			if (w%LOG_POINT == 0)
			{
				ofile<< fixed;
				ofile.precision(3);
				ofile << "Put,"<< w/LOG_POINT <<"," << durationW/w <<",0"<<endl;

				cout << "Put,"<< w/LOG_POINT <<"," << durationW/w <<",0"<<endl;
				w_iostat.print(w, ofile_iostat, "Put");

				if(w>=MAX_WRITE)
					break;

				usleep(1000);
			}


		}
		ifile.close();


		//End of All writes, Read on Primary Begins
		std::string pread;
		ifstream ifile_pr(static_pr.c_str());
		if (!ifile_pr) { cerr << "Can't open query file " << endl; return; }
		vector<leveldb::SKeyReturnVal> svalues;
		i=0;
		while(getline(ifile_pr, pread)) {
			i++;
			std::string pvalue;
			roptions.type = ReadType::PRead;
			gettimeofday(&start, NULL);
			leveldb::Status s = db->Get(roptions, pread , &pvalue);
			gettimeofday(&end, NULL);
			durationRP+= ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
			//get_iostat.update(leveldb::iostat);
			int found =0 ;
			if(s.IsNotFound()==false)
				found =1;
			rp++;


			if (rp%READ_LOG_POINT == 0)
			{
				ofile<< fixed;
				ofile.precision(3);
				ofile << "Get,"<< rp/READ_LOG_POINT <<"," << durationRP/rp <<","<<found<<endl;

				cout << "Get,"<< rp/READ_LOG_POINT <<"," << durationRP/rp <<","<<found<<endl;
				pr_iostat.print(rp, ofile_iostat, "Get");

				if(rp>=MAX_QUERY)
					break;

			}



		}

		ifile_pr.close();


		//End of All Reads, Read on Secondary Lookup Begins
		std::string query;
		//int selectivity=0;

		ifstream ifile_q(static_query.c_str());
		if (!ifile_q) { cerr << "Can't open query file " << endl; return; }

		for(int t=0; t<topksetlen; t++)
		{
			//cout<<"\nInsert topk (insert 0 to end): "; cin>>topk;
			//cout<<"Insert selectivity, (0-1), 0 means Point Lookup, 1 means range "; cin>>selectivity;

			topk = topkset[t];

			if(topk ==0)
				break;

//			double rsrange[5] = {0};
//			double rsrangecount[5] = {0};
//			double durationRSRange[5] = {0};

			for(int s = 0 ; s < numberofselectivity; s++)
			{

				vector<leveldb::SKeyReturnVal> svalues;

				durationRS=0, rs=0, rscount =0;
				sr_iostat.clear();
				sr_range_iostat.clear();

				ifile_q.seekg(0, ios::beg);

				while(getline(ifile_q, query)) {


					std::vector<std::string> x = split(query, '\t');

					if(x.size()==3) {


						int sel = std::atoi(x[0].c_str());

						if(s!=sel)
							continue;

						if(sel==0)
						{

							roptions.type = ReadType::SRead;
							gettimeofday(&start, NULL);
							leveldb::Status s = db->Get(roptions, x[1] , &svalues, topk);
							gettimeofday(&end, NULL);


						}
						else
						{
							roptions.type = ReadType::SRRead;

							gettimeofday(&start, NULL);
							leveldb::Status s = db->RangeLookUp(roptions, x[1] , x[2] , &svalues, topk);
							gettimeofday(&end, NULL);

						}

						durationRS+= ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
						int size = svalues.size();
						rscount+=size;
						if(svalues.size()>0) {
							svalues.clear();
						}
						rs++;

					}

					if(rs%READ_LOG_POINT == 0)
					{
						ofile<< fixed;
						ofile.precision(3);
						if(s == 0)
						{
							string optype =  "Lookup-k="+to_string(topk);
							ofile << optype << ","<< rs/READ_LOG_POINT <<"," << durationRS/rs <<","<<rscount/rs<< endl;
						    cout<<  optype<< ","<< rs/READ_LOG_POINT <<"," << durationRS/rs <<","<<rscount/rs<< endl;
							sr_iostat.print(rs, ofile_iostat, optype.c_str());
						}
						else
						{
							string optype = "RangeLookup-k="+to_string(topk)+"sel="+to_string(s);
							ofile << optype << ","<< rs/READ_LOG_POINT <<"," << durationRS/rs <<","<<rscount/rs<< endl;
							cout << optype << ","<< rs/READ_LOG_POINT <<"," << durationRS/rs <<","<<rscount/rs<< endl;
							sr_range_iostat.print(rs, ofile_iostat, optype.c_str());
						}

						if(rs>=MAX_QUERY)
							break;

					}

				}

				ifile_q.clear();

			}



		}

		delete db;
		delete options.block_cache;
		delete options.filter_policy;
		ofile.close();
		ofile_iostat.close();
		ifile_q.close();


}


void TestPerformStaticWorkload()
{
		leveldb::DB *db;
	    leveldb::Options options;

//	    w_iostat.clear();
//	    pr_iostat.clear();
//	    sr_iostat.clear();
//	    sr_range_iostat.clear();
//
//	    ofstream ofile_iostat(iostat_file.c_str(),  std::ofstream::out | std::ofstream::app );

		int bloomfilter = 100;

	    options.filter_policy = leveldb::NewBloomFilterPolicy(bloomfilter);
	    options.max_open_files = 2000;
	    options.block_cache = leveldb::NewLRUCache(200 * 1048576);  // 100MB cache

	    options.PrimaryAtt = PrimaryAtt;
	    options.secondaryAtt = SecondaryAtt;
	    options.create_if_missing = true;
	    if(isintervaltree)
	    	options.IntervalTreeFileName = "./interval_tree";
	    else
	    	options.IntervalTreeFileName ="";

	    leveldb::Status status = leveldb::DB::Open(options, database, &db);
	    assert(status.ok());

	    string line;
	    uint64_t i=0;
	    rapidjson::Document d;

	    leveldb::ReadOptions roptions;
	    //roptions.iostat.clear();

	    uint64_t w=0, rs=0, rp=0;
	    double rscount =0;
	    double durationW=0,durationRS=0,durationRP=0;

	    struct timeval start, end;

		ofstream ofile(result_file.c_str(),  std::ofstream::out | std::ofstream::app );
		ifstream ifile(static_dataset.c_str());
		if (!ifile) { cerr << "Can't open input file " << endl; return; }


		ofile << "Op Type"  <<"," << "No of Op (Millions)" <<"," << "Time Per Op." << "," << "Results Per Op" <<endl;

		//w_iostat.print(0, ofile_iostat, "");

		//write all records

		std::string tweet;
		while(getline(ifile, tweet)) {
			i++;
			leveldb::WriteOptions woptions;

			std::string pkey;
			ParseTweetJson(tweet, pkey);

			gettimeofday(&start, NULL);
			leveldb::Status s = db->Put(woptions, pkey, tweet);
			gettimeofday(&end, NULL);

			durationW+= ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
			w++;


			if (w%LOG_POINT == 0)
			{
				ofile<< fixed;
				ofile.precision(3);
				ofile << "Put,"<< w/LOG_POINT <<"," << durationW/w <<",0"<<endl;

				cout << "Put,"<< w/LOG_POINT <<"," << durationW/w <<",0"<<endl;
				//w_iostat.print(w, ofile_iostat, "Put");

				if(w>=MAX_WRITE)
					break;

				//usleep(1000);
			}


		}
		//ifile.close();


		//End of All writes, Read on Primary Begins
		std::string pread;
		ifstream ifile_pr(static_pr.c_str());
		if (!ifile_pr) { cerr << "Can't open query file " << endl; return; }
		vector<leveldb::SKeyReturnVal> svalues;
		i=0;
		while(getline(ifile_pr, pread)) {
			i++;
			std::string pvalue;
			roptions.type = ReadType::PRead;
			gettimeofday(&start, NULL);
			leveldb::Status s = db->Get(roptions, pread , &pvalue);
			gettimeofday(&end, NULL);
			durationRP+= ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
			//get_iostat.update(leveldb::iostat);
			int found =0 ;
			if(s.IsNotFound()==false)
				found =1;
			rp++;


			if (rp%READ_LOG_POINT == 0)
			{
				ofile<< fixed;
				ofile.precision(3);
				ofile << "Get,"<< rp/READ_LOG_POINT <<"," << durationRP/rp <<","<<found<<endl;

				cout << "Get,"<< rp/READ_LOG_POINT <<"," << durationRP/rp <<","<<found<<endl;
				//pr_iostat.print(rp, ofile_iostat, "Get");

				if(rp>=MAX_QUERY)
					break;

			}



		}

		ifile_pr.close();


		delete db;
		delete options.block_cache;
		delete options.filter_policy;
		ofile.close();




}


int main(int argc, char** argv) {


	if(argc == 4)
	{
		benchmark_file = argv[1];
		database = argv[2];
		result_file = argv[3];
		//cout<< argv[3] ;

		testWithBenchMark();

	}
	else
	{
		TestPerformStaticWorkload();
		//testWithBenchMark();
		//cout<<"Please Put arguments in order: \n arg1=benchmarkpath arg2=dbpath arg3=resultpath\n";
	}

	return 0;
}

