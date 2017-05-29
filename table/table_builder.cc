// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"
#include <sstream>
#include <fstream>
#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "rapidjson/document.h"
#include "db/dbformat.h"

#define SSTR( x ) dynamic_cast< std::ostringstream & >( \
        ( std::ostringstream() << std::dec << x ) ).str()
namespace leveldb {

struct TableBuilder::Rep {
  Options options;
  Options index_block_options;
  Options interval_block_options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  BlockBuilder interval_block;
  std::string last_key;
  int64_t num_entries;
  bool closed;          // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;
  FilterBlockBuilder* secondary_filter_block;
  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;

  //Add Min Max and MaxTimestamp secondary attribute value of a Block for Interval Tree
  std::string maxSecValue;
  std::string minSecValue;
  uint64_t maxSecSeqNumber;

  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
		interval_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
		interval_block(&interval_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == NULL ? NULL
                     : new FilterBlockBuilder(opt.filter_policy)),
        secondary_filter_block(opt.filter_policy == NULL ? NULL
                     : new FilterBlockBuilder(opt.filter_policy)),
        maxSecValue(""),
        minSecValue(""),
        maxSecSeqNumber(0),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
    interval_block_options.block_restart_interval = 1;
  }
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file, TwoDITwTopK* intervalTree,
		 uint64_t fileNumber, std::string* minsec, std::string* maxsec)

    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != NULL) {
    rep_->filter_block->StartBlock(0);
  }
  if (rep_->secondary_filter_block != NULL) {
    rep_->secondary_filter_block->StartBlock(0);
  }

  intervalTree_ = intervalTree;
  this->fileNumber =  fileNumber;
  this->smallest_sec = minsec;
  this->largest_sec = maxsec;
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_->secondary_filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  rep_->interval_block_options.block_restart_interval = 1;
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {

  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  if (r->pending_index_entry) {
	 // cout<<"last key: "<<r->last_key<<endl;

	  if(!r->options.IntervalTreeFileName.empty())
		  intervalTree_->insertInterval(SSTR(fileNumber)+"+"+r->last_key.substr(0, r->last_key.size() - 8 ), r->minSecValue , r->maxSecValue, r->maxSecSeqNumber);
	  else
	  {
		  //string s = rep_->minSecValue + "," + rep_->maxSecValue+ "," + std::to_string(rep_->maxSecSeqNumber);
  		Slice value(r->maxSecValue);//"1,1,1";
  		Slice inKey(r->minSecValue);

  		r->interval_block.Add(inKey, value);

		  //r->interval_block.Add(std::to_string(count++), value);
	  }
	  //cout<<SSTR(fileNumber)+"+"+r->last_key.substr(0, r->last_key.size() - 8 ) <<" ,min: "<< rep_->minSecValue <<" ,max: "<< rep_->maxSecValue<<" ,seqno: "<< rep_->maxSecSeqNumber<<std::endl;

	if(smallest_sec->empty())
	{
		smallest_sec->assign(r->minSecValue);
		largest_sec->assign(r->maxSecValue);
	}
	else {
		if(this->smallest_sec->compare(r->minSecValue)>0)
		{
			this->smallest_sec->assign(r->minSecValue);
		}
		else if(this->largest_sec->compare(rep_->maxSecValue)<0)
		{
			this->largest_sec->assign(r->maxSecValue);
		}
	}

	r->minSecValue.clear();
	r->maxSecValue.clear();
	r->maxSecSeqNumber= 0;

    assert(r->data_block.empty());
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
    // Insert in the interval tree the new blocks info
    //cout<<"last key 2: "<<r->last_key<<endl;

  }

  if (r->filter_block != NULL) {
      //outputFile<<key.ToString()<<std::endl;
    r->filter_block->AddKey(key);
  }

  if (r->secondary_filter_block != NULL&&!r->options.secondaryAtt.empty()) {

  rapidjson::Document docToParse;

  docToParse.Parse<0>(value.ToString().c_str());

  ///
  const char* sKeyAtt = r->options.secondaryAtt.c_str();

  if(!docToParse.IsObject()||!docToParse.HasMember(sKeyAtt)||docToParse[sKeyAtt].IsNull())
      return;

  std::ostringstream sKey;
  if(docToParse[sKeyAtt].IsNumber())
  {
        if(docToParse[sKeyAtt].IsUint64())
        {
            unsigned long long int tid = docToParse[sKeyAtt].GetUint64();
            sKey<<tid;

        }
        else if (docToParse[sKeyAtt].IsInt64())
        {
            long long int tid = docToParse[sKeyAtt].GetInt64();
            sKey<<tid;
        }
        else if (docToParse[sKeyAtt].IsDouble())
        {
            double tid = docToParse[sKeyAtt].GetDouble();
            sKey<<tid;
        }

        else if (docToParse[sKeyAtt].IsUint())
        {
            unsigned int tid = docToParse[sKeyAtt].GetUint();
            sKey<<tid;
        }
        else if (docToParse[sKeyAtt].IsInt())
        {
            int tid = docToParse[sKeyAtt].GetInt();
            sKey<<tid;
        }
  }
  else if (docToParse[sKeyAtt].IsString())
  {
        const char* tid = docToParse[sKeyAtt].GetString();
        sKey<<tid;
  }
  else if(docToParse[sKeyAtt].IsBool())
  {
        bool tid = docToParse[sKeyAtt].GetBool();
        sKey<<tid;
  }
  std::string tag = key.ToString().substr(key.size()-8);
  std::string secKeys = sKey.str();

  Slice Key = secKeys+tag;
  //outputFile<<"Sec: "<<Key.ToString()<<std::endl;


   r->secondary_filter_block->AddKey(Key);
   if(r->maxSecValue== "" || r->maxSecValue.compare(secKeys)<0)
   {
       r->maxSecValue.assign(secKeys);
   }

   if(r->minSecValue== "" || r->minSecValue.compare(secKeys)>0)
   {
       r->minSecValue.assign(secKeys);
   }
//
//
//   if(rep_->minSecValue== "" || rep_->minSecValue.compare(secKeys)>0)
//   {
//       rep_->minSecValue = secKeys;
//   }

  /*ParsedInternalKey parsed_key;
  if (!ParseInternalKey(key, &parsed_key)) {
    if(rep_->maxSecSeqNumber < parsed_key.sequence)
    {
        rep_->maxSecSeqNumber = parsed_key.sequence;
    }
  }*/

}

  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);

  const size_t n = key.size();
	if (n >= 8)
	{
		uint64_t num = DecodeFixed64(key.data() + n - 8);
		SequenceNumber seq  = num >> 8;
		if(r->maxSecSeqNumber < seq)
		  {
			  r->maxSecSeqNumber = seq;
		  }
	}

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }
  if (r->filter_block != NULL) {
    r->filter_block->StartBlock(r->offset);
  }
  if(r->secondary_filter_block!=NULL)
  {
    r->secondary_filter_block->StartBlock(r->offset);
  }

}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type,
                                 BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer+1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const {
  return rep_->status;
}

Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, secondary_filter_block_handle, metaindex_block_handle, index_block_handle, interval_block_handle;

  // Write filter block
  if (ok() && r->filter_block != NULL) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }
  // Write Secondary Filer Block
  if (ok() && r->secondary_filter_block != NULL) {
    WriteRawBlock(r->secondary_filter_block->Finish(), kNoCompression,
                  &secondary_filter_block_handle);
  }
  // Write metaindex block
   if (ok()) {
	 BlockBuilder meta_index_block(&r->options);
	 if (r->filter_block != NULL) {
	   // Add mapping from "filter.Name" to location of filter data
	   std::string key = "filter.";
	   key.append(r->options.filter_policy->Name());
	   std::string handle_encoding;
	   filter_block_handle.EncodeTo(&handle_encoding);
	   meta_index_block.Add(key, handle_encoding);
	 }
	 if (r->secondary_filter_block != NULL) {
	   // Add mapping from "filter.Name" to location of filter data
	   std::string key = "secondaryfilter.";
	   key.append(r->options.filter_policy->Name());
	   std::string handle_encoding;
	   secondary_filter_block_handle.EncodeTo(&handle_encoding);
	   meta_index_block.Add(key, handle_encoding);
	 }

	 // TODO(postrelease): Add stats and other meta blocks
	 WriteBlock(&meta_index_block, &metaindex_block_handle);
   }

  if (ok()) {
    if (r->pending_index_entry) {
    	if(!r->options.IntervalTreeFileName.empty())
    	{
    		intervalTree_->insertInterval(SSTR(fileNumber) +"+"+ r->last_key.substr(0, r->last_key.size() - 8 ), r->minSecValue , r->maxSecValue, r->maxSecSeqNumber);
    		intervalTree_->sync();
    	}
    	else
    	{
    		//string s = rep_->minSecValue + "," + rep_->maxSecValue+ "," + std::to_string(rep_->maxSecSeqNumber);
    		//cout<<s;
    		Slice value(r->maxSecValue);//"1,1,1";
    		Slice inKey(r->minSecValue);

    		r->interval_block.Add(inKey, value);
    	}

    	if(smallest_sec->empty())
		{
			smallest_sec->assign(r->minSecValue);
			largest_sec->assign(r->maxSecValue);
		}
		else {
			if(this->smallest_sec->compare(r->minSecValue)>0)
			{
				this->smallest_sec->assign(r->minSecValue);
			}
			else if(this->largest_sec->compare(rep_->maxSecValue)<0)
			{
				this->largest_sec->assign(r->maxSecValue);
			}
		}

	  //outputFile<<SSTR(fileNumber)+"+"+ r->last_key.substr(0, r->last_key.size() - 8 )<<" "<< rep_->minSecValue <<" "<< rep_->maxSecValue<<" "<< rep_->maxSecSeqNumber<<std::endl;
		r->minSecValue.clear();
		r->maxSecValue.clear();
		r->maxSecSeqNumber= 0;

      //outputFile<<r->last_key<<std::endl;
      //r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;

//      if(rand()%100==0)
//    	  intervalTree_->storagePrint();

    }

    // Write index block

	  if(r->options.IntervalTreeFileName.empty())
	  {
		WriteBlock(&r->interval_block, &interval_block_handle);
	  }
	  WriteBlock(&r->index_block, &index_block_handle);

  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    bool isinterval = r->options.IntervalTreeFileName.empty();
    if(isinterval)
	  {
    	footer.set_interval_handle(interval_block_handle);
	  }
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding, isinterval);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }

  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const {
  return rep_->num_entries;
}

uint64_t TableBuilder::FileSize() const {
  return rep_->offset;
}

}  // namespace leveldb
