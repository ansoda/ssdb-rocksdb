/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "ssdb_impl.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/table.h"

#include "iterator.h"
#include "t_kv.h"
#include "t_hash.h"
#include "t_zset.h"
#include "t_queue.h"

SSDBImpl::SSDBImpl(){
	ldb = NULL;
	binlogs = NULL;
}

SSDBImpl::~SSDBImpl(){
	if(binlogs){
		delete binlogs;
	}
	if(ldb){
		delete ldb;
	}
}

SSDB* SSDB::open(const Options &opt, const std::string &dir){
	SSDBImpl *ssdb = new SSDBImpl();
	ssdb->options.create_if_missing = true;
	ssdb->options.max_open_files = opt.max_open_files;

	rocksdb::BlockBasedTableOptions tableOptions;
	tableOptions.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
	tableOptions.block_cache = rocksdb::NewLRUCache(opt.cache_size * 1048576);
	tableOptions.block_size = opt.block_size * 1024;
	ssdb->options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tableOptions));

	ssdb->options.target_file_size_base = 32 * 1024 * 1024;
	ssdb->options.write_buffer_size = opt.write_buffer_size * 1024 * 1024;
	/*ssdb->options.compaction_speed = opt.compaction_speed;*/
	if(opt.compression == "yes"){
		ssdb->options.compression = rocksdb::kSnappyCompression;
	}else{
		ssdb->options.compression = rocksdb::kNoCompression;
	}

	auto env = rocksdb::Env::Default();
	env->SetBackgroundThreads(8, rocksdb::Env::LOW);
	env->SetBackgroundThreads(8, rocksdb::Env::HIGH);
	ssdb->options.env = env;
	ssdb->options.max_background_compactions = 8;
	ssdb->options.max_background_flushes = 8;

	ssdb->options.max_log_file_size = 0;
	ssdb->options.keep_log_file_num = 5;

	rocksdb::Status status;
	status = rocksdb::DB::Open(ssdb->options, dir, &ssdb->ldb);
	if(!status.ok()){
		log_error("open db failed: %s", status.ToString().c_str());
		goto err;
	}
	ssdb->binlogs = new BinlogQueue(ssdb->ldb, opt.binlog);

	return ssdb;
err:
	if(ssdb){
		delete ssdb;
	}
	return NULL;
}

int SSDBImpl::flushdb(){
	Transaction trans(binlogs);
	int ret = 0;
	bool stop = false;
	while(!stop){
		rocksdb::Iterator *it;
		rocksdb::ReadOptions iterate_options;
		iterate_options.fill_cache = false;
		rocksdb::WriteOptions write_opts;

		it = ldb->NewIterator(iterate_options);
		it->SeekToFirst();
		for(int i=0; i<10000; i++){
			if(!it->Valid()){
				stop = true;
				break;
			}
			//log_debug("%s", hexmem(it->key().data(), it->key().size()).c_str());
			rocksdb::Status s = ldb->Delete(write_opts, it->key());
			if(!s.ok()){
				log_error("del error: %s", s.ToString().c_str());
				stop = true;
				ret = -1;
				break;
			}
			it->Next();
		}
		delete it;
	}
	binlogs->flush();
	return ret;
}

Iterator* SSDBImpl::iterator(const std::string &start, const std::string &end, uint64_t limit){
	rocksdb::Iterator *it;
	rocksdb::ReadOptions iterate_options;
	iterate_options.fill_cache = false;
	it = ldb->NewIterator(iterate_options);
	it->Seek(start);
	if(it->Valid() && it->key() == start){
		it->Next();
	}
	return new Iterator(it, end, limit);
}

Iterator* SSDBImpl::rev_iterator(const std::string &start, const std::string &end, uint64_t limit){
	rocksdb::Iterator *it;
	rocksdb::ReadOptions iterate_options;
	iterate_options.fill_cache = false;
	it = ldb->NewIterator(iterate_options);
	it->Seek(start);
	if(!it->Valid()){
		it->SeekToLast();
	}else{
		it->Prev();
	}
	return new Iterator(it, end, limit, Iterator::BACKWARD);
}

/* raw operates */

int SSDBImpl::raw_set(const Bytes &key, const Bytes &val){
	rocksdb::WriteOptions write_opts;
	rocksdb::Status s = ldb->Put(write_opts, slice(key), slice(val));
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::raw_del(const Bytes &key){
	rocksdb::WriteOptions write_opts;
	rocksdb::Status s = ldb->Delete(write_opts, slice(key));
	if(!s.ok()){
		log_error("del error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::raw_get(const Bytes &key, std::string *val){
	rocksdb::ReadOptions opts;
	opts.fill_cache = false;
	rocksdb::Status s = ldb->Get(opts, slice(key), val);
	if(s.IsNotFound()){
		return 0;
	}
	if(!s.ok()){
		log_error("get error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

uint64_t SSDBImpl::size(){
	std::string s = "A";
	std::string e(1, 'z' + 1);
	rocksdb::Range ranges[1];
	ranges[0] = rocksdb::Range(s, e);
	uint64_t sizes[1];
	ldb->GetApproximateSizes(ranges, 1, sizes);
	return sizes[0];
}

std::vector<std::string> SSDBImpl::info(){
	//  "rocksdb.num-files-at-level<N>" - return the number of files at level <N>,
	//     where <N> is an ASCII representation of a level number (e.g. "0").
	//  "rocksdb.stats" - returns a multi-line string that describes statistics
	//     about the internal operation of the DB.
	//  "rocksdb.sstables" - returns a multi-line string that describes all
	//     of the sstables that make up the db contents.
	std::vector<std::string> info;
	std::vector<std::string> keys;
	/*
	for(int i=0; i<7; i++){
		char buf[128];
		snprintf(buf, sizeof(buf), "rocksdb.num-files-at-level%d", i);
		keys.push_back(buf);
	}
	*/
	keys.push_back("rocksdb.stats");
	//keys.push_back("rocksdb.sstables");

	for(size_t i=0; i<keys.size(); i++){
		std::string key = keys[i];
		std::string val;
		if(ldb->GetProperty(key, &val)){
			info.push_back(key);
			info.push_back(val);
		}
	}

	return info;
}

void SSDBImpl::compact(){
	ldb->CompactRange(NULL, NULL);
}

int SSDBImpl::key_range(std::vector<std::string> *keys){
	int ret = 0;
	std::string kstart, kend;
	std::string hstart, hend;
	std::string zstart, zend;
	std::string qstart, qend;
	
	Iterator *it;
	
	it = this->iterator(encode_kv_key(""), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::KV){
			std::string n;
			if(decode_kv_key(ks, &n) == -1){
				ret = -1;
			}else{
				kstart = n;
			}
		}
	}
	delete it;
	
	it = this->rev_iterator(encode_kv_key("\xff"), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::KV){
			std::string n;
			if(decode_kv_key(ks, &n) == -1){
				ret = -1;
			}else{
				kend = n;
			}
		}
	}
	delete it;
	
	it = this->iterator(encode_hsize_key(""), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::HSIZE){
			std::string n;
			if(decode_hsize_key(ks, &n) == -1){
				ret = -1;
			}else{
				hstart = n;
			}
		}
	}
	delete it;
	
	it = this->rev_iterator(encode_hsize_key("\xff"), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::HSIZE){
			std::string n;
			if(decode_hsize_key(ks, &n) == -1){
				ret = -1;
			}else{
				hend = n;
			}
		}
	}
	delete it;
	
	it = this->iterator(encode_zsize_key(""), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::ZSIZE){
			std::string n;
			if(decode_hsize_key(ks, &n) == -1){
				ret = -1;
			}else{
				zstart = n;
			}
		}
	}
	delete it;
	
	it = this->rev_iterator(encode_zsize_key("\xff"), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::ZSIZE){
			std::string n;
			if(decode_hsize_key(ks, &n) == -1){
				ret = -1;
			}else{
				zend = n;
			}
		}
	}
	delete it;
	
	it = this->iterator(encode_qsize_key(""), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::QSIZE){
			std::string n;
			if(decode_qsize_key(ks, &n) == -1){
				ret = -1;
			}else{
				qstart = n;
			}
		}
	}
	delete it;
	
	it = this->rev_iterator(encode_qsize_key("\xff"), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::QSIZE){
			std::string n;
			if(decode_qsize_key(ks, &n) == -1){
				ret = -1;
			}else{
				qend = n;
			}
		}
	}
	delete it;

	keys->push_back(kstart);
	keys->push_back(kend);
	keys->push_back(hstart);
	keys->push_back(hend);
	keys->push_back(zstart);
	keys->push_back(zend);
	keys->push_back(qstart);
	keys->push_back(qend);
	
	return ret;
}
