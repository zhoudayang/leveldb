// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>
#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

struct FileMetaData {
  int refs;  // reference count
  int allowed_seeks;          // Seeks allowed until compaction compact之前允许的seek次数
  uint64_t number;            // number of file
  uint64_t file_size;         // File size in bytes
  InternalKey smallest;       // Smallest internal key served by table / sstable文件的最小key
  InternalKey largest;        // Largest internal key served by table / sstable文件的最大key

  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) { }
};

class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() { }

  void Clear();

  // set comparator name
  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  // set log number
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  // set prev log number
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  // set next file number
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  // set last sequence
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  // set compact pointer
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  /// 添加由number指定的文件，要求version没有被saved，并且smallest和largest是file中最小
  /// 以及最大的key
  void AddFile(int level, uint64_t file,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest) {
    FileMetaData f;
    /// refs(0) allowed_seeks(1 << 30)  ... substring from constructor function
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  // Delete the specified "file" from the specified "level".
  /// 记录删除了file指定的文件
  void DeleteFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  /// 删除文件集合 <层次，文件编号>
  /// 这个容器采用的是std::set
  typedef std::set< std::pair<int, uint64_t> > DeletedFileSet;

  std::string comparator_; // 比较器的名称
  uint64_t log_number_; // 日志文件编号
  uint64_t prev_log_number_; // 上一个日志文件编号
  uint64_t next_file_number_; // 下一个日志文件编号
  SequenceNumber last_sequence_; // 上一个序列号
  bool has_comparator_; // 是否有比较器
  bool has_log_number_; // 是否有log number
  bool has_prev_log_number_; // 是否有上一个log number
  bool has_next_file_number_; // 是否有下一个file number
  bool has_last_sequence_; // 是否有上一个sequence
  // 压缩点<层次，InternalKey键>
  std::vector< std::pair<int, InternalKey> > compact_pointers_;
  // 删除文件集合
  DeletedFileSet deleted_files_;
  // 新添加的文件集合
  std::vector< std::pair<int, FileMetaData> > new_files_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
