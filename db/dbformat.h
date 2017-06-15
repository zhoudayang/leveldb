// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DBFORMAT_H_
#define STORAGE_LEVELDB_DB_DBFORMAT_H_

#include <stdio.h>
#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"
#include "util/logging.h"

// leveldb更新某个key的时候不会操控到db中的数据，每次操作
// 都是直接插入一份kv数据，具体的数据合并和清除由后台的compact完成。
// 所以，每次put，db就会新加入一份新的kv数据。即使该key已经存在。
// 而delete等同于put空的value。为了区分真是的kv数据和删除操作的mock数据，使用
// ValueType来标志
namespace leveldb {

// Grouping of constants.  We may want to make some of these
// parameters set via options.
namespace config {
// level的最大值
static const int kNumLevels = 7;

// Level-0 compaction is started when we hit this many files.
// level-0中的sstable数量超过这个阈值，触发compact
static const int kL0_CompactionTrigger = 4;

// Soft limit on number of level-0 files.  We slow down writes at this point.
// level-0中sstable数量超过这个阈值，慢处理此次写
static const int kL0_SlowdownWritesTrigger = 8;

// Maximum number of level-0 files.  We stop writes at this point.
// level-0中sstable数量超过这个阈值，阻塞至compact memtable完成
static const int kL0_StopWritesTrigger = 12;

// Maximum level to which a new compacted memtable is pushed if it
// does not create overlap.  We try to push to level 2 to avoid the
// relatively expensive level 0=>1 compactions and to avoid some
// expensive manifest file operations.  We do not push all the way to
// the largest level since that can generate a lot of wasted disk
// space if the same key space is being repeatedly overwritten.
// memtable dump成的sstable，允许推向的最高level
static const int kMaxMemCompactLevel = 2;

// Approximate gap in bytes between samples of data read during iteration.
static const int kReadBytesPeriod = 1048576;

}  // namespace config

class InternalKey;

// Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
// data structures.

enum ValueType {
  // 数据已经删除
  kTypeDeletion = 0x0,
  // 是有效的value值
  kTypeValue = 0x1
};
// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
static const ValueType kValueTypeForSeek = kTypeValue;

// leveldb中每次更新操作都有一个版本。由sequenceNumber来标志，
// 整个db有一个全局值保存着当前使用到的sequencenumber. SequenceNumber
// 在leveldb中有重要的地位，key的排序，compact以及snapshot都依赖他。
// 存储时，SequenceNumber只占用56bits，ValueType占用8bits,二者共同占用
// 64bits.
typedef uint64_t SequenceNumber;

// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
/// 最大的sequence number
static const SequenceNumber kMaxSequenceNumber =
    ((0x1ull << 56) - 1);

// db 内部操作的key。
// db 内部需要将user key加入元信息(value_type, sequenceNumber)
// 一起做处理
struct ParsedInternalKey {
  Slice user_key; // 用户的key
  SequenceNumber sequence; // sequence number
  ValueType type; // value 的类型

  ParsedInternalKey() { }  // Intentionally left uninitialized (for speed)
  // normal constructor
  ParsedInternalKey(const Slice& u, const SequenceNumber& seq, ValueType t)
      : user_key(u), sequence(seq), type(t) { }
  std::string DebugString() const;
};

// Return the length of the encoding of "key".
/// key = user_key + value_type
inline size_t InternalKeyEncodingLength(const ParsedInternalKey& key) {
  return key.user_key.size() + 8;
}

// Append the serialization of "key" to *result.
extern void AppendInternalKey(std::string* result,
                              const ParsedInternalKey& key);

// Attempt to parse an internal key from "internal_key".  On success,
// stores the parsed data in "*result", and returns true.
//
// On error, returns false, leaves "*result" in an undefined state.
extern bool ParseInternalKey(const Slice& internal_key,
                             ParsedInternalKey* result);

// Returns the user key portion of an internal key.
/// 从internal key之中提取user_key, 跳过最后8bytes
inline Slice ExtractUserKey(const Slice& internal_key) {
  assert(internal_key.size() >= 8);
  return Slice(internal_key.data(), internal_key.size() - 8);
}

/// internal_key的最后8bytes是value_type，提取出来返回
inline ValueType ExtractValueType(const Slice& internal_key) {
  assert(internal_key.size() >= 8);
  const size_t n = internal_key.size();
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  unsigned char c = num & 0xff;
  return static_cast<ValueType>(c);
}

// A comparator for internal keys that uses a specified comparator for
// the user key portion and breaks ties by decreasing sequence number.
// db内部最key排序时使用的比较方法。排序时，首先使用user_comparator比较user-key,
// 如果user-key相同，则比较SequenceNumber, SequenceNumber大的为小。因为
// SequnceNumber在db中的全局递增，所以，对于相同的user-key，最新的更新排在前面，在查找的时候被首先找到。
// InternalKeyComparator中FindShortestSeparator() / FindShortSuccessor() 的实现
// 仅从传入的内部key的参数，解析出user-key, 然后再调用user-comparator的对应接口。
/// user_key小，sequence number更大的排在前面
class InternalKeyComparator : public Comparator {
 private:
  const Comparator* user_comparator_;
 public:
  explicit InternalKeyComparator(const Comparator* c) : user_comparator_(c) { }
  virtual const char* Name() const;
  virtual int Compare(const Slice& a, const Slice& b) const;
  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const;
  virtual void FindShortSuccessor(std::string* key) const;

  const Comparator* user_comparator() const { return user_comparator_; }

  int Compare(const InternalKey& a, const InternalKey& b) const;
};

// Filter policy wrapper that converts from internal keys to user keys
class InternalFilterPolicy : public FilterPolicy {
 private:
  const FilterPolicy* const user_policy_;
 public:
  explicit InternalFilterPolicy(const FilterPolicy* p) : user_policy_(p) { }
  virtual const char* Name() const;
  virtual void CreateFilter(const Slice* keys, int n, std::string* dst) const;
  virtual bool KeyMayMatch(const Slice& key, const Slice& filter) const;
};

// Modules in this directory should keep internal keys wrapped inside
// the following class instead of plain strings so that we do not
// incorrectly use string comparisons instead of an InternalKeyComparator.
class InternalKey {
 private:
  std::string rep_;
 public:
  InternalKey() { }   // Leave rep_ as empty to indicate it is invalid
  InternalKey(const Slice& user_key, SequenceNumber s, ValueType t) {
    /// 将user_key, sequence number, value type 组合起来存入rep_
    AppendInternalKey(&rep_, ParsedInternalKey(user_key, s, t));
  }

  void DecodeFrom(const Slice& s) { rep_.assign(s.data(), s.size()); }

  Slice Encode() const {
    assert(!rep_.empty());
    return rep_;
  }

  Slice user_key() const { return ExtractUserKey(rep_); }

  void SetFrom(const ParsedInternalKey& p) {
    rep_.clear();
    AppendInternalKey(&rep_, p);
  }

  void Clear() { rep_.clear(); }

  std::string DebugString() const;
};

/// compare two internal key, use Compare function
inline int InternalKeyComparator::Compare(
    const InternalKey& a, const InternalKey& b) const {
  return Compare(a.Encode(), b.Encode());
}

/// parse internal key, store in result
inline bool ParseInternalKey(const Slice& internal_key,
                             ParsedInternalKey* result) {
  const size_t n = internal_key.size();
  if (n < 8) return false;
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  unsigned char c = num & 0xff;
  // get sequence number
  result->sequence = num >> 8;
  // get value type
  result->type = static_cast<ValueType>(c);
  // the remain data is user_key
  result->user_key = Slice(internal_key.data(), n - 8);
  /// simple assert
  return (c <= static_cast<unsigned char>(kTypeValue));
}

// A helper class useful for DBImpl::Get()
// db内部为了查找memtable/sstable方便，包装使用的key结构，保存
// 有userkey与SequenceNumber/ValueType dump在内存的数据
// start        kstart       end
// userkey_len  userkey_data sequenceNumber/ValueType
// varint32     userkey_len  uint64
// 对memtable进行lookup时使用[start,end],对sstable进行look up时使用[kstart,end]
class LookupKey {
 public:
  // Initialize *this for looking up user_key at a snapshot with
  // the specified sequence number.
  LookupKey(const Slice& user_key, SequenceNumber sequence);

  ~LookupKey();

  // Return a key suitable for lookup in a MemTable.
  Slice memtable_key() const { return Slice(start_, end_ - start_); }

  // Return an internal key (suitable for passing to an internal iterator)
  Slice internal_key() const { return Slice(kstart_, end_ - kstart_); }

  // Return the user key
  Slice user_key() const { return Slice(kstart_, end_ - kstart_ - 8); }

 private:
  /// lookup key 内部format如下所示
  // We construct a char array of the form:
  //    klength  varint32               <-- start_
  //    userkey  char[klength]          <-- kstart_
  //    tag      uint64
  //                                    <-- end_
  // The array is a suitable MemTable key.
  // The suffix starting with "userkey" can be used as an InternalKey.
  const char* start_;
  const char* kstart_;
  const char* end_;
  char space_[200];      // Avoid allocation for short keys

  // No copying allowed
  LookupKey(const LookupKey&);
  void operator=(const LookupKey&);
};

/// destructor of LoopupKey
inline LookupKey::~LookupKey() {
  if (start_ != space_) delete[] start_;
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DBFORMAT_H_
