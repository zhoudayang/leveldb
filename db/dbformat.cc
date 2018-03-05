// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include "db/dbformat.h"

namespace leveldb
{

// 将sequence number 和 value type 组合成为 uint64_t
static uint64_t PackSequenceAndType(uint64_t seq, ValueType t)
{
  assert(seq <= kMaxSequenceNumber);
  assert(t <= kValueTypeForSeek);
  return (seq << 8) | t;
}

/// 向result之中存入internal key
/// 首先append userkey, 再存入sequence number 以及 type
void AppendInternalKey(std::string *result, const ParsedInternalKey &key)
{
  result->append(key.user_key.data(), key.user_key.size());
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

/// 生成debug 字符串
/// sequence_number : type + user_key
std::string ParsedInternalKey::DebugString() const
{
  char buf[50];
  snprintf(buf, sizeof(buf), "' @ %llu : %d",
           (unsigned long long) sequence,
           int(type));
  std::string result = "'";
  result += EscapeString(user_key.ToString());
  result += buf;
  return result;
}

std::string InternalKey::DebugString() const
{
  std::string result;
  ParsedInternalKey parsed;
  // 如果能够parse成功
  if (ParseInternalKey(rep_, &parsed))
  {
    result = parsed.DebugString();
  }
  else
  {
    result = "(bad)";
    result.append(EscapeString(rep_));
  }
  return result;
}

// InternalKeyComparator 的名字
const char *InternalKeyComparator::Name() const
{
  return "leveldb.InternalKeyComparator";
}

/// 优先按照user key进行升序排序，然后按照sequence number 降序排序
/// node->key key
int InternalKeyComparator::Compare(const Slice &akey, const Slice &bkey) const
{
  // Order by:
  //    increasing user key (according to user-supplied comparator)
  //    decreasing sequence number
  //    decreasing type (though sequence# should be enough to disambiguate)
  int r = user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
  if (r == 0)
  {
    const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
    const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
    if (anum > bnum)
    {
      r = -1;
    }
    else if (anum < bnum)
    {
      r = +1;
    }
  }
  return r;
}

void InternalKeyComparator::FindShortestSeparator(
    std::string *start,
    const Slice &limit) const
{
  // Attempt to shorten the user portion of the key
  Slice user_start = ExtractUserKey(*start);
  Slice user_limit = ExtractUserKey(limit);
  std::string tmp(user_start.data(), user_start.size());
  /// 找到比user_start大，比user_limit小的key
  user_comparator_->FindShortestSeparator(&tmp, user_limit);
  if (tmp.size() < user_start.size() &&
      user_comparator_->Compare(user_start, tmp) < 0)
  {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*start, tmp) < 0);
    assert(this->Compare(tmp, limit) < 0);
    start->swap(tmp);
  }
}

void InternalKeyComparator::FindShortSuccessor(std::string *key) const
{
  Slice user_key = ExtractUserKey(*key);
  std::string tmp(user_key.data(), user_key.size());
  /// 找到比key大的key, 存于tmp中
  user_comparator_->FindShortSuccessor(&tmp);
  if (tmp.size() < user_key.size() &&
      user_comparator_->Compare(user_key, tmp) < 0)
  {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*key, tmp) < 0);
    key->swap(tmp);
  }
}

const char *InternalFilterPolicy::Name() const
{
  return user_policy_->Name();
}

/// n 输入的key的个数
void InternalFilterPolicy::CreateFilter(const Slice *keys, int n,
                                        std::string *dst) const
{
  // We rely on the fact that the code in table.cc does not mind us
  // adjusting keys[].
  Slice *mkey = const_cast<Slice *>(keys);
  for (int i = 0; i < n; i++)
  {
    mkey[i] = ExtractUserKey(keys[i]);
    // TODO(sanjay): Suppress dups?
  }
  // create filter
  user_policy_->CreateFilter(keys, n, dst);
}

bool InternalFilterPolicy::KeyMayMatch(const Slice &key, const Slice &f) const
{
  return user_policy_->KeyMayMatch(ExtractUserKey(key), f);
}

LookupKey::LookupKey(const Slice &user_key, SequenceNumber s)
{
  size_t usize = user_key.size();
  /// varint32 + sequence number + value type
  size_t needed = usize + 13;  // A conservative estimate
  char *dst;
  // 空间足够了
  if (needed <= sizeof(space_))
  {
    dst = space_;
  }
  else
  {
    // new 分配空间
    dst = new char[needed];
  }
  start_ = dst;
  // encode length
  dst = EncodeVarint32(dst, usize + 8);
  // start_ 指向user key的开始处
  kstart_ = dst;
  // encode user key
  memcpy(dst, user_key.data(), usize);
  dst += usize;
  // encode sequence number and type
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
  dst += 8;
  // 指向数据结束处
  end_ = dst;
}

}  // namespace leveldb
