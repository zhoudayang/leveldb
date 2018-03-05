// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "db/version_set.h"


/// 实际存储布局可见luodw.cc/2015/10/31/leveldb-16/

namespace leveldb
{

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed.
/// 用于标记的number
enum Tag
{
  kComparator = 1,
  kLogNumber = 2,
  kNextFileNumber = 3,
  kLastSequence = 4,
  kCompactPointer = 5,
  kDeletedFile = 6,
  kNewFile = 7,
  // 8 was used for large value refs
      kPrevLogNumber = 9
};

/// 做好清理工作
void VersionEdit::Clear()
{
  comparator_.clear();
  log_number_ = 0;
  prev_log_number_ = 0;
  last_sequence_ = 0;
  next_file_number_ = 0;
  has_comparator_ = false;
  has_log_number_ = false;
  has_prev_log_number_ = false;
  has_next_file_number_ = false;
  has_last_sequence_ = false;
  deleted_files_.clear();
  new_files_.clear();
}

/// Encode VersionEdit into string specified by dst
void VersionEdit::EncodeTo(std::string *dst) const
{
  /// has comparator, put tag and comparator name into dst
  if (has_comparator_)
  {
    PutVarint32(dst, kComparator);
    PutLengthPrefixedSlice(dst, comparator_);
  }
  /// has log number, put kLogNumber tag and log_number_
  if (has_log_number_)
  {
    PutVarint32(dst, kLogNumber);
    PutVarint64(dst, log_number_);
  }
  /// has prev log number, put kPrevLogNumber tag and prev_log_number_
  if (has_prev_log_number_)
  {
    PutVarint32(dst, kPrevLogNumber);
    PutVarint64(dst, prev_log_number_);
  }
  /// has next file number, put kNextFileNumber tag and next_file_number_
  if (has_next_file_number_)
  {
    PutVarint32(dst, kNextFileNumber);
    PutVarint64(dst, next_file_number_);
  }
  /// has last_sequence, put kLastSequence tag and last_sequence_
  if (has_last_sequence_)
  {
    PutVarint32(dst, kLastSequence);
    PutVarint64(dst, last_sequence_);
  }

  /// put kCompactPointer tag and pair<level, InternelKey> of each elem of compact_pointers_
  for (size_t i = 0; i < compact_pointers_.size(); i++)
  {
    PutVarint32(dst, kCompactPointer);
    PutVarint32(dst, compact_pointers_[i].first);  // level
    PutLengthPrefixedSlice(dst, compact_pointers_[i].second.Encode());
  }

  /// put kDeletedFile tag and pair<level, file number> of each elem of deleted_files_
  for (DeletedFileSet::const_iterator iter = deleted_files_.begin();
       iter != deleted_files_.end();
       ++iter)
  {
    PutVarint32(dst, kDeletedFile);
    PutVarint32(dst, iter->first);   // level
    PutVarint64(dst, iter->second);  // file number
  }

  /// put kNewFile tag and FileMetaData of each elem of new_files_
  for (size_t i = 0; i < new_files_.size(); i++)
  {
    const FileMetaData &f = new_files_[i].second;
    PutVarint32(dst, kNewFile);
    PutVarint32(dst, new_files_[i].first);  // level
    PutVarint64(dst, f.number);
    PutVarint64(dst, f.file_size);
    PutLengthPrefixedSlice(dst, f.smallest.Encode());
    PutLengthPrefixedSlice(dst, f.largest.Encode());
  }
}

/// get InternalKey from dst
static bool GetInternalKey(Slice *input, InternalKey *dst)
{
  Slice str;
  if (GetLengthPrefixedSlice(input, &str))
  {
    // decode InternalKey from str
    dst->DecodeFrom(str);
    return true;
  }
  else
  {
    return false;
  }
}

/// get level from input, level number is encoded in varint32 type
static bool GetLevel(Slice *input, int *level)
{
  uint32_t v;
  if (GetVarint32(input, &v) &&
      v < config::kNumLevels)
  {
    *level = v;
    return true;
  }
  else
  {
    return false;
  }
}

/// Decode VersionEdit from string specified by src
Status VersionEdit::DecodeFrom(const Slice &src)
{
  /// 首先进行清理
  Clear();
  Slice input = src;
  /// 用于记录出错消息
  const char *msg = NULL;
  uint32_t tag;

  // Temporary storage for parsing
  /// temporary usage
  int level;
  uint64_t number;
  FileMetaData f;
  Slice str;
  InternalKey key;

  while (msg == NULL && GetVarint32(&input, &tag))
  {
    /// 读取tag
    switch (tag)
    {
      case kComparator:
        if (GetLengthPrefixedSlice(&input, &str))
        {
          comparator_ = str.ToString();
          has_comparator_ = true;
        }
        else
        {
          msg = "comparator name";
        }
        break;

      case kLogNumber:
        if (GetVarint64(&input, &log_number_))
        {
          has_log_number_ = true;
        }
        else
        {
          msg = "log number";
        }
        break;

      case kPrevLogNumber:
        if (GetVarint64(&input, &prev_log_number_))
        {
          has_prev_log_number_ = true;
        }
        else
        {
          msg = "previous log number";
        }
        break;

      case kNextFileNumber:
        if (GetVarint64(&input, &next_file_number_))
        {
          has_next_file_number_ = true;
        }
        else
        {
          msg = "next file number";
        }
        break;

      case kLastSequence:
        if (GetVarint64(&input, &last_sequence_))
        {
          has_last_sequence_ = true;
        }
        else
        {
          msg = "last sequence number";
        }
        break;

        /// get compact pointer
      case kCompactPointer:
        if (GetLevel(&input, &level) &&
            GetInternalKey(&input, &key))
        {
          compact_pointers_.push_back(std::make_pair(level, key));
        }
        else
        {
          msg = "compaction pointer";
        }
        break;

        /// get deleted files
      case kDeletedFile:
        if (GetLevel(&input, &level) &&
            GetVarint64(&input, &number))
        {
          deleted_files_.insert(std::make_pair(level, number));
        }
        else
        {
          msg = "deleted file";
        }
        break;
        /// get elem of new_files_
      case kNewFile:
        if (GetLevel(&input, &level) &&
            GetVarint64(&input, &f.number) &&
            GetVarint64(&input, &f.file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest))
        {
          new_files_.push_back(std::make_pair(level, f));
        }
        else
        {
          msg = "new-file entry";
        }
        break;

      default:msg = "unknown tag";
        break;
    }
  }

  /// 输入没有读取完，并且记录的出错消息为NULL
  if (msg == NULL && !input.empty())
  {
    msg = "invalid tag";
  }

  Status result;
  /// 出错了
  if (msg != NULL)
  {
    result = Status::Corruption("VersionEdit", msg);
  }
  return result;
}

/// 生成用于debug的字符串
std::string VersionEdit::DebugString() const
{
  std::string r;
  r.append("VersionEdit {");
  if (has_comparator_)
  {
    r.append("\n  Comparator: ");
    r.append(comparator_);
  }
  if (has_log_number_)
  {
    r.append("\n  LogNumber: ");
    AppendNumberTo(&r, log_number_);
  }
  if (has_prev_log_number_)
  {
    r.append("\n  PrevLogNumber: ");
    AppendNumberTo(&r, prev_log_number_);
  }
  if (has_next_file_number_)
  {
    r.append("\n  NextFile: ");
    AppendNumberTo(&r, next_file_number_);
  }
  if (has_last_sequence_)
  {
    r.append("\n  LastSeq: ");
    AppendNumberTo(&r, last_sequence_);
  }
  for (size_t i = 0; i < compact_pointers_.size(); i++)
  {
    r.append("\n  CompactPointer: ");
    AppendNumberTo(&r, compact_pointers_[i].first);
    r.append(" ");
    r.append(compact_pointers_[i].second.DebugString());
  }
  for (DeletedFileSet::const_iterator iter = deleted_files_.begin();
       iter != deleted_files_.end();
       ++iter)
  {
    r.append("\n  DeleteFile: ");
    AppendNumberTo(&r, iter->first);
    r.append(" ");
    AppendNumberTo(&r, iter->second);
  }
  for (size_t i = 0; i < new_files_.size(); i++)
  {
    const FileMetaData &f = new_files_[i].second;
    r.append("\n  AddFile: ");
    AppendNumberTo(&r, new_files_[i].first);
    r.append(" ");
    AppendNumberTo(&r, f.number);
    r.append(" ");
    AppendNumberTo(&r, f.file_size);
    r.append(" ");
    r.append(f.smallest.DebugString());
    r.append(" .. ");
    r.append(f.largest.DebugString());
  }
  r.append("\n}\n");
  return r;
}

}  // namespace leveldb
