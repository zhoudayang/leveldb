// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
/// 实现的DB包含一系列Versions。最新的version被称为"current", 较旧的version被保存用来保证当前
/// 存活的iterators的一致性视图
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
/// 每一个Version和level上的一系列table文件相关， 完整的versions的集合由VersionSet来维系
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.
///  Version, VersionSet是线程兼容的，但是所有的访问都需要外部的同步措施。

#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include <map>
#include <set>
#include <vector>
#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"

/// todo: 需要和db_impl一起考虑
namespace leveldb
{

/// pre declare of Writer
namespace log
{
class Writer;
}

class Compaction;
class Iterator;
class MemTable;
class TableBuilder;
class TableCache;
class Version;
class VersionSet;
class WritableFile;

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
/// 找到files[i]->largest >= key 的最小的index
extern int FindFile(const InternalKeyComparator &icmp,
                    const std::vector<FileMetaData *> &files,
                    const Slice &key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==NULL represents a key smaller than all keys in the DB.
// largest==NULL represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
/// 若files中的一些文件和[*smallest,*largest]指定的user key区间重合就返回true
extern bool SomeFileOverlapsRange(
    const InternalKeyComparator &icmp,
    bool disjoint_sorted_files, // files重合了吗？
    const std::vector<FileMetaData *> &files, // 输入的是同一个level的文件
    const Slice *smallest_user_key,
    const Slice *largest_user_key);

/// 版本
/*
 将每次compact后的最新数据状态定义为Version，也就是当前db元信息以及每个level上具有最新
 数据状态的sstable的集合。compact会在某个level上新加入或者删除一些sstable，但可能这个时候，
 那些要删除的sstable正在被读，为了处理这样的读写竞争情况，基于sstable文件一旦生成就不会改动的
 特点，每个Version加入引用计数，读以及解除读操作会将引用计数相应加减一。这样，db中可能有多个Version
 同时存在，它们通过链表链接起来。当Version的引用计数为0并且不是当前最新的Version的时候，它会从
 链表中移除，对应的，该Version内的sstable就可以删除了。(这些废弃的sstable会在下一次compact完成时
 被清理掉)。
 */
class Version
{
 public:
  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  /// 向iters加入当前Version的内容对应的iterator，在合并之前内容不会修改
  /// 对level0和其他level做了区分处理
  /// 当前level所有文件的iterator加入iters中
  void AddIterators(const ReadOptions &, std::vector<Iterator *> *iters);

  struct GetStats
  {
    FileMetaData *seek_file; // 查找的文件
    int seek_file_level; // 查找的文件的level
  };

  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // REQUIRES: lock is not held
  /// 查找key的value，若找到了存储在*val之中，并返回OK，否则返回non-OK status。记录stats。
  /// 要求：没有持有锁
  Status Get(const ReadOptions &, const LookupKey &key, std::string *val,
             GetStats *stats);

  // Adds "stats" into the current state.  Returns true if a new
  // compaction may need to be triggered, false otherwise.
  // REQUIRES: lock is held
  /// 将stats添加到当前的state之中。如果需要compaction返回true，否则返回false。
  /// 要求：已经持有锁
  bool UpdateStats(const GetStats &stats);

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.  Returns true if a new compaction may need to be triggered.
  // REQUIRES: lock is held
  /// 读 指定key的a sample of bytes 。具体读多少数据取决于config::kReadBytesPeriod
  /// 如果触发了新的compaction就返回true。
  /// 要求：持有锁
  bool RecordReadSample(Slice key);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  /// 引用计数管理, 所以对应iterator存活时，Versions不会消失
  void Ref();
  void Unref();

  /// 在*input保存所有level对应的和[begin,end]overlap的文件
  void GetOverlappingInputs(
      int level,
      const InternalKey *begin,         // NULL means before all keys
      const InternalKey *end,           // NULL means after all keys
      std::vector<FileMetaData *> *inputs);

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==NULL represents a key smaller than all keys in the DB.
  // largest_user_key==NULL represents a key largest than all keys in the DB.
  /// 若指定level中的文件和[*smallest_user_key,*largest_user_key]区间重叠就返回true
  bool OverlapInLevel(int level,
                      const Slice *smallest_user_key,
                      const Slice *largest_user_key);

  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].
  /// 新的dump的memtable的结果涵盖的key的范围是[smallest_user_key,largest_user_key]
  /// 返回应该被放置在哪一个level之上
  int PickLevelForMemTableOutput(const Slice &smallest_user_key,
                                 const Slice &largest_user_key);

  /// 返回对应level的文件数目
  int NumFiles(int level) const { return files_[level].size(); }

  // Return a human readable string that describes this version's contents.
  /// debug 字符串
  std::string DebugString() const;

 private:
  friend class Compaction;
  friend class VersionSet;

  class LevelFileNumIterator;
  Iterator *NewConcatenatingIterator(const ReadOptions &, int level) const;

  // Call func(arg, level, f) for every file that overlaps user_key in
  // order from newest to oldest.  If an invocation of func returns
  // false, makes no more calls.
  //
  // REQUIRES: user portion of internal_key == user_key.
  /// 按照由新到旧的顺序，和user_key重合的file，都执行函数func(arg, level, f), 若执行func的结果
  /// 为false，就break
  void ForEachOverlapping(Slice user_key, Slice internal_key,
                          void *arg,
                          bool (*func)(void *, int, FileMetaData *));

  /// 当前Version属于哪一个VersionSet
  VersionSet *vset_;            // VersionSet to which this Version belongs
  /// 链表中的下一个version
  Version *next_;               // Next version in linked list
  /// 链表中的上一个version
  Version *prev_;               // Previous version in linked list
  /// 引用计数
  int refs_;                    // Number of live refs to this version

  // List of files per level
  /// 每个level对应的链表
  std::vector<FileMetaData *> files_[config::kNumLevels];

  /*
   sstable的seek次数达到一定的阈值的时候，可以认为它处在不最优的情况，而我们认为compact后
   会倾向于均衡的状态，所以在一个sstable的seek次数达到一定的阈值后，主动对其进行compact是合理的。
   */
  /*
 这个具体 seek 次数阈值(allowed_seeks)的确定，依赖于 sas 盘的 IO 性能:
  a. 一次磁盘寻道seek耗费10ms。
  b. 读或者写 1M 数据耗费 10ms (按 100M/s IO 吞吐能力)。
  c. compact 1M 的数据需要 25M 的 IO:从 level-n 中读 1M 数据，从 level-n+1 中读 10~12M 数据，写入 level-n+1 中 10~12M 数据。
  所以，compact 1M 的数据的时间相当于做 25 次磁盘 seek，反过来说就是，1 次 seek 相当于 compact 40k 数据。那么，可以得到 seek 阈值
   allowed_seeks=sstable_size / 40k。保守设置， 当前实际的 allowed_seeks = sstable_size / 10k。
   每次 compact 完成，构造新的 Version 时 (Builder::Apply()),每个 sstable 的 allowed_seeks 会计算出来保存在 FileMetaData。
   在每次 get 操作的时候，如果有超过一个 sstable 文件进行了 IO，会将最后一个 IO 的 sstable 的 allowed_seeks 减一，
   并检查其是否已经用光了 allowed_seeks,若是，则将该 sstable 记录成当前 Version 的 file_to_compact_,并记录其所在的 level(file_to_compact_level_)。
   */
  // Next file to compact based on seek stats.
  /// 经seek stats评估的下一个需要compact的文件
  FileMetaData *file_to_compact_;
  /// 将要compact的文件的level
  int file_to_compact_level_;

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
  /// compaction 评分，Score < 1 意味着compaction在严格意义上不需要
  double compaction_score_;
  /// compaction 操作所在的level
  int compaction_level_;

  explicit Version(VersionSet *vset)
      :
      vset_(vset), next_(this), prev_(this), refs_(0),
      file_to_compact_(NULL),
      file_to_compact_level_(-1),
      compaction_score_(-1),
      compaction_level_(-1)
  {
  }

  /// destructor of Version
  ~Version();

  // No copying allowed
  Version(const Version &);
  void operator=(const Version &);
};

/// 所有version的集合
/*
 整个db的当前状态被VersionSet管理，其中有当前最新的Version以及其他正在服务的Version链表；全局的SequenceNumber，
 FileNumber；当前的manifest_file_number; 封装sstable的TableCache。每个level中下一次compact要选取的start_key等
 */
class VersionSet
{
 public:
  /// constructor of VersionSet
  VersionSet(const std::string &dbname,
             const Options *options,
             TableCache *table_cache,
             const InternalKeyComparator *);

  /// destructor of VersionSet
  ~VersionSet();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  /// 应用当前version，生成新的log文件，保存持久状态，并且安装作为当前的version，
  /// 在真正写入到文件之后会release *mu
  Status LogAndApply(VersionEdit *edit, port::Mutex *mu)
  EXCLUSIVE_LOCKS_REQUIRED(mu);

  // Recover the last saved descriptor from persistent storage.
  /// 从存储中恢复最近保存的描述信息
  Status Recover(bool *save_manifest);

  // Return the current version.
  /// 返回当前的版本
  Version *current() const { return current_; }

  // Return the current manifest file number
  /// 返回当前manifest文件数目
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
  /// 分配并返回一个新的file number
  uint64_t NewFileNumber() { return next_file_number_++; }

  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  /// 已经ReuseFileNumber, 故而next_file_number_可以减去1
  void ReuseFileNumber(uint64_t file_number)
  {
    if (next_file_number_ == file_number + 1)
    {
      next_file_number_ = file_number;
    }
  }

  // Return the number of Table files at the specified level.
  /// 返回指定level的table文件的number
  int NumLevelFiles(int level) const;

  // Return the combined file size of all files at the specified level.
  /// 返回指定level的所有文件合并的文件的大小
  int64_t NumLevelBytes(int level) const;

  // Return the last sequence number.
  /// 返回上一个sequence number
  uint64_t LastSequence() const { return last_sequence_; }

  // Set the last sequence number to s.
  /// 设置 last sequence number
  void SetLastSequence(uint64_t s)
  {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  // Mark the specified file number as used.
  /// 标记number指定的文件已经使用, 并据此更新next_file_number_
  void MarkFileNumberUsed(uint64_t number);

  // Return the current log file number.
  /// 返回当前log文件的number
  uint64_t LogNumber() const { return log_number_; }

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  /// 返回上一个log文件的number
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  // Pick level and inputs for a new compaction.
  // Returns NULL if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  /// 选取文件进行Compaction操作
  Compaction *PickCompaction();

  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns NULL if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  /// 选取指定level上指定范围的文件进行Compaction操作
  Compaction *CompactRange(
      int level,
      const InternalKey *begin,
      const InternalKey *end);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  /// 返回某level和上级文件最大的overlapping的数据大小 (level >= 1)
  int64_t MaxNextLevelOverlappingBytes();

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  /// 在给定的compaction对象上创建iterator, 这是对compaction涉及到的sstable合并之后形成的
  /// 统一的iterator
  Iterator *MakeInputIterator(Compaction *c);

  // Returns true iff some level needs a compaction.
  /// 是否需要进行Compaction ?
  bool NeedsCompaction() const
  {
    Version *v = current_;
    /// 若当前版本的compaction_score >= 1 且 需要进行compact的文件不为NULL 才会需要合并
    return (v->compaction_score_ >= 1) || (v->file_to_compact_ != NULL);
  }

  // Add all files listed in any live version to *live.
  // May also mutate some internal state.
  /// 将当前存活的文件信息存入live之中
  void AddLiveFiles(std::set<uint64_t> *live);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  /// 返回key和version对应的数据在database之中的偏移位置，此值为估计值
  uint64_t ApproximateOffsetOf(Version *v, const InternalKey &key);

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  /// 用于LevelSummary
  struct LevelSummaryStorage
  {
    char buffer[100];
  };

  /// 返回每个level上文件数目的总结
  /// 使用sratch用于backing store
  const char *LevelSummary(LevelSummaryStorage *scratch) const;

 private:
  class Builder;

  friend class Compaction;
  friend class Version;

  bool ReuseManifest(const std::string &dscname, const std::string &dscbase);

  void Finalize(Version *v);

  void GetRange(const std::vector<FileMetaData *> &inputs,
                InternalKey *smallest,
                InternalKey *largest);

  void GetRange2(const std::vector<FileMetaData *> &inputs1,
                 const std::vector<FileMetaData *> &inputs2,
                 InternalKey *smallest,
                 InternalKey *largest);

  void SetupOtherInputs(Compaction *c);

  // Save current contents to *log
  Status WriteSnapshot(log::Writer *log);

  /// 添加version最为当前的version
  void AppendVersion(Version *v);

  Env *const env_;
  const std::string dbname_; // db name
  const Options *const options_; // options
  TableCache *const table_cache_; // sstable cache
  const InternalKeyComparator icmp_; // InternalKey comparator
  uint64_t next_file_number_; // 下一个file number
  uint64_t manifest_file_number_; // number of manifest file
  uint64_t last_sequence_; // 上一个序号
  uint64_t log_number_; // log文件的number
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

  /*
   为了重启db后可以恢复退出前的状态，需要将db中的状态保存下来，这些状态信息就保存在manifest文件中。
   当db出现异常的时候，为了尽可能多的恢复，manifest中不会只保存当前的状态，而是将历史的状态都保存下来。
   又考虑到每次状态的完全保存需要的空间和耗费的时间会较多，当前采用的方式是，只在manifest开始保存完整
   的状态信息(VersionSet::WriteSnapshot())，接下来只保存每次compact产生的操作，重启db时，根据开头
   的起始状态，依次将后续的VersionEdit replay，即可恢复到退出之前的状态。
   */
  // Opened lazily
  // manifest文件的封装
  WritableFile *descriptor_file_;
  // manifest文件的writer
  log::Writer *descriptor_log_;
  // 正在服务的Version链表
  Version dummy_versions_;  // Head of circular doubly-linked list of versions.
  // 当前最新的Version
  Version *current_;        // == dummy_versions_.prev_

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  /// 每一个level的下一次compaction开始进行的key
  /// empty string 或者 InternalKey
  /*
   为了尽量均匀compact每个level，所以会将这一次compact的end-key作为下一次compact的start-key.
   compactor_pointer_就保存着每个level下一次compact的start-key.除了current_外的Version，并不会做
   compact，所以这个值并不保存在Version中。
   */
  std::string compact_pointer_[config::kNumLevels];

  // No copying allowed
  VersionSet(const VersionSet &);
  void operator=(const VersionSet &);
};

// A Compaction encapsulates information about a compaction.
/// compaction 相关信息
class Compaction
{
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  /// compact操作发生的level，从level ~ level + 1
  int level() const { return level_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  ///  return edit_
  VersionEdit *edit() { return &edit_; }

  /// 输入的文件的数目，分别是当前level和level+1
  // "which" must be either 0 or 1
  int num_input_files(int which) const { return inputs_[which].size(); }

  /// 返回which对应level的i index对应的文件
  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  FileMetaData *input(int which, int i) const { return inputs_[which][i]; }

  /// 控制compaction操作过程之中产生的sstable文件的大小，默认控制在2MB
  /// 此值限制了sstable的最大大小
  // Maximum size of files to build during this compaction.
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  /// compaction操作是否只需将single input file 移动到下一个level，无需进行合并和分割操作
  bool IsTrivialMove() const;

  // Add all inputs to this compaction as delete operations to *edit.
  /// 由edit记录所有input文件为可以删除的文件
  void AddInputDeletions(VersionEdit *edit);

  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  /// 若通过现在掌握的信息能够确保compaction操作最终生成的在level+1上的sstable，比现有>level+1的数据都大
  bool IsBaseLevelForKey(const Slice &user_key);

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  /// 若我们应该在internal_key之前停止创建current output返回true
  bool ShouldStopBefore(const Slice &internal_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  /// 当compaction操作完成之后，release 输入的version
  void ReleaseInputs();

 private:
  friend class Version;
  friend class VersionSet;

  /// constructor of Compaction
  Compaction(const Options *options, int level);

  int level_; // level 要compact的level
  uint64_t max_output_file_size_; // max output file size / 生成sstable的最大size
  Version *input_version_; // input version / compact时当前的Version
  VersionEdit edit_; // 在input version 基础之上需要作出的更改，记录compact过程中的操作

  /// 每一次compaction读取操作从level_和level_+1读取数据
  // Each compaction reads inputs from "level_" and "level_+1"
  std::vector<FileMetaData *> inputs_[2];      // The two sets of inputs

  // State used to check for number of of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  /*
   位于level-n+1,并且与compact的key-range有overlap的sstable。保存grandparents_是因为
   compact最终会生成一系列level-n+1的sstable，而如果生成的sstable与level-n+1中有过多的overlap
   的话，当compact level-n+1时，会产生过多的merge，为了尽量避免这种情况，compact过程中需要检查
   与level-n+1产生的overlap的size并与阈值kMaxGrandParentOverlapBytes做比较，以便提前终止compact。
   */
  std::vector<FileMetaData *> grandparents_;
  /// 记录compact时grandparents_中已经overlap的index
  size_t grandparent_index_;  // Index in grandparent_starts_
  /*
   记录是否有key检查overlap。
   如果是第一次检查，发现有overlap，也不会增加overlapped_bytes_
   */
  bool seen_key_;             // Some output key has been seen
  // 记录已经overlap的累积size
  int64_t overlapped_bytes_;  // Bytes of overlap between current output
  // and grandparent files

  // State for implementing IsBaseLevelForKey

  // level_ptrs_ holds indices(目录) into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  /*
    compact时，当key的ValueType是kTypeDeletion时，要检查其在level-n+1以上是否存在来
    决定是否丢弃该key。因为compact时，key的遍历是顺序的，所以每次检查从上一次检查结束的地方
    开始即可。
   */
  size_t level_ptrs_[config::kNumLevels];
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_
