// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

// db数据持久化的文件。文件的size有限制最大值。文件前面为数据，后面是
// 索引元信息。
namespace leveldb
{

struct Table::Rep
{
  ~Rep()
  {
    delete filter;
    delete[] filter_data;
    delete index_block;
  }

  Options options; // options
  Status status;
  RandomAccessFile *file; // files
  uint64_t cache_id; // cache id
  FilterBlockReader *filter; // filter block reader
  const char *filter_data; // filter data

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block *index_block; // index block
};

Status Table::Open(const Options &options,
                   RandomAccessFile *file,
                   uint64_t size,
                   Table **table)
{
  *table = NULL;
  // sstable的文件大小需要大于Footer
  if (size < Footer::kEncodedLength)
  {
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  // 读取footer
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok())
  {
    return s;
  }

  Footer footer;
  // decode from footer Slice
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok())
  {
    return s;
  }

  // Read the index block
  BlockContents contents;
  Block *index_block = NULL;
  if (s.ok())
  {
    ReadOptions opt;
    // 如果设置了paranoid_checks, 那么需要检查校验码
    if (options.paranoid_checks)
    {
      opt.verify_checksums = true;
    }
    // 读取index block
    s = ReadBlock(file, opt, footer.index_handle(), &contents);
    if (s.ok())
    {
      index_block = new Block(contents);
    }
  }

  if (s.ok())
  {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Rep *rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = NULL;
    rep->filter = NULL;
    *table = new Table(rep);
    // 读取meta 元数据
    (*table)->ReadMeta(footer);
  }
  else
  {
    delete index_block;
  }

  return s;
}

void Table::ReadMeta(const Footer &footer)
{
  // 没有设置filter_policy, return
  if (rep_->options.filter_policy == NULL
      || footer.metaindex_handle().size() == 0)
  {
    return;  // Do not need any metadata
  }

  ReadOptions opt;
  if (rep_->options.paranoid_checks)
  {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok())
  {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  // meta index block
  Block *meta = new Block(contents);

  Iterator *iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  // 查找filter 策略对应的filter index block
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key))
  {
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice &filter_handle_value)
{
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok())
  {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks)
  {
    opt.verify_checksums = true;
  }
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok())
  {
    return;
  }
  if (block.heap_allocated)
  {
    rep_->filter_data = block.data.data();     // Will need to delete later
  }
  // 创建filter
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table()
{
  delete rep_;
}

static void DeleteBlock(void *arg, void *ignored)
{
  delete reinterpret_cast<Block *>(arg);
}

static void DeleteCachedBlock(const Slice &key, void *value)
{
  Block *block = reinterpret_cast<Block *>(value);
  delete block;
}

static void ReleaseBlock(void *arg, void *h)
{
  Cache *cache = reinterpret_cast<Cache *>(arg);
  Cache::Handle *handle = reinterpret_cast<Cache::Handle *>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator *Table::BlockReader(void *arg,
                             const ReadOptions &options,
                             const Slice &index_value)
{
  Table *table = reinterpret_cast<Table *>(arg);
  Cache *block_cache = table->rep_->options.block_cache;
  Block *block = NULL;
  Cache::Handle *cache_handle = NULL;

  BlockHandle handle;
  Slice input = index_value;
  // decode block handle from input
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok())
  {
    BlockContents contents;
    if (block_cache != NULL)
    {
      char cache_key_buffer[16];
      // cache_id + handle.offset -> key
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != NULL)
      {
        // 尝试从缓存之中获取handle对应的block
        block = reinterpret_cast<Block *>(block_cache->Value(cache_handle));
      }
      else
      {
        // 直接从文件之中读取
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok())
        {
          block = new Block(contents);
          // 缓存it
          if (contents.cachable && options.fill_cache)
          {
            cache_handle = block_cache->Insert(
                key, block, block->size(), &DeleteCachedBlock);
          }
        }
      }
    }
    else
    {
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok())
      {
        block = new Block(contents);
      }
    }
  }

  Iterator *iter;
  if (block != NULL)
  {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == NULL)
    {
      // 注册cleanup function 为 DeleteBlock
      iter->RegisterCleanup(&DeleteBlock, block, NULL);
    }
    else
    {
      // 注册cleanup function 为ReleaseBlock
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  }
  else
  {
    // 否则创建非法的空的iterator
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator *Table::NewIterator(const ReadOptions &options) const
{
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table *>(this), options);
}

Status Table::InternalGet(const ReadOptions &options, const Slice &k,
                          void *arg,
                          void (*saver)(void *, const Slice &, const Slice &))
{
  Status s;
  Iterator *iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid())
  {
    Slice handle_value = iiter->value();
    FilterBlockReader *filter = rep_->filter;
    BlockHandle handle;
    // 使用filter进行过滤
    if (filter != NULL &&
        handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k))
    { // filter过滤不通过
      // Not found
    }
    else
    {
      Iterator *block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid())
      {
        // 保存对应key，value，以及输入的参数arg
        (*saver)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok())
  {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

// 返回的offset可能不正确，可能是rep_->metaindex_handle.offset()的值来做近似
uint64_t Table::ApproximateOffsetOf(const Slice &key) const
{
  Iterator *index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid())
  {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok())
    {
      result = handle.offset();
    }
    else
    {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  }
  else
  {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
