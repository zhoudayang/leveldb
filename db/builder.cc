// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/env.h"

namespace leveldb
{

/*
 a. 生成新的sstable
 b. 遍历memtable，写入sstable，完成sync
 c. 记录sstable的FileMetaData信息。将新生成的sstable加入TableCache，作为文件正常的验证
 */
Status BuildTable(const std::string &dbname,
                  Env *env,
                  const Options &options,
                  TableCache *table_cache,
                  Iterator *iter,
                  FileMetaData *meta)
{
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  ///  获取sstable的名称
  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid())
  {
    WritableFile *file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok())
    {
      return s;
    }

    TableBuilder *builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());
    for (; iter->Valid(); iter->Next())
    {
      Slice key = iter->key();
      meta->largest.DecodeFrom(key);
      /// 添加key和value
      builder->Add(key, iter->value());
    }

    // Finish and check for builder errors
    if (s.ok())
    {
      s = builder->Finish();
      if (s.ok())
      {
        meta->file_size = builder->FileSize();
        assert(meta->file_size > 0);
      }
    }
    else
    {
      builder->Abandon();
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok())
    {
      /// 若成功，执行sync
      s = file->Sync();
    }
    if (s.ok())
    {
      s = file->Close();
    }
    delete file;
    file = NULL;

    // 测试创建Iterator是否成功
    if (s.ok())
    {
      // Verify that the table is usable
      Iterator *it = table_cache->NewIterator(ReadOptions(),
                                              meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok())
  {
    s = iter->status();
  }

  // if meta->file_size == 0 or !s.ok(), delete the file
  if (s.ok() && meta->file_size > 0)
  {
    // Keep it
  }
  else
  {
    env->DeleteFile(fname);
  }
  return s;
}

}  // namespace leveldb
