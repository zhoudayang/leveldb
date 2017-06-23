// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

/// filename - after dbformat
#include <ctype.h>
#include <stdio.h>
#include "db/filename.h"
#include "db/dbformat.h"
#include "leveldb/env.h"
#include "util/logging.h"

namespace leveldb {

// A utility routine: write "data" to the named file and Sync() it.
extern Status WriteStringToFileSync(Env* env, const Slice& data,
                                    const std::string& fname);

/// static function, only available in filename.cc
/// 格式：name + number. + suffix
static std::string MakeFileName(const std::string& name, uint64_t number,
                                const char* suffix) {
  char buf[100];
  snprintf(buf, sizeof(buf), "/%06llu.%s",
           static_cast<unsigned long long>(number),
           suffix);
  return name + buf;
}

/// name + number.log
std::string LogFileName(const std::string& name, uint64_t number) {
  assert(number > 0);
  return MakeFileName(name, number, "log");
}

/// name + number.ldb
std::string TableFileName(const std::string& name, uint64_t number) {
  assert(number > 0);
  return MakeFileName(name, number, "ldb");
}

/// name + number.sst
std::string SSTTableFileName(const std::string& name, uint64_t number) {
  assert(number > 0);
  return MakeFileName(name, number, "sst");
}

/// dbname + /MANIFEST-number
std::string DescriptorFileName(const std::string& dbname, uint64_t number) {
  assert(number > 0);
  char buf[100];
  snprintf(buf, sizeof(buf), "/MANIFEST-%06llu",
           static_cast<unsigned long long>(number));
  return dbname + buf;
}

/// dbname + /CURRENT
std::string CurrentFileName(const std::string& dbname) {
  return dbname + "/CURRENT";
}

/// dbname + /LOCK
std::string LockFileName(const std::string& dbname) {
  return dbname + "/LOCK";
}

/// dbname + number.dbtmp
std::string TempFileName(const std::string& dbname, uint64_t number) {
  assert(number > 0);
  return MakeFileName(dbname, number, "dbtmp");
}

/// dbname + "/LOG"
std::string InfoLogFileName(const std::string& dbname) {
  return dbname + "/LOG";
}

/// dbname + "/LOG.old"
// Return the name of the old info log file for "dbname".
std::string OldInfoLogFileName(const std::string& dbname) {
  return dbname + "/LOG.old";
}


// Owned filenames have the form:
//    dbname/CURRENT
//    dbname/LOCK
//    dbname/LOG
//    dbname/LOG.old
//    dbname/MANIFEST-[0-9]+
//    dbname/[0-9]+.(log|sst|ldb)
/// fname 指向/之后的字符串
bool ParseFileName(const std::string& fname,
                   uint64_t* number,
                   FileType* type) {
  Slice rest(fname);
  if (rest == "CURRENT") {
    *number = 0;
    *type = kCurrentFile;
  } else if (rest == "LOCK") {
    *number = 0;
    *type = kDBLockFile;
  } else if (rest == "LOG" || rest == "LOG.old") {
    *number = 0;
    *type = kInfoLogFile;
  } else if (rest.starts_with("MANIFEST-")) {
    rest.remove_prefix(strlen("MANIFEST-"));
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    if (!rest.empty()) {
      return false;
    }
    *type = kDescriptorFile;
    *number = num;
  } else {
    // Avoid strtoull() to keep filename format independent of the
    // current locale
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    Slice suffix = rest;
    if (suffix == Slice(".log")) {
      *type = kLogFile;
    } else if (suffix == Slice(".sst") || suffix == Slice(".ldb")) {
      *type = kTableFile;
    } else if (suffix == Slice(".dbtmp")) {
      *type = kTempFile;
    } else {
      return false;
    }
    *number = num;
  }
  return true;
}

/// 首先写入tmp文件，成功则改名为Current file name, 否则删除temp文件，返回status
Status SetCurrentFile(Env* env, const std::string& dbname,
                      uint64_t descriptor_number) {
  // Remove leading "dbname/" and add newline to manifest file name
  std::string manifest = DescriptorFileName(dbname, descriptor_number);
  Slice contents = manifest;
  assert(contents.starts_with(dbname + "/"));
  contents.remove_prefix(dbname.size() + 1);
  /// 首先写入到tmp文件
  std::string tmp = TempFileName(dbname, descriptor_number);
  /// 记录当前的manifest文件名称到tmp文件之中
  Status s = WriteStringToFileSync(env, contents.ToString() + "\n", tmp);
  if (s.ok()) {
    ///重命名tmp文件到current文件
    s = env->RenameFile(tmp, CurrentFileName(dbname));
  }
  if (!s.ok()) {
    env->DeleteFile(tmp);
  }
  return s;
}

}  // namespace leveldb
