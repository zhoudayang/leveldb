#include <leveldb/db.h>
#include <iostream>

using namespace std;

int main()
{
  std::unique_ptr<leveldb::DB> db_ptr;
  leveldb::DB *dbptr;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(options, "/tmp/leveldb", &dbptr);
  assert(status.ok());
  db_ptr.reset(dbptr);

  db_ptr->Put(leveldb::WriteOptions(), "a", "a");
  db_ptr->Put(leveldb::WriteOptions(), "b", "b");
  db_ptr->Put(leveldb::WriteOptions(), "c", "c");

  std::string value;
  auto s = db_ptr->Get(leveldb::ReadOptions(), "a", &value);
  if(s.ok())
  {
    cout << "got it" << endl;
  }

  return 0;
}