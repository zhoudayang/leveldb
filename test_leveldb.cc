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

  for(int i = 0; i < 10; ++ i)
  {
    db_ptr->Put(leveldb::WriteOptions(), std::to_string(i), std::to_string(i));
  }

  std::unique_ptr<leveldb::Iterator> iterator(db_ptr->NewIterator(leveldb::ReadOptions()));
  if(iterator)
  {
    iterator->SeekToFirst();
    while(iterator->Valid())
    {
      cout << iterator->key().ToString() << " : " << iterator->value().ToString() << endl;
      iterator->Next();
    }
  }

}