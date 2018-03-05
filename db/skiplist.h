// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

/// 可以看，和dbformat一起看
#include <assert.h>
#include <stdlib.h>
#include "port/port.h"
#include "util/arena.h"
#include "util/random.h"

namespace leveldb
{

// 内存分配器
class Arena;

template<typename Key, class Comparator>
class SkipList
{
 private:
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit SkipList(Comparator cmp, Arena *arena);

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(const Key &key);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key &key) const;

  // Iteration over the contents of a skip list
  // 此iterator的定义和include/leveldb/iterator头文件中定义的一样
  /// 内部类Iterator
  class Iterator
  {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const SkipList *list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key &key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    // 此操作之后，指向第一条key >= target的记录
    void Seek(const Key &target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private:
    // itertor从属的链表
    const SkipList *list_;
    // 链表中的结点
    Node *node_;
    // Intentionally copyable
  };

 private:
  // skip list 最大高度为12
  enum
  {
    kMaxHeight = 12
  };

  // Immutable after construction
  Comparator const compare_;
  Arena *const arena_;    // Arena used for allocations of nodes
  // 头结点
  Node *const head_;

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  port::AtomicPointer max_height_;   // Height of the entire list

  // 返回跳表中的最高高度
  inline int GetMaxHeight() const
  {
    return static_cast<int>(
        reinterpret_cast<intptr_t>(max_height_.NoBarrier_Load()));
  }

  // Read/written only by Insert().
  // 一个随机数生成器
  Random rnd_;

  // 创建一个新的结点, 此结点的next_数组capacity为height
  Node *NewNode(const Key &key, int height);

  // 返回一个随机的高度，可能因此level得以提升
  int RandomHeight();

  // 判断 a==b 是否成立
  bool Equal(const Key &a, const Key &b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  /// 判断key是否大于n中的key，若n为nil，对应key则设定为无穷大
  bool KeyIsAfterNode(const Key &key, Node *n) const;

  // Return the earliest node that comes at or after key.
  // Return NULL if there is no such node.
  //
  // If prev is non-NULL, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  /// 找到刚好 >= key的节点。 若输入的prev不为nil，设置prev中的每一个位置指向对应level的前一个结点
  Node *FindGreaterOrEqual(const Key &key, Node **prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  // 返回最后比key小的节点。如果没有这样的节点则返回head_
  Node *FindLessThan(const Key &key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  // 返回list中的尾结点
  // 若list为空，返回head_结点
  Node *FindLast() const;

  // No copying allowed
  SkipList(const SkipList &);
  void operator=(const SkipList &);
};

/// definition of Node
// Implementation details follow
template<typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node
{
  explicit Node(const Key &k) :
      key(k) {}

  // 键
  Key const key;

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  // 获取n对应level的链表结点
  Node *Next(int n)
  {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return reinterpret_cast<Node *>(next_[n].Acquire_Load());
  }
  // 设置n对应level上的链表结点
  void SetNext(int n, Node *x)
  {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_[n].Release_Store(x);
  }

  /// No-barrier version
  // No-barrier variants that can be safely used in a few locations.
  Node *NoBarrier_Next(int n)
  {
    assert(n >= 0);
    return reinterpret_cast<Node *>(next_[n].NoBarrier_Load());
  }
  void NoBarrier_SetNext(int n, Node *x)
  {
    assert(n >= 0);
    next_[n].NoBarrier_Store(x);
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  // 指向第一个level，实质上此数组的大小并不为1，这是c中struct的一个tricky
  port::AtomicPointer next_[1];
};

template<typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node *
SkipList<Key, Comparator>::NewNode(const Key &key, int height)
{
  /// 数组的capacity为height
  //调用Arena内存分配器进行分配
  char *mem = arena_->AllocateAligned(
      sizeof(Node) + sizeof(port::AtomicPointer) * (height - 1));
  return new(mem) Node(key);
}

// constructor of Iterator inside skiplist
template<typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList *list)
{
  list_ = list;
  node_ = NULL;
}

// 若node_不为nil，那么Iterator是Valid的
template<typename Key, class Comparator>
inline bool SkipList<Key, Comparator>::Iterator::Valid() const
{
  return node_ != NULL;
}

// 返回node_对应的key_
template<typename Key, class Comparator>
inline const Key &SkipList<Key, Comparator>::Iterator::key() const
{
  assert(Valid());
  return node_->key;
}

// 移动node_指向链表中的下一个结点
template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Next()
{
  assert(Valid());
  node_ = node_->Next(0);
}

// Iterator指向上一个结点
template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Prev()
{
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  // node_应该指向恰好小于当前key的节点
  // 此操作是在skiplist的层面进行
  node_ = list_->FindLessThan(node_->key);
  // 若链表为空，node_指向null，iterator 已经不再 valid了
  if (node_ == list_->head_)
  {
    node_ = NULL;
  }
}

// 转调用skiplist中的FindGreaterOrEqual函数，node指向第一个大于等于target的节点
template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Seek(const Key &target)
{
  node_ = list_->FindGreaterOrEqual(target, NULL);
}

// iterator指向head_对应的结点
template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToFirst()
{
  node_ = list_->head_->Next(0);
}

// node_是skiplist中最后的节点
template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToLast()
{
  node_ = list_->FindLast();
  // 判断链表是否为空，设置iterator的状态
  if (node_ == list_->head_)
  {
    node_ = NULL;
  }
}

// 返回随机的高度
template<typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeight()
{
  // Increase height with probability 1 in kBranching
  // 有四分之一的可能性
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0))
  {
    height++;
  }
  assert(height > 0);
  // 断言返回的高度不可超过kMaxHeight
  assert(height <= kMaxHeight);
  return height;
}

/// 判断key是否大于n中的key，若n为nil，对应key则设定为无穷大
template<typename Key, class Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key &key, Node *n) const
{
  // NULL n is considered infinite
  /// 此处，memtable实际使用时，若key的sequence number更大，user key一样，会返回false，因为compare_返回大于0的值
  /// node->key < key
  return (n != NULL) && (compare_(n->key, key) < 0);
}

/// 找到刚好 >= key的节点。 若输入的prev不为nil，设置prev中的每一个位置指向对应level的前一个结点
template<typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node *SkipList<Key, Comparator>::FindGreaterOrEqual(const Key &key, Node **prev)
const
{
  Node *x = head_;
  int level = GetMaxHeight() - 1;
  while (true)
  {
    Node *next = x->Next(level);
    /// 优先从高层链表往后遍历找到正确的位置
    // 还没有找到正确的位置，继续往前遍历链表
    /// key在链表结点next的后面
    if (KeyIsAfterNode(key, next))
    {
      // Keep searching in this list
      x = next;
    }
      /// key在链表结点next的前面，在x结点的后面
      /// key的prev节点应该指向x
    else
    {
      if (prev != NULL)
      {
        prev[level] = x;
      }
      // 若已经是最底层的链表，直接返回这个结点
      if (level == 0)
      {
        return next;
      }
      else
      {
        // Switch to next list
        /// 进入更底层的链表，继续查找，直到第0层链表
        level--;
      }
    }
  }
}

template<typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node *
SkipList<Key, Comparator>::FindLessThan(const Key &key) const
{
  Node *x = head_;
  int level = GetMaxHeight() - 1;
  while (true)
  {
    // 断言当前x为head_结点，否则必有x->key < key 成立
    assert(x == head_ || compare_(x->key, key) < 0);
    // 拿到当前level层的下一个结点
    Node *next = x->Next(level);
    // 若next指向NULL，或者next->key >= key
    if (next == NULL || compare_(next->key, key) >= 0)
    {
      // 已经是最底层的链表，直接返回对应的结点
      if (level == 0)
      {
        return x;
      }
      else
      { // 转向更底层的链表
        // Switch to next list
        level--;
      } // next && compare_(next->key, key) < 0, 继续遍历到下一个结点
    }
    else
    {
      x = next;
    }
  }
}

// 首先在最上层链表遍历到尾端，再转向底层结点，直到到达最低层链表的末尾结点
template<typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node *SkipList<Key, Comparator>::FindLast()
const
{
  Node *x = head_;
  int level = GetMaxHeight() - 1;
  while (true)
  {
    Node *next = x->Next(level);
    if (next == NULL)
    {
      if (level == 0)
      {
        return x;
      }
      else
      {
        // Switch to next list
        level--;
      }
    }
    else
    {
      x = next;
    }
  }
}

// 每一个链表结点next_数组的容量都为kMaxHeight, 12
template<typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena *arena)
    :
    compare_(cmp),
    arena_(arena),
    head_(NewNode(0 /* any key will do */, kMaxHeight)),
    max_height_(reinterpret_cast<void *>(1)), // 新建skiplist的深度
    rnd_(0xdeadbeef)
{ /// 指定random的seed值
  // head结点的next_数组每一项都为NULL，对应当前skiplist为空
  for (int i = 0; i < kMaxHeight; i++)
  {
    head_->SetNext(i, NULL);
  }
}

// 向skiplist中插入key
template<typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key &key)
{
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  Node *prev[kMaxHeight];
  Node *x = FindGreaterOrEqual(key, prev);

  // Our data structure does not allow duplicate insertion
  assert(x == NULL || !Equal(key, x->key));

  int height = RandomHeight();
  if (height > GetMaxHeight())
  {
    // 更高层的结点，其prev指向head_
    for (int i = GetMaxHeight(); i < height; i++)
    {
      prev[i] = head_;
    }
    //fprintf(stderr, "Change height from %d to %d\n", max_height_, height);

    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (NULL), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since NULL sorts after all
    // keys.  In the latter case the reader will use the new node.
    max_height_.NoBarrier_Store(reinterpret_cast<void *>(height));
  }

  x = NewNode(key, height);
  for (int i = 0; i < height; i++)
  {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    // 复制prev数组
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    // prev->next 也指向key
    prev[i]->SetNext(i, x);
  }
}

// 判断key是否在链表之中
template<typename Key, class Comparator>
bool SkipList<Key, Comparator>::Contains(const Key &key) const
{
  Node *x = FindGreaterOrEqual(key, NULL);
  if (x != NULL && Equal(key, x->key))
  {
    return true;
  }
  else
  {
    return false;
  }
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_
