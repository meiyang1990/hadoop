// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @file SyncUtils.h
 * 本文件属于Hadoop MapReduce本地任务执行模块，提供多线程同步工具类
 * 封装了POSIX线程锁和RAII风格的自动加锁解锁实现，用于本地任务的线程安全控制
 */

#ifndef SYNCUTILS_H_
#define SYNCUTILS_H_

#include <unistd.h>
#include <string.h>
#ifdef __MACH__
#include <libkern/OSAtomic.h>
#endif
#include <pthread.h>

namespace NativeTask {

class Condition;

/**
 * 互斥锁封装类，基于POSIX pthread_mutex实现线程互斥访问
 * 禁止拷贝构造和赋值操作，仅提供基本的加锁解锁功能，供条件变量配合使用
 */
class Lock {
public:
  Lock();
  ~Lock();

  void lock();
  void unlock();

private:
  friend class Condition;
  pthread_mutex_t _mutex;

  // No copying
  Lock(const Lock&);
  void operator=(const Lock&);
};

/**
 * RAII风格作用域锁模板，在构造时自动加锁，析构时自动解锁
 * 避免忘记手动解锁导致死锁问题，可适配任意提供lock/unlock方法的锁类型
 * @tparam LockT 锁类型，需要具备lock()和unlock()方法
 */
template<typename LockT>
class ScopeLock {
public:
  ScopeLock(LockT & lock)
      : _lock(&lock) {
    _lock->lock();
  }
  ~ScopeLock() {
    _lock->unlock();
  }
private:
  LockT * _lock;

  // No copying
  ScopeLock(const ScopeLock&);
  void operator=(const ScopeLock&);
};


} // namespace NativeTask

#endif /* SYNCUTILS_H_ */