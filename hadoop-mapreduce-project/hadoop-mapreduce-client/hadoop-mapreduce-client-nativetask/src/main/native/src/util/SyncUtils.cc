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
 * @file SyncUtils.cc
 * @brief Hadoop NativeTask 同步工具实现，提供跨线程线程同步原语
 *
 * 本文件属于MapReduce本地任务原生模块，实现了可重入互斥锁封装，
 * 用于原生C++代码中多线程共享资源的同步访问，保障线程安全。
 */

#include "lib/commons.h"
#include "lib/jniutils.h"
#include "util/StringUtil.h"
#include "util/SyncUtils.h"

namespace NativeTask {

/**
 * @brief 处理pthread调用结果，异常抛出工具函数
 *
 * 检查POSIX线程函数调用返回结果，如果调用失败则抛出IOException异常
 * 包含错误描述信息，简化错误处理逻辑
 *
 * @param label pthread操作名称，用于错误信息描述
 * @param result pthread调用返回值，0表示成功，非0表示错误码
 */
static void PthreadCall(const char* label, int result) {
  if (result != 0) {
    THROW_EXCEPTION_EX(IOException, "pthread %s: %s", label, strerror(result));
  }
}

/**
 * @class Lock
 * @brief 可重入互斥锁封装，提供RAII风格线程同步
 *
 * 封装POSIX线程可重入互斥锁，支持同一线程多次获取锁不发生死锁，
 * 用于原生代码中多线程访问共享资源时的互斥同步，保障线程安全。
 */

/**
 * @brief 构造函数，初始化可重入互斥锁
 *
 * 初始化互斥锁属性，设置为可重入类型，完成互斥锁创建，
 * 初始化失败则抛出IO异常。
 */
Lock::Lock() {
  pthread_mutexattr_t attr;
  // 初始化互斥锁属性对象
  pthread_mutexattr_init(&attr);
  // 设置互斥锁类型为可重入
  pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
  // 初始化互斥锁
  int ret = pthread_mutex_init(&_mutex, &attr);
  // 销毁互斥锁属性对象
  pthread_mutexattr_destroy(&attr);
  if (ret != 0) {
    THROW_EXCEPTION_EX(IOException, "pthread_mutex_init: %s", strerror(ret));
  }
}

/**
 * @brief 析构函数，销毁互斥锁释放资源
 */
Lock::~Lock() {
  PthreadCall("destroy mutex", pthread_mutex_destroy(&_mutex));
}

/**
 * @brief 获取互斥锁，阻塞直到锁可用
 */
void Lock::lock() {
  PthreadCall("lock", pthread_mutex_lock(&_mutex));
}

/**
 * @brief 释放互斥锁，唤醒等待线程
 */
void Lock::unlock() {
  PthreadCall("unlock", pthread_mutex_unlock(&_mutex));
}

} // namespace NativeTask