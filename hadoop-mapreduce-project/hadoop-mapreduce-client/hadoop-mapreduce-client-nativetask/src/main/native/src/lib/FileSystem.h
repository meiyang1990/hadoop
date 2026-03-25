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
 * @file FileSystem.h
 * @brief 本地MapReduce原生任务文件系统抽象层头文件
 *
 * 该文件定义了原生MapReduce任务使用的文件系统抽象接口，以及本地文件输入输出流实现，
 * 为原生任务提供统一的文件读写能力，屏蔽不同文件系统实现细节。
 */

#ifndef FILESYSTEM_H_
#define FILESYSTEM_H_

#include <string>
#include "NativeTask.h"
#include "lib/Streams.h"

namespace NativeTask {

class FileSystem;

/**
 * @class FileInputStream
 * @brief 本地文件输入流实现，基于阻塞IO读取本地文件
 *
 * 继承自InputStream通用接口，提供本地文件的seek、tell、read、close操作，
 * 用于原生MapReduce任务读取本地磁盘上的输入数据。
 */
/**
 * Local raw filesystem file input stream
 * with blocking semantics
 */
class FileInputStream : public InputStream {
private:
  string _path;
  int _fd;
  Counter * _bytesRead;
public:
  FileInputStream(const string & path);
  virtual ~FileInputStream();

  /**
   * @brief 移动文件读指针到指定位置
   * @param position 目标偏移位置（字节）
   */
  virtual void seek(uint64_t position);

  /**
   * @brief 获取当前文件读指针位置
   * @return 当前偏移位置（字节）
   */
  virtual uint64_t tell();

  /**
   * @brief 从文件读取指定长度数据到缓冲区
   * @param buff 目标缓冲区指针
   * @param length 期望读取长度（字节）
   * @return 实际读取的字节数，返回-1表示到达文件末尾
   */
  virtual int32_t read(void * buff, uint32_t length);

  /**
   * @brief 关闭输入流，释放文件描述符
   */
  virtual void close();
};

/**
 * @class FileOutputStream
 * @brief 本地文件输出流实现，基于阻塞IO写入本地文件
 *
 * 继承自OutputStream通用接口，提供本地文件的tell、write、flush、close操作，
 * 用于原生MapReduce任务将输出数据写入本地磁盘。
 */
/**
 * Local raw filesystem file output stream
 * with blocking semantics
 */
class FileOutputStream : public OutputStream {
private:
  string _path;
  int _fd;
  Counter * _bytesWrite;
public:
  FileOutputStream(const string & path, bool overwite = true);
  virtual ~FileOutputStream();

  /**
   * @brief 获取当前文件写指针位置
   * @return 当前偏移位置（字节）
   */
  virtual uint64_t tell();

  /**
   * @brief 将缓冲区数据写入文件
   * @param buff 源数据缓冲区指针
   * @param length 要写入的数据长度（字节）
   */
  virtual void write(const void * buff, uint32_t length);

  /**
   * @brief 刷新缓冲区，将内核缓存数据写入磁盘
   */
  virtual void flush();

  /**
   * @brief 关闭输出流，释放文件描述符
   */
  virtual void close();
};


/**
 * @class FileEntry
 * @brief 文件条目信息，用于文件系统列表操作返回结果
 *
 * 存储单个文件/目录的名称和类型信息，供list操作使用。
 */
class FileEntry {
public:
  string name;
  bool isDirectory;
};

/**
 * @class FileSystem
 * @brief 文件系统抽象接口，定义统一文件操作接口
 *
 * 为不同文件系统实现提供统一抽象基类，定义了打开文件、创建文件、获取文件长度、
 * 列出目录内容、删除文件、检查文件存在、创建目录等核心操作。
 * 提供获取本地文件系统实例的静态方法。
 */
/**
 * FileSystem interface
 */
class FileSystem {
protected:
  FileSystem() {
  }
public:
  virtual ~FileSystem() {
  }

  /**
   * @brief 打开指定路径的文件，返回输入流
   * @param path 文件路径
   * @return 分配的输入流对象，失败返回NULL
   */
  virtual InputStream * open(const string & path) {
    return NULL;
  }

  /**
   * @brief 创建指定路径的文件，返回输出流
   * @param path 文件路径
   * @param overwrite 是否覆盖已存在文件，默认为true
   * @return 分配的输出流对象，失败返回NULL
   */
  virtual OutputStream * create(const string & path, bool overwrite = true) {
    return NULL;
  }

  /**
   * @brief 获取指定路径文件的长度
   * @param path 文件路径
   * @return 文件长度（字节）
   */
  virtual uint64_t getLength(const string & path) {
    return 0;
  }

  /**
   * @brief 列出指定目录下的所有文件条目
   * @param path 目录路径
   * @param status 输出参数，存储返回的文件条目列表
   * @return 列表操作成功返回true，失败返回false
   */
  virtual bool list(const string & path, vector<FileEntry> & status) {
    return false;
  }

  /**
   * @brief 删除指定路径的文件或目录
   * @param path 要删除的路径
   */
  virtual void remove(const string & path) {
  }

  /**
   * @brief 检查指定路径的文件是否存在
   * @param path 要检查的路径
   * @return 存在返回true，不存在返回false
   */
  virtual bool exists(const string & path) {
    return false;
  }

  /**
   * @brief 递归创建指定路径的所有目录
   * @param path 要创建的目录路径
   */
  virtual void mkdirs(const string & path) {
  }

  /**
   * @brief 获取本地文件系统单例实例
   * @return 本地文件系统实例引用
   */
  static FileSystem & getLocal();
};

} // namespace NativeTask

#endif /* FILESYSTEM_H_ */