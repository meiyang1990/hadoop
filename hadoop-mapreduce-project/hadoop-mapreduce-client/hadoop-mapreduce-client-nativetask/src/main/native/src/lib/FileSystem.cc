// 这个文件已经全部加上中文注释
/*
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
 * @file FileSystem.cc
 * 本地文件系统操作实现，提供MapReduce本地任务的文件读写能力
 * 属于Hadoop MapReduce NativeTask模块，实现本地文件系统的输入输出流和基础文件操作
 */

#include <errno.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
#include "lib/commons.h"
#include "util/StringUtil.h"
#include "lib/jniutils.h"
#include "NativeTask.h"
#include "lib/TaskCounters.h"
#include "lib/NativeObjectFactory.h"
#include "lib/Path.h"
#include "lib/FileSystem.h"

namespace NativeTask {

/////////////////////////////////////////////////////////////

/**
 * @brief 构造函数，打开指定文件用于读取
 * @param path 要打开的文件路径
 */
FileInputStream::FileInputStream(const string & path) {
  _fd = ::open(path.c_str(), O_RDONLY);
  if (_fd >= 0) {
    _path = path;
  } else {
    _fd = -1;
    THROW_EXCEPTION_EX(IOException, "Can't open file for read: [%s]", path.c_str());
  }
  // 获取文件读流量计数器，用于监控指标统计
  _bytesRead = NativeObjectFactory::GetCounter(TaskCounters::FILESYSTEM_COUNTER_GROUP,
      TaskCounters::FILE_BYTES_READ);
}

FileInputStream::~FileInputStream() {
  close();
}

/**
 * @brief 移动文件读取指针到指定位置
 * @param position 目标偏移位置
 */
void FileInputStream::seek(uint64_t position) {
  ::lseek(_fd, position, SEEK_SET);
}

/**
 * @brief 获取当前文件读取指针位置
 * @return 当前偏移位置
 */
uint64_t FileInputStream::tell() {
  return ::lseek(_fd, 0, SEEK_CUR);
}

/**
 * @brief 从文件读取指定长度数据到缓冲区
 * @param buff 目标缓冲区
 * @param length 要读取的字节数
 * @return 实际读取的字节数，-1表示读取失败
 */
int32_t FileInputStream::read(void * buff, uint32_t length) {
  int32_t ret = ::read(_fd, buff, length);
  if (ret > 0) {
    // 增加读流量统计
    _bytesRead->increase(ret);
  }
  return ret;
}

/**
 * @brief 关闭输入流，释放文件描述符
 */
void FileInputStream::close() {
  if (_fd >= 0) {
    ::close(_fd);
    _fd = -1;
  }
}

/////////////////////////////////////////////////////////////

/**
 * @brief 构造函数，打开指定文件用于写入
 * @param path 要打开的文件路径
 * @param overwite 是否覆盖已有文件
 */
FileOutputStream::FileOutputStream(const string & path, bool overwite) {
  int flags = 0;
  if (overwite) {
    flags = O_WRONLY | O_CREAT | O_TRUNC;
  } else {
    flags = O_WRONLY | O_CREAT | O_EXCL;
  }
  // 保存原有umask，计算最终文件权限
  mode_t mask = umask(0);
  umask(mask);
  _fd = ::open(path.c_str(), flags, (0666 & ~mask));
  if (_fd >= 0) {
    _path = path;
  } else {
    _fd = -1;
    THROW_EXCEPTION_EX(IOException, "Can't open file for write: [%s]", path.c_str());
  }
  // 获取文件写流量计数器，用于监控指标统计
  _bytesWrite = NativeObjectFactory::GetCounter(TaskCounters::FILESYSTEM_COUNTER_GROUP,
      TaskCounters::FILE_BYTES_WRITTEN);
}

FileOutputStream::~FileOutputStream() {
  close();
}

/**
 * @brief 获取当前文件写入指针位置
 * @return 当前偏移位置
 */
uint64_t FileOutputStream::tell() {
  return ::lseek(_fd, 0, SEEK_CUR);
}

/**
 * @brief 将缓冲区数据写入文件
 * @param buff 源数据缓冲区
 * @param length 要写入的字节数
 */
void FileOutputStream::write(const void * buff, uint32_t length) {
  if (::write(_fd, buff, length) < length) {
    THROW_EXCEPTION(IOException, "::write error");
  }
  // 增加写流量统计
  _bytesWrite->increase(length);
}

/**
 * @brief 刷新缓冲区，本地写不需要额外刷新
 */
void FileOutputStream::flush() {
}

/**
 * @brief 关闭输出流，释放文件描述符
 */
void FileOutputStream::close() {
  if (_fd >= 0) {
    ::close(_fd);
    _fd = -1;
  }
}

/////////////////////////////////////////////////////////////

/**
 * @class RawFileSystem
 * @brief 本地原生文件系统实现，提供基础文件系统操作能力
 * 支持本地文件的打开、创建、删除、目录创建、列表查询等操作，处理file://格式路径
 */
class RawFileSystem : public FileSystem {
 protected:
  /**
   * @brief 处理路径格式，去除file:前缀获取实际本地路径
   * @param path 输入路径
   * @return 实际本地文件路径
   */
  string getRealPath(const string & path) {
    if (StringUtil::StartsWith(path, "file:")) {
      return path.substr(5);
    }
    return path;
  }

 public:
  /**
   * @brief 打开指定路径文件用于读取
   * @param path 要打开的文件路径
   * @return 文件输入流指针
   */
  InputStream * open(const string & path) {
    return new FileInputStream(getRealPath(path));
  }

  /**
   * @brief 创建指定路径文件用于写入，自动创建不存在的父目录
   * @param path 要创建的文件路径
   * @param overwrite 是否覆盖已有文件
   * @return 文件输出流指针
   */
  OutputStream * create(const string & path, bool overwrite) {
    string np = getRealPath(path);
    string parent = Path::GetParent(np);
    if (parent.length() > 0) {
      if (!exists(parent)) {
        mkdirs(parent);
      }
    }
    return new FileOutputStream(np, overwrite);
  }

  /**
   * @brief 获取指定文件的长度
   * @param path 文件路径
   * @return 文件大小（字节）
   */
  uint64_t getLength(const string & path) {
    struct stat st;
    if (::stat(getRealPath(path).c_str(), &st) != 0) {
      char buff[256];
      strerror_r(errno, buff, 256);
      THROW_EXCEPTION(IOException,
          StringUtil::Format("stat path %s failed, %s", path.c_str(), buff));
    }
    return st.st_size;
  }

  /**
   * @brief 列出指定目录下的所有文件条目
   * @param path 目录路径
   * @param status 输出，保存文件条目列表
   * @return 列出成功返回true，否则返回false
   */
  bool list(const string & path, vector<FileEntry> & status) {
    DIR * dp;
    struct dirent * dirp;
    if ((dp = opendir(path.c_str())) == NULL) {
      return false;
    }

    FileEntry temp;
    while ((dirp = readdir(dp)) != NULL) {
      temp.name = dirp->d_name;
      // 跳过当前目录和上级目录条目
      if (temp.name == "." || temp.name == "..") {
        continue;
      }
/* Use Linux d_type if available, otherwise stat(2) the path */
#ifdef DT_DIR
      // 如果系统支持d_type，直接从dirent获取目录类型
      temp.isDirectory = dirp->d_type & DT_DIR;
#else
      // 否则通过stat获取文件类型
      const string p = path + "/" + temp.name;
      struct stat sb;
      temp.isDirectory = stat(p.c_str(), &sb) == 0 && S_ISDIR(sb.st_mode) == 0;
#endif
      status.push_back(temp);
    }
    closedir(dp);
    return true;
  }

  /**
   * @brief 删除指定路径的文件或目录
   * @param path 要删除的路径
   */
  void remove(const string & path) {
    if (!exists(path)) {
      LOG("[FileSystem] remove file %s not exists, ignore", path.c_str());
      return;
    }
    if (::remove(getRealPath(path).c_str()) != 0) {
      int err = errno;
      // 如果remove失败，尝试调用系统rm -rf命令递归删除
      if (::system(StringUtil::Format("rm -rf %s", path.c_str()).c_str()) == 0) {
        return;
      }
      char buff[256];
      strerror_r(err, buff, 256);
      THROW_EXCEPTION(IOException,
          StringUtil::Format("FileSystem: remove path %s failed, %s", path.c_str(), buff));
    }
  }

  /**
   * @brief 判断指定路径是否存在
   * @param path 目标路径
   * @return 存在返回true，否则返回false
   */
  bool exists(const string & path) {
    struct stat st;
    if (::stat(getRealPath(path).c_str(), &st) != 0) {
      return false;
    }
    return true;
  }

  /**
   * @brief 递归创建多级目录
   * @param path 要创建的目录路径
   * @param nmode 目录权限模式
   * @return 0表示成功，非0表示失败
   */
  int mkdirs(const string & path, mode_t nmode) {
    string np = getRealPath(path);
    struct stat sb;

    // 如果路径已经存在
    if (stat(np.c_str(), &sb) == 0) {
      // 不是目录则返回错误
      if (S_ISDIR(sb.st_mode) == 0) {
        return 1;
      }
      // 已经是目录则返回成功
      return 0;
    }

    string npathstr = np;
    char * npath = const_cast<char*>(npathstr.c_str());

    // 跳过开头的多个斜杠
    char * p = npath;
    while (*p == '/')
      p++;

    // 逐段创建目录
    while (NULL != (p = strchr(p, '/'))) {
      *p = '\0';
      if (stat(npath, &sb) != 0) {
        // 当前分段不存在，创建目录
        if (mkdir(npath, nmode)) {
          return 1;
        }
      } else if (S_ISDIR(sb.st_mode) == 0) {
        // 当前分段已存在但不是目录，返回错误
        return 1;
      }
      // 恢复斜杠，继续处理下一段
      *p++ = '/';
      while (*p == '/')
        p++;
    }

    // 创建最后一级目录
    if (stat(npath, &sb) && mkdir(npath, nmode)) {
      return 1;
    }
    return 0;
  }

  /**
   * @brief 递归创建多级目录，默认权限0755，失败抛出异常
   * @param path 要创建的目录路径
   */
  void mkdirs(const string & path) {
    int ret = mkdirs(path, 0755);
    if (ret != 0) {
      THROW_EXCEPTION_EX(IOException, "mkdirs [%s] failed", path.c_str());
    }
  }
};

///////////////////////////////////////////////////////////

extern RawFileSystem RawFileSystemInstance;

// 全局单例本地文件系统实例
RawFileSystem RawFileSystemInstance = RawFileSystem();

/**
 * @brief 获取本地文件系统单例实例
 * @return 本地文件系统引用
 */
FileSystem & FileSystem::getLocal() {
  return RawFileSystemInstance;
}

} // namespace NativeTask