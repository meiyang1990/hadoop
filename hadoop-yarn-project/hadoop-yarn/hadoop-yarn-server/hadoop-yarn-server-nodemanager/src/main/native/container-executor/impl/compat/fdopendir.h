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
 * @file fdopendir.h
 * @brief fdopendir函数兼容实现，为不支持该函数的老版本系统提供实现
 * @details 用于将文件描述符转换为目录流DIR*结构体，适配不同操作系统兼容性
 */

#ifndef _FDOPENDIR_H_
#define _FDOPENDIR_H_

#include <fcntl.h>
#include <stdarg.h>
#include <unistd.h>

/**
 * @brief 将指定文件描述符转换为目录流，兼容实现
 * @param fd 要转换的目录文件描述符
 * @return 成功返回目录流指针，失败返回NULL并设置errno
 */
DIR *
fdopendir(int fd)
{
  int cfd, error;
  DIR *dfd;

  // 保存当前工作目录的文件描述符
  cfd = open(".", O_RDONLY | O_DIRECTORY);
  if (cfd == -1)
    return (NULL);

  // 切换到目标文件描述符对应的目录
  if (fchdir(fd) == -1) {
    error = errno;
    (void)close(cfd);
    errno = error;
    return (NULL);
  }

  // 打开当前目录(已切换到目标目录)获取DIR结构体
  dfd=opendir(".");
  error = errno;
  // 切回原来的工作目录
  (void)fchdir(cfd);
  // 关闭保存的工作目录描述符
  (void)close(cfd);
  // 恢复错误码
  errno = error;
  return (dfd);
}

#endif  /* !_FDOPENDIR_H_ */