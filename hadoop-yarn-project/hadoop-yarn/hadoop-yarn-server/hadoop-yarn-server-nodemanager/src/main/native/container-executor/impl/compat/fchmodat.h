// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain copy of the License at
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
 * @file fchmodat.h
 * @brief fchmodat函数兼容实现头文件，为不支持该系统调用的平台提供兼容实现
 * @details YARN NodeManager容器执行器需要修改指定目录下文件权限，本文件提供fchmodat的兼容实现
 */

#ifndef _FCHMODAT_H_
#define _FCHMODAT_H_

#include <sys/stat.h>

#include <unistd.h>

/** 不跟随符号链接修改权限标志位定义 */
#define AT_SYMLINK_NOFOLLOW 0x01

/**
 * @brief 兼容实现fchmodat系统调用，修改指定目录下文件的权限
 * 
 * @param[in] fd 目标文件所在目录的文件描述符
 * @param[in] path 相对于fd目录的目标文件路径
 * @param[in] mode 要设置的权限模式
 * @param[in] flag 控制标志，支持AT_SYMLINK_NOFOLLOW不跟随符号链接
 * @return int 成功返回0，失败返回-1并设置errno
 */
static int
fchmodat(int fd, const char *path, mode_t mode, int flag)
{
  int cfd, error, ret;

  // 保存当前工作目录的文件描述符
  cfd = open(".", O_RDONLY | O_DIRECTORY);
  if (cfd == -1)
    return (-1);

  // 切换工作目录到目标目录
  if (fchdir(fd) == -1) {
    error = errno;
    (void)close(cfd);
    errno = error;
    return (-1);
  }

  // 根据标志选择是否跟随符号链接修改权限
  if (flag == AT_SYMLINK_NOFOLLOW)
    ret = lchmod(path, mode);
  else
    ret = chmod(path, mode);

  // 恢复原始工作目录，清理资源
  error = errno;
  (void)fchdir(cfd);
  (void)close(cfd);
  errno = error;
  return (ret);
}

#endif  /* !_FCHMODAT_H_ */