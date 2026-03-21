// 这个文件已经全部加上中文注释
/*-
 * Copyright (c) 2012 The FreeBSD Foundation
 * All rights reserved.
 *
 * This software was developed by Pawel Jakub Dawidek under sponsorship from
 * the FreeBSD Foundation.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHORS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/**
 * @file openat.h
 * @brief 为不支持openat系统调用的系统提供openat兼容性实现
 * @details 该头文件提供基于fchdir模拟实现的openat兼容函数，解决旧版本Linux系统
 *          用于YARN NodeManager容器执行器在不同系统间的文件操作兼容性
 */

#ifndef _OPENAT_H_
#define _OPENAT_H_

#include <fcntl.h>
#include <stdarg.h>
#include <unistd.h>

/**
 * @brief 模拟openat系统调用：相对于指定目录打开文件
 *
 * @param fd 目标目录的文件描述符
 * @param path 相对于目标目录的文件路径
 * @param flags 打开标志位
 * @param ... 如果是O_CREAT模式，则传入文件权限mode
 * @return 成功返回打开的文件描述符，失败返回-1并设置errno
 */
static int
openat(int fd, const char *path, int flags, ...)
{
  int cfd, ffd, error;

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

  // 处理可变参数：如果是创建文件，提取权限参数
  if ((flags & O_CREAT) != 0) {
    va_list ap;
    int mode;

    va_start(ap, flags);
    mode = va_arg(ap, int);
    va_end(ap);

    ffd = open(path, flags, mode);
  } else {
    ffd = open(path, flags);
  }

  // 恢复原来的工作目录并清理资源
  error = errno;
  (void)fchdir(cfd);
  (void)close(cfd);
  errno = error;
  return (ffd);
}

#endif  /* !_OPENAT_H_ */