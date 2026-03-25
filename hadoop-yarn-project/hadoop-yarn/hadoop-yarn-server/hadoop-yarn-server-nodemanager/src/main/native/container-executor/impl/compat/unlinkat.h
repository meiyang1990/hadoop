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
 * @file unlinkat.h
 * @brief unlinkat系统调用兼容性实现，为不支持该系统调用的平台提供兼容方案
 * @details 该头文件提供了unlinkat函数的用户态兼容实现，YARN NodeManager的容器执行器
 *          在不同系统平台上编译时，确保统一的目录文件删除接口
 */

#ifndef _UNLINKAT_H_
#define _UNLINKAT_H_

#include <fcntl.h>
#include <unistd.h>

// 标识需要删除的是目录，对应remove directory操作
#define AT_REMOVEDIR  0x01

/**
 * @brief 兼容实现unlinkat系统调用，在指定目录下删除文件或目录
 * 
 * @param fd 目标文件/目录所在父目录的文件描述符
 * @param path 需要删除的目标文件/目录路径
 * @param flag 操作标志：AT_REMOVEDIR表示删除目录，否则删除文件
 * @return 成功返回0，失败返回-1并设置errno
 */
static int
unlinkat(int fd, const char *path, int flag)
{
  int cfd, error, ret;

  // 保存当前工作目录的文件描述符，用于后续切回
  cfd = open(".", O_RDONLY | O_DIRECTORY);
  if (cfd == -1)
    return (-1);

  // 切换工作目录到目标父目录
  if (fchdir(fd) == -1) {
    error = errno;
    (void)close(cfd);
    errno = error;
    return (-1);
  }

  // 根据标志选择删除目录还是普通文件
  if (flag == AT_REMOVEDIR)
    ret = rmdir(path);
  else
    ret = unlink(path);

  // 恢复原始工作目录并返回结果，保存原有错误码
  error = errno;
  (void)fchdir(cfd);
  (void)close(cfd);
  errno = error;
  return (ret);
}

#endif  /* !_UNLINKAT_H_ */