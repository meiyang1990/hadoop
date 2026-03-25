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
 * @file fstatat.h
 * @brief 为不支持fstatat系统调用的系统提供兼容实现，支持基于目录文件描述符获取文件状态
 *
 * 该头文件提供POSIX标准fstatat函数的兼容实现，用于在NodeManager本地容器执行器中
 * 处理相对于指定目录描述符的文件状态查询，适配不同操作系统的系统调用差异。
 */

#ifndef _FSTATAT_H_
#define _FSTATAT_H_

#include <sys/stat.h>

#include <unistd.h>

/** 不跟随符号链接标志：查询符号链接自身状态而非指向目标的状态 */
#define AT_SYMLINK_NOFOLLOW 0x01

/**
 * @brief fstatat系统调用兼容实现，获取相对于目录描述符的文件状态
 *
 * @param fd 目标目录的文件描述符，path是相对于该目录的相对路径
 * @param path 要查询状态的文件路径，可以是绝对路径或相对于fd的相对路径
 * @param buf 输出参数，存储获取到的文件状态信息
 * @param flag 控制标志，支持AT_SYMLINK_NOFOLLOW（不跟随符号链接）
 * @return 成功返回0，失败返回-1并设置errno
 */
static int
fstatat(int fd, const char *path, struct stat *buf, int flag)
{
  int cfd, error, ret;

  // 保存当前工作目录的文件描述符，后续用于恢复
  cfd = open(".", O_RDONLY | O_DIRECTORY);
  if (cfd == -1)
    return (-1);

  // 切换当前工作目录到目标目录fd
  if (fchdir(fd) == -1) {
    error = errno;
    (void)close(cfd);
    errno = error;
    return (-1);
  }

  // 根据标志选择是否跟随符号链接获取文件状态
  if (flag == AT_SYMLINK_NOFOLLOW)
    ret = lstat(path, buf);
  else
    ret = stat(path, buf);

  // 恢复原来的工作目录并清理资源，保留错误码返回
  error = errno;
  (void)fchdir(cfd);
  (void)close(cfd);
  errno = error;
  return (ret);
}

#endif  /* !_FSTATAT_H_ */