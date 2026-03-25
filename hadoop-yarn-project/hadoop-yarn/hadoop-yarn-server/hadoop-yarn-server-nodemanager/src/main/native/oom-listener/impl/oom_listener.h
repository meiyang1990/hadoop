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
 * @file oom_listener.h
 * YARN NodeManager Linux平台cgroup内存OOM事件监听头文件
 * 负责监听指定内存cgroup的OOM事件，当容器发生OOM时通知NodeManager
 * 仅在Linux平台下编译生效
 */

#if __linux

#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <sys/eventfd.h>
#include <sys/stat.h>

#include <linux/limits.h>

/*
This file implements a standard cgroups out of memory listener.
*/

/**
 * OOM监听器描述符结构，保存监听所需的所有文件描述符和路径信息
 */
typedef struct _oom_listener_descriptors {
  /* 启动本进程的命令行参数 */
  const char *command;
  /* 监听OOM事件的eventfd描述符，未初始化时为-1 */
  int event_fd;
  /* cgroup.event_control控制文件的文件描述符 */
  int event_control_fd;
  /* memory.oom_control控制文件的文件描述符 */
  int oom_control_fd;
  /* cgroup.event_control文件的完整路径 */
  char event_control_path[PATH_MAX];
  /* memory.oom_control文件的完整路径 */
  char oom_control_path[PATH_MAX];
  /* 写入cgroup.event_control的绑定命令，格式为<event_fd> <oom_control_fd> */
  char oom_command[25];
  /* oom_command的实际长度 */
  size_t oom_command_len;
  /* 目录监听超时时间（毫秒） */
  int watch_timeout;
} _oom_listener_descriptors;

/**
 * 清理OOM监听器描述符中分配的所有文件资源，重置状态
 */
inline void cleanup(_oom_listener_descriptors *descriptors) {
  close(descriptors->event_fd);
  descriptors->event_fd = -1;
  close(descriptors->event_control_fd);
  descriptors->event_control_fd = -1;
  close(descriptors->oom_control_fd);
  descriptors->oom_control_fd = -1;
  descriptors->watch_timeout = 1000;
}

/**
 * 在指定内存cgroup上启动OOM事件监听器
 * @param descriptors 保存监听器状态的描述符结构，支持测试复用
 * @param cgroup 要监听的内存cgroup路径
 * @param fd 用于转发OOM事件的文件描述符，通常为标准输出
 * @return 成功返回0，失败返回错误码
 */
int oom_listener(_oom_listener_descriptors *descriptors, const char *cgroup, int fd);

#endif