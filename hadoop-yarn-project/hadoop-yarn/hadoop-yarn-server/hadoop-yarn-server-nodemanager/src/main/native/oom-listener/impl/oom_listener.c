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
 * @file oom_listener.c
 * @brief Linux cgroup OOM事件监听实现，用于YARN NodeManager监控容器内存溢出事件
 *
 * 该文件实现了针对Linux memory cgroup的OOM（内存溢出）事件监听，
 * 当容器发生OOM时，将事件通知回NodeManager，帮助YARN及时发现容器OOM情况。
 */

#if __linux

#include <sys/param.h>
#include <poll.h>
#include "oom_listener.h"

/**
 * @brief 打印格式化错误信息到标准错误输出
 *
 * @param file 发生错误的命令/文件名
 * @param message 格式化错误消息格式字符串
 * @param ... 可变参数列表
 */
/*
 * Print an error.
*/
static inline void print_error(const char *file, const char *message,
                        ...) {
  fprintf(stderr, "%s ", file);
  va_list arguments;
  va_start(arguments, message);
  vfprintf(stderr, message, arguments);
  va_end(arguments);
}

/**
 * @brief 监听指定cgroup的OOM事件，持续将事件转发到指定文件描述符
 *
 * @param descriptors 保存监听所需的各文件描述符与路径信息结构体
 * @param cgroup 目标内存cgroup路径
 * @param fd 用于转发OOM事件的输出文件描述符
 * @return int EXIT_SUCCESS 监听正常结束，EXIT_FAILURE 发生错误退出
 */
/*
 * Listen to OOM events in a memory cgroup. See declaration for details.
 */
int oom_listener(_oom_listener_descriptors *descriptors, const char *cgroup, int fd) {
  // 根据cgroup路径末尾是否有斜杠，选择不同的路径拼接格式
  const char *pattern =
          cgroup[MAX(strlen(cgroup), 1) - 1] == '/'
          ? "%s%s" :"%s/%s";

  /* Create an event handle, if we do not have one already*/
  // 如果还未创建eventfd，创建用于接收OOM事件的eventfd
  if (descriptors->event_fd == -1 &&
      (descriptors->event_fd = eventfd(0, 0)) == -1) {
    print_error(descriptors->command, "eventfd() failed. errno:%d %s\n",
                errno, strerror(errno));
    return EXIT_FAILURE;
  }

  /*
   * open the file to listen to (memory.oom_control)
   * and write the event handle and the file handle
   * to cgroup.event_control
   */
  // 拼接cgroup.event_control文件的完整路径
  if (snprintf(descriptors->event_control_path,
               sizeof(descriptors->event_control_path),
               pattern,
               cgroup,
               "cgroup.event_control") < 0) {
    print_error(descriptors->command, "path too long %s\n", cgroup);
    return EXIT_FAILURE;
  }

  // 打开cgroup.event_control文件，用于注册OOM事件监听
  if ((descriptors->event_control_fd = open(
      descriptors->event_control_path,
      O_WRONLY|O_CREAT, 0600)) == -1) {
    print_error(descriptors->command, "Could not open %s. errno:%d %s\n",
                descriptors->event_control_path,
                errno, strerror(errno));
    return EXIT_FAILURE;
  }

  // 拼接memory.oom_control文件的完整路径
  if (snprintf(descriptors->oom_control_path,
               sizeof(descriptors->oom_control_path),
               pattern,
               cgroup,
               "memory.oom_control") < 0) {
    print_error(descriptors->command, "path too long %s\n", cgroup);
    return EXIT_FAILURE;
  }

  // 打开memory.oom_control文件，这是OOM事件监听的目标文件
  if ((descriptors->oom_control_fd = open(
      descriptors->oom_control_path,
      O_RDONLY)) == -1) {
    print_error(descriptors->command, "Could not open %s. errno:%d %s\n",
                descriptors->oom_control_path,
                errno, strerror(errno));
    return EXIT_FAILURE;
  }

  // 将eventfd和oom_control_fd编号写入注册字符串
  if ((descriptors->oom_command_len = (size_t) snprintf(
      descriptors->oom_command,
      sizeof(descriptors->oom_command),
      "%d %d",
      descriptors->event_fd,
      descriptors->oom_control_fd)) < 0) {
    print_error(descriptors->command, "Could print %d %d\n",
                descriptors->event_control_fd,
                descriptors->oom_control_fd);
    return EXIT_FAILURE;
  }

  // 将注册信息写入cgroup.event_control，完成OOM事件监听注册
  if (write(descriptors->event_control_fd,
            descriptors->oom_command,
            descriptors->oom_command_len) == -1) {
    print_error(descriptors->command, "Could not write to %s errno:%d\n",
                descriptors->event_control_path, errno);
    return EXIT_FAILURE;
  }

  // 注册完成后关闭event_control文件
  if (close(descriptors->event_control_fd) == -1) {
    print_error(descriptors->command, "Could not close %s errno:%d\n",
                descriptors->event_control_path, errno);
    return EXIT_FAILURE;
  }
  descriptors->event_control_fd = -1;

  /*
   * Listen to events as long as the cgroup exists
   * and forward them to the fd in the argument.
   */
  // 循环监听OOM事件，直到cgroup被删除
  for (;;) {
    uint64_t u;
    ssize_t ret = 0;
    struct stat stat_buffer = {0};
    // 配置poll监听，监听eventfd的可读事件
    struct pollfd poll_fd = {
        .fd = descriptors->event_fd,
        .events = POLLIN
    };

    // 调用poll等待事件，超时时间为配置的watch_timeout
    ret = poll(&poll_fd, 1, descriptors->watch_timeout);
    if (ret < 0) {
      /* Error calling poll */
      print_error(descriptors->command,
                  "Could not poll eventfd %d errno:%d %s\n", ret,
                  errno, strerror(errno));
      return EXIT_FAILURE;
    }

    if (ret > 0) {
      /* Event counter values are always 8 bytes */
      // 从eventfd读取OOM事件，eventfd事件数据固定为8字节
      if ((ret = read(descriptors->event_fd, &u, sizeof(u)) != sizeof(u) {
        print_error(descriptors->command,
                    "Could not read from eventfd %d errno:%d %s\n", ret,
                    errno, strerror(errno));
        return EXIT_FAILURE;
      }

      /* Forward the value to the caller, typically stdout */
      // 将OOM事件转发给调用方提供的文件描述符（通常是管道）
      if ((ret = write(fd, &u, sizeof(u))) != sizeof(u)) {
        print_error(descriptors->command,
                    "Could not write to pipe %d errno:%d %s\n", ret,
                    errno, strerror(errno));
        return EXIT_FAILURE;
      }
    } else if (ret == 0) {
      /* Timeout has elapsed*/

      /* Quit, if the cgroup is deleted */
      // 超时时检查cgroup目录是否存在，不存在则退出监听
      if (stat(cgroup, &stat_buffer) != 0) {
        break;
      }
    }
  }
  return EXIT_SUCCESS;
}

#endif