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
 * @file oom_listener_main.c
 * YARN NodeManager Linux cgroup OOM事件监听器主程序
 * 监听指定内存cgroup的OOM事件，发生OOM时向标准输出输出事件通知
 * 同时提供OOM测试功能，用于验证监听器功能
 */

#if __linux

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>

#include "oom_listener.h"

extern inline void cleanup(_oom_listener_descriptors *descriptors);

/**
 * 打印程序使用帮助信息并退出
 */
void print_usage(void) {
  fprintf(stderr, "oom-listener");
  fprintf(stderr, "Listen to OOM events in a cgroup");
  fprintf(stderr, "usage to listen: oom-listener <cgroup directory>\n");
  fprintf(stderr, "usage to test: oom-listener oom [<pgid>]\n");
  fprintf(stderr, "example listening: oom-listener /sys/fs/cgroup/memory/hadoop-yarn | xxd -c 8\n");
  fprintf(stderr, "example oom to test: bash -c 'echo $$ >/sys/fs/cgroup/memory/hadoop-yarn/tasks;oom-listener oom'\n");
  fprintf(stderr, "example container overload: sudo -u <user> bash -c 'echo $$ && oom-listener oom 0' >/sys/fs/cgroup/memory/hadoop-yarn/<container>/tasks\n");
  exit(EXIT_FAILURE);
}

/*
  Test an OOM situation adding the pid
  to the group pgid and calling malloc in a loop
  This can be used to test OOM listener. See examples above.
*/
/**
 * OOM测试函数：通过持续分配内存触发OOM，用于监听器功能测试
 * @param pgids 要加入的进程组ID，为NULL则不修改进程组
 */
void test_oom_infinite(char* pgids) {
  if (pgids != NULL) {
    int pgid = atoi(pgids);
    // 将当前进程移动到指定进程组
    setpgid(0, pgid);
  }
  // 持续分配内存直到分配失败触发OOM
  while(1) {
    char* p = (char*)malloc(4096);
    if (p != NULL) {
      // 触摸内存页确保实际分配物理内存
      p[0] = 0xFF;
    } else {
      // 内存分配失败，退出
      exit(1);
    }
  }
}

/*
 A command that receives a memory cgroup directory and
 listens to the events in the directory.
 It will print a new line on every out of memory event
 to the standard output.
 usage:
 oom-listener <cgroup>
*/
/**
 * 主函数：处理命令行参数，启动OOM监听或OOM测试
 * @param argc 命令行参数个数
 * @param argv 命令行参数数组
 * @return 退出码，0表示正常退出，非0表示异常
 */
int main(int argc, char *argv[]) {
  // 如果参数是测试模式，进入OOM测试
  if (argc >= 2 &&
      strcmp(argv[1], "oom") == 0)
    test_oom_infinite(argc < 3 ? NULL : argv[2]);

  // 参数个数不对，打印帮助
  if (argc != 2)
    print_usage();

  // 初始化OOM监听器描述符，设置默认值
  _oom_listener_descriptors descriptors = {
      .command = argv[0],
      .event_fd = -1,
      .event_control_fd = -1,
      .oom_control_fd = -1,
      .event_control_path = {0},
      .oom_control_path = {0},
      .oom_command = {0},
      .oom_command_len = 0,
      .watch_timeout = 1000
  };

  // 启动OOM监听，事件输出到标准输出
  int ret = oom_listener(&descriptors, argv[1], STDOUT_FILENO);

  // 清理打开的文件描述符
  cleanup(&descriptors);

  return ret;
}

#else

/*
 This tool uses Linux specific functionality,
 so it is not available for other operating systems
*/
/**
 * 非Linux平台下不支持该工具，直接返回错误
 * @return 固定返回1，表示不支持
 */
int main() {
  return 1;
}

#endif