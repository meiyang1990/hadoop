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
 * @file path-utils.c
 * @brief YARN NodeManager 容器执行器路径工具实现，提供路径安全校验和目录存在性检查
 */

#include "util.h"

#include <dirent.h>
#include <errno.h>
#include <strings.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/**
 * @brief 验证路径安全性，检查是否包含上级目录跳转符".."，防止路径遍历攻击
 * @param path 待校验的路径字符串
 * @return 1 路径安全，0 路径不安全或内存分配失败
 */
int verify_path_safety(const char* path) {
  if (!path || path[0] == 0) {
    return 1;
  }

  // 复制路径字符串，避免修改原路径
  char* dup = strdup(path);
  if (!dup) {
    fprintf(ERRORFILE, "%s: Failed to allocate memory for path.\n", __func__);
    return 0;
  }

  // 按/分割路径，逐个分段检查
  char* p = strtok(dup, "/");
  int succeeded = 1;

  while (p != NULL) {
    if (0 == strcmp(p, "..")) {
      fprintf(ERRORFILE, "%s: Path included \"..\", path=%s.\n", __func__, path);
      succeeded = 0;
      break;
    }

    p = strtok(NULL, "/");
  }
  free(dup);

  return succeeded;
}

/**
 * @brief 检查目录是否不存在
 * @param path 待检查的目录路径
 * @return 0 目录存在，1 目录不存在，-1 发生错误（除不存在外的其他错误）
 */
int dir_exists(const char* path) {
  DIR* dir = opendir(path);
  if (dir) {
    closedir(dir);
    return 0;
  } else if (ENOENT == errno) {
    return 1;
  } else {
    return -1;
  }
}