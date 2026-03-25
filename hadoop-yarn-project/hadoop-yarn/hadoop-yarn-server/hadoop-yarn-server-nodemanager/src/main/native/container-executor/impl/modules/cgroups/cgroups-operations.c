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
 * @file cgroups-operations.c
 * @brief Linux cgroups 操作实现，为YARN NodeManager容器提供cgroups资源限制配置能力
 */

#include "configuration.h"
#include "container-executor.h"
#include "utils/string-utils.h"
#include "utils/path-utils.h"
#include "modules/common/module-configs.h"
#include "modules/common/constants.h"
#include "modules/cgroups/cgroups-operations.h"
#include "util.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

#define MAX_PATH_LEN 4096

static const struct section* cgroup_cfg_section = NULL;

/**
 * @brief 重新加载cgroups相关配置，从全局配置中获取cgroups配置段
 */
void reload_cgroups_configuration() {
  cgroup_cfg_section = get_configuration_section(CGROUPS_SECTION_NAME, get_cfg());
}

/**
 * @brief 构造待写入的cgroups参数文件完整路径
 * @param hierarchy_name cgroups层级名称（如cpu、memory）
 * @param param_name cgroups参数名称
 * @param group_id 容器对应的cgroups分组ID
 * @return 构造成功返回完整路径字符串，失败返回NULL
 */
char* get_cgroups_path_to_write(
    const char* hierarchy_name,
    const char* param_name,
    const char* group_id) {
  int failed = 0;
  char* buffer = NULL;
  const char* cgroups_root = get_section_value(CGROUPS_ROOT_KEY,
     cgroup_cfg_section);
  const char* yarn_hierarchy_name = get_section_value(
     CGROUPS_YARN_HIERARCHY_KEY, cgroup_cfg_section);

  // 检查cgroups根路径配置是否存在
  if (!cgroups_root || cgroups_root[0] == 0) {
    fprintf(ERRORFILE, "%s is not defined in container-executor.cfg\n",
      CGROUPS_ROOT_KEY);
    failed = 1;
    goto cleanup;
  }

  // 检查YARN cgroups层级名称配置是否存在
  if (!yarn_hierarchy_name || yarn_hierarchy_name[0] == 0) {
    fprintf(ERRORFILE, "%s is not defined in container-executor.cfg\n",
      CGROUPS_YARN_HIERARCHY_KEY);
    failed = 1;
    goto cleanup;
  }

  // 分配路径缓冲区
  buffer = malloc(MAX_PATH_LEN + 1);
  if (!buffer) {
    fprintf(ERRORFILE, "Failed to allocate memory for output path.\n");
    failed = 1;
    goto cleanup;
  }

  // 拼接完整cgroups参数文件路径
  if (snprintf(buffer, MAX_PATH_LEN, "%s/%s/%s/%s/%s.%s",
    cgroups_root, hierarchy_name, yarn_hierarchy_name,
    group_id, hierarchy_name, param_name) < 0) {
    fprintf(ERRORFILE, "Failed to print output path.\n");
    failed = 1;
    goto cleanup;
  }

cleanup:
  free((void *) cgroups_root);
  free((void *) yarn_hierarchy_name);
  if (failed) {
    if (buffer) {
      free(buffer);
    }
    return NULL;
  }
  return buffer;
}

/**
 * @brief 更新cgroups参数值，将指定值写入对应cgroups参数文件
 * @param hierarchy_name cgroups层级名称（如cpu、memory）
 * @param param_name cgroups参数名称
 * @param group_id 容器对应的cgroups分组ID
 * @param value 待写入的参数值
 * @return 成功返回0，失败返回-1
 */
int update_cgroups_parameters(
   const char* hierarchy_name,
   const char* param_name,
   const char* group_id,
   const char* value) {
// 非Linux系统不支持cgroups，直接返回错误
#ifndef __linux
  fprintf(ERRORFILE, "Failed to update cgroups parameters, not supported\n");
  return -1;
#endif
  int failure = 0;

  // 如果配置未加载，先加载配置
  if (!cgroup_cfg_section) {
    reload_cgroups_configuration();
  }

  // 获取参数文件完整路径
  char* full_path = get_cgroups_path_to_write(hierarchy_name, param_name,
    group_id);

  if (!full_path) {
    fprintf(ERRORFILE,
      "Failed to get cgroups path to write, it should be a configuration issue\n");
    failure = 1;
    goto cleanup;
  }

  // 验证路径安全性，防止路径遍历攻击
  if (!verify_path_safety(full_path)) {
    failure = 1;
    goto cleanup;
  }

  // 检查参数文件是否存在
  struct stat sb;
  if (stat(full_path, &sb) != 0) {
    fprintf(ERRORFILE, "CGroups: Could not find file to write, %s\n", full_path);
    failure = 1;
    goto cleanup;
  }

  // 记录更新操作日志
  fprintf(ERRORFILE, "CGroups: Updating cgroups, path=%s, value=%s\n",
    full_path, value);

  // 打开参数文件进行追加写入
  FILE *f;
  f = fopen(full_path, "a");
  if (!f) {
    fprintf(ERRORFILE, "CGroups: Failed to open cgroups file, %s\n", full_path);
    failure = 1;
    goto cleanup;
  }
  // 写入参数值
  if (fprintf(f, "%s", value) < 0) {
    fprintf(ERRORFILE, "CGroups: Failed to write cgroups file, %s\n", full_path);
    fclose(f);
    failure = 1;
    goto cleanup;
  }
  // 关闭文件
  if (fclose(f) != 0) {
    fprintf(ERRORFILE, "CGroups: Failed to close cgroups file, %s\n", full_path);
    failure = 1;
    goto cleanup;
  }

cleanup:
  if (full_path) {
    free(full_path);
  }
  return -failure;
}