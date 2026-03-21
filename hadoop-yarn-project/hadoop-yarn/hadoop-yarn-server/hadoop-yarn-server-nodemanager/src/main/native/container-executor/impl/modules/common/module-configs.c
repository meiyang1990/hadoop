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
 * @file module-configs.c
 * YARN NodeManager 容器执行器公共模块配置处理实现
 * 提供模块启用状态检查等配置处理能力
 */

#include "module-configs.h"
#include "util.h"
#include "modules/common/constants.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

// 模块启用状态配置项的键名
#define ENABLED_CONFIG_KEY "module.enabled"

/**
 * 检查指定模块是否在配置中启用
 * @param section_cfg 配置节指针，包含模块的配置信息
 * @param module_name 模块名称，用于日志输出
 * @return 1表示模块启用，0表示模块禁用
 */
int module_enabled(const struct section* section_cfg, const char* module_name) {
  // 读取模块启用配置项的值
  char* enabled_str = get_section_value(ENABLED_CONFIG_KEY, section_cfg);
  int enabled = 0;
  if (enabled_str && 0 == strcmp(enabled_str, "true")) {
    enabled = 1;
  } else {
    // 模块未启用，输出禁用日志
    fprintf(LOGFILE, "Module %s is disabled\n", module_name);
  }

  // 释放配置值分配的内存
  free(enabled_str);
  return enabled;
}