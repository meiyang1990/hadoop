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
 * @file devices-module.c
 * YARN NodeManager 容器设备管控模块实现，通过cgroups控制容器可访问的设备权限
 */

#include "configuration.h"
#include "container-executor.h"
#include "utils/string-utils.h"
#include "modules/devices/devices-module.h"
#include "modules/cgroups/cgroups-operations.h"
#include "modules/common/module-configs.h"
#include "modules/common/constants.h"
#include "util.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <getopt.h>
#include <unistd.h>
#include <sys/stat.h>

#define EXCLUDED_DEVICES_OPTION "excluded_devices"
#define ALLOWED_DEVICES_OPTION "allowed_devices"
#define CONTAINER_ID_OPTION "container_id"
#define MAX_CONTAINER_ID_LEN 128

static const struct section* cfg_section;

/**
 * 在字符串列表中搜索指定令牌，支持部分匹配
 * @param list 待搜索的字符串列表
 * @param token 要查找的令牌
 * @return 1表示找到，0表示未找到
 */
// Search a string in a string list, return 1 when found
static int search_in_list(char** list, char* token) {
  int i = 0;
  char** iterator = list;
  // search token in  list
  while (iterator[i] != NULL) {
    if (strstr(token, iterator[i]) != NULL ||
        strstr(iterator[i], token) != NULL) {
      // Found deny device in allowed list
      return 1;
    }
    i++;
  }
  return 0;
}

/**
 * 判断给定设备号是否对应一个块设备
 * @param value 设备号字符串，格式为"主设备号:次设备号"
 * @return 1表示是块设备，0表示不是或出错
 */
static int is_block_device(const char* value) {
  int is_block = 0;
  int max_path_size = 512;
  char* block_path = malloc(max_path_size);
  if (block_path == NULL) {
    fprintf(ERRORFILE, "Failed to allocate memory for sys device path string.\n");
    goto cleanup;
  }
  if (snprintf(block_path, max_path_size, "/sys/dev/block/%s",
    value) < 0) {
    fprintf(ERRORFILE, "Failed to construct system block device path.\n");
    goto cleanup;
  }
  struct stat sb;
  // 判断/sys下对应路径是否存在，存在即为块设备
  if (stat(block_path, &sb) == 0) {
    is_block = 1;
  }
cleanup:
  if (block_path) {
    free(block_path);
  }
  return is_block;
}

/**
 * 内部处理设备权限请求，更新容器cgroups设备配置
 * @param update_cgroups_parameters_func_p 更新cgroups参数的函数指针
 * @param deny_devices_number_tokens 待禁用设备列表
 * @param allow_devices_number_tokens 待允许设备列表
 * @param container_id 容器ID
 * @return 0表示成功，非0表示失败
 */
static int internal_handle_devices_request(
    update_cgroups_param_function update_cgroups_parameters_func_p,
    char** deny_devices_number_tokens,
    char** allow_devices_number_tokens,
    const char* container_id) {
  int return_code = 0;

  char** ce_denied_numbers = NULL;
  // 从容器执行器配置读取全局禁用设备列表
  char* ce_denied_str = get_section_value(DEVICES_DENIED_NUMBERS,
     cfg_section);
  // Get denied "major:minor" device numbers from cfg, if not set, means all
  // devices can be used by YARN.
  if (ce_denied_str != NULL) {
    ce_denied_numbers = split_delimiter(ce_denied_str, ",");
    if (NULL == ce_denied_numbers) {
      fprintf(ERRORFILE,
          "Invalid value set for %s, value=%s\n",
          DEVICES_DENIED_NUMBERS,
          ce_denied_str);
      return_code = -1;
      goto cleanup;
    }
    // 检查待允许设备是否在全局禁用列表中，如有则拒绝请求
    char** allow_iterator = allow_devices_number_tokens;
    int allow_count = 0;
    while (allow_iterator[allow_count] != NULL) {
      if (search_in_list(ce_denied_numbers, allow_iterator[allow_count])) {
        fprintf(ERRORFILE,
          "Trying to allow device with device number=%s which is not permitted in container-executor.cfg. %s\n",
          allow_iterator[allow_count],
          "It could be caused by a mismatch of devices reported by device plugin");
        return_code = -1;
        goto cleanup;
      }
      allow_count++;
    }

    // 将全局禁用设备添加到cgroups deny规则
    char** ce_iterator = ce_denied_numbers;
    int ce_count = 0;
    while (ce_iterator[ce_count] != NULL) {
      // 跳过已在传入禁用列表中的重复设备
      if (search_in_list(deny_devices_number_tokens, ce_iterator[ce_count])) {
        ce_count++;
        continue;
      }
      char param_value[128];
      // 默认假设是字符设备
      char type = 'c';
      memset(param_value, 0, sizeof(param_value));
      if (is_block_device(ce_iterator[ce_count])) {
        type = 'b';
      }
      // 构造符合cgroups格式的规则
      snprintf(param_value, sizeof(param_value), "%c %s rwm",
               type,
               ce_iterator[ce_count]);
      // 更新设备cgroups配置
      int rc = update_cgroups_parameters_func_p("devices", "deny",
        container_id, param_value);

      if (0 != rc) {
        fprintf(ERRORFILE, "CGroups: Failed to update cgroups. %s\n", param_value);
        return_code = -1;
        goto cleanup;
      }
      ce_count++;
    }
  }

  // 处理Java侧传入的待禁用设备
  char** iterator = deny_devices_number_tokens;
  int count = 0;
  char* value = NULL;
  int index = 0;
  while (iterator[count] != NULL) {
    // 将传入格式的横杠替换为空格，适配cgroups参数格式："c-242:0-rwm" -> "c 242:0 rwm"
    value = iterator[count];
    index = 0;
    while (value[index] != '\0') {
      if (value[index] == '-') {
        value[index] = ' ';
      }
      index++;
    }
    // 更新设备cgroups配置
    int rc = update_cgroups_parameters_func_p("devices", "deny",
      container_id, iterator[count]);

    if (0 != rc) {
      fprintf(ERRORFILE, "CGroups: Failed to update cgroups\n");
      return_code = -1;
      goto cleanup;
    }
    count++;
  }

cleanup:
  if (ce_denied_numbers != NULL) {
    free_values(ce_denied_numbers);
  }
  return return_code;
}

/**
 * 重新加载设备模块配置，从全局配置获取设备模块配置段
 */
void reload_devices_configuration() {
  cfg_section = get_configuration_section(DEVICES_MODULE_SECTION_NAME, get_cfg());
}

/*
 * Format of devices request commandline:
 * The excluded_devices is comma separated device cgroups values with device type.
 * The "-" will be replaced with " " to match the cgroups parameter
 * c-e --module-devices \
 * --excluded_devices b-8:16-rwm,c-244:0-rwm,c-244:1-rwm \
 * --allowed_devices 8:32,8:48,243:2 \
 * --container_id container_x_y
 */
/**
 * 处理外部传入的设备管控请求，解析参数并调用内部处理逻辑
 * @param func 更新cgroups参数的函数指针
 * @param module_name 模块名称
 * @param module_argc 模块参数个数
 * @param module_argv 模块参数数组
 * @return 0表示成功，非0表示失败
 */
int handle_devices_request(update_cgroups_param_function func,
    const char* module_name, int module_argc, char** module_argv) {
  if (!cfg_section) {
    reload_devices_configuration();
  }

  // 检查模块是否已在配置中启用
  if (!module_enabled(cfg_section, DEVICES_MODULE_SECTION_NAME)) {
    fprintf(ERRORFILE,
      "Please make sure devices module is enabled before using it.\n");
    return -1;
  }

  // 定义命令行长选项
  static struct option long_options[] = {
    {EXCLUDED_DEVICES_OPTION, required_argument, 0, 'e' },
    {ALLOWED_DEVICES_OPTION, required_argument, 0, 'a' },
    {CONTAINER_ID_OPTION, required_argument, 0, 'c' },
    {0, 0, 0, 0}
  };

  int c = 0;
  int option_index = 0;

  char** deny_device_value_tokens = NULL;
  char** allow_device_value_tokens = NULL;
  char container_id[MAX_CONTAINER_ID_LEN];
  memset(container_id, 0, sizeof(container_id));
  int failed = 0;

  optind = 1;
  // 解析命令行参数
  while((c = getopt_long(module_argc, module_argv, "e:a:c:",
                         long_options, &option_index)) != -1) {
    switch(c) {
      case 'e':
        // 分割禁用设备列表
        deny_device_value_tokens = split_delimiter(optarg, ",");
        break;
      case 'a':
        // 分割允许设备列表
        allow_device_value_tokens = split_delimiter(optarg, ",");
        break;
      case 'c':
        // 验证并保存容器ID
        if (!validate_container_id(optarg)) {
          fprintf(ERRORFILE,
            "Specified container_id=%s is invalid\n", optarg);
          failed = 1;
          goto cleanup;
        }
        strncpy(container_id, optarg, MAX_CONTAINER_ID_LEN);
        break;
      default:
        fprintf(ERRORFILE,
          "Unknown option in devices command character %d %c, optionindex = %d\n",
          c, c, optind);
        failed = 1;
        goto cleanup;
    }
  }

  // 检查必须传入容器ID参数
  if (0 == container_id[0]) {
    fprintf(ERRORFILE,
      "[%s] --container_id must be specified.\n", __func__);
    failed = 1;
    goto cleanup;
  }

  // 检查必须传入禁用设备列表参数
  if (NULL == deny_device_value_tokens) {
     // Devices number is null, skip following call.
     fprintf(ERRORFILE, "--excluded_devices is not specified, skip cgroups call.\n");
     goto cleanup;
  }

  // 调用内部处理逻辑更新cgroups配置
  failed = internal_handle_devices_request(func,
         deny_device_value_tokens,
         allow_device_value_tokens,
         container_id);

cleanup:
  // 释放分配的内存
  if (deny_device_value_tokens) {
    free_values(deny_device_value_tokens);
  }
  if (allow_device_value_tokens) {
    free_values(allow_device_value_tokens);
  }
  return failed;
}