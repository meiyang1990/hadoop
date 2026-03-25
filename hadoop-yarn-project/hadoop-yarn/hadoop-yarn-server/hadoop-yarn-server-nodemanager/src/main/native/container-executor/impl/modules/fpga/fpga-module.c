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
 * @file fpga-module.c
 * @brief YARN NodeManager FPGA设备隔离模块，通过cgroups实现容器对FPGA设备的访问控制
 *
 * 核心功能：根据请求将指定FPGA设备加入cgroup黑名单，禁止容器访问未分配的FPGA设备
 * 实现机制：利用Linux cgroups devices子系统控制容器对FPGA字符设备的访问权限
 */

#include "configuration.h"
#include "container-executor.h"
#include "utils/string-utils.h"
#include "modules/fpga/fpga-module.h"
#include "modules/cgroups/cgroups-operations.h"
#include "modules/common/module-configs.h"
#include "modules/common/constants.h"
#include "util.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <getopt.h>
#include <unistd.h>

#define EXCLUDED_FPGAS_OPTION "excluded_fpgas"
#define CONTAINER_ID_OPTION "container_id"
#define DEFAULT_INTEL_MAJOR_NUMBER 246
#define MAX_CONTAINER_ID_LEN 128

/** FPGA模块配置节缓存 */
static const struct section* cfg_section;

/**
 * @brief 内部处理FPGA设备黑名单请求，更新容器cgroups配置
 * @param update_cgroups_parameters_func_p 更新cgroups参数的回调函数
 * @param n_minor_devices_to_block 需要禁止访问的次设备号数量
 * @param minor_devices 需要禁止访问的次设备号数组
 * @param container_id 目标容器ID
 * @return 0表示成功，非0表示失败
 */
static int internal_handle_fpga_request(
    update_cgroups_parameters_function update_cgroups_parameters_func_p,
    size_t n_minor_devices_to_block, int minor_devices[],
    const char* container_id) {
  char* allowed_minor_numbers_str = NULL;
  int* allowed_minor_numbers = NULL;
  size_t n_allowed_minor_numbers = 0;
  int return_code = 0;

  if (n_minor_devices_to_block == 0) {
    // 没有需要禁止的设备，直接返回
    return 0;
  }

  // 从配置中获取FPGA主设备号，未配置则使用默认Intel FPGA主设备号
  int major_device_number;
  char* major_number_str = get_section_value(FPGA_MAJOR_NUMBER_CONFIG_KEY,
     cfg_section);
  if (!major_number_str || 0 == major_number_str[0]) {
    // Intel FPGA默认主设备号
    major_device_number = DEFAULT_INTEL_MAJOR_NUMBER;
  } else {
    major_device_number = strtol(major_number_str, NULL, 0);
  }

  // 从配置中获取允许YARN管理的FPGA次设备号列表，未配置表示全部允许
  allowed_minor_numbers_str = get_section_value(
      FPGA_ALLOWED_DEVICES_MINOR_NUMBERS,
      cfg_section);
  if (!allowed_minor_numbers_str || 0 == allowed_minor_numbers_str[0]) {
    allowed_minor_numbers = NULL;
  } else {
    // 解析逗号分隔的次设备号列表
    int rc = get_numbers_split_by_comma(allowed_minor_numbers_str,
                                        &allowed_minor_numbers,
                                        &n_allowed_minor_numbers);
    if (0 != rc) {
      fprintf(ERRORFILE,
          "Failed to get allowed minor device numbers from cfg, value=%s\n",
          allowed_minor_numbers_str);
      return_code = -1;
      goto cleanup;
    }

    // 校验要禁止的设备都在允许管理列表内
    for (int i = 0; i < n_minor_devices_to_block; i++) {
      int found = 0;
      for (int j = 0; j < n_allowed_minor_numbers; j++) {
        if (minor_devices[i] == allowed_minor_numbers[j]) {
          found = 1;
          break;
        }
      }

      if (!found) {
        fprintf(ERRORFILE,
          "Trying to blacklist device with minor-number=%d which is not on allowed list\n",
          minor_devices[i]);
        return_code = -1;
        goto cleanup;
      }
    }
  }

  // 调用cgroups接口将每个要禁止的设备加入黑名单
  for (int i = 0; i < n_minor_devices_to_block; i++) {
    char param_value[128];
    memset(param_value, 0, sizeof(param_value));
    snprintf(param_value, sizeof(param_value), "c %d:%d rwm",
             major_device_number, minor_devices[i]);

    int rc = update_cgroups_parameters_func_p("devices", "deny",
      container_id, param_value);

    if (0 != rc) {
      fprintf(ERRORFILE, "CGroups: Failed to update cgroups\n");
      return_code = -1;
      goto cleanup;
    }
  }

cleanup:
  // 释放分配的内存
  if (major_number_str) {
    free(major_number_str);
  }
  if (allowed_minor_numbers) {
    free(allowed_minor_numbers);
  }
  if (allowed_minor_numbers_str) {
    free(allowed_minor_numbers_str);
  }

  return return_code;
}

/**
 * @brief 重新加载FPGA模块配置，从全局配置中读取FPGA模块配置节
 */
void reload_fpga_configuration() {
  cfg_section = get_configuration_section(FPGA_MODULE_SECTION_NAME, get_cfg());
}

/*
 * FPGA请求命令行格式:
 *
 * c-e --module-fpga --excluded_fpgas 0,1,3 --container_id container_x_y
 */

/**
 * @brief 处理FPGA设备隔离请求，解析命令行参数并调用内部处理逻辑
 * @param func 更新cgroups参数的回调函数
 * @param module_name 模块名称
 * @param module_argc 模块参数个数
 * @param module_argv 模块参数数组
 * @return 0表示成功，非0表示失败
 */
int handle_fpga_request(update_cgroups_parameters_function func,
    const char* module_name, int module_argc, char** module_argv) {
  // 如果配置未加载，先加载配置
  if (!cfg_section) {
    reload_fpga_configuration();
  }

  // 检查模块是否启用
  if (!module_enabled(cfg_section, FPGA_MODULE_SECTION_NAME)) {
    fprintf(ERRORFILE,
      "Please make sure fpga module is enabled before using it.\n");
    return -1;
  }

  // 定义长命令行参数
  static struct option long_options[] = {
    {EXCLUDED_FPGAS_OPTION, required_argument, 0, 'e' },
    {CONTAINER_ID_OPTION, required_argument, 0, 'c' },
    {0, 0, 0, 0}
  };

  int rc = 0;
  int c = 0;
  int option_index = 0;

  int* minor_devices = NULL;
  char container_id[MAX_CONTAINER_ID_LEN];
  memset(container_id, 0, sizeof(container_id));
  size_t n_minor_devices_to_block = 0;
  int failed = 0;

  // 重置getopt索引
  optind = 1;
  // 解析命令行参数
  while((c = getopt_long(module_argc, module_argv, "e:c:",
                         long_options, &option_index)) != -1) {
    switch(c) {
      case 'e':
        // 解析需要禁止的FPGA次设备号列表
        rc = get_numbers_split_by_comma(optarg, &minor_devices,
          &n_minor_devices_to_block);
        if (0 != rc) {
          fprintf(ERRORFILE,
            "Failed to get minor devices number from command line, value=%s\n",
            optarg);
          failed = 1;
          goto cleanup;
        }
        break;
      case 'c':
        // 校验并保存容器ID
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
          "Unknown option in fpga command character %d %c, optionindex = %d\n",
          c, c, optind);
        failed = 1;
        goto cleanup;
    }
  }

  // 检查容器ID是否提供
  if (0 == container_id[0]) {
    fprintf(ERRORFILE,
      "[%s] --container_id must be specified.\n", __func__);
    failed = 1;
    goto cleanup;
  }

  // 检查排除设备列表是否提供
  if (!minor_devices) {
     // 没有需要排除的设备，跳过处理
     fprintf(ERRORFILE,
     "--excluded-fpgas is not specified, skip cgroups call.\n");
     goto cleanup;
  }

  // 调用内部处理逻辑更新cgroups
  failed = internal_handle_fpga_request(func, n_minor_devices_to_block,
         minor_devices,
         container_id);

cleanup:
  // 释放分配的内存
  if (minor_devices) {
    free(minor_devices);
  }
  return failed;
}