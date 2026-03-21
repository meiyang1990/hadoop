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
 * @file gpu-module.c
 * YARN NodeManager GPU资源隔离模块实现
 * 基于cgroups devices子系统实现容器GPU设备访问控制，为YARN容器分配指定GPU设备
 */

#include "configuration.h"
#include "container-executor.h"
#include "utils/string-utils.h"
#include "modules/gpu/gpu-module.h"
#include "modules/cgroups/cgroups-operations.h"
#include "modules/common/module-configs.h"
#include "modules/common/constants.h"
#include "util.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <getopt.h>
#include <unistd.h>

#define EXCLUDED_GPUS_OPTION "excluded_gpus"
#define CONTAINER_ID_OPTION "container_id"
#define DEFAULT_NVIDIA_MAJOR_NUMBER 195
#define MAX_CONTAINER_ID_LEN 128

// 保存GPU模块配置节指针
static const struct section* cfg_section;

/**
 * 内部处理GPU设备隔离请求，通过cgroups禁止容器访问未分配的GPU设备
 * @param update_cgroups_parameters_func_p 更新cgroups参数的函数指针
 * @param n_minor_devices_to_block 需要禁止访问的GPU设备次设备号数量
 * @param minor_devices[] 需要禁止访问的GPU设备次设备号数组
 * @param container_id 目标容器ID
 * @return 0成功，非0失败
 */
static int internal_handle_gpu_request(
    update_cgroups_parameters_func update_cgroups_parameters_func_p,
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

  // 从配置获取主设备号，未配置则使用NVIDIA默认值
  int major_device_number;
  char* major_number_str = get_section_value(GPU_MAJOR_NUMBER_CONFIG_KEY,
     cfg_section);
  if (!major_number_str || 0 == major_number_str[0]) {
    // NVIDIA设备默认主设备号
    major_device_number = DEFAULT_NVIDIA_MAJOR_NUMBER;
  } else {
    major_device_number = strtol(major_number_str, NULL, 0);
  }

  // 从配置获取允许被YARN管理的GPU次设备号列表，未配置则表示全部设备都可使用
  allowed_minor_numbers_str = get_section_value(
      GPU_ALLOWED_DEVICES_MINOR_NUMBERS,
      cfg_section);
  if (!allowed_minor_numbers_str || 0 == allowed_minor_numbers_str[0]) {
    allowed_minor_numbers = NULL;
  } else {
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

    // 校验要禁止的设备确实在允许管理的列表中
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

  // 调用cgroups接口将需要禁止的设备加入黑名单
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
 * 重新加载GPU模块配置，从全局配置中获取GPU模块配置节
 */
void reload_gpu_configuration() {
  cfg_section = get_configuration_section(GPU_MODULE_SECTION_NAME, get_cfg());
}

/*
 * GPU请求命令行格式:
 *
 * c-e --module-gpu --excluded_gpus 0,1,3 --container_id container_x_y
 */

/**
 * 处理GPU资源隔离请求，解析命令行参数并调用内部逻辑完成cgroups配置
 * @param func 更新cgroups参数的函数指针
 * @param module_name 模块名称
 * @param module_argc 模块参数个数
 * @param module_argv 模块参数数组
 * @return 0成功，非0失败
 */
int handle_gpu_request(update_cgroups_parameters_func func,
    const char* module_name, int module_argc, char** module_argv) {
  if (!cfg_section) {
    reload_gpu_configuration();
  }

  // 检查GPU模块是否启用
  if (!module_enabled(cfg_section, GPU_MODULE_SECTION_NAME)) {
    fprintf(ERRORFILE,
      "Please make sure gpu module is enabled before using it.\n");
    return -1;
  }

  // 定义长命令行参数
  static struct option long_options[] = {
    {EXCLUDED_GPUS_OPTION, required_argument, 0, 'e' },
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
        // 解析需要禁止的GPU次设备号列表（逗号分隔）
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
          "Unknown option in gpu command character %d %c, optionindex = %d\n",
          c, c, optind);
        failed = 1;
        goto cleanup;
    }
  }

  // 校验容器ID必须提供
  if (0 == container_id[0]) {
    fprintf(ERRORFILE,
      "[%s] --container_id must be specified.\n", __func__);
    failed = 1;
    goto cleanup;
  }

  // 校验必须提供需要禁止的GPU列表
  if (!minor_devices) {
     // 没有要禁止的设备，跳过处理
     fprintf(ERRORFILE,
     "--excluded_gpus is not specified, skip cgroups call.\n");
     goto cleanup;
  }

  // 调用内部处理逻辑完成GPU隔离配置
  failed = internal_handle_gpu_request(func, n_minor_devices_to_block,
         minor_devices,
         container_id);

cleanup:
  // 释放分配的内存
  if (minor_devices) {
    free(minor_devices);
  }
  return failed;
}