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
 * @file docker-util.c
 * @brief YARN NodeManager容器执行器Docker工具实现，提供Docker命令构建、权限校验、镜像信任检查等能力
 *
 * 该文件属于YARN NodeManager本地容器执行器模块，负责在Linux节点上处理Docker容器的启动、管理命令构建，
 * 并实现了安全管控逻辑：镜像信任校验、允许列表管控、特权容器权限检查等，保障Docker容器运行安全。
 */

#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <ctype.h>
#include "../modules/common/module-configs.h"
#include "docker-util.h"
#include "string-utils.h"
#include "util.h"
#include "container-executor.h"
#include <grp.h>
#include <pwd.h>
#include <errno.h>
#include "mount-utils.h"

// entry point模式全局标志，控制是否允许特权容器
int entry_point = 0;

/**
 * @brief 读取并校验Docker命令配置文件
 * @param command_file 命令配置文件路径
 * @param docker_command 预期的Docker命令类型
 * @param command_config 输出，存储读取到的配置
 * @return 0表示成功，否则返回对应错误码
 */
static int read_and_verify_command_file(const char *command_file, const char *docker_command,
                                        struct configuration *command_config) {
  int ret = 0;
  // 读取配置文件到结构体
  ret = read_config(command_file, command_config);
  if (ret != 0) {
    return INVALID_DOCKER_COMMAND_FILE;
  }
  // 获取配置中指定的命令类型
  char *command = get_configuration_value("docker-command", DOCKER_COMMAND_FILE_SECTION, command_config);
  // 检查命令是否匹配预期
  if (command == NULL || (strcmp(command, docker_command) != 0)) {
    ret = INCORRECT_DOCKER_COMMAND;
  }
  free(command);
  return ret;
}

/**
 * @brief 添加参数到Docker命令参数列表
 * @param args 参数列表结构体
 * @param string 要添加的参数字符串
 * @return 0表示成功，-1表示失败
 */
static int add_to_args(args *args, const char *string) {
  if (string == NULL) {
    return -1;
  }
  if (args->data == NULL || args->length >= DOCKER_ARG_MAX) {
    return -1;
  }
  // 复制参数字符串，保证内存独立
  char *clone = strdup(string);
  if (clone == NULL) {
    return -1;
  }
  if (args->data != NULL) {
    args->data[args->length] = clone;
    args->length++;
  }
  return 0;
}

/**
 * @brief 重置参数列表，释放所有已分配参数内存
 */
void reset_args(args *args) {
  int i = 0;
  if (args == NULL) {
    return;
  }
  for (i = 0; i < args->length; i++) {
    free(args->data[i]);
  }
  args->length = 0;
}

/**
 * @brief 提取参数列表，生成可供execv使用的NULL结尾的参数数组
 * @param args 输入参数列表
 * @return 分配好的execv格式参数数组，调用者需要释放内存
 */
char** extract_execv_args(args* args) {
  char** copy = (char**)malloc((args->length + 1) * sizeof(char*));
  for (int i = 0; i < args->length; i++) {
    copy[i] = args->data[i];
  }
  copy[args->length] = NULL;
  args->length = 0;
  return copy;
}

/**
 * @brief 从配置读取参数并添加到Docker命令
 * @param command_config Docker命令配置
 * @param key 配置项键名
 * @param param Docker命令参数前缀
 * @param with_argument 是否需要拼接配置值
 * @param args 输出参数列表
 * @return 0表示成功，否则返回错误码
 */
static int add_param_to_command(const struct configuration *command_config, const char *key, const char *param,
                                const int with_argument, args *args) {
  int ret = 0;
  char *tmp_buffer = NULL;
  char *value = get_configuration_value(key, DOCKER_COMMAND_FILE_SECTION, command_config);
  if (value != NULL) {
    if (with_argument) {
      // 拼接参数前缀和配置值
      tmp_buffer = make_string("%s%s", param, value);
      ret = add_to_args(args, tmp_buffer);
      free(tmp_buffer);
    } else if (strcmp(value, "true") == 0) {
      // 布尔参数，值为true才添加
      ret = add_to_args(args, param);
    }
    free(value);
    if (ret != 0) {
      ret = BUFFER_TOO_SMALL;
    }
  }
  return ret;
}

/**
 * @brief 检查Docker镜像是否在受信任注册中心列表中
 * @param command_config Docker命令配置，包含镜像名称和特权标记
 * @param conf 节点执行器全局配置，包含受信任/特权注册中心列表
 * @return 0表示信任，否则返回错误码
 */
int check_trusted_image(const struct configuration *command_config, const struct configuration *conf) {
  int found = 0;
  int i = 0;
  int ret = 0;
  int no_registry_prefix_in_image_name = 0;
  char *image_name = get_configuration_value("image", DOCKER_COMMAND_FILE_SECTION, command_config);
  char *privileged = NULL;
  char **privileged_registry = NULL;
  privileged = get_configuration_value("privileged", DOCKER_COMMAND_FILE_SECTION, command_config);
  // 如果容器请求特权模式，从特权注册中心列表校验
  if (privileged != NULL && strcasecmp(privileged, "true") == 0 ) {
    privileged_registry = get_configuration_values_delimiter("docker.privileged-containers.registries", CONTAINER_EXECUTOR_CFG_DOCKER_SECTION, conf, ",");
  }
  // 非特权模式，从通用受信任注册中心列表校验
  if (privileged_registry == NULL) {
    privileged_registry = get_configuration_values_delimiter("docker.trusted.registries", CONTAINER_EXECUTOR_CFG_DOCKER_SECTION, conf, ",");
  }
  char *registry_ptr = NULL;
  if (image_name == NULL) {
    ret = INVALID_DOCKER_IMAGE_NAME;
    goto free_and_exit;
  }
  // 判断镜像名称是否不包含注册中心前缀（官方library镜像）
  if (strchr(image_name, '/') == NULL) {
    no_registry_prefix_in_image_name = 1;
  }
  if (privileged_registry != NULL) {
    for (i = 0; privileged_registry[i] != NULL; i++) {
      // "library"表示信任Docker Hub官方顶级镜像
      if (strncmp(privileged_registry[i], "library", strlen("library")) == 0) {
        if (no_registry_prefix_in_image_name) {
          // 镜像不带前缀命中library规则，标记为信任
          found = 1;
          fprintf(LOGFILE, "image: %s is a trusted top-level image.\n", image_name);
          break;
        }
      }
      // 统一处理注册中心域名格式，自动补全末尾斜杠
      int len = strlen(privileged_registry[i]);
      if (privileged_registry[i][len - 1] != '/') {
        registry_ptr = (char *) alloc_and_clear_memory(len + 2, sizeof(char));
        strncpy(registry_ptr, privileged_registry[i], len);
        registry_ptr[len] = '/';
        registry_ptr[len + 1] = '\0';
      } else {
        registry_ptr = strdup(privileged_registry[i]);
      }
      // 检查镜像名称前缀是否匹配受信任注册中心
      if (strncmp(image_name, registry_ptr, strlen(registry_ptr))==0) {
        found=1;
        free(registry_ptr);
        break;
      }
      free(registry_ptr);
    }
  }
  if (found==0) {
    fprintf(ERRORFILE, "image: %s is not trusted.\n", image_name);
    ret = INVALID_DOCKER_IMAGE_TRUST;
  }

free_and_exit:
  free(privileged);
  free(image_name);
  free_values(privileged_registry);
  return ret;
}

/**
 * @brief 校验tmpfs挂载格式是否合法
 * @param mount 挂载路径字符串
 * @return 0表示合法，非0表示不合法
 */
static int is_valid_tmpfs_mount(const char *mount) {
  const char *regex_str = "^/[^:]+$";
  // execute_regex_match返回0表示匹配成功
  return execute_regex_match(regex_str, mount) == 0;
}

/**
 * @brief 校验端口映射格式是否合法
 * @param ports_mapping 端口映射字符串
 * @return 0表示合法，非0表示不合法
 */
static int is_valid_ports_mapping(const char *ports_mapping) {
  const char *regex_str = "^:[0-9]+|^[0-9]+:[0-9]+|^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.)"
                          "{3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]):[0-9]+:[0-9]+$";
  // execute_regex_match返回0表示匹配成功
  return execute_regex_match(regex_str, ports_mapping) == 0;
}

/**
 * @brief 条件性添加Docker参数，会进行权限校验和信任检查
 * @param command_config Docker命令配置
 * @param executor_cfg 节点执行器全局配置
 * @param key 配置项键名
 * @param allowed_key 允许列表配置键名
 * @param param Docker参数前缀
 * @param multiple_values 是否支持多个值
 * @param prefix 前缀分隔字符（用于提取前缀匹配允许列表）
 * @param args 输出参数列表
 * @return 0表示成功，否则返回错误码
 */
static int add_param_to_command_if_allowed(const struct configuration *command_config,
                                           const struct configuration *executor_cfg,
                                           const char *key, const char *allowed_key, const char *param,
                                           const int multiple_values, const char prefix,
                                           args *args) {
  char *tmp_buffer = NULL;
  char *tmp_ptr = NULL;
  char **values = NULL;
  // 获取节点配置中允许该参数的值列表
  char **permitted_values = get_configuration_values_delimiter(allowed_key,
                                                               CONTAINER_EXECUTOR_CFG_DOCKER_SECTION, executor_cfg,
                                                               ",");
  int i = 0, j = 0, permitted = 0, ret = 0;
  // 读取用户请求的参数值
  if (multiple_values) {
    values = get_configuration_values_delimiter(key, DOCKER_COMMAND_FILE_SECTION, command_config, ",");
  } else {
    values = (char **) alloc_and_clear_memory(2, sizeof(char *));
    values[0] = get_configuration_value(key, DOCKER_COMMAND_FILE_SECTION, command_config);
    values[1] = NULL;
    if (values[0] == NULL) {
      ret = 0;
      goto free_and_exit;
    }
  }

  if (values != NULL) {
    // 非网络参数需要先校验镜像信任，不信任镜像不允许添加特殊权限参数
    if (strcmp(key, "net") != 0) {
      if (check_trusted_image(command_config, executor_cfg) != 0) {
        fprintf(ERRORFILE, "Disable %s for untrusted image\n", key);
        ret = INVALID_DOCKER_IMAGE_TRUST;
        goto free_and_exit;
      }
    }

    if (permitted_values != NULL) {
      // 遍历用户请求的每个参数值
      for (i = 0; values[i] != NULL; ++i) {
        permitted = 0;
        // 如果需要前缀匹配，提取前缀部分
        if(prefix != 0) {
          tmp_ptr = strchr(values[i], prefix);
          if (tmp_ptr == NULL) {
            fprintf(ERRORFILE, "Prefix char '%c' not found in '%s'\n",
                    prefix, values[i]);
            ret = -1;
            goto free_and_exit;
          }
        }
        // 和允许列表逐一匹配
        char* dst = NULL;
        char* pattern = NULL;

        for (j = 0; permitted_values[j] != NULL; ++j) {
          if (prefix == 0) {
            // 无前缀，直接全匹配
            ret = strcmp(values[i], permitted_values[j]);
          } else {
            // 允许值是正则表达式，使用正则匹配
            if (is_regex(permitted_values[j])) {
              dst = strndup(values[i], tmp_ptr - values[i]);
              pattern = strdup(permitted_values[j] + 6);
              ret = execute_regex_match(pattern, dst);
              free(dst);
              free(pattern);
            } else {
              // 前缀匹配
              ret = strncmp(values[i], permitted_values[j], tmp_ptr - values[i]);
            }
          }
          if (ret == 0) {
            permitted = 1;
            break;
          }
        }
        // 匹配通过，添加参数到命令
        if (permitted == 1) {
          tmp_buffer = make_string("%s%s", param, values[i]);
          ret = add_to_args(args, tmp_buffer);
          free(tmp_buffer);
          if (ret != 0) {
            fprintf(ERRORFILE, "Output buffer too small\n");
            ret = BUFFER_TOO_SMALL;
            goto free_and_exit;
          }
        } else {
          fprintf(ERRORFILE, "Invalid param '%s' requested\n", values[i]);
          ret = -1;
          goto free_and_exit;
        }
      }
    } else {
      // 允许列表为空，拒绝所有请求
      fprintf(ERRORFILE, "Invalid param '%s' requested, "
          "permitted values list is empty\n", values[0]);
      ret = -1;
      goto free_and_exit;
    }
  }

free_and_exit:
  free_values(values);
  free_values(permitted_values);
  return ret;
}

/**
 * @brief 添加docker config路径参数到命令
 */
static int add_docker_config_param(const struct configuration *command_config, args *args) {
  return add_param_to_command(command_config, "docker-config", "--config=", 1, args);
}

/**
 * @brief 校验Docker卷名称格式是否合法
 * @param volume_name 卷名称
 * @return 0表示合法，非0表示不合法
 */
static int validate_volume_name(const char *volume_name) {
  const char *regex_str = "^[a-zA-Z0-9]([a-zA-Z0-9_.-]*)$";
  return execute_regex_match(regex_str, volume_name);
}

/**
 * @brief 校验YARN容器名称格式是否合法
 * @param container_name 容器名称
 * @return 0表示合法，否则返回错误码
 */
static int validate_container_name(const char *container_name) {
  const char *CONTAINER_NAME_PREFIX = "container_";
  // 必须以container_开头，后接合法容器ID
  if (0 == strncmp(container_name, CONTAINER_NAME_PREFIX, strlen(CONTAINER_NAME_PREFIX))) {
    if (1 == validate_container_id(container_name)) {
      return 0;
    }
  }
  fprintf(ERRORFILE, "Specified container_id=%s is invalid\n", container_name);
  return INVALID_DOCKER_CONTAINER_NAME;
}

/**
 * @brief 从配置获取Docker inspect最大重试次数
 * @param conf 节点执行器配置
 * @return 重试次数，默认10次
 */