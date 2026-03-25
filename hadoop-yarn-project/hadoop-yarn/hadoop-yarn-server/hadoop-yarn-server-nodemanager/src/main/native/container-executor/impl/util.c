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
 * @file util.c
 * @brief YARN NodeManager容器执行器通用工具函数实现
 * @details 提供字符串分割、内存管理、字符串修剪、正则匹配、参数转义等基础工具能力
 */

#include "util.h"
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <regex.h>
#include <stdio.h>

/**
 * @brief 按指定分隔符分割字符串，返回动态分配的字符串数组
 * @param value 待分割的输入字符串，会被修改
 * @param delim 分隔符字符串
 * @return 动态分配的字符串数组，以NULL结尾，分配失败返回NULL
 */
char** split_delimiter(char *value, const char *delim) {
  char **return_values = NULL;
  char **new_return_values;
  char *temp_tok = NULL;
  char *tempstr = NULL;
  int size = 0;
  int per_alloc_size = 10;
  int return_values_size = per_alloc_size;
  int failed = 0;

  // 初始分配容纳10个指针的数组空间
  if(value != NULL) {
    return_values = (char **) malloc(sizeof(char *) * return_values_size);
    if (!return_values) {
      fprintf(ERRORFILE, "Allocation error for return_values in %s.\n",
              __func__);
      failed = 1;
      goto cleanup;
    }
    memset(return_values, 0, sizeof(char *) * return_values_size);

    // 分割第一个token
    temp_tok = strtok_r(value, delim, &tempstr);
    if (NULL == temp_tok) {
      return_values[size++] = strdup(value);
    }
    // 循环分割剩余token
    while (temp_tok != NULL) {
      // 复制token字符串到堆内存
      temp_tok = strdup(temp_tok);
      if (NULL == temp_tok) {
        fprintf(ERRORFILE, "Allocation error in %s.\n", __func__);
        failed = 1;
        goto cleanup;
      }

      return_values[size++] = temp_tok;

      // 检查空间，预留末尾NULL指针位置，空间不足则扩容
      if (size >= return_values_size - 1) {
        return_values_size += per_alloc_size;
        new_return_values = (char **) realloc(return_values,(sizeof(char *) *
          return_values_size));
        if (!new_return_values) {
          fprintf(ERRORFILE, "Reallocation error for return_values in %s.\n",
                  __func__);
          failed = 1;
          goto cleanup;
        }
        return_values = new_return_values;

        // 将新增内存初始化为NULL
        for (int i = size; i < return_values_size; i++) {
          return_values[i] = NULL;
        }
      }
      // 获取下一个token
      temp_tok = strtok_r(NULL, delim, &tempstr);
    }
  }

  // 在数组末尾添加NULL标记，标识数组结束
  if (return_values != NULL) {
    return_values[size] = NULL;
  }

cleanup:
  // 分配失败则释放已分配内存，返回NULL
  if (failed) {
    free_values(return_values);
    return NULL;
  }

  return return_values;
}

/**
 * @brief 按%分隔符分割字符串，用于解析配置项
 */
char** split(char *value) {
  return split_delimiter(value, "%");
}

/**
 * @brief 释放split_delimiter返回的字符串数组
 * @param values 待释放的字符串数组
 */
void free_values(char** values) {
  if (values != NULL) {
    int idx = 0;
    while (values[idx]) {
      free(values[idx]);
      idx++;
    }
    free(values);
  }
}

/**
 * @brief 修剪字符串首尾空白字符
 * @param input 输入字符串
 * @return 动态分配的修剪后字符串，分配失败直接退出进程
 */
char* trim(const char* input) {
    const char *val_begin;
    const char *val_end;
    char *ret;

    if (input == NULL) {
      return NULL;
    }

    val_begin = input;
    val_end = input + strlen(input);

    // 跳过开头所有空白字符
    while (val_begin < val_end && isspace(*val_begin))
      val_begin++;
    // 回跳末尾所有空白字符
    while (val_end > val_begin && isspace(*(val_end - 1)))
      val_end--;

    // 分配存储修剪后字符串的空间，多留一个字节给结束符
    ret = (char *) malloc(
            sizeof(char) * (val_end - val_begin + 1));
    if (ret == NULL) {
      fprintf(ERRORFILE, "Allocation error\n");
      exit(OUT_OF_MEMORY);
    }

    // 复制有效内容并添加结束符
    strncpy(ret, val_begin, val_end - val_begin);
    ret[val_end - val_begin] = '\0';
    return ret;
}

/**
 * @brief 执行正则表达式匹配
 * @param regex_str 正则表达式字符串
 * @param input 待匹配输入字符串
 * @return 匹配成功返回0，匹配失败或编译错误返回1，编译错误直接退出进程
 */
int execute_regex_match(const char *regex_str, const char *input) {
  regex_t regex;
  int regex_match;
  // 编译正则表达式
  if (0 != regcomp(&regex, regex_str, REG_EXTENDED|REG_NOSUB)) {
    fprintf(LOGFILE, "Unable to compile regex.\n");
    exit(ERROR_COMPILING_REGEX);
  }
  // 执行匹配
  regex_match = regexec(&regex, input, (size_t) 0, NULL, 0);
  // 释放正则表达式资源
  regfree(&regex);
  if(0 == regex_match) {
    return 0;
  }
  return 1;
}

/**
 * @brief 转义字符串中的单引号，用于shell命令拼接
 * @param str 输入字符串
 * @return 动态分配的转义后字符串，分配失败直接退出进程
 */
char* escape_single_quote(const char *str) {
  int p = 0;
  int i = 0;
  char replacement[] = "'\"'\"'";
  size_t replacement_length = strlen(replacement);
  // 计算最坏情况下转义后所需空间（每个字符都转义）
  size_t ret_size = strlen(str) * replacement_length + 1;
  char *ret = (char *) alloc_and_clear_memory(ret_size, sizeof(char));
  if(ret == NULL) {
    exit(OUT_OF_MEMORY);
  }
  // 遍历原字符串逐个处理
  while(str[p] != '\0') {
    if(str[p] == '\'') {
      // 遇到单引号替换为转义序列
      strncat(ret, replacement, ret_size - strlen(ret));
      i += replacement_length;
    }
    else {
      // 普通字符直接复制
      ret[i] = str[p];
      i++;
    }
    p++;
  }
  // 添加字符串结束符
  ret[i] = '\0';
  return ret;
}

/**
 * @brief 将参数转义单引号后添加到参数字符串末尾，用于构建shell命令
 * @param str 目标字符串指针，会动态扩容
 * @param size 当前分配的缓冲区大小
 * @param param 参数名
 * @param arg 参数值
 */
void quote_and_append_arg(char **str, size_t *size, const char* param, const char *arg) {
  // 转义参数值中的单引号
  char *tmp = escape_single_quote(arg);
  const char *append_format = "%s'%s' ";
  // 计算拼接后所需的长度
  size_t append_size = snprintf(NULL, 0, append_format, param, tmp);
  append_size += 1;   // 为结束符预留空间
  size_t len_str = strlen(*str);
  size_t new_size = len_str + append_size;
  // 如果当前缓冲区不足，扩容
  if (new_size > *size) {
      *size = new_size + QUOTE_AND_APPEND_ARG_GROWTH;
      *str = (char *) realloc(*str, *size);
      if (*str == NULL) {
          exit(OUT_OF_MEMORY);
      }
  }
  // 拼接参数到目标字符串末尾
  char *cur_ptr = *str + len_str;
  sprintf(cur_ptr, append_format, param, tmp);
  // 释放转义后字符串
  free(tmp);
}

/**
 * @brief 根据错误码获取对应的错误描述字符串
 * @param error_code 错误码
 * @return 错误描述字符串常量指针
 */
const char *get_error_message(const int error_code) {
    switch (error_code) {
      case INVALID_ARGUMENT_NUMBER:
        return "Invalid argument number";
      case INVALID_COMMAND_PROVIDED:
        return "Invalid command provided";
      case INVALID_NM_ROOT_DIRS:
        return "Invalid NM root dirs";
      case SETUID_OPER_FAILED:
        return "setuid operation failed";
      case UNABLE_TO_EXECUTE_CONTAINER_SCRIPT:
        return "Unable to execute container script";
      case UNABLE_TO_SIGNAL_CONTAINER:
        return "Unable to signal container";
      case INVALID_CONTAINER_PID:
        return "Invalid container PID";
      case OUT_OF_MEMORY:
        return "Out of memory";
      case INITIALIZE_USER_FAILED:
        return "Initialize user failed";
      case PATH_TO_DELETE_IS_NULL:
        return "Path to delete is null";
      case INVALID_CONTAINER_EXEC_PERMISSIONS:
        return "Invalid container-executor permissions";
      case INVALID_CONFIG_FILE:
        return "Invalid config file";
      case SETSID_OPER_FAILED:
        return "setsid operation failed";
      case WRITE_PIDFILE_FAILED:
        return "Write to pidfile failed";
      case WRITE_CGROUP_FAILED:
        return "Write to cgroup failed";
      case TRAFFIC_CONTROL_EXECUTION_FAILED:
        return "Traffic control execution failed";
      case DOCKER_RUN_FAILED:
        return "Docker run failed";
      case ERROR_OPENING_DOCKER_FILE:
        return "Error opening Docker file";
      case ERROR_READING_DOCKER_FILE:
        return "Error reading Docker file";
      case FEATURE_DISABLED:
        return "Feature disabled";
      case COULD_NOT_CREATE_SCRIPT_COPY:
        return "Could not create script copy";
      case COULD_NOT_CREATE_CREDENTIALS_COPY:
        return "Could not create credentials copy";
      case COULD_NOT_CREATE_WORK_DIRECTORIES:
        return "Could not create work dirs";
      case COULD_NOT_CREATE_APP_LOG_DIRECTORIES:
        return "Could not create app log dirs";
      case COULD_NOT_CREATE_TMP_DIRECTORIES:
        return "Could not create tmp dirs";
      case ERROR_CREATE_CONTAINER_DIRECTORIES_ARGUMENTS:
        return "Error in create container directories arguments";
      case ERROR_SANITIZING_DOCKER_COMMAND:
        return "Error sanitizing Docker command";
      case DOCKER_IMAGE_INVALID:
        return "Docker image invalid";
      case ERROR_COMPILING_REGEX:
        return "Error compiling regex";
      case INVALID_CONTAINER_ID:
        return "Invalid container id";
      case DOCKER_EXEC_FAILED:
        return "Docker exec failed";
      case COULD_NOT_CREATE_KEYSTORE_COPY:
        return "Could not create keystore copy";
      case COULD_NOT_CREATE_TRUSTSTORE_COPY:
        return "Could not create truststore copy";
      case ERROR_CALLING_SETVBUF:
        return "Error calling setvbuf";
      case BUFFER_TOO_SMALL:
        return "Buffer too small";
      case INVALID_MOUNT:
        return "Invalid mount";
      case INVALID_RO_MOUNT:
        return "Invalid read-only mount";
      case INVALID_RW_MOUNT:
        return "Invalid read-write mount";
      case MOUNT_ACCESS_ERROR:
        return "Mount access error";
      case INVALID_DOCKER_COMMAND_FILE:
        return "Invalid docker command file passed";
      case INCORRECT_DOCKER_COMMAND:
        return "Incorrect command";
      case INVALID_DOCKER_CONTAINER_NAME:
        return "Invalid docker container name";
      case INVALID_DOCKER_IMAGE_NAME:
        return "Invalid docker image name";
      case INVALID_DOCKER_USER_NAME:
        return "Invalid docker user name";
      case INVALID_DOCKER_INSPECT_FORMAT:
        return "Invalid docker inspect format";
      case UNKNOWN_DOCKER_COMMAND:
        return "Unknown docker command";
      case INVALID_DOCKER_NETWORK:
        return "Invalid docker network";
      case INVALID_DOCKER_PORTS_MAPPING:
        return "Invalid docker ports mapping";
      case INVALID_DOCKER_CAPABILITY:
        return "Invalid docker capability";
      case PRIVILEGED_DOCKER_CONTAINERS_DISABLED:
        return "Privileged docker containers are disabled";
      case INVALID_DOCKER_DEVICE:
        return "Invalid docker device";
      case INVALID_DOCKER_STOP_COMMAND:
        return "Invalid docker stop command";
      case INVALID_DOCKER_KILL_COMMAND:
        return "Invalid docker kill command";
      case INVALID_DOCKER_VOLUME_DRIVER:
        return "Invalid docker volume-driver";
      case INVALID_DOCKER_VOLUME_NAME:
        return "Invalid docker volume name";
      case INVALID_DOCKER_VOLUME_COMMAND:
        return "Invalid docker volume command";
      case DOCKER_PID_HOST_DISABLED:
        return "Docker host pid namespace is disabled";
      case INVALID_DOCKER_PID_NAMESPACE:
        return "Invalid docker pid namespace";
      case INVALID_DOCKER_IMAGE_TRUST:
        return "Docker image is not trusted";
      case INVALID_DOCKER_TMPFS_MOUNT:
        return "Invalid docker tmpfs mount";
      case INVALID_DOCKER_RUNTIME:
        return "Invalid docker runtime";
      case DOCKER_SERVICE_MODE_DISABLED:
        return "Docker service mode disabled";
      case ERROR_RUNC_SETUP_FAILED:
        return "runC setup failed";
      case ERROR_RUNC_RUN_FAILED:
        return "runC run failed";
      case ERROR_RUNC_REAP_LAYER_MOUNTS_FAILED:
        return "runC reap layer mounts mounts failed";
      case CANNOT_GET_EXECUTABLE_NAME_FROM_READLINK:
        return "Cannot get executable name from readlink";
      case TOO_LONG_EXECUTOR_PATH:
        return "Too long executor path";
      case CANNOT_GET_EXECUTABLE_NAME_FROM_KERNEL:
        return "Cannot get executable name from kernel";
      case CANNOT_GET_EXECUTABLE_NAME_FROM_PID:
        return "Cannot get executable name from pid";
      case WRONG_PATH_OF_EXECUTABLE:
        return "Wrong path of executable";
      default:
        return "Unknown error code";
    }
}

/**
 * @brief 判断字符串是否为正则表达式（以regex:开头）
 * @param str 输入字符串
 * @return 是正则表达式返回非0，否则返回0
 */
int is_regex(const char *str) {
    // 正则应该以 "regex:" 前缀开头
    return (strncmp(str, "regex:", 6) == 0);
}