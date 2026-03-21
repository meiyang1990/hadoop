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
 * @file mount-utils.c
 * @brief YARN NodeManager容器执行器挂载工具实现，提供挂载权限校验、路径规范化等核心能力
 *
 * 该文件属于YARN NodeManager本地容器执行器的底层工具模块，负责处理用户容器挂载请求的合法性校验，
 * 基于配置的允许挂载列表，通过路径规范化、正则匹配、目录前缀匹配等方式控制容器可挂载范围，
 * 保障集群节点安全性，防止用户容器越权访问敏感路径。
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "util.h"
#include "mount-utils.h"
#include "configuration.h"

/**
 * 释放挂载选项结构体占用的内存
 * @param options 待释放的挂载选项结构体指针
 * @return 无
 */
void free_mount_options(mount_options *options) {
  if (options == NULL) {
      return;
  }

  if (options->opts != NULL) {
      for (unsigned int i = 0; i < options->num_opts; i++) {
          free(options->opts[i]);
      }
      free(options->opts);
  }

  free(options);
}

/**
 * 释放挂载信息数组占用的内存
 * @param mounts 待释放的挂载信息数组指针
 * @param num_mounts 数组中挂载信息的数量
 * @return 无
 */
void free_mounts(mount *mounts, const unsigned int num_mounts) {
    if (mounts == NULL) {
        return;
    }

    for (unsigned int i = 0; i < num_mounts; i++) {
       free(mounts[i].src);
       free(mounts[i].dest);
       free_mount_options(mounts[i].options);
    }
    free(mounts);
}

/**
 * 检查输入字符串是否符合命名卷的命名规则
 * @param volume_name 待检查的字符串
 * @return 1表示符合规则，0表示不符合规则
 */
static int is_volume_name(const char *volume_name) {
    const char *regex_str = "^[a-zA-Z0-9]([a-zA-Z0-9_.-]*)$";
    // execute_regex_match 返回0表示匹配成功
    return execute_regex_match(regex_str, volume_name) == 0;
}

/**
 * 检查输入卷名是否满足正则表达式匹配要求
 * @param requested 待检查的卷名字符串
 * @param pattern 配置的正则表达式模式（前缀包含regex:）
 * @return 1表示匹配成功，0表示匹配失败
 */
static int is_volume_name_matched_by_regex(const char* requested, const char* pattern) {
    // execute_regex_match 返回0表示匹配成功
    return is_volume_name(requested) && (execute_regex_match(pattern + sizeof("regex:"), requested) == 0);
}

/**
 * 规范化挂载路径，处理获取绝对路径和目录尾斜杠标准化
 * @param mount 待规范化的原始挂载路径
 * @param isRegexAllowed 是否允许正则表达式模式匹配
 * @return 规范化后的路径指针（需要调用者释放），失败返回NULL
 */
static char* normalize_mount(const char* mount, const int isRegexAllowed) {
    int ret = 0;
    struct stat buff;
    char *ret_ptr = NULL, *real_mount = NULL;
    if (mount == NULL) {
        return NULL;
    }
    // 调用realpath获取绝对路径
    real_mount = realpath(mount, NULL);
    if (real_mount == NULL) {
        // 如果是合法命名卷，直接返回原字符串，由容器运行时后续处理
        if (is_volume_name(mount)) {
            ret_ptr = strdup(mount);
            goto free_and_exit;
        }
        // 仅允许允许挂载列表使用正则，若为正则模式则直接返回
        if (isRegexAllowed) {
            if (is_regex(mount)) {
                ret_ptr = strdup(mount);
                goto free_and_exit;
            }
        }
        fprintf(ERRORFILE, "Could not determine real path of mount '%s'\n", mount);
        ret_ptr = NULL;
        goto free_and_exit;
    }
    // stat获取路径文件属性
    ret = stat(real_mount, &buff);
    if (ret == 0) {
        // 如果是目录，确保结尾有斜杠
        if (S_ISDIR(buff.st_mode)) {
            size_t len = strlen(real_mount);
            if (len <= 0) {
                ret_ptr = NULL;
                goto free_and_exit;
            }
            if (real_mount[len - 1] != '/') {
                ret_ptr = (char *) alloc_and_clear_memory(len + 2, sizeof(char));
                strncpy(ret_ptr, real_mount, len);
                ret_ptr[len] = '/';
                ret_ptr[len + 1] = '\0';
            } else {
                ret_ptr = strdup(real_mount);
            }
        } else {
            ret_ptr = strdup(real_mount);
        }
    } else {
        fprintf(ERRORFILE, "Could not stat path '%s'\n", real_mount);
        ret_ptr = NULL;
    }

free_and_exit:
    free(real_mount);
    return ret_ptr;
}

/**
 * 批量规范化挂载路径数组中的所有路径
 * @param mounts 待规范化的路径数组，结果会覆盖原数组内容，原字符串会被释放
 * @param isRegexAllowed 是否允许正则表达式模式
 * @return 0成功，-1失败
 */
static int normalize_mounts(char **mounts, const int isRegexAllowed) {
    unsigned int i = 0;
    char *tmp = NULL;
    if (mounts == NULL) {
        return 0;
    }
    for (i = 0; mounts[i] != NULL; ++i) {
        tmp = normalize_mount(mounts[i], isRegexAllowed);
        if (tmp == NULL) {
            return -1;
        }
        free(mounts[i]);
        mounts[i] = tmp;
    }
    return 0;
}

/**
 * 获取容器执行器配置文件的规范化路径
 * @param container_executor_cfg_path 输出参数，指向分配得到的规范化路径，需要调用者释放
 * @return 0成功，MOUNT_ACCESS_ERROR失败
 */
static int get_normalized_config_path(const char **container_executor_cfg_path) {
    char *config_path = NULL;
    int ret = 0;

    config_path = get_config_path("");
    *container_executor_cfg_path = normalize_mount(config_path, 0);
    if (*container_executor_cfg_path == NULL) {
        ret = MOUNT_ACCESS_ERROR;
        goto free_and_exit;
    }

free_and_exit:
    free(config_path);
    return ret;
}

/**
 * 检查请求挂载路径是否在允许挂载列表范围内
 * @param permitted_mounts 允许挂载路径数组
 * @param requested 请求挂载的源路径
 * @return 0不允许，1允许，-1内部错误
 */
static int check_mount_permitted(const char **permitted_mounts, const char *requested) {
    int ret = 0;
    unsigned int i;
    size_t permitted_mount_len = 0;
    if (permitted_mounts == NULL) {
        return 0;
    }
    // 规范化请求路径
    char *normalized_path = normalize_mount(requested, 0);
    if (normalized_path == NULL) {
        return -1;
    }
    // 遍历允许列表逐一匹配
    for (i = 0; permitted_mounts[i] != NULL; ++i) {
        // 精确路径匹配
        if (strcmp(normalized_path, permitted_mounts[i]) == 0) {
            ret = 1;
            break;
        }
        // 如果允许列表项是正则，尝试正则匹配卷名
        if (is_regex(permitted_mounts[i]) &&
            is_volume_name_matched_by_regex(normalized_path, permitted_mounts[i])) {
            ret = 1;
            break;
        }

        // 目录前缀匹配：允许路径是目录，则请求路径在该目录下都允许
        permitted_mount_len = strlen(permitted_mounts[i]);
        struct stat path_stat;
        stat(permitted_mounts[i], &path_stat);
        if (S_ISDIR(path_stat.st_mode)) {
            if (strncmp(normalized_path, permitted_mounts[i], permitted_mount_len) == 0) {
                ret = 1;
                break;
            }
        }
    }
    free(normalized_path);
    return ret;
}

/**
 * 验证单个挂载请求是否合法，检查是否在允许列表且不包含容器执行器配置文件
 * @param permitted_ro_mounts 允许只读挂载路径数组
 * @param permitted_rw_mounts 允许读写挂载路径数组
 * @param requested 待验证的挂载信息结构体
 * @return 0合法，否则返回对应错误码（INVALID_MOUNT/INVALID_RW_MOUNT等）
 */
static int validate_mount(const char **permitted_ro_mounts, const char **permitted_rw_mounts, const mount *requested) {
    const char *container_executor_cfg_path = NULL;
    const char *tmp_path_buffer[2] = {NULL, NULL};
    int permitted_rw, permitted_ro;
    int ret = 0;

    if (requested == NULL) {
        goto free_and_exit;
    }

    // 获取容器执行器配置文件规范化路径
    ret = get_normalized_config_path(&container_executor_cfg_path);
    if (ret != 0) {
        goto free_and_exit;
    }

    // 分别检查是否在只读和读写允许列表中
    permitted_rw = check_mount_permitted(permitted_rw_mounts, requested->src);
    permitted_ro = check_mount_permitted(permitted_ro_mounts, requested->src);

    if (permitted_ro == -1 || permitted_rw == -1) {
        fprintf(ERRORFILE, "Invalid mount src='%s', dest='%s'\n",
                requested->src, requested->dest);
        ret = INVALID_MOUNT;
        goto free_and_exit;
    }

    if (requested->options != NULL && requested->options->rw == 1) {
        // 处理读写挂载
        if (permitted_rw == 0) {
            fprintf(ERRORFILE, "Configuration does not allow mount src='%s', dest='%s'\n",
                    requested->src, requested->dest);
            ret = INVALID_RW_MOUNT;
            goto free_and_exit;
        } else {
            // 安全检查：禁止读写挂载包含容器执行器配置文件的路径，防止篡改配置
            tmp_path_buffer[0] = normalize_mount(requested->src, 0);
            // 反转参数检查配置文件路径是否在请求挂载路径范围内
            ret = check_mount_permitted(tmp_path_buffer, container_executor_cfg_path);
            free((void *) tmp_path_buffer[0]);
            if (ret == 1) {
                fprintf(ERRORFILE, "Attempting to mount a parent directory of container-executor.cfg as read-write. src='%s', dest='%s'\n",
                        requested->src, requested->dest);
                ret = INVALID_RW_MOUNT;
                goto free_and_exit;
            }
        }
    } else {
        // 处理只读挂载，只要在任意允许列表中即可
        if (permitted_ro == 0 && permitted_rw == 0) {
            fprintf(ERRORFILE, "Configuration does not allow mount src='%s', dest='%s'\n",
                    requested->src, requested->dest);
            ret = INVALID_RO_MOUNT;
            goto free_and_exit;
        }
    }

free_and_exit:
    free((void *) container_executor_cfg_path);
    return ret;
}

/**
 * 批量验证所有挂载请求的合法性
 * @param permitted_ro_mounts 允许只读挂载路径数组
 * @param permitted_rw_mounts 允许读写挂载路径数组
 * @param mounts 待验证的挂载信息数组
 * @param num_mounts 待验证挂载数量
 * @return 0全部合法，否则返回对应错误码
 */
int validate_mounts(char **permitted_ro_mounts, char **permitted_rw_mounts, mount *mounts, const unsigned int num_mounts) {
    int ret = 0;
    unsigned int i;

    // 先规范化允许列表中的所有路径
    ret = normalize_mounts(permitted_ro_mounts, 1);
    ret |= normalize_mounts(permitted_rw_mounts, 1);
    if (ret != 0) {
        fprintf(ERRORFILE, "Unable to find permitted mounts on disk\n");
        ret = MOUNT_ACCESS_ERROR;
        goto free_and_exit;
    }

    // 逐个验证每个挂载请求
    for (i = 0; i < num_mounts; i++) {
        ret = validate_mount((const char **) permitted_ro_mounts, (const char **) permitted_rw_mounts, &mounts[i]);
        if (ret != 0) {
            goto free_and_exit;
        }
    }

free_and_exit:
    return ret;
}