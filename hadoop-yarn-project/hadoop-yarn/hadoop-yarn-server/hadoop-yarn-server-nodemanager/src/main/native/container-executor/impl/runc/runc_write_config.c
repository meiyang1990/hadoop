// 这个文件已经全部加上中文注释
/*
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
 * @file runc_write_config.c
 * @brief YARN NodeManager runC容器配置生成模块
 * @details 负责按照OCI/runc规范生成容器配置JSON文件，包含rootfs、用户权限、挂载点、
 *          Linux命名空间、cgroups资源限制等配置，用于runC启动YARN容器
 */

#include <sys/utsname.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hadoop_user_info.h"

#include "container-executor.h"
#include "utils/cJSON/cJSON.h"
#include "utils/file-utils.h"
#include "util.h"

#include "runc_launch_cmd.h"
#include "runc_write_config.h"

#define RUNC_CONFIG_FILENAME    "config.json"
#define STARTING_JSON_BUFFER_SIZE  (128*1024)

/**
 * @brief 构建runc配置中root字段（根文件系统配置）
 * @param rootfs_path 容器根文件系统路径
 * @return 构建完成的cJSON对象，失败返回NULL
 */
static cJSON* build_runc_config_root(const char* rootfs_path) {
  cJSON* root = cJSON_CreateObject();
  if (cJSON_AddStringToObject(root, "path", rootfs_path) == NULL) {
    goto fail;
  }
  if (cJSON_AddTrueToObject(root, "readonly") == NULL) {
    goto fail;
  }
  return root;

fail:
  cJSON_Delete(root);
  return NULL;
}

/**
 * @brief 构建runc配置中process.user字段（容器用户信息配置）
 * @param username 容器运行用户名
 * @return 构建完成的cJSON对象，失败返回NULL
 */
static cJSON* build_runc_config_process_user(const char* username) {
  cJSON* user_json = cJSON_CreateObject();
  struct hadoop_user_info* hui = hadoop_user_info_alloc();
  if (hui == NULL) {
    return NULL;
  }

  // 查询用户信息
  int rc = hadoop_user_info_fetch(hui, username);
  if (rc != 0) {
    fprintf(ERRORFILE, "Error looking up user %s : %s\n", username,
        strerror(rc));
    goto fail;
  }

  // 添加用户UID
  if (cJSON_AddNumberToObject(user_json, "uid", hui->pwd.pw_uid) == NULL) {
    goto fail;
  }
  // 添加用户GID
  if (cJSON_AddNumberToObject(user_json, "gid", hui->pwd.pw_gid) == NULL) {
    goto fail;
  }

  // 获取用户附属组列表
  rc = hadoop_user_info_getgroups(hui);
  if (rc != 0) {
    fprintf(ERRORFILE, "Error getting groups for user %s : %s\n", username,
        strerror(rc));
    goto fail;
  }

  // 添加额外附属组（第一个为主组已经在上面添加过）
  if (hui->num_gids > 1) {
    cJSON* garray = cJSON_AddArrayToObject(user_json, "additionalGids");
    if (garray == NULL) {
      goto fail;
    }

    // first gid entry is the primary group which is accounted for above
    for (int i = 1; i < hui->num_gids; ++i) {
      cJSON* g = cJSON_CreateNumber(hui->gids[i]);
      if (g == NULL) {
        goto fail;
      }
      cJSON_AddItemToArray(garray, g);
    }
  }

  return user_json;

fail:
  hadoop_user_info_free(hui);
  cJSON_Delete(user_json);
  return NULL;
}

/**
 * @brief 构建runc配置中process字段（容器进程配置）
 * @param rlc runc启动命令结构体，包含容器启动参数
 * @return 构建完成的cJSON对象，失败返回NULL
 */
static cJSON* build_runc_config_process(const runc_launch_cmd* rlc) {
  cJSON* process = cJSON_CreateObject();
  if (process == NULL) {
    return NULL;
  }

  // 复用传入配置中的参数列表、工作目录、环境变量
  cJSON_AddItemReferenceToObject(process, "args", rlc->config.process.args);
  cJSON_AddItemReferenceToObject(process, "cwd", rlc->config.process.cwd);
  cJSON_AddItemReferenceToObject(process, "env", rlc->config.process.env);
  // 禁止容器进程获取新权限，提升安全性
  if (cJSON_AddTrueToObject(process, "noNewPrivileges") == NULL) {
    goto fail;
  }

  // 构建用户信息
  cJSON* user_json = build_runc_config_process_user(rlc->run_as_user);
  if (user_json == NULL) {
    goto fail;
  }
  cJSON_AddItemToObjectCS(process, "user", user_json);

  return process;

fail:
  cJSON_Delete(process);
  return NULL;
}

/**
 * @brief 向挂载配置中添加挂载选项
 * @param mount_json 挂载配置对象
 * @param opts 可变参数列表，挂载选项字符串
 * @return 成功返回true，失败返回false
 */
static bool add_mount_opts(cJSON* mount_json, va_list opts) {
  const char* opt = va_arg(opts, const char*);
  if (opt == NULL) {
    return true;
  }

  // 创建选项数组
  cJSON* opts_array = cJSON_AddArrayToObject(mount_json, "options");
  if (opts_array == NULL) {
    return false;
  }

  // 逐个添加所有选项
  do {
    cJSON* opt_json = cJSON_CreateString(opt);
    if (opt_json == NULL) {
      return false;
    }
    cJSON_AddItemToArray(opts_array, opt_json);
    opt = va_arg(opts, const char*);
  } while (opt != NULL);

  return true;
}

/**
 * @brief 添加一个挂载点到挂载数组
 * @param mounts_array 挂载点数组
 * @param src 挂载源
 * @param dest 挂载目标路径
 * @param fstype 文件系统类型
 * @param ... 可变参数，挂载选项列表，以NULL结尾
 * @return 成功返回true，失败返回false
 */
static bool add_mount_json(cJSON* mounts_array, const char* src,
    const char* dest, const char* fstype, ...) {
  bool result = false;
  cJSON* m = cJSON_CreateObject();
  if (cJSON_AddStringToObject(m, "source", src) == NULL) {
    goto cleanup;
  }
  if (cJSON_AddStringToObject(m, "destination", dest) == NULL) {
    goto cleanup;
  }
  if (cJSON_AddStringToObject(m, "type", fstype) == NULL) {
    goto cleanup;
  }

  va_list vargs;
  va_start(vargs, fstype);
  result = add_mount_opts(m, vargs);
  va_end(vargs);

  if (result) {
    cJSON_AddItemToArray(mounts_array, m);
  }

cleanup:
  if (!result) {
    cJSON_Delete(m);
  }
  return result;
}

/**
 * @brief 添加容器标准挂载点，符合OCI运行时规范
 * @param mounts_array 挂载点数组
 * @return 全部添加成功返回true，否则返回false
 */
static bool add_std_mounts_json(cJSON* mounts_array) {
  bool result = true;
  result &= add_mount_json(mounts_array, "proc", "/proc", "proc", NULL);
  result &= add_mount_json(mounts_array, "tmpfs", "/dev", "tmpfs",
      "nosuid", "strictatime", "mode=755", "size=65536k", NULL);
  result &= add_mount_json(mounts_array, "devpts", "/dev/pts", "devpts",
      "nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5",
       NULL);
  result &= add_mount_json(mounts_array, "shm", "/dev/shm", "tmpfs",
      "nosuid", "noexec", "nodev", "mode=1777", "size=8g", NULL);
  result &= add_mount_json(mounts_array, "mqueue", "/dev/mqueue", "mqueue",
      "nosuid", "noexec", "nodev", NULL);
  result &= add_mount_json(mounts_array, "sysfs", "/sys", "sysfs",
      "nosuid", "noexec", "nodev", "ro", NULL);
  result &= add_mount_json(mounts_array, "cgroup", "/sys/fs/cgroup", "cgroup",
      "nosuid", "noexec", "nodev", "relatime", "ro", NULL);
  return result;
}

/**
 * @brief 构建runc配置中mounts字段（容器挂载点配置）
 * @param rlc runc启动命令结构体，包含用户自定义挂载点
 * @return 构建完成的cJSON数组，失败返回NULL
 */
static cJSON* build_runc_config_mounts(const runc_launch_cmd* rlc) {
  // 创建挂载点数组
  cJSON* mjson = cJSON_CreateArray();
  // 添加标准挂载点
  if (!add_std_mounts_json(mjson)) {
    goto fail;
  }

  // 添加用户自定义挂载点（复用传入配置中的挂载项）
  cJSON* e;
  cJSON_ArrayForEach(e, rlc->config.mounts) {
    cJSON_AddItemReferenceToArray(mjson, e);
  }

  return mjson;

fail:
  cJSON_Delete(mjson);
  return NULL;
}

/**
 * @brief 获取默认Linux设备白名单配置，默认禁止所有设备访问
 * @return 构建设备规则数组，失败返回NULL
 */
static cJSON* get_default_linux_devices_json() {
  cJSON* devs = cJSON_CreateArray();
  if (devs == NULL) {
    return NULL;
  }

  // 默认规则：禁止访问所有设备，后续添加允许的设备
  cJSON* o = cJSON_CreateObject();
  if (o == NULL) {
    goto fail;
  }
  cJSON_AddItemToArray(devs, o);

  if (cJSON_AddStringToObject(o, "access", "rwm") == NULL) {
    goto fail;
  }

  if (cJSON_AddFalseToObject(o, "allow") == NULL) {
    goto fail;
  }

  return devs;

fail:
  cJSON_Delete(devs);
  return NULL;
}

/**
 * @brief 添加cgroups路径配置到Linux配置中
 * @param ljson Linux配置对象
 * @param rlc runc启动命令结构体
 * @return 成功返回true，失败返回false
 */
static bool add_linux_cgroups_json(cJSON* ljson, const runc_launch_cmd* rlc) {
    cJSON* cj = cJSON_GetObjectItemCaseSensitive(rlc->config.linux_config,
                                                 "cgroupsPath");
    // 如果传入配置中存在cgroups路径，复用该配置
    if (cj != NULL) {
        cJSON_AddItemReferenceToObject(ljson, "cgroupsPath", cj);
    }
    return true;
}

/**
 * @brief 添加资源限制配置到Linux配置中
 * @param ljson Linux配置对象
 * @param rlc runc启动命令结构体
 * @return 成功返回true，失败返回false
 */
static bool add_linux_resources_json(cJSON* ljson, const runc_launch_cmd* rlc) {
  // 创建resources对象
  cJSON* robj = cJSON_AddObjectToObject(ljson, "resources");
  if (robj == NULL) {
    return false;
  }

  // 添加默认设备白名单配置
  cJSON* devs = get_default_linux_devices_json();
  if (devs == NULL) {
    return false;
  }
  cJSON_AddItemToObjectCS(robj, "devices", devs);

  // 复用传入配置中的资源限制
  const cJSON* rlc_rsrc = cJSON_GetObjectItemCaseSensitive(
      rlc->config.linux_config, "resources");
  cJSON* e;
  cJSON_ArrayForEach(e, rlc_rsrc) {
    // 设备规则需要合并到默认配置中
    if (strcmp("devices", e->string) == 0) {
      cJSON* dev_e;
      cJSON_ArrayForEach(dev_e, e) {
        cJSON_AddItemReferenceToArray(devs, dev_e);
      }
    // 其他资源限制直接复用
    } else {
      cJSON_AddItemReferenceToObject(robj, e->string, e);
    }
  }

  return true;
}

/**
 * @brief 添加一个命名空间到命名空间数组
 * @param ljson 命名空间数组
 * @param ns_type 命名空间类型（pid/ipc/uts/mount等）
 * @return 成功返回true，失败返回false
 */
static bool add_linux_namespace_json(cJSON* ljson, const char* ns_type) {
  cJSON* ns = cJSON_CreateObject();
  if (ns == NULL) {
    return false;
  }
  cJSON_AddItemToArray(ljson, ns);
  return (cJSON_AddStringToObject(ns, "type", ns_type) != NULL);
}

/**
 * @brief 添加容器默认命名空间配置，为容器创建独立命名空间
 * @param ljson Linux配置对象
 * @return 成功返回true，失败返回false
 */
static bool add_linux_namespaces_json(cJSON* ljson) {
  cJSON* ns_array = cJSON_AddArrayToObject(ljson, "namespaces");
  if (ns_array == NULL) {
    return false;
  }
  bool result = add_linux_namespace_json(ns_array, "pid");
  result &= add_linux_namespace_json(ns_array, "ipc");
  result &= add_linux_namespace_json(ns_array, "uts");
  result &= add_linux_namespace_json(ns_array, "mount");
  return result;
}

// 需要被masked（隐藏）的敏感路径列表
static const char* runc_masked_paths[] = {
  "/proc/kcore",
  "/proc/latency_stats",
  "/proc/timer_list",
  "/proc/timer_stats",
  "/proc/sched_debug",
  "/proc/scsi",
  "/sys/firmware"
};

/**
 * @brief 添加需要被隐藏的敏感路径配置
 * @param ljson Linux配置对象
 * @return 成功返回true，失败返回false
 */
static bool add_linux_masked_paths_json(cJSON* ljson) {
  size_t num_paths = sizeof(runc_masked_paths) / sizeof(runc_masked_paths[0]);
  cJSON* paths = cJSON_CreateStringArray(runc_masked_paths, num_paths);
  if (paths == NULL) {
    return false;
  }
  cJSON_AddItemToObject(ljson, "maskedPaths", paths);
  return true;
}

// 需要设置为只读的敏感路径列表
static const char* runc_readonly_paths[] = {
  "/proc/asound",
  "/proc/bus",
  "/proc/fs",
  "/proc/irq",
  "/proc/sys",
  "/proc/sysrq-trigger"
};

/**
 * @brief 添加只读敏感路径配置
 * @param ljson Linux配置对象
 * @return 成功返回true，失败返回false
 */
static bool add_linux_readonly_paths_json(cJSON* ljson) {
  size_t num_paths = sizeof(runc_readonly_paths) / sizeof(runc_readonly_paths[0]);
  cJSON* paths = cJSON_CreateStringArray(runc_readonly_paths, num_paths);
  if (paths == NULL) {
    return false;
  }
  cJSON_AddItemToObject(ljson, "readonlyPaths", paths);
  return true;
}

/**
 * @brief 添加seccomp系统调用过滤配置（如果传入配置中有）
 * @param