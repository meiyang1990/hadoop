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
 * @file runc_base_ctx.c
 * @brief runC容器运行时基础上下文管理实现，为YARN NodeManager使用runC启动容器提供底层支持
 *
 * 主要功能包括：runC运行根目录和镜像分层目录管理、分层挂载点路径处理、分层目录锁管理
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "configuration.h"
#include "container-executor.h"
#include "util.h"

#include "runc_base_ctx.h"
#include "runc_config.h"

#define LAYER_MOUNT_SUFFIX      "/mnt"
#define LAYER_MOUNT_SUFFIX_LEN  (sizeof(LAYER_MOUNT_SUFFIX) -1)

/**
 * 获取runC运行时分层根目录路径
 *
 * @param run_root runC运行根目录
 * @return 堆分配的分层根目录路径，失败返回NULL
 */
char* get_runc_layers_path(const char* run_root) {
  char* layers_path = NULL;
  if (asprintf(&layers_path, "%s/layers", run_root) == -1) {
    layers_path = NULL;
  }
  return layers_path;
}

/**
 * 获取指定分层目录路径
 *
 * @param run_root runC运行根目录
 * @param layer_name 分层名称
 * @return 堆分配的分层目录路径，失败返回NULL
 */
char* get_runc_layer_path(const char* run_root, const char* layer_name) {
  char* layer_path = NULL;
  if (asprintf(&layer_path, "%s/layers/%s", run_root, layer_name) == -1) {
    layer_path = NULL;
  }
  return layer_path;
}

/**
 * 获取分层挂载点路径
 *
 * @param layer_path 分层目录路径
 * @return 堆分配的分层挂载点路径，失败返回NULL
 */
char* get_runc_layer_mount_path(const char* layer_path) {
  char* mount_path = NULL;
  if (asprintf(&mount_path, "%s" LAYER_MOUNT_SUFFIX, layer_path) == -1) {
    mount_path = NULL;
  }
  return mount_path;
}

/**
 * 从挂载点路径反向解析出分层目录路径
 *
 * @param mount_path 分层挂载点路径
 * @return 堆分配的分层目录路径，失败返回NULL
 */
char* get_runc_layer_path_from_mount_path(const char* mount_path) {
  size_t mount_path_len = strlen(mount_path);
  if (mount_path_len <= LAYER_MOUNT_SUFFIX_LEN) {
    return NULL;
  }
  size_t layer_path_len = mount_path_len - LAYER_MOUNT_SUFFIX_LEN;
  const char* suffix = mount_path + layer_path_len;
  if (strcmp(suffix, LAYER_MOUNT_SUFFIX)) {
    return NULL;
  }
  return strndup(mount_path, layer_path_len);
}

/**
 * 创建runC运行根目录和分层目录结构（不存在时创建）
 * @return 堆分配的运行根目录路径，失败返回NULL
 */
static char* setup_runc_run_root_directories() {
  char* layers_path = NULL;
  // 从配置读取runC运行根目录配置
  char* run_root = get_configuration_value(RUNC_RUN_ROOT_KEY,
      CONTAINER_EXECUTOR_CFG_RUNC_SECTION, get_cfg());
  if (run_root == NULL) {
    // 配置不存在，使用默认路径
    run_root = strdup(DEFAULT_RUNC_ROOT);
    if (run_root == NULL) {
      goto mem_fail;
    }
  }

  // 创建运行根目录，目录已存在不算错误
  if (mkdir(run_root, S_IRWXU) != 0 && errno != EEXIST) {
    fprintf(ERRORFILE, "Error creating runC run root at %s : %s\n", run_root,
        strerror(errno));
    goto fail;
  }

  // 构造分层根目录路径
  layers_path = get_runc_layers_path(run_root);
  if (layers_path == NULL) {
    goto mem_fail;
  }

  // 创建分层根目录，目录已存在不算错误
  if (mkdir(layers_path, S_IRWXU) != 0 && errno != EEXIST) {
    fprintf(ERRORFILE, "Error creating layers directory at %s : %s\n",
        layers_path, strerror(errno));
    goto fail;
  }

  free(layers_path);
  return run_root;

fail:
  free(layers_path);
  free(run_root);
  return NULL;

mem_fail:
  fputs("Cannot allocate memory\n", ERRORFILE);
  goto fail;
}


/**
 * 初始化未初始化的runC基础上下文
 * @param ctx 待初始化的runC基础上下文
 */
void init_runc_base_ctx(runc_base_ctx* ctx) {
  memset(ctx, 0, sizeof(*ctx));
  ctx->layers_lock_fd = -1;
  ctx->layers_lock_state = F_UNLCK;
}

/**
 * 释放runC基础上下文持有的资源，不释放上下文本身
 * 适用于栈分配或嵌入其他结构中的上下文对象
 * @param ctx 待销毁的runC基础上下文
 */
void destroy_runc_base_ctx(runc_base_ctx* ctx) {
  if (ctx != NULL) {
    free(ctx->run_root);
    if (ctx->layers_lock_fd != -1) {
      close(ctx->layers_lock_fd);
    }
  }
}

/**
 * 分配并初始化runC基础上下文
 * @return 分配初始化完成的上下文指针，失败返回NULL
 */
runc_base_ctx* alloc_runc_base_ctx() {
  runc_base_ctx* ctx = malloc(sizeof(*ctx));
  if (ctx != NULL) {
    init_runc_base_ctx(ctx);
  }
  return ctx;
}

/**
 * 释放整个runC基础上下文及其关联的所有内存
 * @param ctx 待释放的runC基础上下文
 */
void free_runc_base_ctx(runc_base_ctx* ctx) {
  destroy_runc_base_ctx(ctx);
  free(ctx);
}

/**
 * 打开runC基础上下文，创建必要的目录和锁文件
 * @param ctx 待打开的上下文
 * @return 成功返回true，失败返回false
 */
bool open_runc_base_ctx(runc_base_ctx* ctx) {
  // 创建运行根目录和分层目录结构
  ctx->run_root = setup_runc_run_root_directories();
  if (ctx->run_root == NULL) {
    return false;
  }

  // 构造分层锁文件路径
  char* lock_path = get_runc_layer_path(ctx->run_root, "lock");
  if (lock_path == NULL) {
    fputs("Cannot allocate memory\n", ERRORFILE);
    return false;
  }

  bool result = true;
  // 打开锁文件，不存在则创建
  ctx->layers_lock_fd = open(lock_path, O_RDWR | O_CREAT | O_CLOEXEC, S_IRWXU);
  if (ctx->layers_lock_fd == -1) {
    fprintf(ERRORFILE, "Cannot open lock file %s : %s\n", lock_path,
        strerror(errno));
    result = false;
  }

  free(lock_path);
  return result;
}

/**
 * 分配并打开runC基础上下文
 * @return 初始化完成的上下文指针，失败返回NULL
 */
runc_base_ctx* setup_runc_base_ctx() {
  runc_base_ctx* ctx = alloc_runc_base_ctx();
  if (ctx != NULL) {
    if (!open_runc_base_ctx(ctx)) {
      free_runc_base_ctx(ctx);
      ctx = NULL;
    }
  }
  return ctx;
}


/**
 * 执行文件锁操作，处理中断自动重试
 * @param fd 锁文件描述符
 * @param lock_cmd fcntl锁命令（F_RDLCK/F_WRLCK/F_UNLCK)
 * @return 成功返回true，失败返回false
 */
static bool do_lock_cmd(int fd, int lock_cmd) {
  struct flock fl;
  memset(&fl, 0, sizeof(fl));
  fl.l_type = lock_cmd;
  fl.l_whence = SEEK_SET;
  fl.l_start = 0;
  fl.l_len = 0;
  // 阻塞加锁，被中断自动重试
  while (true) {
    int rc = fcntl(fd, F_SETLKW, &fl);
    if (rc == 0) {
      return true;
    }
    if (errno != EINTR) {
      fprintf(ERRORFILE, "Error updating lock: %s\n", strerror(errno));
      return false;
    }
  }
}

/**
 * 获取分层目录读锁
 * @param ctx runC基础上下文
 * @return 成功返回true，失败返回false
 */
bool acquire_runc_layers_read_lock(runc_base_ctx* ctx) {
  if (ctx->layers_lock_state == F_RDLCK) {
    return true;
  }
  if (do_lock_cmd(ctx->layers_lock_fd, F_RDLCK)) {
    ctx->layers_lock_state = F_RDLCK;
    return true;
  }
  return false;
}

/**
 * 获取分层目录写锁
 * @param ctx runC基础上下文
 * @return 成功返回true，失败返回false
 */
bool acquire_runc_layers_write_lock(runc_base_ctx* ctx) {
  if (ctx->layers_lock_state == F_WRLCK) {
    return true;
  }
  if (ctx->layers_lock_state == F_RDLCK) {
    // 从读锁升级为写锁前先释放读锁，避免死锁
    if (!release_runc_layers_lock(ctx)) {
      return false;
    }
  }
  if (do_lock_cmd(ctx->layers_lock_fd, F_WRLCK)) {
    ctx->layers_lock_state = F_WRLCK;
    return true;
  }
  return false;
}

/**
 * 释放分层目录锁
 * @param ctx runC基础上下文
 * @return 成功返回true，失败返回false
 */
bool release_runc_layers_lock(runc_base_ctx* ctx) {
  if (ctx->layers_lock_state == F_UNLCK) {
    return true;
  }
  if (do_lock_cmd(ctx->layers_lock_fd, F_UNLCK)) {
    ctx->layers_lock_state = F_UNLCK;
    return true;
  }
  return false;
}