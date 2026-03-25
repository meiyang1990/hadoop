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
 * @file runc.c
 * @brief YARN NodeManager 基于runc的容器执行器核心实现
 * @details 负责使用runc运行OCI镜像格式容器，实现分层镜像的挂载管理、OverlayFS联合挂载
 */
#include <linux/loop.h>
#include <sys/types.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include "../modules/common/module-configs.h"
// TODO: Figure out how to address new openssl dependency for container-executor
#include <openssl/evp.h>

// workaround for building on RHEL6 but running on RHEL7
#ifndef LOOP_CTL_GET_FREE
#define LOOP_CTL_GET_FREE  0x4C82
#endif

#include "utils/string-utils.h"
#include "util.h"
#include "configuration.h"
#include "container-executor.h"

#include "runc.h"
#include "runc_base_ctx.h"
#include "runc_config.h"
#include "runc_launch_cmd.h"
#include "runc_reap.h"
#include "runc_write_config.h"

/** 根文件系统卸载最大重试次数 */
#define NUM_ROOTFS_UNMOUNT_ATTEMPTS      40
/** 根文件系统卸载退避最大毫秒数 */
#define MAX_ROOTFS_UNMOUNT_BACKOFF_MSEC  1000

// NOTE: Update init_runc_overlay_desc and destroy_runc_overlay_desc
//       when this is changed.
/** OverlayFS联合挂载描述符 */
typedef struct runc_overlay_desc_struct {
  char* top_path;             // 容器顶级工作目录
  char* mount_path;           // 根文件系统挂载点
  char* upper_path;           // 可写上层目录路径
  char* work_path;            // overlay工作目录路径
} runc_overlay_desc;

// NOTE: Update init_runc_mount_context and destroy_runc_mount_context
//       when this is changed.
/** 单个镜像分层挂载上下文 */
typedef struct runc_mount_context_struct {
  char* src_path;             // 原始分层镜像文件路径
  char* layer_path;           // 分层在本地数据库中的目录路径
  char* mount_path;           // 分层文件系统挂载点
  int fd;                     // 打开的文件描述符，-1表示未打开
} runc_mount_ctx;

// NOTE: Update init_runc_launch_cmd_ctx and destroy_runc_launch_cmd_ctx
//       when this is changed.
/** runc容器启动上下文 */
typedef struct runc_launch_cmd_context_struct {
  runc_base_ctx base_ctx;      // 运行根目录与分层锁上下文
  runc_overlay_desc upper;     // 可写上层描述符
  runc_mount_ctx* layers;      // 所有分层挂载信息数组
  unsigned int num_layers;    // 分层总数
} runc_launch_cmd_ctx;

/**
 * 检查runc模块是否启用
 * @param conf 配置对象
 * @return 1表示启用，0表示禁用
 */
int runc_module_enabled(const struct configuration *conf) {
  struct section *section = get_configuration_section(CONTAINER_EXECUTOR_CFG_RUNC_SECTION, conf);
  if (section != NULL) {
    return module_enabled(section, CONTAINER_EXECUTOR_CFG_RUNC_SECTION);
  }
  return 0;
}

static void init_runc_overlay_desc(runc_overlay_desc* desc) {
  memset(desc, 0, sizeof(*desc));
}

static void destroy_runc_overlay_desc(runc_overlay_desc* desc) {
  if (desc != NULL) {
    free(desc->top_path);
    free(desc->mount_path);
    free(desc->upper_path);
    free(desc->work_path);
  }
}

static void init_runc_mount_ctx(runc_mount_ctx* ctx) {
  memset(ctx, 0, sizeof(*ctx));
  ctx->fd = -1;
}

static void destroy_runc_mount_ctx(runc_mount_ctx* ctx) {
  if (ctx != NULL) {
    free(ctx->src_path);
    free(ctx->layer_path);
    free(ctx->mount_path);
    if (ctx->fd != -1) {
      close(ctx->fd);
    }
  }
}

static void init_runc_launch_cmd_ctx(runc_launch_cmd_ctx* ctx) {
  memset(ctx, 0, sizeof(*ctx));
  init_runc_base_ctx(&ctx->base_ctx);
  init_runc_overlay_desc(&ctx->upper);
}

static void destroy_runc_launch_cmd_ctx(runc_launch_cmd_ctx* ctx) {
  if (ctx != NULL) {
    if (ctx->layers != NULL) {
      for (unsigned int i = 0; i < ctx->num_layers; ++i) {
        destroy_runc_mount_ctx(&ctx->layers[i]);
      }
      free(ctx->layers);
    }
    destroy_runc_overlay_desc(&ctx->upper);
    destroy_runc_base_ctx(&ctx->base_ctx);
  }
}

static runc_launch_cmd_ctx* alloc_runc_launch_cmd_ctx() {
  runc_launch_cmd_ctx* ctx = malloc(sizeof(*ctx));
  if (ctx != NULL) {
    init_runc_launch_cmd_ctx(ctx);
  }
  return ctx;
}

static void free_runc_launch_cmd_ctx(runc_launch_cmd_ctx* ctx) {
  if (ctx != NULL) {
    destroy_runc_launch_cmd_ctx(ctx);
    free(ctx);
  }
}

static runc_launch_cmd_ctx* setup_runc_launch_cmd_ctx() {
  runc_launch_cmd_ctx* ctx = alloc_runc_launch_cmd_ctx();
  if (ctx == NULL) {
    fputs("Cannot allocate memory\n", ERRORFILE);
    return NULL;
  }

  if (!open_runc_base_ctx(&ctx->base_ctx)) {
    free_runc_launch_cmd_ctx(ctx);
    return NULL;
  }

  return ctx;
}

/**
 * 根据分层路径计算SHA256哈希，作为分层缓存键
 * @param path 分层文件路径
 * @return 分配的哈希十六进制字符串，失败返回NULL
 */
static char* compute_layer_hash(const char* path) {
  char* digest = NULL;
  EVP_MD_CTX* mdctx = EVP_MD_CTX_create();
  if (mdctx == NULL) {
    fputs("Unable to create EVP MD context\n", ERRORFILE);
    goto cleanup;
  }

  if (!EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL)) {
    fputs("Unable to initialize SHA256 digester\n", ERRORFILE);
    goto cleanup;
  }

  if (!EVP_DigestUpdate(mdctx, path, strlen(path))) {
    fputs("Unable to compute layer path digest\n", ERRORFILE);
    goto cleanup;
  }

  unsigned char raw_digest[EVP_MAX_MD_SIZE];
  unsigned int raw_digest_len = 0;
  if (!EVP_DigestFinal_ex(mdctx, raw_digest, &raw_digest_len)) {
    fputs("Unable to compute layer path digest\n", ERRORFILE);
    goto cleanup;
  }

  digest = to_hexstring(raw_digest, raw_digest_len);

cleanup:
  if (mdctx != NULL) {
    EVP_MD_CTX_destroy(mdctx);
  }
  return digest;
}

/**
 * 打开指定挂载点并验证是否为有效挂载点
 * @param path 挂载点路径
 * @return 有效挂载点返回文件描述符，否则返回-1
 * @note 调用前必须持有对应读锁
 */
static int open_mountpoint(const char* path) {
  int fd = open(path, O_RDONLY | O_CLOEXEC);
  if (fd == -1) {
    if (errno != ENOENT) {
      fprintf(ERRORFILE, "Error accessing mount point at %s : %s\n", path,
          strerror(errno));
    }
    return fd;
  }

  struct stat mstat, pstat;
  if (fstat(fd, &mstat) == -1) {
    fprintf(ERRORFILE, "Error accessing mount point at %s : %s\n", path,
        strerror(errno));
    goto close_fail;
  }
  if (!S_ISDIR(mstat.st_mode)) {
    fprintf(ERRORFILE, "Mount point %s is not a directory\n", path);
    goto close_fail;
  }

  if (fstatat(fd, "..", &pstat, 0) == -1) {
    fprintf(ERRORFILE, "Error accessing mount point parent of %s : %s\n", path,
        strerror(errno));
    goto close_fail;
  }

  // If the parent directory's device matches the child directory's device
  // then we didn't cross a device boundary in the filesystem and therefore
  // this is likely not a mount point.
  // TODO: This assumption works for loopback mounts but would not work for
  //       bind mounts or some other situations. Worst case would need to
  //       walk the mount table and otherwise replicate the mountpoint(1) cmd.
  if (mstat.st_dev == pstat.st_dev) {
    goto close_fail;
  }

  return fd;

close_fail:
  close(fd);
  return -1;
}

/**
 * 初始化容器OverlayFS描述符，生成各路径
 * @param desc 描述符对象
 * @param run_root 运行根目录
 * @param container_id 容器ID
 * @return 初始化成功返回true，否则false
 */
static bool init_overlay_descriptor(runc_overlay_desc* desc,
    const char* run_root, const char* container_id) {
  if (asprintf(&desc->top_path, "%s/%s", run_root, container_id) == -1) {
    return false;
  }
  if (asprintf(&desc->mount_path, "%s/rootfs", desc->top_path) == -1) {
    return false;
  }
  if (asprintf(&desc->upper_path, "%s/upper", desc->top_path) == -1) {
    return false;
  }
  if (asprintf(&desc->work_path, "%s/work", desc->top_path) == -1) {
    return false;
  }
  return true;
}

/**
 * 初始化单个分层挂载上下文
 * @param ctx 挂载上下文对象
 * @param spec 分层规格描述
 * @param run_root 运行根目录
 * @return 成功返回true，否则false
 */
static bool init_layer_mount_ctx(runc_mount_ctx* ctx, const rlc_layer_spec* spec,
    const char* run_root) {
  char* hash = compute_layer_hash(spec->path);
  if (hash == NULL) {
    return false;
  }

  ctx->layer_path = get_runc_layer_path(run_root, hash);
  free(hash);
  if (ctx->layer_path == NULL) {
    return false;
  }

  ctx->mount_path = get_runc_layer_mount_path(ctx->layer_path);
  if (ctx->mount_path == NULL) {
    return false;
  }

  ctx->fd = open(spec->path, O_RDONLY | O_CLOEXEC);
  if (ctx->fd == -1) {
    fprintf(ERRORFILE, "Error opening layer image at %s : %s\n", spec->path,
        strerror(errno));
    return false;
  }

  ctx->src_path = strdup(spec->path);
  return ctx->src_path != NULL;
}

/**
 * 批量初始化所有分层挂载上下文，验证用户访问权限
 * @param ctx 容器启动上下文
 * @param layer_specs 分层规格数组
 * @param num_layers 分层数量
 * @return 成功返回true，否则false
 */
static bool init_layer_mount_ctxs(runc_launch_cmd_ctx* ctx,
    const rlc_layer_spec* layer_specs, unsigned int num_layers) {
  ctx->layers = malloc(num_layers * sizeof(*ctx->layers));
  if (ctx->layers == NULL) {
    fputs("Unable to allocate memory\n", ERRORFILE);
    return false;
  }

  for (unsigned int i = 0; i < num_layers; ++i) {
    init_runc_mount_ctx(&ctx->layers[i]);
  }
  ctx->num_layers = num_layers;

  for (unsigned int i = 0; i < num_layers; ++i) {
    if (!init_layer_mount_ctx(&ctx->layers[i], &layer_specs[i],
        ctx->base_ctx.run_root)) {
      return false;
    }
  }

  return true;
}

/**
 * 分配并打开回环设备，绑定源文件
 * @param loopdev_name_out 输出分配得到的回环设备名称
 * @param src_fd 源文件描述符
 * @return 分配好的回环设备文件描述符，失败返回-1
 */
static int allocate_and_open_loop_device(char** loopdev_name_out, int src_fd) {
  *loopdev_name_out = NULL;
  int loopctl = open("/dev/loop-control", O_RDWR);
  if (loopctl == -1) {
    fprintf(ERRORFILE, "Error opening /dev/loop-control : %s\n",
        strerror(errno));
    return -1;
  }

  char* loopdev_name = NULL;
  int loop_fd = -1;
  while (true) {
    int loop_num = ioctl(loopctl, LOOP_CTL_GET_FREE);
    if (loop_num < 0) {
      fprintf(ERRORFILE, "Error allocating a new loop device: %s\n",
          strerror(errno));
      goto fail;
    }

    if (asprintf(&loopdev_name, "/dev/loop%d", loop_num) == -1) {
      fputs("Unable to allocate memory\n", ERRORFILE);
      goto fail;
    }
    loop_fd = open(loopdev_name, O_RDWR | O_CLOEXEC);
    if (loop_fd == -1) {
      fprintf(ERRORFILE, "Unable to open loop device at %s : %s\n",
          loopdev_name, strerror(errno));
      goto fail;
    }

    if (ioctl(loop_fd, LOOP_SET_FD, src_fd) != -1) {
      break;
    }

    // EBUSY 表示另一个进程抢占了该回环设备，重试下一个
    if (errno != EBUSY) {
      fprintf(ERRORFILE, "Error setting loop source file: %s\n",
          strerror(errno));
      goto fail;
    }

    close(loop_fd);
    loop_fd = -1;
    free(loopdev_name);
    loopdev_name = NULL;
  }

  struct loop_info64 loop_info;
  memset(&loop_info, 0, sizeof(loop_info));
  // 设置只读和自动清除标志
  loop_info.lo_flags = LO_FLAGS_READ_ONLY | LO_FLAGS_AUTOCLEAR;
  if (ioctl(loop_fd, LOOP_SET_STATUS64, &loop_info) == -1) {
    fprintf(ERRORFILE, "Error setting loop flags: %s\n", strerror(errno));
    goto fail;
  }

  close(loopctl);
  *loopdev_name_out = loopdev_name;
  return loop_fd;

fail:
  if (loop_fd != -1) {
    close(loop_fd);
  }
  close(loopctl);
  free(loopdev_name);
  return -1;
}

/**
 * 执行挂载操作，失败打印错误日志
 * @param src 挂载源
 * @param target 挂载目标
 * @param fs_type 文件系统类型
 * @param mount_flags 挂载标志
 * @param mount_options 挂载选项
 * @return 挂载成功返回true，否则false
 */
static bool do_mount(const char* src, const char* target,
    const char* fs_type, unsigned long mount_flags, const char* mount_options) {
  if (mount(src, target, fs_type, mount_flags, mount_options) == -1) {
    const char* nullstr = "NULL";
    src = (src != NULL) ? src : nullstr;
    fs_type = (fs_type != NULL)