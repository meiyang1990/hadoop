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
 * @file runc_reap.c
 * @brief YARN NodeManager runC容器层挂载回收实现
 * 负责回收不再使用的runc overlayfs层挂载，清理已删除文件对应的回环设备挂载，
 * 通过LRU策略保留指定数量的最近使用层挂载，节省系统资源
 */
#include <sys/types.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <mntent.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "container-executor.h"

#include "runc_base_ctx.h"
#include "runc_reap.h"

#include "util.h"

#define DEV_LOOP_PREFIX       "/dev/loop"
#define DEV_LOOP_PREFIX_LEN   (sizeof(DEV_LOOP_PREFIX) - 1)
#define DELETED_SUFFIX        " (deleted)\n"
#define DELETED_SUFFIX_LEN    (sizeof(DELETED_SUFFIX) - 1)

// The size of the buffer to use when reading the mount table. This should be
// large enough so ideally the mount table is read all at once.
// Otherwise the mount table could change in-between underlying read() calls
// and result in a table with missing or corrupted entries.
#define MOUNT_TABLE_BUFFER_SIZE (1024*1024)

// NOTE: Update destroy_dent_stat when this is updated.
/**
 * @brief 目录条目信息结构，存储文件名和修改时间
 */
typedef struct dent_stat_struct {
  char* basename;         // 目录条目的文件名
  struct timespec mtime;  // 修改时间，用于LRU排序
} dent_stat;

// NOTE: Update init_dent_stats and destroy_dent_stats when this is changed.
/**
 * @brief 目录条目信息数组结构，动态扩容存储
 */
typedef struct dent_stats_array_struct {
  dent_stat* stats;       // 目录条目信息数组
  size_t capacity;        // 数组容量
  size_t length;          // 数组中有效条目数量
} dent_stats_array;


/**
 * 释放单个目录条目信息的内存，不释放结构本身
 * 适用于栈分配或嵌入其他结构的场景
 */
static void destroy_dent_stat(dent_stat* ds) {
  if (ds != NULL) {
    free(ds->basename);
    ds->basename = NULL;
  }
}

/**
 * 初始化目录条目数组，指定初始容量
 * @return 初始化成功返回true，失败返回false
 */
static bool init_dent_stats(dent_stats_array* dsa, size_t initial_size) {
  memset(dsa, 0, sizeof(*dsa));
  dsa->stats = malloc(sizeof(*dsa->stats) * initial_size);
  if (dsa->stats == NULL) {
    return false;
  }
  dsa->capacity = initial_size;
  dsa->length = 0;
  return true;
}

/**
 * 分配并初始化目录条目数组，指定初始容量
 * @return 成功返回数组指针，失败返回NULL
 */
static dent_stats_array* alloc_dent_stats(size_t initial_size) {
  dent_stats_array* dsa = malloc(sizeof(*dsa));
  if (dsa != NULL) {
    if (!init_dent_stats(dsa, initial_size)) {
      free(dsa);
      dsa = NULL;
    }
  }
  return dsa;
}

/**
 * 扩容目录条目数组到指定新容量
 * @return 扩容成功返回true，失败返回false
 */
static bool realloc_dent_stats(dent_stats_array* dsa, size_t new_size) {
  if (new_size < dsa->length) {
    // New capacity would result in a truncation.
    return false;
  }

  dent_stat* new_stats = realloc(dsa->stats, new_size * sizeof(*dsa->stats));
  if (new_stats == NULL) {
    return false;
  }

  dsa->stats = new_stats;
  dsa->capacity = new_size;
  return true;
}

/**
 * 追加新目录条目到数组，容量不足时自动扩容
 * @return 追加成功返回true，失败返回false
 */
static bool append_dent_stat(dent_stats_array* dsa, size_t stats_size_incr,
    const char* basename, const struct timespec* mtime) {
  if (dsa->length == dsa->capacity) {
    if (!realloc_dent_stats(dsa, dsa->capacity + stats_size_incr)) {
      return false;
    }
  }

  char* ds_name = strdup(basename);
  if (ds_name == NULL) {
    return false;
  }

  dent_stat* ds = &dsa->stats[dsa->length++];
  ds->basename = ds_name;
  ds->mtime = *mtime;
  return true;
}

/**
 * 释放目录条目数组所有元素的内存，不释放数组结构本身
 * 适用于栈分配或嵌入其他结构的场景
 */
static void destroy_dent_stats(dent_stats_array* dsa) {
  if (dsa != NULL ) {
    for (size_t i = 0; i < dsa->length; ++i) {
      destroy_dent_stat(&dsa->stats[i]);
    }
    free(dsa->stats);
    dsa->capacity = 0;
    dsa->length = 0;
  }
}

/**
 * 释放目录条目数组及其所有元素的全部内存
 */
static void free_dent_stats(dent_stats_array* dsa) {
  destroy_dent_stats(dsa);
  free(dsa);
}

/**
 * 读取layers目录下所有合法层条目，获取其名称和修改时间
 * @param layers_fd layers目录的文件描述符
 * @return 成功返回目录条目数组，失败返回NULL
 */
static dent_stats_array* get_dent_stats(int layers_fd) {
  DIR* layers_dir = NULL;
  // number of stat buffers to allocate each time we run out
  const size_t stats_size_incr = 8192;
  dent_stats_array* dsa = alloc_dent_stats(stats_size_incr);
  if (dsa == NULL) {
    return NULL;
  }

  // 复制目录文件描述符，用于fdopendir
  int dir_fd = dup(layers_fd);
  if (dir_fd == -1) {
    fprintf(ERRORFILE, "Unable to duplicate layer dir fd: %s\n",
        strerror(errno));
    goto fail;
  }

  // 通过文件描述符打开目录流
  layers_dir = fdopendir(dir_fd);
  if (layers_dir == NULL) {
    fprintf(ERRORFILE, "Cannot open layers directory: %s\n", strerror(errno));
    goto fail;
  }

  struct dirent* de;
  // 遍历目录所有条目
  while ((de = readdir(layers_dir)) != NULL) {
    // 跳过不符合层命名规则的条目
    if (strlen(de->d_name) != LAYER_NAME_LENGTH) {
      continue;
    }

    struct stat statbuf;
    // 获取条目的元信息，不跟随符号链接
    if (fstatat(layers_fd, de->d_name, &statbuf, AT_SYMLINK_NOFOLLOW) == -1) {
      // 条目已被删除，跳过
      if (errno == ENOENT) {
        continue;
      }
      fprintf(ERRORFILE, "Error getting stats for layer %s : %s\n", de->d_name,
          strerror(errno));
      goto fail;
    }

    // 将条目信息添加到数组
    if (!append_dent_stat(dsa, stats_size_incr, de->d_name,
        &statbuf.st_mtim)) {
      fputs("Unable to allocate memory\n", ERRORFILE);
      goto fail;
    }
  }

cleanup:
  if (layers_dir != NULL) {
    closedir(layers_dir);
  }
  return dsa;

fail:
  free_dent_stats(dsa);
  dsa = NULL;
  goto cleanup;
}

/**
 * 卸载指定层的挂载，并删除层目录和挂载点
 * @param layer_dir_path 层目录路径
 * @return 成功返回true，失败返回false
 */
static bool unmount_layer(const char* layer_dir_path) {
  // 构造层挂载点路径
  char* mount_path = get_runc_layer_mount_path(layer_dir_path);
  if (mount_path == NULL) {
    fputs("Unable to allocate memory\n", ERRORFILE);
    return false;
  }

  bool result = false;
  // 执行卸载
  if (umount(mount_path) == -1) {
    // 挂载正被使用，不处理
    if (errno == EBUSY) {
      // Layer is in use by another container.
      goto cleanup;
    } else if (errno != ENOENT && errno != EINVAL) {
      // 非"不存在"错误，打印日志
      fprintf(ERRORFILE, "Error unmounting %s : %s\n", mount_path,
          strerror(errno));
      goto cleanup;
    }
  } else {
    // 卸载成功，即使后续删除目录失败也标记为成功
    // unmount was successful so report success even if directory removals
    // fail after this.
    result = true;
  }

  // 删除挂载点目录，不存在则忽略错误
  if (rmdir(mount_path) == -1 && errno != ENOENT) {
    fprintf(ERRORFILE, "Error removing %s : %s\n", mount_path,
        strerror(errno));
    goto cleanup;
  }

  // 删除层目录，不存在则忽略错误
  if (rmdir(layer_dir_path) == -1 && errno != ENOENT) {
    fprintf(ERRORFILE, "Error removing %s : %s\n", layer_dir_path,
        strerror(errno));
    goto cleanup;
  }

  result = true;

cleanup:
  free(mount_path);
  return result;
}

/**
 * qsort比较函数，按修改时间从小到大排序目录条目
 */
static int compare_dent_stats_mtime(const void* va, const void* vb) {
  const dent_stat* a = (const dent_stat*)va;
  const dent_stat* b = (const dent_stat*)vb;
  if (a->mtime.tv_sec < b->mtime.tv_sec) {
    return -1;
  } else if (a->mtime.tv_sec > b->mtime.tv_sec) {
    return 1;
  }
  // 秒数相等，比较纳秒
  return a->mtime.tv_nsec - b->mtime.tv_nsec;
}

/**
 * 在已获取锁的上下文执行层挂载回收，按LRU保留指定数量层
 * @param ctx runc基础上下文
 * @param layers_fd layers目录文件描述符
 * @param num_preserve 需要保留的层数量
 * @return 成功返回true，失败返回false
 */
static bool do_reap_layer_mounts_with_lock(runc_base_ctx* ctx,
    int layers_fd, int num_preserve) {
  // 获取所有层条目信息
  dent_stats_array* dsa = get_dent_stats(layers_fd);
  if (dsa == NULL) {
    return false;
  }

  // 按修改时间从小到大排序，最早修改的在前
  qsort(&dsa->stats[0], dsa->length, sizeof(*dsa->stats),
      compare_dent_stats_mtime);

  bool result = false;
  size_t num_remain = dsa->length;
  // 当前层数小于等于需要保留的数量，无需回收
  if (num_remain <= num_preserve) {
    result = true;
    goto cleanup;
  }

  // 获取层目录写锁
  if (!acquire_runc_layers_write_lock(ctx)) {
    fputs("Unable to acquire layer write lock\n", ERRORFILE);
    goto cleanup;
  }

  // 从最早修改的层开始卸载，直到剩余层数不超过保留数量
  for (size_t i = 0; i < dsa->length && num_remain > num_preserve; ++i) {
    // 构造层目录路径
    char* layer_dir_path = get_runc_layer_path(ctx->run_root,
        dsa->stats[i].basename);
    if (layer_dir_path == NULL) {
      fputs("Unable to allocate memory\n", ERRORFILE);
      goto cleanup;
    }
    // 卸载该层，成功则剩余数量减一
    if (unmount_layer(layer_dir_path)) {
      --num_remain;
      printf("Unmounted layer %s\n", dsa->stats[i].basename);
    }
    free(layer_dir_path);
  }

  result = true;

cleanup:
  free_dent_stats(dsa);
  return result;
}

/**
 * 检查指定回环设备对应的后端文件是否已被删除
 * @param loopdev 回环设备路径
 * @return 已删除返回true，否则或错误返回false
 */
bool is_loop_file_deleted(const char* loopdev) {
  bool result = false;
  FILE* f = NULL;
  char* path = NULL;
  char* linebuf = NULL;

  // 提取回环设备编号部分
  const char* loop_num_str = loopdev + DEV_LOOP_PREFIX_LEN;

  // 构造sysfs中回环设备后端文件路径
  if (asprintf(&path, "/sys/devices/virtual/block/loop%s/loop/backing_file",
      loop_num_str) == -1) {
    return false;
  }

  // 打开后端文件信息文件
  f = fopen(path, "r");
  if (f == NULL) {
    goto cleanup;
  }

  size_t linebuf_len = 0;
  // 读取一行内容
  ssize_t len = getline(&linebuf, &linebuf_len, f);
  // 长度不足以包含删除标记，直接返回未删除
  if (len <= DELETED_SUFFIX_LEN) {
    goto cleanup;
  }

  // 检查末尾是否匹配已删除后缀
  result = !strcmp(DELETED_SUFFIX, linebuf + len - DELETED_SUFFIX_LEN);

cleanup:
  if (f != NULL) {
    fclose(f);
  }
  free(linebuf);
  free(path);
  return result;
}

/**
 * 复制挂载表条目，深度复制字符串字段
 * @param dest 目标条目
 * @param src 源条目
 * @return 复制成功返回true，失败返回false
 */
static bool copy_mntent(struct mntent* dest, const struct mntent* src) {
  memset(dest, 0, sizeof(*dest));
  if (src->mnt_fsname != NULL) {
    dest->mnt_fsname = strdup(src->mnt_fsname);
    if (dest->mnt_fsname == NULL) {
      return false;
    }
  }
  if (src->mnt_dir != NULL) {
    dest->mnt_dir = strdup(src->mnt_dir);
    if (dest->mnt_dir == NULL) {
      return false;
    }
  }
  if (src->mnt_type != NULL) {
    dest->mnt_type = strdup(src->mnt_type);
    if (dest->mnt_type == NULL) {
      return false;
    }
  }
  if (src->mnt_opts != NULL) {
    dest->mnt_opts = strdup(src->mnt_opts);
    if (dest->mnt_opts == NULL) {
      return false;
    }
  }
  dest->mnt_freq = src->mnt_freq;
  dest->mnt_passno = src->mnt_passno;
  return true;
}

/**
 * 释放挂载表数组所有条目和数组本身的内存
 */
static void free_mntent_array(struct mntent* entries, size_t num_entries) {
  if (entries != NULL) {
    for (size_t i = 0; i < num_entries; ++i) {
      struct mntent* me = entries + i;
      free(me->mnt_fsname);
      free(me->mnt_dir);
      free(me->mnt_type);
      free(me->mnt_opts);
    }
    free(entries);
  }
}

/**
 * 从系统挂载表中筛选出所有位于layers路径下的回环设备层挂载