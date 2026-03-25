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
 * YARN NodeManager 容器执行器配置模块头文件
 * 负责容器执行器配置文件的读取、解析和查询，提供权限检查功能保障配置安全
 */

#ifndef __YARN_CONTAINER_EXECUTOR_CONFIG_H__
#define __YARN_CONTAINER_EXECUTOR_CONFIG_H__

#ifdef __FreeBSD__
// FreeBSD系统需要启用getline函数支持
#define _WITH_GETLINE
#endif

#include "config.h"

// 容器执行器默认配置文件名
#define CONF_FILENAME "container-executor.cfg"

// 当通过Maven构建时，该值由container-executor.conf.dir属性自动定义
// 详情参见 hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/pom.xml
// 注意：如果是相对路径，会相对于容器执行器二进制文件所在目录解析，而非当前工作目录
#ifndef HADOOP_CONF_DIR
#error HADOOP_CONF_DIR must be defined
#endif

#include <stddef.h>

// 配置数据结构定义
// 键值对结构，保存单个配置项
struct kv_pair {
  const char *key;
  const char *value;
};

// 配置section结构，保存一组同属一个段的配置项
struct section {
  int size;
  char *name;
  struct kv_pair **kv_pairs;
};

// 整体配置结构，保存所有配置段
struct configuration {
  int size;
  struct section **sections;
};

/**
 * 检查配置文件及其所在目录的权限，确保只有root用户可写
 * 防止攻击者修改配置导致安全问题，是容器执行器必要的安全检查
 *
 * @param file_name 配置文件路径名
 *
 * @returns 0 权限正确，非0 检查出错
 */
int check_configuration_permissions(const char *file_name);

/**
 * 通过realpath解析得到配置文件的绝对路径
 * 相对路径相对于传入的root参数解析，而非当前工作目录
 * 返回值需要调用者手动释放内存
 *
 * @param file_name 配置文件名
 * @param root 用于解析相对路径的根目录
 *
 * @returns 解析完成的配置文件绝对路径
 */
char* resolve_config_path(const char *file_name, const char *root);

/**
 * 读取指定配置文件，解析结果存入配置结构中
 * 调用者需要调用free_configuration释放分配的内存
 * 函数内部会自动检查配置文件权限是否符合要求
 *
 * @param file_path 待读取的配置文件路径
 * @param cfg 用于存放解析结果的配置结构
 *
 * @return 0 成功，非-zero 读取/解析错误
 */
int read_config(const char *file_path, struct configuration *cfg);

/**
 * 在指定配置段中查找指定key对应的值
 * 返回值需要调用者手动释放内存
 *
 * @param key 配置项key名称
 * @param section 待查找的配置段
 *
 * @return 找到则返回值指针，未找到返回NULL
 */
char* get_section_value(const char *key, const struct section *section);

/**
 * 在指定配置段中查找指定key对应的值，按逗号切分为多个值
 * 返回的数组需要调用者手动释放内存
 *
 * @param key 待查找的key
 * @param section 待查找的配置段
 *
 * @return 切分后的值数组，未找到key返回NULL
 */
char** get_section_values(const char *key, const struct section *section);

/**
 * 在指定配置段中查找指定key对应的值，按指定分隔符切分为多个值
 * 返回的数组需要调用者手动释放内存
 *
 * @param key 待查找的key
 * @param section 待查找的配置段
 * @param delim 用于切分的分隔符
 *
 * @return 切分后的值数组，未找到key返回NULL
 */
char** get_section_values_delimiter(const char *key, const struct section *section,
    const char *delim);

/**
 * 在整个配置中查找指定段、指定key对应的值
 * 返回值需要调用者手动释放内存
 *
 * @param key 配置项key名称
 * @param section 配置段名称
 * @param cfg 整个配置结构
 *
 * @return 找到则返回值指针，未找到返回NULL
 */
char* get_configuration_value(const char *key, const char* section,
    const struct configuration *cfg);

/**
 * 在整个配置中查找指定段、指定key对应的值，按逗号切分为多个值
 * 返回的数组需要调用者手动释放内存
 *
 * @param key 待查找的key
 * @param section 配置段名称
 * @param cfg 整个配置结构
 *
 * @return 切分后的值数组，未找到key返回NULL
 */
char** get_configuration_values(const char *key, const char* section,
    const struct configuration *cfg);

/**
 * 在整个配置中查找指定段、指定key对应的值，按指定分隔符切分为多个值
 * 返回的数组需要调用者手动释放内存
 *
 * @param key 待查找的key
 * @param section 配置段名称
 * @param cfg 整个配置结构
 * @param delimiter 用于切分的分隔符
 *
 * @return 切分后的值数组，未找到key返回NULL
 */
char** get_configuration_values_delimiter(const char *key, const char* section,
    const struct configuration *cfg, const char *delimiter);

/**
 * 根据名称从配置中获取指定配置段
 *
 * @param section 要获取的配置段名称
 * @param cfg 整个配置结构
 *
 * @return 配置段结构指针，出错返回NULL
 */
struct section* get_configuration_section(const char *section,
    const struct configuration *cfg);

/**
 * 释放整个配置结构分配的内存
 *
 * @param cfg 要释放的配置结构指针
 */
void free_configuration(struct configuration *cfg);

/**
 * 从key=val格式的字符串中解析出key部分
 *
 * @param input 输入字符串
 * @param out 输出缓冲区，存放解析出的key
 * @param out_len 输出缓冲区长度
 *
 * @return -ENAMETOOLONG 缓冲区长度不足；-EINVAL 输入中没有等号；0 解析成功
 */
int get_kv_key(const char *input, char *out, size_t out_len);

/**
 * 从key=val格式的字符串中解析出value部分
 *
 * @param input 输入字符串
 * @param out 输出缓冲区，存放解析出的value
 * @param out_len 输出缓冲区长度
 *
 * @return -ENAMETOOLONG 缓冲区长度不足；-EINVAL 输入中没有等号；0 解析成功
 */
int get_kv_value(const char *input, char *out, size_t out_len);

// 根据执行器路径得到配置文件完整路径
char *get_config_path(const char* argv0);

#endif