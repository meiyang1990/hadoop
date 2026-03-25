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
 * @file cgroups-operations.h
 * @brief YARN NodeManager cgroups操作接口头文件，提供对Linux cgroups的参数操作能力
 */

#ifndef _CGROUPS_OPERATIONS_H_
#define _CGROUPS_OPERATIONS_H_

// 配置文件中cgroups配置段名称
#define CGROUPS_SECTION_NAME "cgroups"
// 配置项：cgroups根路径key
#define CGROUPS_ROOT_KEY "root"
// 配置项：YARN用户自定义层级key
#define CGROUPS_YARN_HIERARCHY_KEY "yarn-hierarchy"

/**
 * 更新指定cgroup层级的参数值
 * 
 * @param hierarchy_name cgroup层级名称，例如: devices / cpu,cpuacct
 * @param param_name 要更新的参数名称，例如: deny
 * @param group_id 目标cgroup分组ID，例如: container_x_y
 * @param value 参数值，例如: "a *:* rwm"
 * @return 操作成功返回0，失败返回非0错误码
 */
int update_cgroups_parameters(
   const char* hierarchy_name,
   const char* param_name,
   const char* group_id,
   const char* value);

 /**
  * 获取待写入参数的cgroup文件完整路径，暴露用于单元测试
  * 
  * @param hierarchy_name cgroup层级名称
  * @param param_name 参数名称
  * @param group_id cgroup分组ID
  * @return 成功返回分配的路径字符串指针，失败返回NULL，调用者需要释放内存
  */
 char* get_cgroups_path_to_write(
    const char* hierarchy_name,
    const char* param_name,
    const char* group_id);

 /**
  * 从文件系统重新加载cgroups配置，暴露用于单元测试
  */
 void reload_cgroups_configuration();

#endif