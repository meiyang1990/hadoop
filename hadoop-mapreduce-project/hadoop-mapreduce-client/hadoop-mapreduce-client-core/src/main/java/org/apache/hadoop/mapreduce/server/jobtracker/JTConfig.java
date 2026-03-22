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
package org.apache.hadoop.mapreduce.server.jobtracker;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.MRConfig;

/**
 * JobTracker服务端配置项容器，集中定义所有JobTracker相关的配置常量
 * 所有配置键统一使用 "mapreduce.jobtracker." 作为前缀
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface JTConfig extends MRConfig {
  // JobTracker配置参数常量定义
  /** JobTracker RPC服务地址配置键 */
  public static final String JT_IPC_ADDRESS  = "mapreduce.jobtracker.address";
  /** 是否持久化作业状态配置键 */
  public static final String JT_PERSIST_JOBSTATUS =
    "mapreduce.jobtracker.persist.jobstatus.active";

  /** 是否启用作业自动退休配置键 */
  public static final String JT_RETIREJOBS =
    "mapreduce.jobtracker.retirejobs";
  /** 任务缓存层级配置键 */
  public static final String JT_TASKCACHE_LEVELS =
    "mapreduce.jobtracker.taskcache.levels";
  /** JobTracker系统目录配置键，用于存储JobTracker元数据 */
  public static final String JT_SYSTEM_DIR = "mapreduce.jobtracker.system.dir";
  /** 作业 staging 区域根目录配置键，存储作业临时文件 */
  public static final String JT_STAGING_AREA_ROOT = 
    "mapreduce.jobtracker.staging.root.dir";
  /** 允许单个Map任务最大内存配置键，单位MB */
  public static final String JT_MAX_MAPMEMORY_MB =
    "mapreduce.jobtracker.maxmapmemory.mb";
  /** 允许单个Reduce任务最大内存配置键，单位MB */
  public static final String JT_MAX_REDUCEMEMORY_MB = 
    "mapreduce.jobtracker.maxreducememory.mb";
  /** JobTracker Kerberos认证主体名称配置键 */
  public static final String JT_USER_NAME = "mapreduce.jobtracker.kerberos.principal";
}