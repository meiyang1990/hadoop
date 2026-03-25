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
package org.apache.hadoop.mapreduce;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * MapReduce框架配置项常量定义接口
 * 
 * 集中存放MapReduce集群级别和作业级别的所有配置键名和默认值，
 * 所有集群级配置键统一使用"mapreduce.cluster."作为前缀。
 * 为整个MapReduce框架提供统一的配置常量集合，避免魔法字符串分散在代码各处。
 *
 */
@InterfaceAudience.Private
public interface MRConfig {

  // 集群级配置参数
  /** 集群临时目录配置键 */
  public static final String TEMP_DIR = "mapreduce.cluster.temp.dir";
  /** 集群本地目录配置键 */
  public static final String LOCAL_DIR = "mapreduce.cluster.local.dir";
  /** Map任务容器内存大小配置键，单位MB */
  public static final String MAPMEMORY_MB = "mapreduce.cluster.mapmemory.mb";
  /** Reduce任务容器内存大小配置键，单位MB */
  public static final String REDUCEMEMORY_MB = 
    "mapreduce.cluster.reducememory.mb";
  /** 是否开启服务级ACL权限控制配置键 */
  public static final String MR_ACLS_ENABLED = "mapreduce.cluster.acls.enabled";
  /** MapReduce集群管理员列表配置键 */
  public static final String MR_ADMINS =
    "mapreduce.cluster.administrators";
  /** 已废弃：超级用户组配置键 */
  @Deprecated
  public static final String MR_SUPERGROUP =
    "mapreduce.cluster.permissions.supergroup";

  // 代理令牌相关配置键
  /** 代理密钥更新间隔配置键 */
  public static final String  DELEGATION_KEY_UPDATE_INTERVAL_KEY = 
    "mapreduce.cluster.delegation.key.update-interval";
  /** 代理密钥更新间隔默认值：1天 */
  public static final long    DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT = 
    24*60*60*1000; // 1 day
  /** 代理令牌续期间隔配置键 */
  public static final String  DELEGATION_TOKEN_RENEW_INTERVAL_KEY = 
    "mapreduce.cluster.delegation.token.renew-interval";
  /** 代理令牌续期间隔默认值：1天 */
  public static final long    DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT = 
    24*60*60*1000;  // 1 day
  /** 代理令牌最大生命周期配置键 */
  public static final String  DELEGATION_TOKEN_MAX_LIFETIME_KEY = 
    "mapreduce.cluster.delegation.token.max-lifetime";
  /** 代理令牌最大生命周期默认值：7天 */
  public static final long    DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT = 
    7*24*60*60*1000; // 7 days
  
  /** 进程树资源计算器实现类配置键 */
  public static final String RESOURCE_CALCULATOR_PROCESS_TREE =
    "mapreduce.job.process-tree.class";
  /** 静态地址解析配置配置键 */
  public static final String STATIC_RESOLUTIONS = 
    "mapreduce.job.net.static.resolutions";

  /** JobTracker地址配置键 */
  public static final String MASTER_ADDRESS  = "mapreduce.jobtracker.address";
  /** JobTracker Kerberos主体名称配置键 */
  public static final String MASTER_USER_NAME = 
    "mapreduce.jobtracker.kerberos.principal";

  /** MapReduce运行框架名称配置键 */
  public static final String FRAMEWORK_NAME  = "mapreduce.framework.name";
  /** 经典框架名称：classic（对应旧的MapReduce框架） */
  public static final String CLASSIC_FRAMEWORK_NAME  = "classic";
  /** YARN框架名称：yarn（对应YARN运行框架） */
  public static final String YARN_FRAMEWORK_NAME  = "yarn";
  /** 本地运行框架名称：local（对应本地调试模式） */
  public static final String LOCAL_FRAMEWORK_NAME = "local";

  /** 任务本地输出实现类配置键 */
  public static final String TASK_LOCAL_OUTPUT_CLASS =
  "mapreduce.task.local.output.class";

  /** 任务进度状态信息最大长度配置键 */
  public static final String PROGRESS_STATUS_LEN_LIMIT_KEY =
    "mapreduce.task.max.status.length";
  /** 任务进度状态信息最大长度默认值：512 */
  public static final int PROGRESS_STATUS_LEN_LIMIT_DEFAULT = 512;

  /** 作业分片最大块位置数默认值 */
  public static final int MAX_BLOCK_LOCATIONS_DEFAULT = 15;
  /** 作业分片最大块位置数配置键 */
  public static final String MAX_BLOCK_LOCATIONS_KEY =
    "mapreduce.job.max.split.locations";

  /** Shuffle阶段是否启用SSL加密配置键 */
  public static final String SHUFFLE_SSL_ENABLED_KEY =
    "mapreduce.shuffle.ssl.enabled";

  /** Shuffle阶段启用SSL加密默认值：不启用 */
  public static final boolean SHUFFLE_SSL_ENABLED_DEFAULT = false;

  /** Reduce端Shuffle消费者插件实现类配置键 */
  public static final String SHUFFLE_CONSUMER_PLUGIN =
    "mapreduce.job.reduce.shuffle.consumer.plugin.class";

  /**
   * 配置键：是否启用IFile预读功能。
   */
  public static final String MAPRED_IFILE_READAHEAD =
    "mapreduce.ifile.readahead";

  /** IFile预读功能默认值：启用 */
  public static final boolean DEFAULT_MAPRED_IFILE_READAHEAD = true;

  /**
   * 配置键：IFile预读缓冲区大小，单位字节。
   */
  public static final String MAPRED_IFILE_READAHEAD_BYTES =
    "mapreduce.ifile.readahead.bytes";

  /** IFile预读缓冲区大小默认值：4MB */
  public static final int DEFAULT_MAPRED_IFILE_READAHEAD_BYTES =
    4 * 1024 * 1024;

  /**
   * 配置项：是否由用户显式控制MiniMR集群的资源监控配置，默认关闭。
   */
  public static final String MAPREDUCE_MINICLUSTER_CONTROL_RESOURCE_MONITORING
      = "mapreduce.minicluster.control-resource-monitoring";
  /** 小型集群资源监控默认值：关闭用户控制 */
  public static final boolean
      DEFAULT_MAPREDUCE_MINICLUSTER_CONTROL_RESOURCE_MONITORING = false;

  /**
   * 配置键：是否启用跨平台作业提交，解决不同操作系统路径格式兼容问题。
   */
  @Public
  @Unstable
  public static final String MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM =
      "mapreduce.app-submission.cross-platform";
  /** 跨平台作业提交默认值：关闭 */
  @Public
  @Unstable
  public static final boolean DEFAULT_MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM =
      false;

  /**
   * 配置键：是否启用ApplicationMaster Web UI操作功能。
   */
  String MASTER_WEBAPP_UI_ACTIONS_ENABLED =
      "mapreduce.webapp.ui-actions.enabled";
  /** ApplicationMaster Web UI操作默认值：启用 */
  boolean DEFAULT_MASTER_WEBAPP_UI_ACTIONS_ENABLED = true;
  /** MultipleOutputs关闭输出并发线程数配置键 */
  String MULTIPLE_OUTPUTS_CLOSE_THREAD_COUNT = "mapreduce.multiple-outputs-close-threads";
  /** MultipleOutputs关闭输出并发线程数默认值：10 */
  int DEFAULT_MULTIPLE_OUTPUTS_CLOSE_THREAD_COUNT = 10;
}