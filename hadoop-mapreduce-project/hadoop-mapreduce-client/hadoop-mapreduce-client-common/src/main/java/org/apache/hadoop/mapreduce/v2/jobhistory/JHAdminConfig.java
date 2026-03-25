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

package org.apache.hadoop.mapreduce.v2.jobhistory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.http.HttpConfig;

/**
 * 存放MapReduce作业历史服务器（Job History Server）可配置参数的常量类，
 * 定义了所有支持管理员配置的历史服务配置键和默认值，供整个历史服务模块统一使用。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class JHAdminConfig {
  /** 所有作业历史配置属性的公共前缀 */
  public static final String MR_HISTORY_PREFIX = "mapreduce.jobhistory.";
  
  /** 历史服务器API服务的主机:端口地址配置键 */
  public static final String MR_HISTORY_ADDRESS = MR_HISTORY_PREFIX + "address";
  public static final int DEFAULT_MR_HISTORY_PORT = 10020;
  public static final String DEFAULT_MR_HISTORY_ADDRESS = "0.0.0.0:" +
      DEFAULT_MR_HISTORY_PORT;
  /** 历史服务绑定网卡地址配置键 */
  public static final String MR_HISTORY_BIND_HOST = MR_HISTORY_PREFIX
      + "bind-host";

  /** 历史服务器管理接口地址配置键 */
  public static final String JHS_ADMIN_ADDRESS = MR_HISTORY_PREFIX
      + "admin.address";
  public static final int DEFAULT_JHS_ADMIN_PORT = 10033;
  public static final String DEFAULT_JHS_ADMIN_ADDRESS = "0.0.0.0:"
      + DEFAULT_JHS_ADMIN_PORT;

  /** 允许访问历史服务器管理接口的ACL配置键 */
  public static final String JHS_ADMIN_ACL = MR_HISTORY_PREFIX + "admin.acl";
  public static final String DEFAULT_JHS_ADMIN_ACL = "*";
  
  /** 是否启用历史文件清理功能配置键 */
  public static final String MR_HISTORY_CLEANER_ENABLE = 
    MR_HISTORY_PREFIX + "cleaner.enable";
  
  /** 历史清理器运行间隔（毫秒）配置键 */
  public static final String MR_HISTORY_CLEANER_INTERVAL_MS = 
    MR_HISTORY_PREFIX + "cleaner.interval-ms";
  public static final long DEFAULT_MR_HISTORY_CLEANER_INTERVAL_MS = 
    1 * 24 * 60 * 60 * 1000l; //1 day

  /** 是否强制每次都扫描用户目录，忽略目录修改时间判断配置键 */
  public static final String MR_HISTORY_ALWAYS_SCAN_USER_DIR =
      MR_HISTORY_PREFIX + "always-scan-user-dir";
  public static final boolean DEFAULT_MR_HISTORY_ALWAYS_SCAN_USER_DIR =
      false;

  /** 处理客户端API请求的线程数配置键 */
  public static final String MR_HISTORY_CLIENT_THREAD_COUNT = 
    MR_HISTORY_PREFIX + "client.thread-count";
  public static final int DEFAULT_MR_HISTORY_CLIENT_THREAD_COUNT = 10;
  
  /**
   * 日期字符串缓存大小配置键，该大小会影响查找作业时需要扫描的目录数量。
   */
  public static final String MR_HISTORY_DATESTRING_CACHE_SIZE = 
    MR_HISTORY_PREFIX + "datestring.cache.size";
  public static final int DEFAULT_MR_HISTORY_DATESTRING_CACHE_SIZE = 200000;
  
  /** 已完成作业历史文件存储路径配置键 */
  public static final String MR_HISTORY_DONE_DIR =
    MR_HISTORY_PREFIX + "done-dir";

  /**
   * 历史服务器启动时，等待历史文件文件系统就绪的最大等待时间，默认值-1表示永久等待。
   */
  public static final String MR_HISTORY_MAX_START_WAIT_TIME =
      MR_HISTORY_PREFIX + "maximum-start-wait-time-millis";
  public static final long DEFAULT_MR_HISTORY_MAX_START_WAIT_TIME = -1;
  /**
   *  作业完成后、被移入历史服务器正式目录前，历史文件存储的中间目录路径配置键。
   */
  public static final String MR_HISTORY_INTERMEDIATE_DONE_DIR =
    MR_HISTORY_PREFIX + "intermediate-done-dir";
  /** 中间用户完成目录权限配置键 */
  public static final String MR_HISTORY_INTERMEDIATE_USER_DONE_DIR_PERMISSIONS =
      MR_HISTORY_PREFIX + "intermediate-user-done-dir.permissions";
  public static final short
      DEFAULT_MR_HISTORY_INTERMEDIATE_USER_DONE_DIR_PERMISSIONS = 0770;
  
  /** 作业列表缓存大小配置键 */
  public static final String MR_HISTORY_JOBLIST_CACHE_SIZE =
    MR_HISTORY_PREFIX + "joblist.cache.size";
  public static final int DEFAULT_MR_HISTORY_JOBLIST_CACHE_SIZE = 20000;

  /** Kerberos认证keytab文件路径配置键 */
  public static final String MR_HISTORY_KEYTAB = MR_HISTORY_PREFIX + "keytab";
  
  /** 已加载作业缓存大小（作业数）配置键 */
  public static final String MR_HISTORY_LOADED_JOB_CACHE_SIZE = 
    MR_HISTORY_PREFIX + "loadedjobs.cache.size";
  public static final int DEFAULT_MR_HISTORY_LOADED_JOB_CACHE_SIZE = 5;

  /** 已加载作业缓存大小（任务数）配置键 */
  public static final String MR_HISTORY_LOADED_TASKS_CACHE_SIZE =
      MR_HISTORY_PREFIX + "loadedtasks.cache.size";

  /**
   * 历史文件被删除前允许保留的最长时间配置键。
   */
  public static final String MR_HISTORY_MAX_AGE_MS =
    MR_HISTORY_PREFIX + "max-age-ms";
  public static final long DEFAULT_MR_HISTORY_MAX_AGE = 
    7 * 24 * 60 * 60 * 1000L; //1 week
  
  /**
   * 从中间目录移动历史文件到正式目录的扫描间隔（毫秒）配置键。
   */
  public static final String MR_HISTORY_MOVE_INTERVAL_MS = 
    MR_HISTORY_PREFIX + "move.interval-ms";
  public static final long DEFAULT_MR_HISTORY_MOVE_INTERVAL_MS = 
    3 * 60 * 1000l; //3 minutes
  
  /** 移动历史文件使用的线程数配置键 */
  public static final String MR_HISTORY_MOVE_THREAD_COUNT = 
    MR_HISTORY_PREFIX + "move.thread-count";
  public static final int DEFAULT_MR_HISTORY_MOVE_THREAD_COUNT = 3;
  
  /** 历史服务器Kerberos主体名称配置键 */
  public static final String MR_HISTORY_PRINCIPAL = 
    MR_HISTORY_PREFIX + "principal";
  
  /** 历史服务器HTTP策略配置键，用于控制是否启用HTTPS */
  public static final String MR_HS_HTTP_POLICY = MR_HISTORY_PREFIX
      + "http.policy";
  public static String DEFAULT_MR_HS_HTTP_POLICY =
          HttpConfig.Policy.HTTP_ONLY.name();
  
  /** 历史服务器Web服务HTTP地址配置键 */
  public static final String MR_HISTORY_WEBAPP_ADDRESS =
    MR_HISTORY_PREFIX + "webapp.address";
  public static final int DEFAULT_MR_HISTORY_WEBAPP_PORT = 19888;
  public static final String DEFAULT_MR_HISTORY_WEBAPP_ADDRESS =
    "0.0.0.0:" + DEFAULT_MR_HISTORY_WEBAPP_PORT;
  
  /** 历史服务器Web服务HTTPS地址配置键 */
  public static final String MR_HISTORY_WEBAPP_HTTPS_ADDRESS =
      MR_HISTORY_PREFIX + "webapp.https.address";
  public static final int DEFAULT_MR_HISTORY_WEBAPP_HTTPS_PORT = 19890;
  public static final String DEFAULT_MR_HISTORY_WEBAPP_HTTPS_ADDRESS =
      "0.0.0.0:" + DEFAULT_MR_HISTORY_WEBAPP_HTTPS_PORT;
  
  /** SPNEGO认证使用的Kerberos主体名称配置键 */
  public static final String MR_WEBAPP_SPNEGO_USER_NAME_KEY =
      MR_HISTORY_PREFIX + "webapp.spnego-principal";
  
  /** SPNEGO认证使用的Kerberos keytab文件路径配置键 */
  public static final String MR_WEBAPP_SPNEGO_KEYTAB_FILE_KEY =
      MR_HISTORY_PREFIX + "webapp.spnego-keytab-file";

  /*
   * 历史服务安全授权相关配置
   */
  /** 历史服务客户端协议ACL配置键 */
  public static final String MR_HS_SECURITY_SERVICE_AUTHORIZATION =
      "security.mrhs.client.protocol.acl";
  /** 历史服务管理刷新协议ACL配置键 */
  public static final String MR_HS_SECURITY_SERVICE_AUTHORIZATION_ADMIN_REFRESH =
      "security.mrhs.admin.refresh.protocol.acl";

  /**
   * 用于缓存历史数据的HistoryStorage实现类配置键。
   */
  public static final String MR_HISTORY_STORAGE =
    MR_HISTORY_PREFIX + "store.class";

  /**
   * 是否启用历史服务器状态恢复功能，启动时恢复服务状态配置键。
   */
  public static final String MR_HS_RECOVERY_ENABLE =
      MR_HISTORY_PREFIX + "recovery.enable";
  public static final boolean DEFAULT_MR_HS_RECOVERY_ENABLE = false;

  /**
   * 存储和恢复服务状态的HistoryServerStateStoreService实现类配置键。
   */
  public static final String MR_HS_STATE_STORE =
      MR_HISTORY_PREFIX + "recovery.store.class";

  /**
   * 当使用HDFS作为状态存储时，服务状态存储URI配置键。
   */
  public static final String MR_HS_FS_STATE_STORE_URI =
      MR_HISTORY_PREFIX + "recovery.store.fs.uri";

  /**
   * 当使用LevelDB作为状态存储时，服务状态存储本地路径配置键。
   */
  public static final String MR_HS_LEVELDB_STATE_STORE_PATH =
      MR_HISTORY_PREFIX + "recovery.store.leveldb.path";

  /** 迷你集群是否使用固定端口配置键 */
  public static final String MR_HISTORY_MINICLUSTER_FIXED_PORTS = MR_HISTORY_PREFIX
       + "minicluster.fixed.ports";
  
  /**
   * 默认值为false，允许并发运行测试，避免端口冲突。
   */
  public static boolean DEFAULT_MR_HISTORY_MINICLUSTER_FIXED_PORTS = false;

  /**
   * 历史服务器Web页面显示作业名称允许的最大字符数配置键。
   */
  public static final String MR_HS_JOBNAME_LIMIT = MR_HISTORY_PREFIX
      + "jobname.limit";
  public static final int DEFAULT_MR_HS_JOBNAME_LIMIT = 50;


  /**
   * CSRF防护配置前缀。
   */
  public static final String MR_HISTORY_CSRF_PREFIX = MR_HISTORY_PREFIX +
                                                      "webapp.rest-csrf.";
  /** CSRF防护是否启用配置键 */
  public static final String MR_HISTORY_CSRF_ENABLED = MR_HISTORY_CSRF_PREFIX +
                                                       "enabled";
  /** CSRF自定义请求头配置键 */
  public static final String MR_HISTORY_CSRF_CUSTOM_HEADER =
      MR_HISTORY_CSRF_PREFIX + "custom-header";
  /** CSRF防护需要忽略的请求方法配置键 */
  public static final String MR_HISTORY_METHODS_TO_IGNORE =
      MR_HISTORY_CSRF_PREFIX + "methods-to-ignore";

  /**
   * X-Frame-Options配置前缀。
   */
  public static final String MR_HISTORY_XFS_PREFIX = MR_HISTORY_PREFIX +
      "webapp.xfs-filter.";
  /** X-Frame-Options选项值配置键 */
  public static final String MR_HISTORY_XFS_OPTIONS = MR_HISTORY_XFS_PREFIX +
      "xframe-options";

  /**
   * CORS跨域资源共享配置。
   */
  /** 是否启用CORS过滤器配置键 */
  public static final String MR_HISTORY_ENABLE_CORS_FILTER = MR_HISTORY_PREFIX +
      "webapp.cross-origin.enabled";
  public static final boolean DEFAULT_MR_HISTORY_ENABLE_CORS_FILTER = false;

  /**
   * .jhist历史文件格式配置。
   */
  /** 历史文件格式配置键 */
  public static final String MR_HS_JHIST_FORMAT =
      MR_HISTORY_PREFIX + "jhist.format";
  public static final String DEFAULT_MR_HS_JHIST_FORMAT =
      "binary";

  /**
   * 历史服务器允许加载单个作业的最大任务数配置键。
   */
  public static final String MR_HS_LOADED_JOBS_TASKS_MAX =
      MR_HISTORY_PREFIX + "loadedjob.tasks.max";
  public static final int DEFAULT_MR_HS_LOADED_JOBS_TASKS_MAX = -1;
}