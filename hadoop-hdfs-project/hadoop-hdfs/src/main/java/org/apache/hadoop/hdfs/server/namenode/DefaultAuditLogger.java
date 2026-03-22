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

package org.apache.hadoop.hdfs.server.namenode;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * HDFS审计日志的抽象基类，为NameNode和Router提供审计事件日志记录能力。
 * 当配置文件未指定自定义访问日志记录器时，系统会使用该类的具体实现，
 * 开发者可以继承此类扩展自定义审计日志逻辑。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class DefaultAuditLogger extends HdfsAuditLogger {
  // 线程本地缓存StringBuilder，避免重复创建对象优化性能
  protected static final ThreadLocal<StringBuilder> STRING_BUILDER =
      new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
          return new StringBuilder();
        }
      };

  // 调用者上下文功能是否启用，volatile保证多线程可见性
  protected volatile boolean isCallerContextEnabled;

  /** 调用者上下文字符串的最大长度限制 */
  protected int callerContextMaxLen;
  // 调用者签名字符串的最大长度限制
  protected int callerSignatureMaxLen;

  /** 是否为所有审计日志事件添加追踪ID */
  protected boolean logTokenTrackingId;

  /** 需要输出调试日志的命令集合 */
  protected Set<String> debugCmdSet = new HashSet<>();

  /**
   * 设置是否启用调用者上下文日志记录。
   * @param value true表示启用，false表示禁用
   */
  void setCallerContextEnabled(final boolean value) {
    isCallerContextEnabled = value;
  }

  /**
   * 获取调用者上下文是否启用的状态。
   * @return true表示已启用，false表示已禁用
   */
  boolean getCallerContextEnabled() {
    return isCallerContextEnabled;
  }

  /**
   * 初始化审计日志记录器，从配置中加载参数。
   * @param conf Hadoop配置对象
   */
  public abstract void initialize(Configuration conf);

  /**
   * 直接记录自定义审计消息。
   * @param message 要记录的审计消息内容
   */
  public abstract void logAuditMessage(String message);

  /**
   * 记录HDFS操作审计事件。
   * @param succeeded 操作是否成功
   * @param userName 操作用户名
   * @param addr 客户端地址
   * @param cmd 操作命令
   * @param src 源路径
   * @param dst 目标路径
   * @param status 目标文件状态
   * @param ugi 用户组信息
   * @param dtSecretManager 令牌密钥管理器，用于获取追踪ID
   */
  public abstract void logAuditEvent(boolean succeeded, String userName,
      InetAddress addr, String cmd, String src, String dst, FileStatus status,
      UserGroupInformation ugi, DelegationTokenSecretManager dtSecretManager);

  /**
   * 记录带调用者上下文信息的HDFS操作审计事件。
   * @param succeeded 操作是否成功
   * @param userName 操作用户名
   * @param addr 客户端地址
   * @param cmd 操作命令
   * @param src 源路径
   * @param dst 目标路径
   * @param status 目标文件状态
   * @param callerContext 调用者上下文信息
   * @param ugi 用户组信息
   * @param dtSecretManager 令牌密钥管理器，用于获取追踪ID
   */
  public abstract void logAuditEvent(boolean succeeded, String userName,
      InetAddress addr, String cmd, String src, String dst, FileStatus status,
      CallerContext callerContext, UserGroupInformation ugi,
      DelegationTokenSecretManager dtSecretManager);

}