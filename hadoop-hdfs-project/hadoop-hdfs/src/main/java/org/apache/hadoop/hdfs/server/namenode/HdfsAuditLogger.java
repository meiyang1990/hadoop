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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.security.UserGroupInformation;

import java.net.InetAddress;

/**
 * HDFS 审计日志扩展抽象基类，继承自通用的 {@link AuditLogger} 接口。
 * 提供了支持代理令牌追踪、调用上下文等HDFS特有审计信息的抽象方法，
 * 供不同的审计日志实现类扩展，用于记录HDFS名称节点上的所有文件操作审计事件。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class HdfsAuditLogger implements AuditLogger {

  @Override
  public void logAuditEvent(boolean succeeded, String userName,
      InetAddress addr, String cmd, String src, String dst,
      FileStatus status) {
    // 调用完整参数的抽象方法，缺省填充调用上下文、用户信息和令牌管理器为null
    logAuditEvent(succeeded, userName, addr, cmd, src, dst, status,
        null /*callerContext*/, null /*ugi*/, null /*dtSecretManager*/);
  }

  /**
   * 记录HDFS审计事件，支持代理令牌追踪ID等额外HDFS特有信息。
   * 
   * @param succeeded 授权是否成功
   * @param userName 执行请求的用户名
   * @param addr 请求来源的远程地址
   * @param cmd 请求执行的操作命令
   * @param src 受影响的源文件路径
   * @param dst 受影响的目标文件路径（如果存在）
   * @param stat 修改元数据操作对应的文件信息（权限、所有者、时间等）
   * @param callerContext 调用方上下文信息
   * @param ugi 当前用户的UserGroupInformation，不记录令牌追踪信息时为null
   * @param dtSecretManager 代理令牌密钥管理器，不记录令牌追踪信息时为null
   */
  public abstract void logAuditEvent(boolean succeeded, String userName,
      InetAddress addr, String cmd, String src, String dst,
      FileStatus stat, CallerContext callerContext, UserGroupInformation ugi,
      DelegationTokenSecretManager dtSecretManager);

  /**
   * 记录HDFS审计事件，不包含调用上下文信息。
   * 
   * @param succeeded 授权是否成功
   * @param userName 执行请求的用户名
   * @param addr 请求来源的远程地址
   * @param cmd 请求执行的操作命令
   * @param src 受影响的源文件路径
   * @param dst 受影响的目标文件路径（如果存在）
   * @param stat 修改元数据操作对应的文件信息（权限、所有者、时间等）
   * @param ugi 当前用户的UserGroupInformation，不记录令牌追踪信息时为null
   * @param dtSecretManager 代理令牌密钥管理器，不记录令牌追踪信息时为null
   */
  public abstract void logAuditEvent(boolean succeeded, String userName,
      InetAddress addr, String cmd, String src, String dst,
      FileStatus stat, UserGroupInformation ugi,
      DelegationTokenSecretManager dtSecretManager);
}