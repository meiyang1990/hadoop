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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import java.net.InetAddress;

/**
 * HDFS NameNode审计日志接口，定义审计日志记录的统一规范。
 * 负责为NameNode的所有权限校验和文件操作请求生成审计日志，
 * 支持自定义扩展不同的日志输出实现，满足安全合规审计需求。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AuditLogger {

  /**
   * 初始化审计日志器，在NameNode启动时调用。
   *
   * @param conf Hadoop配置对象，用于读取审计日志相关配置
   */
  void initialize(Configuration conf);

  /**
   * 记录一次HDFS操作的审计事件。
   * <p>
   * 此方法必须尽可能快速返回，因为它会在NameNode核心处理流程中被调用，
   * 阻塞会严重影响NameNode整体性能。
   *
   * @param succeeded 权限认证是否成功
   * @param userName 执行请求的用户名
   * @param addr 请求来源的远程IP地址
   * @param cmd 请求执行的HDFS操作命令
   * @param src 受操作影响的源文件路径
   * @param dst 受操作影响的目标文件路径（若无则为null）
   * @param stat 修改元数据操作（权限、所有者、时间等）对应的文件信息，非修改元类操作为null
   */
  void logAuditEvent(boolean succeeded, String userName,
      InetAddress addr, String cmd, String src, String dst,
      FileStatus stat);

}