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

package org.apache.hadoop.mapreduce.v2.hs;

import java.net.InetAddress;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.ipc.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 历史服务器审计日志工具类，为MapReduce历史服务器提供结构化的操作审计日志能力，
 * 记录用户对历史服务的所有操作，支持成功/失败事件分类，便于安全审计和问题排查。
 */
@Private
public class HSAuditLogger {
  private static final Logger LOG =
      LoggerFactory.getLogger(HSAuditLogger.class);

  /**
   * 审计日志字段枚举，定义结构化日志中包含的键名。
   */
  enum Keys {
    USER, OPERATION, TARGET, RESULT, IP, PERMISSIONS, DESCRIPTION
  }

  /**
   * 审计日志常量定义类，包含审计结果状态和分隔符等常量。
   */
  public static class AuditConstants {
    static final String SUCCESS = "SUCCESS";
    static final String FAILURE = "FAILURE";
    static final String KEY_VAL_SEPARATOR = "=";
    static final char PAIR_SEPARATOR = '\t';

    // Some commonly used descriptions
    public static final String UNAUTHORIZED_USER = "Unauthorized user";
  }

  /**
   * 记录操作成功的审计日志。
   * @param user 发起请求的用户名
   * @param operation 请求执行的操作类型
   * @param target 操作的目标资源
   */
  public static void logSuccess(String user, String operation, String target) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target));
    }
  }

  /**
   * 构造操作成功的结构化审计日志字符串。
   * @param user 发起请求的用户名
   * @param operation 请求执行的操作类型
   * @param target 操作的目标资源
   * @return 格式化后的审计日志字符串
   */
  static String createSuccessLog(String user, String operation, String target) {
    StringBuilder b = new StringBuilder();
    start(Keys.USER, user, b);
    addRemoteIP(b);
    add(Keys.OPERATION, operation, b);
    add(Keys.TARGET, target, b);
    add(Keys.RESULT, AuditConstants.SUCCESS, b);
    return b.toString();
  }

  /**
   * 添加请求来源IP地址到审计日志。
   * @param b 日志字符串构建器
   */
  static void addRemoteIP(StringBuilder b) {
    InetAddress ip = Server.getRemoteIp();
    // ip address can be null for testcases
    if (ip != null) {
      add(Keys.IP, ip.getHostAddress(), b);
    }
  }

  /**
   * 向日志构建器添加键值对，使用制表符分隔不同键值对。
   * @param key 键名
   * @param value 键值
   * @param b 日志字符串构建器
   */
  static void add(Keys key, String value, StringBuilder b) {
    b.append(AuditConstants.PAIR_SEPARATOR).append(key.name())
        .append(AuditConstants.KEY_VAL_SEPARATOR).append(value);
  }

  /**
   * 向日志构建器添加第一个键值对，无需前置分隔符。
   * @param key 键名
   * @param value 键值
   * @param b 日志字符串构建器
   */
  static void start(Keys key, String value, StringBuilder b) {
    b.append(key.name()).append(AuditConstants.KEY_VAL_SEPARATOR).append(value);
  }

  /**
   * 记录操作失败的审计日志。
   * @param user 发起请求的用户名
   * @param operation 请求执行的操作类型
   * @param perm 目标资源权限
   * @param target 操作的目标资源
   * @param description 操作失败原因描述
   */
  public static void logFailure(String user, String operation, String perm,
      String target, String description) {
    if (LOG.isWarnEnabled()) {
      LOG.warn(createFailureLog(user, operation, perm, target, description));
    }
  }

  /**
   * 构造操作失败的结构化审计日志字符串。
   * @param user 发起请求的用户名
   * @param operation 请求执行的操作类型
   * @param perm 目标资源权限
   * @param target 操作的目标资源
   * @param description 操作失败原因描述
   * @return 格式化后的审计日志字符串
   */
  static String createFailureLog(String user, String operation, String perm,
      String target, String description) {
    StringBuilder b = new StringBuilder();
    start(Keys.USER, user, b);
    addRemoteIP(b);
    add(Keys.OPERATION, operation, b);
    add(Keys.TARGET, target, b);
    add(Keys.RESULT, AuditConstants.FAILURE, b);
    add(Keys.DESCRIPTION, description, b);
    add(Keys.PERMISSIONS, perm, b);

    return b.toString();
  }
}