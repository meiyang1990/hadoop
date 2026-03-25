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
package org.apache.hadoop.yarn.server.nodemanager;

import java.net.InetAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;

/** 
 * NodeManager审计日志管理器，负责记录NodeManager上的操作审计日志。
 * 
 * 审计日志格式采用键=值对，使用制表符分隔各个键值对。
 */
public class NMAuditLogger {
  private static final Logger LOG =
       LoggerFactory.getLogger(NMAuditLogger.class);

  /** 审计日志字段枚举，定义了日志中包含的各个字段 */
  enum Keys {USER, OPERATION, TARGET, RESULT, IP,
                    DESCRIPTION, APPID, CONTAINERID}

  /** 审计日志常量定义，包含结果状态、分隔符和常用操作描述 */
  public static class AuditConstants {
    static final String SUCCESS = "SUCCESS";
    static final String FAILURE = "FAILURE";
    static final String KEY_VAL_SEPARATOR = "=";
    static final char PAIR_SEPARATOR = '\t';

    // 常用操作描述常量
    public static final String START_CONTAINER = "Start Container Request";
    public static final String STOP_CONTAINER = "Stop Container Request";
    public static final String START_CONTAINER_REINIT =
        "Container ReInitialization - Started";
    public static final String FINISH_CONTAINER_REINIT =
        "Container ReInitialization - Finished";
    public static final String FINISH_SUCCESS_CONTAINER = "Container Finished - Succeeded";
    public static final String FINISH_FAILED_CONTAINER = "Container Finished - Failed";
    public static final String FINISH_KILLED_CONTAINER = "Container Finished - Killed";
  }

  /**
   * 构造成功操作的审计日志字符串
   * @param user 操作发起用户
   * @param operation 操作类型
   * @param target 操作目标
   * @param appId 所属应用ID
   * @param containerId 所属容器ID
   * @return 格式化后的审计日志字符串
   */
  static String createSuccessLog(String user, String operation, String target, 
      ApplicationId appId, ContainerId containerId) {
    StringBuilder b = new StringBuilder();
    start(Keys.USER, user, b);
    addRemoteIP(b);
    add(Keys.OPERATION, operation, b);
    add(Keys.TARGET, target ,b);
    add(Keys.RESULT, AuditConstants.SUCCESS, b);
    if (appId != null) {
      add(Keys.APPID, appId.toString(), b);
    }
    if (containerId != null) {
      add(Keys.CONTAINERID, containerId.toString(), b);
    }
    return b.toString();
  }

  /**
   * 记录成功操作的审计日志
   *
   * @param user 发起请求的用户
   * @param operation 请求的操作类型
   * @param target 操作作用的目标
   * @param appId 操作所属的应用ID
   * @param containerId 操作所属的容器ID
   *
   * <br><br>
   * 注意：NMAuditLogger使用制表符('\t')作为键值对分隔符，因此值字段不能包含制表符。
   */
  public static void logSuccess(String user, String operation, String target,
      ApplicationId appId, ContainerId containerId) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, appId, containerId));
    }
  }

  /**
   * 记录不关联应用和容器的成功操作审计日志
   *
   * @param user 发起请求的用户
   * @param operation 请求的操作类型
   * @param target 操作作用的目标
   *
   * <br><br>
   * 注意：NMAuditLogger使用制表符('\t')作为键值对分隔符，因此值字段不能包含制表符。
   */
  public static void logSuccess(String user, String operation, String target) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, null, null));
    }
  }

  /**
   * 构造失败操作的审计日志字符串，抽取出来方便测试
   * @param user 操作发起用户
   * @param operation 操作类型
   * @param target 操作目标
   * @param description 失败原因描述
   * @param appId 所属应用ID
   * @param containerId 所属容器ID
   * @return 格式化后的审计日志字符串
   */
  static String createFailureLog(String user, String operation, String target, 
      String description, ApplicationId appId, ContainerId containerId) {
    StringBuilder b = new StringBuilder();
    start(Keys.USER, user, b);
    addRemoteIP(b);
    add(Keys.OPERATION, operation, b);
    add(Keys.TARGET, target ,b);
    add(Keys.RESULT, AuditConstants.FAILURE, b);
    add(Keys.DESCRIPTION, description, b);
    if (appId != null) {
      add(Keys.APPID, appId.toString(), b);
    }
    if (containerId != null) {
      add(Keys.CONTAINERID, containerId.toString(), b);
    }
    return b.toString();
  }

  /**
   * 记录失败操作的审计日志
   *
   * @param user 发起请求的用户
   * @param operation 请求的操作类型
   * @param target 操作作用的目标
   * @param description 操作失败的额外描述信息
   * @param appId 操作所属的应用ID
   * @param containerId 操作所属的容器ID
   *
   * <br><br>
   * 注意：NMAuditLogger使用制表符('\t')作为键值对分隔符，因此值字段不能包含制表符。
   */
  public static void logFailure(String user, String operation, String target, 
      String description, ApplicationId appId, ContainerId containerId) {
    if (LOG.isWarnEnabled()) {
      LOG.warn(createFailureLog(user, operation, target, description, appId, containerId));
    }
  }

  /**
   * 记录不关联应用和容器的失败操作审计日志
   *
   * @param user 发起请求的用户
   * @param operation 请求的操作类型
   * @param target 操作作用的目标
   * @param description 操作失败的额外描述信息
   *
   * <br><br>
   * 注意：NMAuditLogger使用制表符('\t')作为键值对分隔符，因此值字段不能包含制表符。
   */
  public static void logFailure(String user, String operation, 
                         String target, String description) {
    if (LOG.isWarnEnabled()) {
      LOG.warn(createFailureLog(user, operation, target, description, null, null));
    }
  }

  /**
   * 添加请求来源IP地址到审计日志
   */
  static void addRemoteIP(StringBuilder b) {
    InetAddress ip = Server.getRemoteIp();
    // 测试场景下IP可能为null
    if (ip != null) {
      add(Keys.IP, ip.getHostAddress(), b);
    }
  }

  /**
   * 添加第一个键值对到日志生成器，格式为key=value
   */
  static void start(Keys key, String value, StringBuilder b) {
    b.append(key.name()).append(AuditConstants.KEY_VAL_SEPARATOR).append(value);
  }

  /**
   * 追加键值对到日志生成器，格式为\tkey=value
   */
  static void add(Keys key, String value, StringBuilder b) {
    b.append(AuditConstants.PAIR_SEPARATOR).append(key.name())
     .append(AuditConstants.KEY_VAL_SEPARATOR).append(value);
  }
}