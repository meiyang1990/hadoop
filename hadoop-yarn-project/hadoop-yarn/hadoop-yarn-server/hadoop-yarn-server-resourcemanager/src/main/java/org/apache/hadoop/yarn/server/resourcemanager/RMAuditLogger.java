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
package org.apache.hadoop.yarn.server.resourcemanager;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;

/** 
 * YARN ResourceManager 审计日志管理类
 *
 * 审计日志格式采用制表符分隔的 key=value 键值对
 */
public class RMAuditLogger {
  private static final Logger LOG =
      LoggerFactory.getLogger(RMAuditLogger.class);

  // 审计日志可记录的字段枚举
  enum Keys {USER, OPERATION, TARGET, RESULT, IP, PERMISSIONS,
                    DESCRIPTION, APPID, APPATTEMPTID, CONTAINERID, 
                    CALLERCONTEXT, CALLERSIGNATURE, RESOURCE, QUEUENAME,
                    INCLUDEAPPS, INCLUDECHILDQUEUES, RECURSIVE, NODELABEL}

  /**
   * 审计日志常量定义，包含操作类型和分隔符常量
   */
  public static class AuditConstants {
    static final String SUCCESS = "SUCCESS";
    static final String FAILURE = "FAILURE";
    static final String KEY_VAL_SEPARATOR = "=";
    static final char PAIR_SEPARATOR = '\t';

    public static final String FAIL_ATTEMPT_REQUEST = "Fail Attempt Request";
    public static final String KILL_APP_REQUEST = "Kill Application Request";
    public static final String SUBMIT_APP_REQUEST = "Submit Application Request";
    public static final String MOVE_APP_REQUEST = "Move Application Request";
    public static final String GET_APP_STATE = "Get Application State";
    public static final String GET_APP_PRIORITY = "Get Application Priority";
    public static final String GET_APP_QUEUE = "Get Application Queue";
    public static final String GET_APP_ATTEMPTS = "Get Application Attempts";
    public static final String GET_APP_REPORT = "Get Application Report";
    public static final String GET_APP_ATTEMPT_REPORT
        = "Get Application Attempt Report";
    public static final String GET_CONTAINERS = "Get Containers";
    public static final String GET_CONTAINER_REPORT = "Get Container Report";
    public static final String GET_QUEUE_INFO_REQUEST =
        "Get Queue Info Request";
    public static final String GET_APPLICATIONS_REQUEST =
        "Get Applications Request";
    public static final String FINISH_SUCCESS_APP = "Application Finished - Succeeded";
    public static final String FINISH_FAILED_APP = "Application Finished - Failed";
    public static final String FINISH_KILLED_APP = "Application Finished - Killed";
    public static final String REGISTER_AM = "Register App Master";
    public static final String UNREGISTER_AM = "Unregister App Master";
    public static final String ALLOC_CONTAINER = "AM Allocated Container";
    public static final String RELEASE_CONTAINER = "AM Released Container";
    public static final String UPDATE_APP_PRIORITY =
        "Update Application Priority";
    public static final String UPDATE_APP_TIMEOUTS =
        "Update Application Timeouts";
    public static final String GET_APP_TIMEOUTS = "Get Application Timeouts";
    public static final String SIGNAL_CONTAINER = "Signal Container Request";

    // Some commonly used descriptions
    public static final String UNAUTHORIZED_USER = "Unauthorized user";
    
    // For Reservation system
    public static final String SUBMIT_RESERVATION_REQUEST = "Submit Reservation Request";
    public static final String UPDATE_RESERVATION_REQUEST = "Update Reservation Request";
    public static final String DELETE_RESERVATION_REQUEST = "Delete Reservation Request";
    public static final String LIST_RESERVATION_REQUEST = "List " +
            "Reservation Request";
  }

  /** 创建成功操作审计日志，自动获取客户端IP */
  static String createSuccessLog(String user, String operation, String target,
      ApplicationId appId, ApplicationAttemptId attemptId,
      ContainerId containerId, Resource resource) {
    return createSuccessLog(user, operation, target, appId, attemptId,
        containerId, resource, null, Server.getRemoteIp(), null, null);
  }

  /**
   * A helper function for creating the common portion of a successful
   * log message.
   */
  private static StringBuilder createStringBuilderForSuccessEvent(String user,
      String operation, String target, InetAddress ip) {
    StringBuilder b = new StringBuilder();
    start(Keys.USER, user, b);
    if (ip != null) {
      add(Keys.IP, ip.getHostAddress(), b);
    }
    add(Keys.OPERATION, operation, b);
    add(Keys.TARGET, target ,b);
    add(Keys.RESULT, AuditConstants.SUCCESS, b);
    return b;
  }

  /**
   * A helper api for creating an audit log for a successful event.
   */
  static String createSuccessLog(String user, String operation, String target,
      ApplicationId appId, ApplicationAttemptId attemptId,
      ContainerId containerId, Resource resource, CallerContext callerContext,
      InetAddress ip, String queueName, String partition) {
    // 构建成功事件基础部分
    StringBuilder b =
        createStringBuilderForSuccessEvent(user, operation, target, ip);
    // 添加应用ID
    if (appId != null) {
      add(Keys.APPID, appId.toString(), b);
    }
    // 添加应用尝试ID
    if (attemptId != null) {
      add(Keys.APPATTEMPTID, attemptId.toString(), b);
    }
    // 添加容器ID
    if (containerId != null) {
      add(Keys.CONTAINERID, containerId.toString(), b);
    }
    // 添加资源信息
    if (resource != null) {
      add(Keys.RESOURCE, resource.toString(), b);
    }
    // 添加调用者上下文信息
    appendCallerContext(b, callerContext);
    // 添加队列名称
    if (queueName != null) {
      add(Keys.QUEUENAME, queueName, b);
    }
    // 添加节点标签分区
    if (partition != null) {
      add(Keys.NODELABEL, partition, b);
    }
    return b.toString();
  }
  
  /** 添加调用者上下文信息到日志，包含上下文内容和签名 */
  private static void appendCallerContext(StringBuilder sb, CallerContext callerContext) {
    String context = null;
    byte[] signature = null;
    
    if (callerContext != null) {
      context = callerContext.getContext();
      signature = callerContext.getSignature();
    }
    
    if (context != null) {
      add(Keys.CALLERCONTEXT, context, sb);
    }
    
    if (signature != null) {
      String sigStr = new String(signature, StandardCharsets.UTF_8);
      add(Keys.CALLERSIGNATURE, sigStr, sb);
    }
  }

  /**
   * A general helper api for creating an audit log for a successful event.
   */
  @SuppressWarnings("rawtypes")
  static String createSuccessLog(String user, String operation, String target,
      InetAddress ip, ArgsBuilder args) {
    // 构建成功事件基础部分
    StringBuilder b =
        createStringBuilderForSuccessEvent(user, operation, target, ip);
    // 添加自定义参数
    if(args != null) {
      add(args, b);
    }
    return b.toString();
  }

  /**
   * Create a readable and parseable audit log string for a successful event.
   *
   * @param user User who made the service request to the ResourceManager
   * @param operation Operation requested by the user.
   * @param target The target on which the operation is being performed. 
   * @param appId Application Id in which operation was performed.
   * @param containerId Container Id in which operation was performed.
   * @param resource Resource associated with container.
   *
   * <br><br>
   * Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val delimiter
   * and hence the value fields should not contains tabs ('\t').
   */
  public static void logSuccess(String user, String operation, String target, 
      ApplicationId appId, ContainerId containerId, Resource resource) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, appId, null, 
          containerId, resource));
    }
  }

  /**
   * Create a readable and parseable audit log string for a successful event.
   *
   * @param user User who made the service request to the ResourceManager
   * @param operation Operation requested by the user.
   * @param target The target on which the operation is being performed.
   * @param appId Application Id in which operation was performed.
   * @param containerId Container Id in which operation was performed.
   * @param resource Resource associated with container.
   * @param queueName Name of queue.
   * @param partition Name of labeled partition.
   *
   * <br><br>
   * Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val delimiter
   * and hence the value fields should not contains tabs ('\t').
   */
  public static void logSuccess(String user, String operation, String target,
      ApplicationId appId, ContainerId containerId, Resource resource,
      String queueName, String partition) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, appId, null,
          containerId, resource, null, Server.getRemoteIp(), queueName,
          partition));
    }
  }

  /**
   * Create a general readable and parseable audit log string for a successful
   * event.
   *
   * @param user User who made the service request to the ResourceManager.
   * @param operation Operation requested by the user.
   * @param target The target on which the operation is being performed.
   * @param args The ArgsBuilder arguments for the operation request.
   *
   * <br><br>
   * Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val delimiter
   * and hence the value fields should not contains tabs ('\t').
   * <br>
   * This method will attempt to retrieve the remote IP
   */
  public static void logSuccess(String user, String operation, String target,
      ArgsBuilder args) {
    logSuccess(user, operation, target, Server.getRemoteIp(), args);
  }

  /**
   * Create a general readable and parseable audit log string for a successful
   * event.
   *
   * @param user User who made the service request to the ResourceManager.
   * @param operation Operation requested by the user.
   * @param target The target on which the operation is being performed.
   * @param ip The ip address of the caller.
   * @param args The ArgsBuilder arguments for the operation request.
   *
   * <br><br>
   * Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val delimiter
   * and hence the value fields should not contains tabs ('\t').
   */
  public static void logSuccess(String user, String operation, String target,
      InetAddress ip, ArgsBuilder args) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, ip, args));
    }
  }

  public static void logSuccess(String user, String operation, String target,
      ApplicationId appId, CallerContext callerContext, String queueName,
      String partition) {
    if (LOG.isInfoEnabled()) {
      LOG.info(
          createSuccessLog(user, operation, target, appId, null, null, null,
              callerContext, Server.getRemoteIp(), queueName, partition));
    }
  }

  /**
   * Create a readable and parseable audit log string for a successful event.
   *
   * @param user User who made the service request to the ResourceManager.
   * @param operation Operation requested by the user.
   * @param target The target on which the operation is being performed.
   * @param appId Application Id in which operation was performed.
   * @param attemptId Application Attempt Id in which operation was performed.
   *
   * <br><br>
   * Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val delimiter
   * and hence the value fields should not contains tabs ('\t').
   */
  public static void logSuccess(String user, String operation, String target, 
      ApplicationId appId, ApplicationAttemptId attemptId) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, appId, attemptId,
          null, null));
    }
  }

  public static void logSuccess(String user, String operation, String target,
      ApplicationId appId, CallerContext callerContext) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, appId, null, null,
          null, callerContext, Server.getRemoteIp(), null, null));
    }
  }

  public static void logSuccess(String user, String operation, String target,
      ApplicationId appId, CallerContext callerContext, String queueName) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, appId, null, null,
          null, callerContext, Server.getRemoteIp(), queueName, null));
    }
  }

  /**
   * Create a readable and parseable audit log string for a successful event.
   *
   * @param user
   *          User who made the service request to the ResourceManager.
   * @param operation
   *          Operation requested by the user.
   * @param target
   *          The target on which the operation is being performed.
   * @param appId
   *          Application Id in which operation was performed.
   * @param ip
   *          The ip address of the caller.
   *
   *          <br>
   *          <br>
   *          Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val
   *          delimiter and hence the value fields should not contains tabs
   *          ('\t').
   */
  public static void logSuccess(String user, String operation, String target,
      ApplicationId appId, InetAddress ip) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, appId, null, null,
          null, null, ip, null, null));
    }
  }

  /**
   * Create a readable and parseable audit log string for a successful event.
   *
   * @param user User who made the service request to the ResourceManager.
   * @param operation Operation requested by the user.
   * @param target The target on which the operation is being performed. 
   * @param appId Application Id in which operation was performed.
   *
   * <br><br>
   * Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val delimiter
   * and hence the value fields should not contains tabs ('\t').
   */
  public static void logSuccess(String user, String operation, String target,
      ApplicationId appId) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, appId, null, null, null));
    }
  }

  /**
   * Create a readable and parseable audit log string for a successful event.
   *
   * @param user User who made the service request. 
   * @param operation Operation requested by the user.
   * @param target The target on which the operation is being performed. 
   *
   * <br><br>
   * Note that the {@link RMAuditLogger} uses tabs ('\t') as a key-val delimiter
   * and hence the value fields should not contains tabs ('\t').
   */
  public static void logSuccess(String user, String operation, String target) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, null, null, null, null));
    }
  }
  
  /** 构建失败操作审计日志基础部分 */
  private static StringBuilder createStringBuilderForFailureLog(String user,
      String operation, String target, String description, String perm) {
    StringBuilder b = new StringBuilder();
    start(Keys.USER, user, b);
    addRemoteIP(b);
    add(Keys.OPERATION, operation, b);
    add(Keys.TARGET, target ,