// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.router;

import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

/**
 * YARN Router联邦路由审计日志管理器，负责记录所有经过Router的操作审计日志。
 * 审计日志格式为键值对，使用制表符分隔不同键值对。
 */
public final class RouterAuditLogger {
  private static final Logger LOG =
      LoggerFactory.getLogger(RouterAuditLogger.class);

  private RouterAuditLogger() {
  }

  /**
   * 审计日志字段枚举，定义所有支持的日志字段。
   */
  enum Keys {USER, OPERATION, TARGET, RESULT, IP, PERMISSIONS, DESCRIPTION, APPID, SUBCLUSTERID}

  /**
   * 审计日志常量定义，包含操作结果、分隔符和所有支持的操作类型常量。
   */
  public static class AuditConstants {
    static final String SUCCESS = "SUCCESS";
    static final String FAILURE = "FAILURE";
    static final String KEY_VAL_SEPARATOR = "=";
    static final char PAIR_SEPARATOR = '\t';

    public static final String GET_NEW_APP = "Get New App";
    public static final String SUBMIT_NEW_APP = "Submit New App";
    public static final String FORCE_KILL_APP = "Force Kill App";
    public static final String GET_APP_REPORT = "Get Application Report";
    public static final String TARGET_CLIENT_RM_SERVICE = "RouterClientRMService";
    public static final String TARGET_WEB_SERVICE = "RouterWebServices";
    public static final String UNKNOWN = "UNKNOWN";
    public static final String GET_APPLICATIONS = "Get Applications";
    public static final String GET_CLUSTERMETRICS = "Get ClusterMetrics";
    public static final String GET_CLUSTERNODES = "Get ClusterNodes";
    public static final String GET_QUEUEINFO = "Get QueueInfo";
    public static final String GET_QUEUE_USER_ACLS = "Get QueueUserAcls";
    public static final String MOVE_APPLICATION_ACROSS_QUEUES = "Move ApplicationAcrossQueues";
    public static final String GET_NEW_RESERVATION = "Get NewReservation";
    public static final String SUBMIT_RESERVATION = "Submit Reservation";
    public static final String LIST_RESERVATIONS = "List Reservations";
    public static final String UPDATE_RESERVATION = "Update Reservation";
    public static final String DELETE_RESERVATION = "Delete Reservation";
    public static final String GET_NODETOLABELS = "Get NodeToLabels";
    public static final String GET_LABELSTONODES = "Get LabelsToNodes";
    public static final String GET_CLUSTERNODELABELS = "Get ClusterNodeLabels";
    public static final String GET_APPLICATION_ATTEMPT_REPORT = "Get ApplicationAttemptReport";
    public static final String GET_APPLICATION_ATTEMPTS = "Get ApplicationAttempts";
    public static final String GET_CONTAINERREPORT = "Get ContainerReport";
    public static final String GET_CONTAINERS = "Get Containers";
    public static final String GET_DELEGATIONTOKEN = "Get DelegationToken";
    public static final String RENEW_DELEGATIONTOKEN = "Renew DelegationToken";
    public static final String CANCEL_DELEGATIONTOKEN = "Cancel DelegationToken";
    public static final String FAIL_APPLICATIONATTEMPT = "Fail ApplicationAttempt";
    public static final String UPDATE_APPLICATIONPRIORITY = "Update ApplicationPriority";
    public static final String SIGNAL_TOCONTAINER = "Signal ToContainer";
    public static final String UPDATE_APPLICATIONTIMEOUTS = "Update ApplicationTimeouts";
    public static final String GET_RESOURCEPROFILES = "Get ResourceProfiles";
    public static final String GET_RESOURCEPROFILE = "Get ResourceProfile";
    public static final String GET_RESOURCETYPEINFO = "Get ResourceTypeInfo";
    public static final String GET_ATTRIBUTESTONODES = "Get AttributesToNodes";
    public static final String GET_CLUSTERNODEATTRIBUTES = "Get ClusterNodeAttributes";
    public static final String GET_NODESTOATTRIBUTES = "Get NodesToAttributes";
    public static final String GET_CLUSTERINFO = "Get ClusterInfo";
    public static final String GET_CLUSTERUSERINFO = "Get ClusterUserInfo";
    public static final String GET_SCHEDULERINFO = "Get SchedulerInfo";
    public static final String DUMP_SCHEDULERLOGS = "Dump SchedulerLogs";
    public static final String GET_ACTIVITIES = "Get Activities";
    public static final String GET_BULKACTIVITIES = "Get BulkActivities";
    public static final String GET_APPACTIVITIES = "Get AppActivities";
    public static final String GET_APPSTATISTICS = "Get AppStatistics";
    public static final String GET_RMNODELABELS = "Get RMNodeLabels";
    public static final String REPLACE_LABELSONNODES = "Replace LabelsOnNodes";
    public static final String REPLACE_LABELSONNODE = "Replace LabelsOnNode";
    public static final String GET_CLUSTER_NODELABELS = "Get ClusterNodeLabels";
    public static final String ADD_TO_CLUSTER_NODELABELS = "Add To ClusterNodeLabels";
    public static final String REMOVE_FROM_CLUSTERNODELABELS = "Remove From ClusterNodeLabels";
    public static final String GET_LABELS_ON_NODE = "Get LabelsOnNode";
    public static final String GET_APP_PRIORITY = "Get AppPriority";
    public static final String UPDATE_APP_QUEUE = "Update AppQueue";
    public static final String POST_DELEGATION_TOKEN = "Post DelegationToken";
    public static final String POST_DELEGATION_TOKEN_EXPIRATION = "Post DelegationTokenExpiration";
    public static final String GET_APP_TIMEOUT = "Get App Timeout";
    public static final String GET_APP_TIMEOUTS = "Get App Timeouts";
    public static final String CHECK_USER_ACCESS_TO_QUEUE = "Check User AccessToQueue";
    public static final String GET_APP_ATTEMPT = "Get AppAttempt";
    public static final String GET_CONTAINER = "Get Container";
    public static final String UPDATE_SCHEDULER_CONFIGURATION = "Update SchedulerConfiguration";
    public static final String GET_SCHEDULER_CONFIGURATION = "Get SchedulerConfiguration";
  }

  /**
   * 记录成功操作审计日志。
   * @param user 请求用户名
   * @param operation 操作类型
   * @param target 操作目标服务
   */
  public static void logSuccess(String user, String operation, String target) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, null, null));
    }
  }

  /**
   * Create a readable and parseable audit log string for a successful event.
   *
   * @param user User who made the service request to the Router
   * @param operation Operation requested by the user.
   * @param target The target on which the operation is being performed.
   * @param appId Application Id in which operation was performed.
   *
   * <br><br>
   * Note that the {@link RouterAuditLogger} uses tabs ('\t') as a key-val
   * delimiter and hence the value fields should not contains tabs ('\t').
   */
  public static void logSuccess(String user, String operation, String target,
      ApplicationId appId) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, appId, null));
    }
  }

  /**
   * Create a readable and parseable audit log string for a successful event.
   *
   * @param user         User who made the service request to the Router
   * @param operation    Operation requested by the user.
   * @param target       The target on which the operation is being performed.
   * @param appId        Application Id in which operation was performed.
   * @param subClusterId Subcluster Id in which operation is performed.
   *
   * <br><br>
   * Note that the {@link RouterAuditLogger} uses tabs ('\t') as a key-val
   * delimiter and hence the value fields should not contains tabs ('\t').
   */
  public static void logSuccess(String user, String operation, String target,
      ApplicationId appId, SubClusterId subClusterId) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createSuccessLog(user, operation, target, appId, subClusterId));
    }
  }

  /**
   * A helper api for creating an audit log for a successful event.
   */
  static String createSuccessLog(String user, String operation, String target,
      ApplicationId appId, SubClusterId subClusterID) {
    // 构建成功事件基础日志
    StringBuilder b =
        createStringBuilderForSuccessEvent(user, operation, target);
    // 添加应用ID字段
    if (appId != null) {
      add(Keys.APPID, appId.toString(), b);
    }
    // 添加子集群ID字段
    if (subClusterID != null) {
      add(Keys.SUBCLUSTERID, subClusterID.toString(), b);
    }
    return b.toString();
  }

  /**
   * A helper function for creating the common portion of a successful
   * log message.
   */
  private static StringBuilder createStringBuilderForSuccessEvent(String user,
      String operation, String target) {
    StringBuilder b = new StringBuilder();
    // 添加第一个字段：用户名
    start(Keys.USER, user, b);
    // 添加远程IP地址
    addRemoteIP(b);
    // 添加操作类型
    add(Keys.OPERATION, operation, b);
    // 添加操作目标
    add(Keys.TARGET, target, b);
    // 添加操作结果：成功
    add(Keys.RESULT, AuditConstants.SUCCESS, b);
    return b;
  }

  /**
   * Create a readable and parseable audit log string for a failed event.
   *
   * @param user User who made the service request.
   * @param operation Operation requested by the user.
   * @param perm Target permissions.
   * @param target The target on which the operation is being performed.
   * @param description Some additional information as to why the operation
   *                    failed.
   *
   * <br><br>
   * Note that the {@link RouterAuditLogger} uses tabs ('\t') as a key-val
   * delimiter and hence the value fields should not contains tabs ('\t').
   */
  public static void logFailure(String user, String operation, String perm,
      String target, String description) {
    if (LOG.isInfoEnabled()) {
      LOG.info(
          createFailureLog(user, operation, perm, target, description, null,
              null));
    }
  }

  /**
   * Create a readable and parseable audit log string for a failed event.
   *
   * @param user User who made the service request.
   * @param operation Operation requested by the user.
   * @param perm Target permissions.
   * @param target The target on which the operation is being performed.
   * @param descriptionFormat the description message format string.
   * @param args format parameter.
   *
   * <br><br>
   * Note that the {@link RouterAuditLogger} uses tabs ('\t') as a key-val
   * delimiter and hence the value fields should not contains tabs ('\t').
   */
  public static void logFailure(String user, String operation, String perm,
      String target, String descriptionFormat, Object... args) {
    if (LOG.isInfoEnabled()) {
      // 格式化失败描述信息
      String description = String.format(descriptionFormat, args);
      LOG.info(createFailureLog(user, operation, perm, target, description, null, null));
    }
  }

  /**
   * Create a readable and parseable audit log string for a failed event.
   *
   * @param user User who made the service request.
   * @param operation Operation requested by the user.
   * @param perm Target permissions.
   * @param target The target on which the operation is being performed.
   * @param description Some additional information as to why the operation
   *                    failed.
   * @param appId Application Id in which operation was performed.
   *
   * <br><br>
   * Note that the {@link RouterAuditLogger} uses tabs ('\t') as a key-val
   * delimiter and hence the value fields should not contains tabs ('\t').
   */
  public static void logFailure(String user, String operation, String perm,
      String target, String description, ApplicationId appId) {
    if (LOG.isInfoEnabled()) {
      LOG.info(
          createFailureLog(user, operation, perm, target, description, appId,
              null));
    }
  }

  /**
   * Create a readable and parseable audit log string for a failed event.
   *
   * @param user User who made the service request.
   * @param operation Operation requested by the user.
   * @param perm Target permissions.
   * @param target The target on which the operation is being performed.
   * @param description Some additional information as to why the operation
   *                    failed.
   * @param appId Application Id in which operation was performed.
   * @param subClusterId SubCluster Id in which operation was performed.
   *
   * <br><br>
   * Note that the {@link RouterAuditLogger} uses tabs ('\t') as a key-val
   * delimiter and hence the value fields should not contains tabs ('\t').
   */
  public static void logFailure(String user, String operation, String perm,
      String target, String description, ApplicationId appId,
      SubClusterId subClusterId) {
    if (LOG.isInfoEnabled()) {
      LOG.info(
          createFailureLog(user, operation, perm, target, description, appId,
              subClusterId));
    }
  }

  /**
   * Create a readable and parsable audit log string for a failed event.
   *
   * @param user User who made the service request.
   * @param operation Operation requested by the user.
   * @param perm Target permissions.
   * @param target The target on which the operation is being performed.
   * @param description Some additional information as to why the operation failed.
   * @param subClusterId SubCluster Id in which operation was performed.
   */
  public static void logFailure(String user, String operation, String perm,
      String target, String description, SubClusterId subClusterId) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createFailureLog(user, operation, perm, target, description, null,
          subClusterId));
    }
  }

  /**
   * A helper api for creating an audit log for a failure event.
   */
  static String createFailureLog(String user, String operation, String perm,
      String target, String description, ApplicationId appId,
      SubClusterId subClusterId) {
    // 构建失败事件基础日志
    StringBuilder b =
        createStringBuilderForFailureLog(user, operation, target, description,
            perm);
    // 添加应用ID字段
    if (appId != null) {
      add(Keys.APPID, appId.toString(), b);
    }
    // 添加子集群ID字段
    if (subClusterId != null) {
      add(Keys.SUBCLUSTERID, subClusterId.toString(), b);
    }
    return b.toString();
  }

  /**
   * A helper function for creating the common portion of a failure
   * log message.
   */
  private static StringBuilder createStringBuilderForFailureLog(String user,
      String operation, String target, String description, String perm) {
    StringBuilder b = new StringBuilder();
    // 添加第一个字段：用户名
    start(Keys.USER, user, b);
    // 添加远程IP地址
    addRemoteIP(b);
    // 添加操作类型
    add(Keys.OPERATION, operation, b);
    // 添加操作目标
    add(Keys.TARGET, target, b);
    // 添加操作结果：失败
    add(Keys.RESULT, AuditConstants.FAILURE, b);
    // 添加失败描述
    add(Keys.DESCRIPTION, description, b);
    // 添加权限信息
    add(Keys.PERMISSIONS, perm, b);
    return b;
  }

  /**
   * Adds the first key-val pair to the passed builder in the following format
   * key=value.
   */
  static void start(Keys key, String value, StringBuilder b) {
    b.append(key.name()).append(AuditConstants.KEY_VAL_SEPARATOR).append(value);
  }