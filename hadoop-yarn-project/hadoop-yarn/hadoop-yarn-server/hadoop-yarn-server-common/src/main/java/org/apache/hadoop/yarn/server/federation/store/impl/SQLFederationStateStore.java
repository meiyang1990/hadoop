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

package org.apache.hadoop.yarn.server.federation.store.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Blob;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationSubmissionContextProto;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreInvalidInputException;
import org.apache.hadoop.yarn.server.federation.store.metrics.FederationStateStoreClientMetrics;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.DeletePoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeletePoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteSubClusterPoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationApplicationHomeSubClusterStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationMembershipStateStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationPolicyStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationReservationHomeSubClusterStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationRouterRMTokenInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationStateStoreUtils;
import org.apache.hadoop.yarn.server.federation.store.sql.FederationSQLOutParameter;
import org.apache.hadoop.yarn.server.federation.store.sql.FederationQueryRunner;
import org.apache.hadoop.yarn.server.federation.store.sql.RouterMasterKeyHandler;
import org.apache.hadoop.yarn.server.federation.store.sql.RouterStoreTokenHandler;
import org.apache.hadoop.yarn.server.federation.store.sql.RowCountHandler;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import com.zaxxer.hikari.HikariDataSource;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static java.sql.Types.BIGINT;
import static org.apache.hadoop.yarn.server.federation.store.sql.FederationQueryRunner.YARN_ROUTER_CURRENT_KEY_ID;
import static org.apache.hadoop.yarn.server.federation.store.sql.FederationQueryRunner.YARN_ROUTER_SEQUENCE_NUM;
import static org.apache.hadoop.yarn.server.federation.store.utils.FederationStateStoreUtils.convertMasterKeyToDelegationKey;

/**
 * YARN联邦状态存储的SQL实现，基于关系型数据库存储联邦集群的元数据信息。
 * 实现了FederationStateStore接口，提供子集群注册、应用归属、策略配置、令牌密钥等信息的持久化存储。
 */
public class SQLFederationStateStore implements FederationStateStore {

  public static final Logger LOG =
      LoggerFactory.getLogger(SQLFederationStateStore.class);

  // 存储过程调用模板定义

  private static final String CALL_SP_REGISTER_SUBCLUSTER =
      "{call sp_registerSubCluster(?, ?, ?, ?, ?, ?, ?, ?, ?)}";

  private static final String CALL_SP_DEREGISTER_SUBCLUSTER =
      "{call sp_deregisterSubCluster(?, ?, ?)}";

  private static final String CALL_SP_GET_SUBCLUSTER =
      "{call sp_getSubCluster(?, ?, ?, ?, ?, ?, ?, ?, ?)}";

  private static final String CALL_SP_GET_SUBCLUSTERS =
      "{call sp_getSubClusters()}";

  private static final String CALL_SP_SUBCLUSTER_HEARTBEAT =
      "{call sp_subClusterHeartbeat(?, ?, ?, ?)}";

  private static final String CALL_SP_ADD_APPLICATION_HOME_SUBCLUSTER =
      "{call sp_addApplicationHomeSubCluster(?, ?, ?, ?, ?)}";

  private static final String CALL_SP_UPDATE_APPLICATION_HOME_SUBCLUSTER =
      "{call sp_updateApplicationHomeSubCluster(?, ?, ?, ?)}";

  private static final String CALL_SP_DELETE_APPLICATION_HOME_SUBCLUSTER =
      "{call sp_deleteApplicationHomeSubCluster(?, ?)}";

  private static final String CALL_SP_GET_APPLICATION_HOME_SUBCLUSTER =
      "{call sp_getApplicationHomeSubCluster(?, ?, ?, ?)}";

  private static final String CALL_SP_GET_APPLICATIONS_HOME_SUBCLUSTER =
      "{call sp_getApplicationsHomeSubCluster(?, ?)}";

  private static final String CALL_SP_SET_POLICY_CONFIGURATION =
      "{call sp_setPolicyConfiguration(?, ?, ?, ?)}";

  private static final String CALL_SP_GET_POLICY_CONFIGURATION =
      "{call sp_getPolicyConfiguration(?, ?, ?)}";

  private static final String CALL_SP_GET_POLICIES_CONFIGURATIONS =
      "{call sp_getPoliciesConfigurations()}";

  protected static final String CALL_SP_ADD_RESERVATION_HOME_SUBCLUSTER =
      "{call sp_addReservationHomeSubCluster(?, ?, ?, ?)}";

  protected static final String CALL_SP_GET_RESERVATION_HOME_SUBCLUSTER =
      "{call sp_getReservationHomeSubCluster(?, ?)}";

  protected static final String CALL_SP_GET_RESERVATIONS_HOME_SUBCLUSTER =
      "{call sp_getReservationsHomeSubCluster()}";

  protected static final String CALL_SP_DELETE_RESERVATION_HOME_SUBCLUSTER =
      "{call sp_deleteReservationHomeSubCluster(?, ?)}";

  protected static final String CALL_SP_UPDATE_RESERVATION_HOME_SUBCLUSTER =
      "{call sp_updateReservationHomeSubCluster(?, ?, ?)}";

  protected static final String CALL_SP_ADD_MASTERKEY =
      "{call sp_addMasterKey(?, ?, ?)}";

  protected static final String CALL_SP_GET_MASTERKEY =
      "{call sp_getMasterKey(?, ?)}";

  protected static final String CALL_SP_DELETE_MASTERKEY =
      "{call sp_deleteMasterKey(?, ?)}";

  protected static final String CALL_SP_ADD_DELEGATIONTOKEN =
      "{call sp_addDelegationToken(?, ?, ?, ?, ?)}";

  protected static final String CALL_SP_GET_DELEGATIONTOKEN =
      "{call sp_getDelegationToken(?, ?, ?, ?)}";

  protected static final String CALL_SP_UPDATE_DELEGATIONTOKEN =
      "{call sp_updateDelegationToken(?, ?, ?, ?, ?)}";

  protected static final String CALL_SP_DELETE_DELEGATIONTOKEN =
      "{call sp_deleteDelegationToken(?, ?)}";

  private static final String CALL_SP_STORE_VERSION =
      "{call sp_storeVersion(?, ?, ?)}";

  private static final String CALL_SP_LOAD_VERSION =
      "{call sp_getVersion(?, ?)}";

  // 联邦状态存储涉及的所有表列表，清空状态存储时会 truncate 所有表
  private static final List<String> TABLES = new ArrayList<>(
      Arrays.asList("applicationsHomeSubCluster", "membership", "policies", "versions",
      "reservationsHomeSubCluster", "masterKeys", "delegationTokens", "sequenceTable"));

  // UTC时区日历，用于时间戳统一转换
  private Calendar utcCalendar =
      Calendar.getInstance(TimeZone.getTimeZone("UTC"));

  // SQL数据库连接池配置参数
  private String userName;
  private String password;
  private String driverClass;
  private String url;
  private int maximumPoolSize;
  private HikariDataSource dataSource = null;
  private final Clock clock = new MonotonicClock();
  @VisibleForTesting
  private Connection conn = null;
  private int maxAppsInStateStore;
  private int minimumIdle;
  private String dataSourcePoolName;
  private long maxLifeTime;
  private long idleTimeout;
  private long connectionTimeout;

  // 当前状态存储的版本信息
  protected static final Version CURRENT_VERSION_INFO = Version.newInstance(1, 1);

  @Override
  /**
   * 初始化SQL联邦状态存储，加载配置并创建数据库连接池
   * @param conf Yarn配置对象
   * @throws YarnException 初始化失败时抛出异常
   */
  public void init(Configuration conf) throws YarnException {
    // 加载数据库连接配置
    driverClass = conf.get(YarnConfiguration.FEDERATION_STATESTORE_SQL_JDBC_CLASS,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_SQL_JDBC_CLASS);
    maximumPoolSize = conf.getInt(YarnConfiguration.FEDERATION_STATESTORE_SQL_MAXCONNECTIONS,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_SQL_MAXCONNECTIONS);
    minimumIdle = conf.getInt(YarnConfiguration.FEDERATION_STATESTORE_SQL_MINIMUMIDLE,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_SQL_MINIMUMIDLE);
    dataSourcePoolName = conf.get(YarnConfiguration.FEDERATION_STATESTORE_POOL_NAME,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_POOL_NAME);
    maxLifeTime = conf.getTimeDuration(YarnConfiguration.FEDERATION_STATESTORE_CONN_MAX_LIFE_TIME,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_CONN_MAX_LIFE_TIME, TimeUnit.MILLISECONDS);
    idleTimeout = conf.getTimeDuration(
        YarnConfiguration.FEDERATION_STATESTORE_CONN_IDLE_TIMEOUT_TIME,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_CONN_IDLE_TIMEOUT_TIME,
        TimeUnit.MILLISECONDS);
    connectionTimeout = conf.getTimeDuration(
        YarnConfiguration.FEDERATION_STATESTORE_CONNECTION_TIMEOUT,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_CONNECTION_TIMEOUT_TIME,
        TimeUnit.MILLISECONDS);

    // 获取用户名密码和JDBC URL
    userName = conf.get(YarnConfiguration.FEDERATION_STATESTORE_SQL_USERNAME);
    password = conf.get(YarnConfiguration.FEDERATION_STATESTORE_SQL_PASSWORD);
    url = conf.get(YarnConfiguration.FEDERATION_STATESTORE_SQL_URL);

    try {
      // 加载JDBC驱动类
      Class.forName(driverClass);
    } catch (ClassNotFoundException e) {
      FederationStateStoreUtils.logAndThrowException(LOG, "Driver class not found.", e);
    }

    // 创建HikariCP连接池，线程安全管理数据库连接
    dataSource = new HikariDataSource();
    dataSource.setDataSourceClassName(driverClass);
    FederationStateStoreUtils.setUsername(dataSource, userName);
    FederationStateStoreUtils.setPassword(dataSource, password);
    FederationStateStoreUtils.setProperty(dataSource,
        FederationStateStoreUtils.FEDERATION_STORE_URL, url);

    // 配置连接池参数
    dataSource.setMaximumPoolSize(maximumPoolSize);
    dataSource.setPoolName(dataSourcePoolName);
    dataSource.setMinimumIdle(minimumIdle);
    dataSource.setMaxLifetime(maxLifeTime);
    dataSource.setIdleTimeout(idleTimeout);
    dataSource.setConnectionTimeout(connectionTimeout);

    LOG.info("Initialized connection pool to the Federation StateStore database at address: {}.",
        url);

    try {
      // 获取初始测试连接
      conn = getConnection();
      LOG.debug("Connection created");
    } catch (SQLException e) {
      FederationStateStoreUtils.logAndThrowRetriableException(LOG, "Not able to get Connection", e);
    }

    // 加载状态存储中最多保留的应用数量配置
    maxAppsInStateStore = conf.getInt(
        Yarn