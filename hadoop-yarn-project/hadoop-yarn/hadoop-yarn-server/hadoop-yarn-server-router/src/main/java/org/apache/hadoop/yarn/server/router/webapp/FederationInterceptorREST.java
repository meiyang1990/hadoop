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

package org.apache.hadoop.yarn.server.router.webapp;

import java.io.IOException;
import java.lang.reflect.Method;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.impl.prefetch.Validate;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.thirdparty.com.google.common.net.HttpHeaders;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.RouterPolicyFacade;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.resolver.SubClusterResolver;
import org.apache.hadoop.yarn.server.federation.retry.FederationActionRetry;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.NodeIDsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebAppUtil;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppPriority;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppQueue;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationStatisticsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterUserInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.DelegationToken;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LabelsToNodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewApplication;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntryList;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.RMQueueAclInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDeleteRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationSubmissionRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationUpdateRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceOptionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.BulkActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDefinitionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntry;
import org.apache.hadoop.yarn.server.router.RouterAuditLogger;
import org.apache.hadoop.yarn.server.router.RouterMetrics;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.server.router.clientrm.ClientMethod;
import org.apache.hadoop.yarn.server.router.clientrm.RouterClientRMService;
import org.apache.hadoop.yarn.server.router.security.RouterDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.router.webapp.cache.RouterAppInfoCacheKey;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationBulkActivitiesInfo;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationRMQueueAclInfo;
import org.apache.hadoop.yarn.server.router.webapp.dao.SubClusterResult;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationSchedulerTypeInfo;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationConfInfo;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationClusterUserInfo;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationClusterInfo;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.util.LRUCacheHashMap;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.dao.ConfInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import static org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade.getRandomActiveSubCluster;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_NEW_APP;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.SUBMIT_NEW_APP;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_CLUSTERINFO;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_CLUSTERUSERINFO;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_SCHEDULERINFO;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.DUMP_SCHEDULERLOGS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_ACTIVITIES;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_BULKACTIVITIES;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_APPACTIVITIES;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_APPSTATISTICS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_NODETOLABELS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_RMNODELABELS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_LABELSTONODES;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.UNKNOWN;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.TARGET_WEB_SERVICE;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.REPLACE_LABELSONNODES;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.REPLACE_LABELSONNODE;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_CLUSTER_NODELABELS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.ADD_TO_CLUSTER_NODELABELS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.REMOVE_FROM_CLUSTERNODELABELS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_LABELS_ON_NODE;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_APP_PRIORITY;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_QUEUEINFO;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.UPDATE_APPLICATIONPRIORITY;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.UPDATE_APP_QUEUE;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.POST_DELEGATION_TOKEN;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.POST_DELEGATION_TOKEN_EXPIRATION;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.CANCEL_DELEGATIONTOKEN;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_NEW_RESERVATION;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.SUBMIT_RESERVATION;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.UPDATE_RESERVATION;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.DELETE_RESERVATION;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.LIST_RESERVATIONS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_APP_TIMEOUT;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_APP_TIMEOUTS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.UPDATE_APPLICATIONTIMEOUTS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_APPLICATION_ATTEMPTS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.CHECK_USER_ACCESS_TO_QUEUE;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_APP_ATTEMPT;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_CONTAINERS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_CONTAINER;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.UPDATE_SCHEDULER_CONFIGURATION;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_SCHEDULER_CONFIGURATION;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.SIGNAL_TOCONTAINER;
import static org.apache.hadoop.yarn.server.router.webapp.RouterWebServiceUtil.extractToken;
import static org.apache.hadoop.yarn.server.router.webapp.RouterWebServiceUtil.getKerberosUserGroupInformation;

/**
 * YARN Router联邦REST请求拦截器，是拦截器链的最后一环，封装了所有YARN联邦的REST请求处理逻辑。
 * 负责将用户REST请求路由到对应子集群，并聚合多个子集群的返回结果。
 */
public class FederationInterceptorREST extends AbstractRESTRequestInterceptor {

  private static final Logger LOG = LoggerFactory.getLogger(FederationInterceptorREST.class);

  private int numSubmitRetries;
  private FederationStateStoreFacade federationFacade;
  private RouterPolicyFacade policyFacade;
  private RouterMetrics routerMetrics;
  private final Clock clock = new MonotonicClock();
  private boolean returnPartialReport;
  private boolean appInfosCacheEnabled;
  private int appInfosCacheCount;
  private boolean allowPartialResult;
  private long submitIntervalTime;

  private Map<SubClusterId, DefaultRequestInterceptorREST> interceptors;
  private LRUCacheHashMap<RouterAppInfoCacheKey, AppsInfo> appInfosCaches;

  /**
   * 异步并行请求子集群的线程池。
   */
  private ExecutorService threadpool;

  @Override
  public void init(String user) {

    super.init(user);

    federationFacade = FederationStateStoreFacade.getInstance(getConf());

    final Configuration conf = this.getConf();

    try {
      SubClusterResolver subClusterResolver =
          this.federationFacade.getSubClusterResolver();
      policyFacade = new RouterPolicyFacade(
          conf, federationFacade, subClusterResolver, null);
    } catch (FederationPolicyInitializationException e) {
      throw new YarnRuntimeException(e);
    }

    numSubmitRetries = conf.getInt(
        YarnConfiguration.ROUTER_CLIENTRM_SUBMIT_RETRY,
        YarnConfiguration.DEFAULT_ROUTER_CLIENTRM_SUBMIT_RETRY);

    interceptors = new HashMap<>();
    routerMetrics = RouterMetrics.getMetrics();
    threadpool = HadoopExecutors.newCachedThreadPool(new ThreadFactoryBuilder()
        .setNameFormat("FederationInterceptorREST #%d")
        .build());

    returnPartialReport = conf.getBoolean(
        YarnConfiguration.ROUTER_WEBAPP_PARTIAL_RESULTS_ENABLED,
        YarnConfiguration.DEFAULT_ROUTER_WEBAPP_PARTIAL_RESULTS_ENABLED);

    appInfosCacheEnabled = conf.getBoolean(
        YarnConfiguration.ROUTER_APPSINFO_ENABLED,
        YarnConfiguration.DEFAULT_ROUTER_APPSINFO_ENABLED);

    if(appInfosCacheEnabled) {
      appInfosCacheCount = conf.getInt(
          YarnConfiguration.ROUTER_APPSINFO_CACHED_COUNT,
          YarnConfiguration.DEFAULT_ROUTER_APPSINFO_CACHED_COUNT);
      appInfosCaches = new LRUCacheHashMap<>(appInfosCacheCount, true);
    }

    allowPartialResult = conf.getBoolean(
        YarnConfiguration.ROUTER_INTERCEPTOR_ALLOW_PARTIAL_RESULT_ENABLED,
        YarnConfiguration.DEFAULT_ROUTER_INTERCEPTOR_ALLOW_PARTIAL_RESULT_ENABLED);

    submitIntervalTime = conf.getTimeDuration(
        YarnConfiguration.ROUTER_CLIENTRM_SUBMIT_INTERVAL_TIME,
        YarnConfiguration.DEFAULT_CLIENTRM_SUBMIT_INTERVAL_TIME, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  protected DefaultRequestInterceptorREST getInterceptorForSubCluster(SubClusterId subClusterId) {
    if (interceptors.containsKey(subClusterId)) {
      return interceptors.get(subClusterId);
    } else {
      LOG.error("The interceptor for SubCluster {} does not exist in the cache.",
          subClusterId);
      return null;
    }
  }

  private DefaultRequestInterceptorREST createInterceptorForSubCluster(
      SubClusterId subClusterId, String webAppAddress) {

    final Configuration conf = this.getConf();