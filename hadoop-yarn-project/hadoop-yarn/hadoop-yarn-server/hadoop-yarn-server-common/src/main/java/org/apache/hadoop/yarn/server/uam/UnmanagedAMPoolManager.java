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

package org.apache.hadoop.yarn.server.uam;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.client.AMRMClientUtils;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.AMRMClientRelayer;
import org.apache.hadoop.yarn.util.AsyncCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 非托管应用管理器(UAM)池管理服务，负责管理一组UnmanagedApplicationManager实例的生命周期
 * 主要用于联邦YARN场景下跨子集群管理多个非托管应用master
 */
@Public
@Unstable
public class UnmanagedAMPoolManager extends AbstractService {
  public static final Logger LOG =
      LoggerFactory.getLogger(UnmanagedAMPoolManager.class);

  // UAM ID到UAM实例的映射表
  private Map<String, UnmanagedApplicationManager> unmanagedAppMasterMap;

  // UAM ID对应ApplicationId的映射表
  private Map<String, ApplicationId> appIdMap;

  // 异步任务执行线程池
  private ExecutorService threadpool;

  private final String dispatcherThreadName = "UnmanagedAMPoolManager-Finish-Thread";

  // 服务停止时强制结束UAM的后台线程
  private Thread finishApplicationThread;

  /**
   * 构造函数，使用外部提供的线程池
   * @param threadpool 外部线程池
   */
  public UnmanagedAMPoolManager(ExecutorService threadpool) {
    super(UnmanagedAMPoolManager.class.getName());
    this.threadpool = threadpool;
  }

  @Override
  protected void serviceStart() throws Exception {
    // 如果未传入线程池，创建缓存线程池
    if (this.threadpool == null) {
      this.threadpool = Executors.newCachedThreadPool();
    }
    // 初始化并发存储映射
    this.unmanagedAppMasterMap = new ConcurrentHashMap<>();
    this.appIdMap = new ConcurrentHashMap<>();
    super.serviceStart();
  }

  /**
   * 服务停止方法，若还有运行中的UAM，启动后台线程强制杀死所有UAM
   */
  @Override
  protected void serviceStop() throws Exception {
    // 存在未结束的UAM，启动强制结束线程
    if (!this.unmanagedAppMasterMap.isEmpty()) {
      finishApplicationThread = new SubjectInheritingThread(createForceFinishApplicationThread());
      finishApplicationThread.setName(dispatcherThreadName);
      finishApplicationThread.start();
    }

    super.serviceStop();
  }

  /**
   * 创建并注册新的UAM，自动从RM申请ApplicationId作为UAM ID
   *
   * @param registerRequest UAM注册请求
   * @param conf UAM配置
   * @param queueName 应用队列名称
   * @param submitter 提交者用户名
   * @param appNameSuffix 应用名称后缀
   * @param keepContainersAcrossApplicationAttempts 应用尝试恢复时是否保留容器
   * @param rmName YARN RM名称
   * @param originalAppSubmissionContext 原始应用提交上下文
   * @return 新创建UAM的ID
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  public String createAndRegisterNewUAM(
      RegisterApplicationMasterRequest registerRequest, Configuration conf,
      String queueName, String submitter, String appNameSuffix,
      boolean keepContainersAcrossApplicationAttempts, String rmName,
      ApplicationSubmissionContext originalAppSubmissionContext)
      throws YarnException, IOException {
    ApplicationId appId;
    ApplicationClientProtocol rmClient;
    try {
      // 创建远程提交用户UGI
      UserGroupInformation appSubmitter =
          UserGroupInformation.createRemoteUser(submitter);
      // 创建RM代理客户端
      rmClient = AMRMClientUtils.createRMProxy(conf,
          ApplicationClientProtocol.class, appSubmitter, null);

      // 从RM申请新的ApplicationId
      GetNewApplicationResponse response =
          rmClient.getNewApplication(GetNewApplicationRequest.newInstance());
      if (response == null) {
        throw new YarnException("getNewApplication got null response");
      }
      appId = response.getApplicationId();
      LOG.info("Received new application ID {} from RM", appId);
    } finally {
      rmClient = null;
    }

    // 启动UAM
    launchUAM(appId.toString(), conf, appId, queueName, submitter,
        appNameSuffix, keepContainersAcrossApplicationAttempts, rmName,
        originalAppSubmissionContext);

    // 向RM注册UAM
    registerApplicationMaster(appId.toString(), registerRequest);

    // 使用applicationId作为uamId返回
    return appId.toString();
  }

  /**
   * 使用指定的UAM ID和ApplicationId启动新UAM
   *
   * @param uamId UAM ID
   * @param conf UAM配置
   * @param appId 应用ID
   * @param queueName 应用队列名称
   * @param submitter 提交者用户名
   * @param appNameSuffix 应用名称后缀
   * @param keepContainersAcrossApplicationAttempts 应用尝试恢复时是否保留容器
   * @param rmName YARN RM名称
   * @param originalAppSubmissionContext 原始应用提交上下文
   * @return UAM的AMRM令牌
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  public Token<AMRMTokenIdentifier> launchUAM(String uamId, Configuration conf,
      ApplicationId appId, String queueName, String submitter,
      String appNameSuffix, boolean keepContainersAcrossApplicationAttempts,
      String rmName, ApplicationSubmissionContext originalAppSubmissionContext)
      throws YarnException, IOException {

    // 检查UAM ID是否已存在
    if (this.unmanagedAppMasterMap.containsKey(uamId)) {
      throw new YarnException("UAM " + uamId + " already exists");
    }

    // 创建UAM实例
    UnmanagedApplicationManager uam = createUAM(conf, appId, queueName,
        submitter, appNameSuffix, keepContainersAcrossApplicationAttempts,
        rmName, originalAppSubmissionContext);

    // 先存入映射表保证并发下同一个UAM ID只会创建一个实例
    this.unmanagedAppMasterMap.put(uamId, uam);

    Token<AMRMTokenIdentifier> amrmToken;
    try {
      LOG.info("Launching UAM id {} for application {}", uamId, appId);
      // 执行UAM启动流程
      amrmToken = uam.launchUAM();
    } catch (Exception e) {
      // 启动失败移除映射表
      this.unmanagedAppMasterMap.remove(uamId);
      throw e;
    }

    // 保存ApplicationId映射并返回令牌
    this.appIdMap.put(uamId, uam.getAppId());
    return amrmToken;
  }

  /**
   * 重新关联已存在的UAM实例，用于UAM恢复场景
   *
   * @param uamId UAM ID
   * @param conf UAM配置
   * @param appId 应用ID
   * @param queueName 应用队列名称
   * @param submitter 提交者用户名
   * @param appNameSuffix 应用名称后缀
   * @param uamToken UAM的AMRM令牌
   * @param rmName YARN RM名称
   * @param originalAppSubmissionContext 原始应用提交上下文
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  public void reAttachUAM(String uamId, Configuration conf, ApplicationId appId,
      String queueName, String submitter, String appNameSuffix,
      Token<AMRMTokenIdentifier> uamToken, String rmName,
      ApplicationSubmissionContext originalAppSubmissionContext)
      throws YarnException, IOException {

    // 检查UAM ID是否已存在
    if (this.unmanagedAppMasterMap.containsKey(uamId)) {
      throw new YarnException("UAM " + uamId + " already exists");
    }
    // 创建UAM实例，恢复场景默认保留容器
    UnmanagedApplicationManager uam = createUAM(conf, appId, queueName,
        submitter, appNameSuffix, true, rmName, originalAppSubmissionContext);
    // 先存入映射表保证并发安全
    this.unmanagedAppMasterMap.put(uamId, uam);

    try {
      LOG.info("Reattaching UAM id {} for application {}", uamId, appId);
      // 使用已有令牌重新关联UAM
      uam.reAttachUAM(uamToken);
    } catch (Exception e) {
      // 关联失败移除映射表
      this.unmanagedAppMasterMap.remove(uamId);
      throw e;
    }

    // 保存ApplicationId映射
    this.appIdMap.put(uamId, uam.getAppId());
  }

  /**
   * 创建UAM实例，抽取该方法方便单元测试
   *
   * @param conf 配置
   * @param appId 应用ID
   * @param queueName 队列名称
   * @param submitter 提交者用户名
   * @param appNameSuffix 应用名称后缀
   * @param keepContainersAcrossApplicationAttempts 是否保留容器
   * @param rmName RM名称
   * @param originalAppSubmissionContext 原始应用提交上下文
   * @return 创建好的UAM实例
   */
  @VisibleForTesting
  protected UnmanagedApplicationManager createUAM(Configuration conf,
      ApplicationId appId, String queueName, String submitter,
      String appNameSuffix, boolean keepContainersAcrossApplicationAttempts,
      String rmName, ApplicationSubmissionContext originalAppSubmissionContext) {
    return new UnmanagedApplicationManager(conf, appId, queueName, submitter,
        appNameSuffix, keepContainersAcrossApplicationAttempts, rmName,
        originalAppSubmissionContext);
  }

  /**
   * 向RM注册指定UAM
   *
   * @param uamId UAM ID
   * @param registerRequest 注册请求
   * @return 注册响应
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  public RegisterApplicationMasterResponse registerApplicationMaster(
      String uamId, RegisterApplicationMasterRequest registerRequest)
      throws YarnException, IOException {
    if (!this.unmanagedAppMasterMap.containsKey(uamId)) {
      throw new YarnException("UAM " + uamId + " does not exist");
    }
    LOG.info("Registering UAM id {} for application {}", uamId,
        this.appIdMap.get(uamId));
    return this.unmanagedAppMasterMap.get(uamId)
        .registerApplicationMaster(registerRequest);
  }

  /**
   * 对指定UAM执行异步资源分配请求
   *
   * @param uamId UAM ID
   * @param request 分配请求
   * @param callback 结果回调
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  public void allocateAsync(String uamId, AllocateRequest request,
      AsyncCallback<AllocateResponse> callback)
      throws YarnException, IOException {
    if (!this.unmanagedAppMasterMap.containsKey(uamId)) {
      throw new YarnException("UAM " + uamId + " does not exist");
    }
    this.unmanagedAppMasterMap.get(uamId).allocateAsync(request, callback);
  }

  /**
   * 结束指定UAM应用
   *
   * @param uamId UAM ID
   * @param request 结束请求
   * @return 结束响应
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  public FinishApplicationMasterResponse finishApplicationMaster(String uamId,
      FinishApplicationMasterRequest request)
      throws YarnException, IOException {
    if (!this.unmanagedAppMasterMap.containsKey(uamId)) {
      throw new YarnException("UAM " + uamId + " does not exist");
    }
    LOG.info("Finishing UAM id {} for application {}", uamId,
        this.appIdMap.get(uamId));
    FinishApplicationMasterResponse response =
        this.unmanagedAppMasterMap.get(uamId).finishApplicationMaster(request);

    // 注销成功后才移除UAM
    if (response.getIsUnregistered()) {
      this.unmanagedAppMasterMap.remove(uamId);
      this.appIdMap.remove(uamId);
      LOG.info("UAM id {} is unregistered", uamId);
    }
    return response;
  }

  /**
   * 关闭UAM连接但不向RM杀死应用
   *
   * @param uamId UAM ID
   * @throws YarnException YARN异常
   */
  public void shutDownConnections(String uamId)
      throws YarnException {
    if (!this.unmanagedAppMasterMap.containsKey(uamId)) {
      throw new YarnException("UAM " + uamId + " does not exist");
    }
    LOG.info(
        "Shutting down UAM id {} for application {} without killing the UAM",
        uamId, this.appIdMap.get(uamId));
    this.unmanagedAppMasterMap.remove(uamId).shutDownConnections();
  }

  /**
   * 关闭所有UAM连接但不向RM杀死应用
   *
   * @throws YarnException YARN异常
   */
  public void shutDownConnections() throws YarnException {
    for (String uamId : this.unmanagedAppMasterMap.keySet()) {
      shutDownConnections(uamId);
    }
  }

  /**
   * 获取所有正在运行的UAM ID集合
   *
   * @return UAM ID集合
   */
  public Set<String> getAllUAMIds() {
    // 返回副本避免并发修改问题
    return new HashSet<>(this.unmanagedAppMasterMap.keySet());
  }

  /**
   * 检查指定UAM ID是否存在
   *
   * @param uamId UAM ID
   * @return 是否存在
   */
  public boolean hasUAMId(String uamId) {
    return this.unmanagedAppMasterMap.containsKey(uamId);
  }

  /**
   * 获取指定UAM的AMRM客户端中继器
   *
   * @param uamId UAM ID
   * @return AMRM客户端中继器
   * @throws YarnException YARN异常
   */
  public AMRMClientRelayer getAMRMClientRelayer(String uamId)
      throws YarnException {
    if (!this.unmanagedAppMasterMap.containsKey(uamId)) {
      throw new YarnException("UAM " + uamId + " does not exist");
    }
    return this.unmanagedAppMasterMap.get(uamId).getAMRMClientRelayer();
  }

  @VisibleForTesting
  public int getRequestQueueSize(String uamId) throws YarnException {
    if (!this.unmanagedAppMasterMap.containsKey(uamId)) {
      throw new YarnException("UAM " + uamId + " does not exist");