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

package org.apache.hadoop.yarn.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.client.AMRMClientUtils;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.metrics.AMRMClientRelayerMetrics;
import org.apache.hadoop.yarn.server.scheduler.ResourceRequestSet;
import org.apache.hadoop.yarn.server.scheduler.ResourceRequestSetKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 介于AMRMClient(Impl)与YARN RM之间的中转层组件，记忆所有未完成请求，自动处理RM故障切换重同步，无需将重同步异常透传给上层AMRMClient。
 */
public class AMRMClientRelayer implements ApplicationMasterProtocol {
  private static final Logger LOG =
      LoggerFactory.getLogger(AMRMClientRelayer.class);

  // 底层RM客户端代理
  private ApplicationMasterProtocol rmClient;

  /**
   * AM发送的原始注册请求，用于向RM重新注册时复用。
   */
  private RegisterApplicationMasterRequest amRegistrationRequest;

  /**
   * 类似AMRMClientImpl，以下数据结构分为两类：
   *
   * remote端：RM尚未满足的已发送请求，当RM故障切换后，重新注册后会重发所有这些请求
   * 本地端：RM尚未接收的请求，当RM抛出非故障切换异常时，请求被认为未送达，会与新请求合并后在下一次心跳重发
   */
  // RM侧未完成的资源请求集合
  private Map<ResourceRequestSetKey, ResourceRequestSet> remotePendingAsks =
      new HashMap<>();
  /**
   * 与AMRMClientImpl一致，使用自定义比较器忽略容器数量比较，TreeSet支持自定义比较器。
   */
  // 本次心跳待发送的资源请求集合
  private Set<ResourceRequest> ask =
      new TreeSet<>(new ResourceRequest.ResourceRequestComparator());

  /**
   * 待分配请求计数与分配延迟指标存储，仅适用于非零分配请求ID的请求。
   */
  // 每个分配请求ID对应的待分配容器数量
  private Map<Long, Integer> pendingCountForMetrics = new HashMap<>();
  // 每个分配请求ID对应的请求发起时间戳
  private Map<Long, Long> askTimeStamp = new HashMap<>();
  // 已分配容器ID集合，避免指标重复统计
  private Set<ContainerId> knownContainers = new HashSet<>();

  // RM侧待释放容器ID集合
  private Set<ContainerId> remotePendingRelease = new HashSet<>();
  // 本次心跳待发送的待释放容器ID集合
  private Set<ContainerId> release = new HashSet<>();

  // RM侧已拉黑节点集合
  private Set<String> remoteBlacklistedNodes = new HashSet<>();
  // 本次心跳待添加的拉黑节点集合
  private Set<String> blacklistAdditions = new HashSet<>();
  // 本次心跳待移除的拉黑节点集合
  private Set<String> blacklistRemovals = new HashSet<>();

  // RM侧待更新容器请求集合
  private Map<ContainerId, UpdateContainerRequest> remotePendingChange =
      new HashMap<>();
  // 本次心跳待发送的容器更新请求集合
  private Map<ContainerId, UpdateContainerRequest> change = new HashMap<>();
  // 每个容器更新请求的发起时间戳
  private Map<ContainerId, Long> changeTimeStamp = new HashMap<>();

  // RM侧待处理调度请求集合
  private Map<Set<String>, List<SchedulingRequest>> remotePendingSchedRequest =
      new HashMap<>();
  // 本次心跳待发送的调度请求列表
  private List<SchedulingRequest> schedulingRequest = new ArrayList<>();

  // 当前应用ID
  private ApplicationId appId;

  // 正常为-1，非-1时会在下一次心跳覆盖响应ID
  private volatile int resetResponseId;

  // 当前连接的RM标识
  private String rmId = "";
  // 关闭标记
  private volatile boolean shutdown = false;

  // 指标收集器
  private AMRMClientRelayerMetrics metrics;

  // 容器分配历史记录
  private ContainerAllocationHistory allocationHistory;

  /**
   * 构造AMRM中转层实例。
   * @param rmClient 底层RM客户端
   * @param appId 当前应用ID
   * @param rmId 当前RM标识
   */
  public AMRMClientRelayer(ApplicationMasterProtocol rmClient,
      ApplicationId appId, String rmId) {
    this.resetResponseId = -1;
    this.metrics = AMRMClientRelayerMetrics.getInstance();
    this.rmClient = rmClient;
    this.appId = appId;
    this.rmId = rmId;
  }

  /**
   * 构造AMRM中转层实例，支持配置容器分配历史。
   * @param rmClient 底层RM客户端
   * @param appId 当前应用ID
   * @param rmId 当前RM标识
   * @param conf 配置对象
   */
  public AMRMClientRelayer(ApplicationMasterProtocol rmClient,
      ApplicationId appId, String rmId, Configuration conf) {
    this(rmClient, appId, rmId);
    this.allocationHistory = new ContainerAllocationHistory(conf);
  }

  /**
   * 设置AM注册请求。
   * @param registerRequest 注册请求
   */
  public void setAMRegistrationRequest(
      RegisterApplicationMasterRequest registerRequest) {
    this.amRegistrationRequest = registerRequest;
  }

  /**
   * 获取当前连接RM标识。
   * @return RM标识字符串
   */
  public String getRMIdentifier() {
    return this.rmId;
  }

  /**
   * 更新底层RM客户端。
   * @param client 新的RM客户端
   */
  public void setRMClient(ApplicationMasterProtocol client) {
    this.rmClient = client;
  }

  /**
   * 关闭中转层，清理指标数据并停止客户端代理。
   */
  public void shutdown() {
    // 关闭时清理待处理请求指标，设置关闭标记避免新请求加入
    synchronized (this) {
      if (this.shutdown) {
        LOG.warn(
            "Shutdown called twice for AMRMClientRelayer for RM " + this.rmId);
        return;
      }
      this.shutdown = true;
      // 遍历所有远程待处理资源请求，清理对应指标
      for (Map.Entry<ResourceRequestSetKey, ResourceRequestSet> entry
          : this.remotePendingAsks .entrySet()) {
        ResourceRequestSetKey key = entry.getKey();
        if (key.getAllocationRequestId() == 0) {
          this.metrics.decrClientPending(this.rmId,
              AMRMClientRelayerMetrics.getRequestType(key.getExeType()),
              entry.getValue().getNumContainers());
        } else {
          this.askTimeStamp.remove(key.getAllocationRequestId());
          Integer pending =
              this.pendingCountForMetrics.remove(key.getAllocationRequestId());
          if (pending == null) {
            throw new YarnRuntimeException(
                "pendingCountForMetrics not found for key " + key
                    + " during shutdown");
          }
          this.metrics.decrClientPending(this.rmId,
              AMRMClientRelayerMetrics.getRequestType(key.getExeType()),
              pending);
        }
      }
      // 清理容器更新请求指标
      for(UpdateContainerRequest req : remotePendingChange.values()) {
        this.metrics
            .decrClientPending(rmId, req.getContainerUpdateType(), 1);
      }
    }

    if (this.rmClient != null) {
      try {
        RPC.stopProxy(this.rmClient);
        this.rmClient = null;
      } catch (HadoopIllegalArgumentException e) {
      }
    }
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request)
      throws YarnException, IOException {
    this.amRegistrationRequest = request;
    return this.rmClient.registerApplicationMaster(request);
  }

  /**
   * RM故障切换后重新注册，处理并发注册场景，忽略已注册异常。
   * @param request 注册请求
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  private void reRegisterApplicationMaster(
      RegisterApplicationMasterRequest request)
      throws YarnException, IOException {
    try {
      registerApplicationMaster(request);
    } catch (InvalidApplicationMasterRequestException e) {
      if (e.getMessage()
          .contains(AMRMClientUtils.APP_ALREADY_REGISTERED_MESSAGE)) {
        LOG.info("Concurrent thread successfully re-registered, moving on.");
      } else {
        throw e;
      }
    }
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request)
      throws YarnException, IOException {
    try {
      return this.rmClient.finishApplicationMaster(request);
    } catch (ApplicationMasterNotRegisteredException e) {
      LOG.warn("Out of sync with RM " + rmId
          + " for " + this.appId + ", hence resyncing.");
      // 与RM不同步，重新注册后重试
      reRegisterApplicationMaster(this.amRegistrationRequest);
      return finishApplicationMaster(request);
    }
  }

  /**
   * 将新分配请求合并到本地数据结构中。
   * @param allocateRequest 分配请求
   * @throws YarnException YARN异常
   */
  private void addNewAllocateRequest(AllocateRequest allocateRequest)
      throws YarnException {
    // 先更新数据结构
    addNewAsks(allocateRequest.getAskList());

    if (allocateRequest.getReleaseList() != null) {
      this.remotePendingRelease.addAll(allocateRequest.getReleaseList());
      this.release.addAll(allocateRequest.getReleaseList());
    }

    if (allocateRequest.getResourceBlacklistRequest() != null) {
      if (allocateRequest.getResourceBlacklistRequest()
          .getBlacklistAdditions() != null) {
        this.remoteBlacklistedNodes.addAll(allocateRequest
            .getResourceBlacklistRequest().getBlacklistAdditions());
        this.blacklistAdditions.addAll(allocateRequest
            .getResourceBlacklistRequest().getBlacklistAdditions());
      }
      if (allocateRequest.getResourceBlacklistRequest()
          .getBlacklistRemovals() != null) {
        this.remoteBlacklistedNodes.removeAll(allocateRequest
            .getResourceBlacklistRequest().getBlacklistRemovals());
        this.blacklistRemovals.addAll(allocateRequest
            .getResourceBlacklistRequest().getBlacklistRemovals());
      }
    }

    if (allocateRequest.getUpdateRequests() != null) {
      for (UpdateContainerRequest update : allocateRequest
          .getUpdateRequests()) {
        UpdateContainerRequest req =
            this.remotePendingChange.put(update.getContainerId(), update);
        this.changeTimeStamp
            .put(update.getContainerId(), System.currentTimeMillis());
        if (req == null) {
          // 新增请求，指标计数+1
          this.metrics
              .incrClientPending(rmId, update.getContainerUpdateType(), 1);
        } else if (req.getContainerUpdateType() != update
            .getContainerUpdateType()) {
          // 请求更新类型变更，原类型计数-1，新类型计数+1
          this.metrics
              .decrClientPending(rmId, req.getContainerUpdateType(), 1);
          this.metrics
              .incrClientPending(rmId, update.getContainerUpdateType(), 1);
        }
        this.change.put(update.getContainerId(), update);
      }
    }

    if (allocateRequest.getSchedulingRequests() != null) {
      AMRMClientUtils.addToOutstandingSchedulingRequests(
          allocateRequest.getSchedulingRequests(),
          this.remotePendingSchedRequest);
      this.schedulingRequest.addAll(allocateRequest.getSchedulingRequests());
    }
  }

  @Override
  public AllocateResponse allocate(AllocateRequest allocateRequest)
      throws YarnException, IOException {
    AllocateResponse allocateResponse = null;
    long startTime = System.currentTimeMillis();
    synchronized (this) {
      if(this.shutdown){
        throw new YarnException("Allocate called after AMRMClientRelayer for "
            + "RM " + rmId + " shutdown.");
      }
      // 合并新请求到本地数据结构
      addNewAllocateRequest(allocateRequest);

      // 复制待发送资源请求，避免RPC发送过程中被修改
      ArrayList<ResourceRequest> askList = new ArrayList<>(ask.size());
      for (ResourceRequest r : ask) {
        askList.add(ResourceRequest.clone(r));
      }

      // 构造发送给RM的分配请求，合并所有本地待发送请求
      allocateRequest = AllocateRequest.newBuilder()
          .responseId(allocateRequest.getResponseId())
          .progress(allocateRequest.getProgress()).askList(askList)
          .releaseList(new ArrayList<>(this.release))
          .resourceBlacklistRequest(ResourceBlacklistRequest.newInstance(
              new ArrayList<>(this.blacklistAdditions),
              new ArrayList<>(this.blacklistRemovals)))
          .updateRequests(new ArrayList<>(this.change.values()))
          .schedulingRequests(new ArrayList<>(this.schedulingRequest))
          .build();

      // 如果需要重置响应ID，覆盖请求中的响应ID
      if (this.resetResponseId != -1) {
        LOG.info("Override allocate responseId from "
            + allocateRequest.getResponseId() + " to " + this.resetResponseId
            + " for " + this.appId);
        allocateRequest.setResponseId(this.resetResponseId);
      }
    }

    // 执行实际RPC分配调用
    try {
      allocateResponse = this.rmClient.allocate(allocateRequest);

      // 心跳成功，清除响应ID重置标记
      this.resetResponseId = -1;
    } catch (ApplicationMasterNotRegisteredException e) {
      // 捕获未注册异常，自动重同步后重试
      LOG.warn("ApplicationMaster is out of sync with RM " + rmId
          + " for " + this.appId + ", hence resyncing.");

      // RM主备切换计数+1
      this.metrics.incrRMMasterSlaveSwitch(this.rmId);

      synchronized (this) {
        // 将所有远程已发送未完成请求合并到待发送列表，准备重发
        for (ResourceRequestSet requestSet : this.remotePendingAsks
            .values()) {
          for (ResourceRequest rr : requestSet.getRRs()) {
            addResourceRequestToAsk(rr);
          }
        }
        this.release.addAll(this.remotePendingRelease);
        this.blacklistAdditions.addAll(this.remoteBlacklistedNodes);
        this.change.putAll(this.remotePendingChange);
        for (List<SchedulingRequest> reqs : this.remotePendingSchedRequest
            .values()) {
          this.schedulingRequest.addAll(reqs);
        }
      }

      // 重新注册后递归重试分配
      reRegisterApplicationMaster(this.amRegistrationRequest);
      // 重新注册后重置响应ID为0
      allocateRequest.setResponseId(0);
      allocateResponse = allocate(allocateRequest);
      return allocateResponse;
    } catch (Throwable t) {
      // 其他异常，记录心跳失败指标后抛出
      this.metrics.addHeartbeatFailure(this.rmId,
          System.currentTimeMillis() - startTime);

      // 如果是