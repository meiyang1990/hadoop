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

package org.apache.hadoop.yarn.server.nodemanager.recovery;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerRetryContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LogDeleterProto;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;

/**
 * NodeManager状态存储抽象基类，定义了NM重启恢复所需的各类状态存储接口，
 * 负责持久化NM运行时状态，支持NodeManager重启后恢复应用和容器状态。
 */
@Private
@Unstable
public abstract class NMStateStoreService extends AbstractService {

  private NodeStatusUpdater nodeStatusUpdater = null;

  public NMStateStoreService(String name) {
    super(name);
  }

  protected NodeStatusUpdater getNodeStatusUpdater() {
    return nodeStatusUpdater;
  }

  public void setNodeStatusUpdater(NodeStatusUpdater nodeStatusUpdater) {
    this.nodeStatusUpdater = nodeStatusUpdater;
  }

  /**
   * 恢复应用状态的封装类，持有应用状态迭代器
   */
  public static class RecoveredApplicationsState {
    RecoveryIterator<ContainerManagerApplicationProto> it = null;

    public RecoveryIterator<ContainerManagerApplicationProto> getIterator() {
      return it;
    }
  }

  /**
   * Type of post recovery action.
   */
  public enum RecoveredContainerType {
    KILL, RECOVER
  }

  public enum RecoveredContainerStatus {
    REQUESTED,
    QUEUED,
    LAUNCHED,
    COMPLETED,
    PAUSED
  }

  /**
   * 恢复容器状态封装类，存储从状态存储中加载的容器完整信息
   */
  public static class RecoveredContainerState {
    RecoveredContainerStatus status;
    int exitCode = ContainerExitStatus.INVALID;
    boolean killed = false;
    String diagnostics = "";
    StartContainerRequest startRequest;
    Resource capability;
    private int remainingRetryAttempts = ContainerRetryContext.RETRY_INVALID;
    private List<Long> restartTimes;
    private String workDir;
    private String logDir;
    int version;
    private RecoveredContainerType recoveryType =
        RecoveredContainerType.RECOVER;
    private long startTime;
    private ResourceMappings resMappings = new ResourceMappings();
    private final ContainerId containerId;

    RecoveredContainerState(ContainerId containerId){
      this.containerId = containerId;
    }

    public ContainerId getContainerId() {
      return containerId;
    }

    public RecoveredContainerStatus getStatus() {
      return status;
    }

    public int getExitCode() {
      return exitCode;
    }

    public boolean getKilled() {
      return killed;
    }

    public String getDiagnostics() {
      return diagnostics;
    }

    public int getVersion() {
      return version;
    }

    public long getStartTime() {
      return startTime;
    }

    public void setStartTime(long ts) {
      startTime = ts;
    }

    public StartContainerRequest getStartRequest() {
      return startRequest;
    }

    public Resource getCapability() {
      return capability;
    }

    public int getRemainingRetryAttempts() {
      return remainingRetryAttempts;
    }

    public void setRemainingRetryAttempts(int retryAttempts) {
      this.remainingRetryAttempts = retryAttempts;
    }

    public List<Long> getRestartTimes() {
      return restartTimes;
    }

    public void setRestartTimes(
        List<Long> restartTimes) {
      this.restartTimes = restartTimes;
    }

    public String getWorkDir() {
      return workDir;
    }

    public void setWorkDir(String workDir) {
      this.workDir = workDir;
    }

    public String getLogDir() {
      return logDir;
    }

    public void setLogDir(String logDir) {
      this.logDir = logDir;
    }

    @Override
    public String toString() {
      return new StringBuilder("Status: ").append(getStatus())
          .append(", Exit code: ").append(exitCode)
          .append(", Version: ").append(version)
          .append(", Start Time: ").append(startTime)
          .append(", Killed: ").append(getKilled())
          .append(", Diagnostics: ").append(getDiagnostics())
          .append(", Capability: ").append(getCapability())
          .append(", StartRequest: ").append(getStartRequest())
          .append(", RemainingRetryAttempts: ").append(remainingRetryAttempts)
          .append(", RestartTimes: ").append(restartTimes)
          .append(", WorkDir: ").append(workDir)
          .append(", LogDir: ").append(logDir)
          .toString();
    }

    public RecoveredContainerType getRecoveryType() {
      return recoveryType;
    }

    public void setRecoveryType(RecoveredContainerType recoveryType) {
      this.recoveryType = recoveryType;
    }

    public ResourceMappings getResourceMappings() {
      return resMappings;
    }

    public void setResourceMappings(ResourceMappings mappings) {
      this.resMappings = mappings;
    }
  }

  /**
   * 本地资源追踪状态封装类，持有已完成和正在进行的资源本地化迭代器
   */
  public static class LocalResourceTrackerState {
    final private RecoveryIterator<LocalizedResourceProto>
        completedResourcesIterator;
    final private RecoveryIterator<Entry<LocalResourceProto, Path>>
        startedResourcesIterator;

    LocalResourceTrackerState(RecoveryIterator<LocalizedResourceProto> crIt,
        RecoveryIterator<Entry<LocalResourceProto, Path>> srIt) {
      this.completedResourcesIterator = crIt;
      this.startedResourcesIterator = srIt;
    }

    public RecoveryIterator<LocalizedResourceProto>
        getCompletedResourcesIterator() {
      return completedResourcesIterator;
    }

    public RecoveryIterator<Entry<LocalResourceProto, Path>>
        getStartedResourcesIterator() {
      return startedResourcesIterator;
    }
  }

  /**
   * 用户级恢复资源封装类，包含用户私有资源和各应用专属资源状态
   */
  public static class RecoveredUserResources {
    LocalResourceTrackerState privateTrackerState =
        new LocalResourceTrackerState(null, null);
    Map<ApplicationId, LocalResourceTrackerState> appTrackerStates =
        new HashMap<ApplicationId, LocalResourceTrackerState>();

    public LocalResourceTrackerState getPrivateTrackerState() {
      return privateTrackerState;
    }

    public Map<ApplicationId, LocalResourceTrackerState>
    getAppTrackerStates() {
      return appTrackerStates;
    }
  }

  /**
   * 本地化状态恢复封装类，包含公共资源和所有用户资源的迭代器
   */
  public static class RecoveredLocalizationState {
    LocalResourceTrackerState publicTrackerState =
        new LocalResourceTrackerState(null, null);
    RecoveryIterator<Entry<String, RecoveredUserResources>> it = null;

    public LocalResourceTrackerState getPublicTrackerState() {
      return publicTrackerState;
    }

    public RecoveryIterator<Entry<String, RecoveredUserResources>> getIterator() {
      return it;
    }
  }

  /**
   * 删除服务恢复状态封装类，持有待删除任务迭代器
   */
  public static class RecoveredDeletionServiceState {
    RecoveryIterator<DeletionServiceDeleteTaskProto> it = null;

    public RecoveryIterator<DeletionServiceDeleteTaskProto> getIterator(){
      return it;
    }
  }

  /**
   * NM令牌恢复状态封装类，持有当前和前一个主密钥，以及应用尝试主密钥迭代器
   */
  public static class RecoveredNMTokensState {
    MasterKey currentMasterKey;
    MasterKey previousMasterKey;
    RecoveryIterator<Entry<ApplicationAttemptId, MasterKey>> it = null;

    public RecoveryIterator<Entry<ApplicationAttemptId, MasterKey>> getIterator() {
      return it;
    }

    public MasterKey getCurrentMasterKey() {
      return currentMasterKey;
    }

    public MasterKey getPreviousMasterKey() {
      return previousMasterKey;
    }

  }

  /**
   * 容器令牌恢复状态封装类，持有当前和前一个主密钥，以及容器令牌过期时间迭代器
   */
  public static class RecoveredContainerTokensState {
    MasterKey currentMasterKey;
    MasterKey previousMasterKey;
    RecoveryIterator<Entry<ContainerId, Long>> it = null;

    public RecoveryIterator<Entry<ContainerId, Long>> getIterator() {
      return it;
    }

    public MasterKey getCurrentMasterKey() {
      return currentMasterKey;
    }

    public MasterKey getPreviousMasterKey() {
      return previousMasterKey;
    }

  }

  /**
   * 日志删除器恢复状态封装类，持有所有应用日志删除状态映射
   */
  public static class RecoveredLogDeleterState {
    Map<ApplicationId, LogDeleterProto> logDeleterMap;

    public Map<ApplicationId, LogDeleterProto> getLogDeleterMap() {
      return logDeleterMap;
    }
  }

  /**
   * Recovered states for AMRMProxy.
   */
  public static class RecoveredAMRMProxyState {
    private MasterKey currentMasterKey;
    private MasterKey nextMasterKey;
    // For each app, stores amrmToken, user name, as well as various AMRMProxy
    // intercepter states
    private Map<ApplicationAttemptId, Map<String, byte[]>> appContexts;

    public RecoveredAMRMProxyState() {
      appContexts = new HashMap<>();
    }

    public MasterKey getCurrentMasterKey() {
      return currentMasterKey;
    }

    public MasterKey getNextMasterKey() {
      return nextMasterKey;
    }

    public Map<ApplicationAttemptId, Map<String, byte[]>> getAppContexts() {
      return appContexts;
    }

    public void setCurrentMasterKey(MasterKey currentKey) {
      currentMasterKey = currentKey;
    }

    public void setNextMasterKey(MasterKey nextKey) {
      nextMasterKey = nextKey;
    }
  }

  /** Initialize the state storage */
  @Override
  public void serviceInit(Configuration conf) throws IOException {
    initStorage(conf);
  }

  /** Start the state storage for use */
  @Override
  public void serviceStart() throws IOException {
    startStorage();
  }

  /** Shutdown the state storage. */
  @Override
  public void serviceStop() throws IOException {
    closeStorage();
  }

  /**
   * 检查是否支持状态恢复
   * @return 是否可恢复
   */
  public boolean canRecover() {
    return true;
  }

  /**
   * 检查状态存储是否为新建
   * @return 是否是新建存储
   */
  public boolean isNewlyCreated() {
    return false;
  }

  /**
   * Load the state of applications.
   * @return recovered state for applications.
   * @throws IOException IO Exception.
   */
  public abstract RecoveredApplicationsState loadApplicationsState()
      throws IOException;

  /**
   * Record the start of an application
   * @param appId the application ID
   * @param p state to store for the application
   * @throws IOException
   */
  public abstract void storeApplication(ApplicationId appId,
      ContainerManagerApplicationProto p) throws IOException;

  /**
   * Remove records corresponding to an application
   * @param appId the application ID
   * @throws IOException
   */
  public abstract void removeApplication(ApplicationId appId)
      throws IOException;


  /**
   * get the Recovered Container State Iterator
   * @return recovery iterator
   */
  public abstract RecoveryIterator<RecoveredContainerState> getContainerStateIterator()
      throws IOException;

  /**
   * Record a container start request
   * @param containerId the container ID
   * @param containerVersion the container Version
   * @param startTime container start time
   * @param startRequest the container start request
   * @throws IOException
   */
  public abstract void storeContainer(ContainerId containerId,
          int containerVersion, long startTime,
          StartContainerRequest startRequest)
      throws IOException;

  /**
   * Record that a container has been queued at the NM
   * @param containerId the container ID
   * @throws IOException
   */
  public abstract void storeContainerQueued(ContainerId containerId)
      throws IOException;

  /**
   * Record that a container has been paused at the NM.
   * @param containerId the container ID.
   * @throws IOException IO Exception.
   */
  public abstract void storeContainerPaused(ContainerId containerId)
      throws IOException;

  /**
   * Record that a container has been resumed at the NM by removing the
   * fact that it has be paused.
   * @param containerId the container ID.
   * @throws IOException IO Exception.
   */
  public abstract void removeContainerPaused(ContainerId containerId)
      throws IOException;

  /**
   * Record that a container has been launched
   * @param containerId the container ID
   * @throws IOException
   */
  public abstract void storeContainerLaunched(ContainerId containerId)
      throws IOException;

  /**
   * Record that a container has been updated
   * @param containerId the container ID
   * @param containerTokenIdentifier container token identifier
   * @throws IOException
   */
  public abstract void storeContainerUpdateToken(ContainerId containerId,
      ContainerTokenIdentifier containerTokenIdentifier) throws IOException;

  /**
   * Record that a container has completed
   * @param containerId the container ID
   * @param exitCode the exit code from the container
   * @throws IOException
   */
  public abstract void storeContainerCompleted(ContainerId containerId,
      int exitCode) throws IOException;

  /**
   * Record a request to kill a container
   * @param containerId the container ID
   * @throws IOException
   */
  public abstract void storeContainerKilled(ContainerId containerId)
      throws IOException;

  /**
   * Record diagnostics for a container
   * @param containerId the container ID
   * @param diagnostics the container diagnostics
   * @throws IOException
   */
  public abstract void storeContainerDiagnostics(ContainerId containerId,
      StringBuilder diagnostics) throws IOException;

  /**
   * Record remaining retry attempts for a container.
   * @param containerId the container ID
   * @param remainingRetryAttempts the remain retry times when container
   *                               fails to run
   * @throws IOException
   */
  public abstract void storeContainerRemainingRetryAttempts(
      ContainerId containerId, int remainingRetryAttempts) throws IOException;

  /**
   * Record restart times for a container.
   * @param containerId
   * @param restartTimes
   * @throws IOException
   */
  public abstract void storeContainerRestartTimes(
      ContainerId containerId, List<Long> restartTimes)
      throws IOException;

  /**
   * Record working directory for a container.
   * @param