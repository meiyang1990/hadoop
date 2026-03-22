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

package org.apache.hadoop.mapreduce.v2.app.local;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.RMCommunicator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明：本地模式容器分配器，用于MapReduce LocalJobRunner本地运行模式
 * 
 * 核心职责：为本地运行的MapReduce作业分配容器，不需要向YARN集群申请真实容器
 *          直接复用ApplicationMaster所在容器，为所有任务请求生成容器分配事件
 */
public class LocalContainerAllocator extends RMCommunicator
    implements ContainerAllocator {

  private static final Logger LOG =
      LoggerFactory.getLogger(LocalContainerAllocator.class);

  @SuppressWarnings("rawtypes")
  private final EventHandler eventHandler;
  private long retryInterval;
  private long retrystartTime;
  private String nmHost;
  private int nmPort;
  private int nmHttpPort;
  private ContainerId containerId;
  protected int lastResponseID;

  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  /**
   * 构造本地容器分配器
   * @param clientService 客户端服务对象
   * @param context 应用上下文
   * @param nmHost 本地NodeManager主机名
   * @param nmPort 本地NodeManager端口
   * @param nmHttpPort 本地NodeManager HTTP端口
   * @param cId ApplicationMaster容器ID
   */
  public LocalContainerAllocator(ClientService clientService,
    AppContext context, String nmHost, int nmPort, int nmHttpPort
    , ContainerId cId) {
    super(clientService, context);
    this.eventHandler = context.getEventHandler();
    this.nmHost = nmHost;
    this.nmPort = nmPort;
    this.nmHttpPort = nmHttpPort;
    this.containerId = cId;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    // 从配置获取RM重试间隔，使用默认值如果未配置
    retryInterval =
        getConfig().getLong(MRJobConfig.MR_AM_TO_RM_WAIT_INTERVAL_MS,
            MRJobConfig.DEFAULT_MR_AM_TO_RM_WAIT_INTERVAL_MS);
    // 初始化重试起始时间，第一次联系RM成功后会重置
    retrystartTime = System.currentTimeMillis();
  }

  @SuppressWarnings("unchecked")
  @Override
  protected synchronized void heartbeat() throws Exception {
    // 构造心跳分配请求，不带新的资源请求和容器释放请求
    AllocateRequest allocateRequest =
        AllocateRequest.newInstance(this.lastResponseID,
          super.getApplicationProgress(), new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>(), null);
    AllocateResponse allocateResponse = null;
    try {
      // 向ResourceManager发送心跳请求
      allocateResponse = scheduler.allocate(allocateRequest);
      // 无异常则重置重试起始时间
      retrystartTime = System.currentTimeMillis();
    } catch (ApplicationAttemptNotFoundException e) {
      LOG.info("Event from RM: shutting down Application Master");
      // RM重启后无法识别当前应用尝试，触发AM重启流程
      eventHandler.handle(new JobEvent(this.getJob().getID(),
        JobEventType.JOB_AM_REBOOT));
      throw new YarnRuntimeException(
        "Resource Manager doesn't recognize AttemptId: "
            + this.getContext().getApplicationID(), e);
    } catch (ApplicationMasterNotRegisteredException e) {
      LOG.info("ApplicationMaster is out of sync with ResourceManager,"
          + " hence resync and send outstanding requests.");
      // AM与RM状态不同步，重置响应ID重新注册
      this.lastResponseID = 0;
      register();
    } catch (Exception e) {
      // 连接RM失败，检查重试超时时间
      if (System.currentTimeMillis() - retrystartTime >= retryInterval) {
        LOG.error("Could not contact RM after " + retryInterval + " milliseconds.");
        // 超时未联系上RM，触发作业内部错误
        eventHandler.handle(new JobEvent(this.getJob().getID(),
                                         JobEventType.INTERNAL_ERROR));
        throw new YarnRuntimeException("Could not contact RM after " +
                                retryInterval + " milliseconds.");
      }
      // 未超时抛出异常让上层重试
      throw e;
    }

    if (allocateResponse != null) {
      // 更新最新响应ID
      this.lastResponseID = allocateResponse.getResponseId();
      Token token = allocateResponse.getAMRMToken();
      if (token != null) {
        // 更新AM-RM认证令牌
        updateAMRMToken(token);
      }
      // 从响应获取作业优先级
      Priority priorityFromResponse = Priority.newInstance(allocateResponse
          .getApplicationPriority().getPriority());

      // 更新作业优先级到作业对象
      getJob().setJobPriority(priorityFromResponse);
    }
  }

  /**
   * 更新AM与RM通信的认证令牌到当前用户凭证
   * @param token RM返回的新AMRM令牌
   * @throws IOException 令牌处理IO异常
   */
  private void updateAMRMToken(Token token) throws IOException {
    org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> amrmToken =
        new org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>(token
          .getIdentifier().array(), token.getPassword().array(), new Text(
          token.getKind()), new Text(token.getService()));
    UserGroupInformation currentUGI = UserGroupInformation.getCurrentUser();
    currentUGI.addToken(amrmToken);
    amrmToken.setService(ClientRMProxy.getAMRMTokenService(getConfig()));
  }

  /**
   * 处理容器分配事件，为本地任务分配容器
   * @param event 容器分配事件
   */
  @SuppressWarnings("unchecked")
  @Override
  public void handle(ContainerAllocatorEvent event) {
    // 只处理容器请求事件
    if (event.getType() == ContainerAllocator.EventType.CONTAINER_REQ) {
      LOG.info("Processing the event " + event.toString());
      // 复用AM的容器ID创建容器ID，所有任务都在同一容器运行
      ContainerId cID =
          ContainerId.newContainerId(getContext().getApplicationAttemptId(),
            this.containerId.getContainerId());
      // 创建容器对象并初始化
      Container container = recordFactory.newRecordInstance(Container.class);
      container.setId(cID);
      NodeId nodeId = NodeId.newInstance(this.nmHost, this.nmPort);
      container.setResource(Resource.newInstance(0, 0));
      container.setNodeId(nodeId);
      container.setContainerToken(null);
      container.setNodeHttpAddress(this.nmHost + ":" + this.nmHttpPort);

      // 如果是Map任务，更新对应作业计数器
      if (event.getAttemptID().getTaskId().getTaskType() == TaskType.MAP) {
        JobCounterUpdateEvent jce =
            new JobCounterUpdateEvent(event.getAttemptID().getTaskId()
                .getJobId());
        // TODO 暂时统计到OTHER_LOCAL_MAP计数器
        jce.addCounterUpdate(JobCounter.OTHER_LOCAL_MAPS, 1);
        eventHandler.handle(jce);
      }
      // 发送容器已分配事件，通知任务尝试可以开始执行
      eventHandler.handle(new TaskAttemptContainerAssignedEvent(
          event.getAttemptID(), container, applicationACLs));
    }
  }

}