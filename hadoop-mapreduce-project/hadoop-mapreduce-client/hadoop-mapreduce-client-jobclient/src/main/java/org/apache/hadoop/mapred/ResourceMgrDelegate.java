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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.QueueAclsInfo;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeKey;
import org.apache.hadoop.yarn.api.records.NodeAttributeInfo;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.NodeToAttributeValue;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceTypeInfo;
import org.apache.hadoop.yarn.api.records.ShellContainerCommand;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 资源管理器代理，为旧版MapReduce API提供与YARN ResourceManager交互的统一代理层
 * 继承YarnClient，封装对YARN服务端的调用，适配旧版MR接口要求
 */
public class ResourceMgrDelegate extends YarnClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(ResourceMgrDelegate.class);
      
  private YarnConfiguration conf;
  private ApplicationSubmissionContext application;
  private ApplicationId applicationId;
  @Private
  @VisibleForTesting
  protected YarnClient client;
  private Text rmDTService;

  /**
   * 构造与YARN ResourceManager通信的代理实例
   * @param conf YARN配置对象
   */
  public ResourceMgrDelegate(YarnConfiguration conf) {
    super(ResourceMgrDelegate.class.getName());
    this.conf = conf;
    this.client = YarnClient.createYarnClient();
    init(conf);
    start();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    client.init(conf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    client.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    client.stop();
    super.serviceStop();
  }

  /**
   * 获取所有正在运行的TaskTracker信息（适配旧MR接口，转换YARN Node信息）
   * @return 活跃TaskTracker数组
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public TaskTrackerInfo[] getActiveTrackers() throws IOException,
      InterruptedException {
    try {
      return TypeConverter.fromYarnNodes(
          client.getNodeReports(NodeState.RUNNING));
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  /**
   * 获取所有MapReduce作业状态列表
   * @return 所有作业状态数组
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public JobStatus[] getAllJobs() throws IOException, InterruptedException {
    try {
      Set<String> appTypes = new HashSet<String>(1);
      // 只过滤MapReduce类型的应用
      appTypes.add(MRJobConfig.MR_APPLICATION_TYPE);
      EnumSet<YarnApplicationState> appStates =
          EnumSet.noneOf(YarnApplicationState.class);
      return TypeConverter.fromYarnApps(
          client.getApplications(appTypes, appStates), this.conf);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  /**
   * 获取被拉黑的TaskTracker列表（尚未实现）
   * @return 空数组
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public TaskTrackerInfo[] getBlacklistedTrackers() throws IOException,
      InterruptedException {
    // TODO: Implement getBlacklistedTrackers
    LOG.warn("getBlacklistedTrackers - Not implemented yet");
    return new TaskTrackerInfo[0];
  }

  /**
   * 获取集群指标信息，转换为旧版MapReduce ClusterMetrics格式
   * @return 集群指标对象
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public ClusterMetrics getClusterMetrics() throws IOException,
      InterruptedException {
    try {
      YarnClusterMetrics metrics = client.getYarnClusterMetrics();
      // 适配旧版指标结构，部分字段保持兼容值
      ClusterMetrics oldMetrics =
          new ClusterMetrics(1, 1, 1, 1, 1, 1,
              metrics.getNumNodeManagers() * 10,
              metrics.getNumNodeManagers() * 2, 1,
              metrics.getNumNodeManagers(), 0, 0);
      return oldMetrics;
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  /**
   * 获取ResourceManager委派令牌服务名
   * @return 委派令牌服务名
   */
  public Text getRMDelegationTokenService() {
    if (rmDTService == null) {
      rmDTService = ClientRMProxy.getRMDelegationTokenService(conf);
    }
    return rmDTService;
  }
  
  /**
   * 获取ResourceManager委派令牌
   * @param renewer 令牌更新者
   * @return 委派令牌对象
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  @SuppressWarnings("rawtypes")
  public Token getDelegationToken(Text renewer) throws IOException,
      InterruptedException {
    try {
      return ConverterUtils.convertFromYarn(
          client.getRMDelegationToken(renewer), getRMDelegationTokenService());
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  /**
   * 获取默认文件系统名称
   * @return 文件系统URI字符串
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public String getFilesystemName() throws IOException, InterruptedException {
    return FileSystem.get(conf).getUri().toString();
  }

  /**
   * 从YARN获取新的作业ID
   * @return 转换后的旧版MapReduce JobID
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public JobID getNewJobID() throws IOException, InterruptedException {
    try {
      this.application = client.createApplication().getApplicationSubmissionContext();
      this.applicationId = this.application.getApplicationId();
      return TypeConverter.fromYarn(applicationId);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  /**
   * 获取指定队列信息，转换为旧版格式
   * @param queueName 队列名称
   * @return 队列信息对象，不存在则返回null
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public QueueInfo getQueue(String queueName) throws IOException,
  InterruptedException {
    try {
      org.apache.hadoop.yarn.api.records.QueueInfo queueInfo =
          client.getQueueInfo(queueName);
      return (queueInfo == null) ? null : TypeConverter.fromYarn(queueInfo,
          conf);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  /**
   * 获取当前用户有权限访问的队列ACL信息
   * @return 队列ACL信息数组
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException,
      InterruptedException {
    try {
      return TypeConverter.fromYarnQueueUserAclsInfo(client
        .getQueueAclsInfo());
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  /**
   * 获取所有队列信息
   * @return 队列信息数组
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public QueueInfo[] getQueues() throws IOException, InterruptedException {
    try {
      return TypeConverter.fromYarnQueueInfo(client.getAllQueues(), this.conf);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  /**
   * 获取根队列列表
   * @return 根队列信息数组
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public QueueInfo[] getRootQueues() throws IOException, InterruptedException {
    try {
      return TypeConverter.fromYarnQueueInfo(client.getRootQueueInfos(),
          this.conf);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  /**
   * 获取指定父队列的子队列列表
   * @param parent 父队列名称
   * @return 子队列信息数组
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public QueueInfo[] getChildQueues(String parent) throws IOException,
      InterruptedException {
    try {
      return TypeConverter.fromYarnQueueInfo(client.getChildQueueInfos(parent),
        this.conf);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  /**
   * 获取当前用户的作业暂存区目录
   * @return 暂存区目录路径字符串
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public String getStagingAreaDir() throws IOException, InterruptedException {
//    Path path = new Path(MRJobConstants.JOB_SUBMIT_DIR);
    String user = 
      UserGroupInformation.getCurrentUser().getShortUserName();
    Path path = MRApps.getStagingAreaDir(conf, user);
    LOG.debug("getStagingAreaDir: dir=" + path);
    return path.toString();
  }


  /**
   * 获取系统作业提交目录路径
   * @return 系统目录路径字符串
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public String getSystemDir() throws IOException, InterruptedException {
    Path sysDir = new Path(MRJobConfig.JOB_SUBMIT_DIR);
    //FileContext.getFileContext(conf).delete(sysDir, true);
    return sysDir.toString();
  }
  

  /**
   * 获取TaskTracker过期间隔（兼容旧接口）
   * @return 固定返回0
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public long getTaskTrackerExpiryInterval() throws IOException,
      InterruptedException {
    return 0;
  }
  
  /**
   * 设置作业优先级（兼容旧接口，未实现）
   * @param arg0 作业ID
   * @param arg1 优先级字符串
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public void setJobPriority(JobID arg0, String arg1) throws IOException,
      InterruptedException {
    return;
  }


  /**
   * 获取协议版本（兼容旧接口，未实现）
   * @param arg0 协议名称
   * @param arg1 客户端版本
   * @return 固定返回0
   * @throws IOException IO异常
   */
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return 0;
  }

  /**
   * 获取当前代理对应的应用ID
   * @return YARN应用ID对象
   */
  public ApplicationId getApplicationId() {
    return applicationId;
  }

  @Override
  public YarnClientApplication createApplication() throws
      YarnException, IOException {
    return client.createApplication();
  }

  @Override
  public ApplicationId
      submitApplication(ApplicationSubmissionContext appContext)
          throws YarnException, IOException {
    return client.submitApplication(appContext);
  }

  @Override
  public void failApplicationAttempt(ApplicationAttemptId attemptId)
      throws YarnException, IOException {
    client.failApplicationAttempt(attemptId);
  }

  @Override
  public void killApplication(ApplicationId applicationId)
      throws YarnException, IOException {
    client.killApplication(applicationId);
  }

  @Override
  public ApplicationReport getApplicationReport(ApplicationId appId)
      throws YarnException, IOException {
    return client.getApplicationReport(appId);
  }

  @Override
  public Token<AMRMTokenIdentifier> getAMRMToken(ApplicationId appId) 
    throws YarnException, IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ApplicationReport> getApplications() throws YarnException,
      IOException {
    return client.getApplications();
  }

  @Override
  public List<ApplicationReport> getApplications(Set<String> applicationTypes)
      throws YarnException,
      IOException {
    return client.getApplications(applicationTypes);
  }

  @Override
  public List<ApplicationReport> getApplications(
      EnumSet<YarnApplicationState> applicationStates) throws YarnException,
      IOException {
    return client.getApplications(applicationStates);
  }

  @Override
  public List<ApplicationReport> getApplications(
      Set<String> applicationTypes,
      EnumSet<YarnApplicationState> applicationStates)
      throws YarnException, IOException {
    return client.getApplications(applicationTypes, applicationStates);
  }

  @Override
  public List<ApplicationReport> getApplications(
      Set<String> applicationTypes,
      EnumSet<YarnApplicationState> applicationStates,
      Set<String> applicationTags)
      throws YarnException, IOException {
    return client.getApplications(
        applicationTypes, applicationStates, applicationTags);
  }

  @Override
  public List<ApplicationReport> getApplications(Set<String> queues,
      Set<String> users, Set<String> applicationTypes,
      EnumSet<YarnApplicationState> applicationStates) throws YarnException,
      IOException {
    return client.getApplications(queues, users, applicationTypes,
      applicationStates);
  }

  @Override
  public YarnClusterMetrics getYarnClusterMetrics() throws YarnException,
      IOException {
    return client.getYarnClusterMetrics();
  }

  @Override
  public List<NodeReport> getNodeReports(NodeState... states)
      throws Y