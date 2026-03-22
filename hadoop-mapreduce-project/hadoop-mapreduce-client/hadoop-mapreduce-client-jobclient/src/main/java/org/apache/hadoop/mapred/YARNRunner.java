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

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.hadoop.mapreduce.MRJobConfig.MR_AM_RESOURCE_PREFIX;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.mapreduce.Cluster.JobTrackerStatus;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.QueueAclsInfo;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenSelector;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件：YARNRunner.java
 * 所属模块：Hadoop MapReduce 任务客户端
 * 核心职责：适配旧版MapReduce客户端API，将MapReduce作业提交到YARN集群运行，封装与YARN ResourceManager的交互逻辑
 * 功能说明：实现旧版ClientProtocol接口，负责将MapReduce作业转换为YARN应用，完成作业提交、状态查询、作业终止等操作
 */
@SuppressWarnings("unchecked")
public class YARNRunner implements ClientProtocol {

  private static final Logger LOG = LoggerFactory.getLogger(YARNRunner.class);

  private static final String RACK_GROUP = "rack";
  private static final String NODE_IF_RACK_GROUP = "node1";
  private static final String NODE_IF_NO_RACK_GROUP = "node2";

  /**
   * 匹配机架/节点位置表达式，用于AM容器的严格位置调度
   * 支持三种匹配模式：/rack、/rack/node、node（默认/default-rack）
   * 通过分组名获取匹配到的机架和节点名称
   */
  private static final Pattern RACK_NODE_PATTERN =
      Pattern.compile(
          String.format("(?<%s>[^/]+?)|(?<%s>/[^/]+?)(?:/(?<%s>[^/]+?))?",
          NODE_IF_NO_RACK_GROUP, RACK_GROUP, NODE_IF_RACK_GROUP));

  private final static RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  public final static Priority AM_CONTAINER_PRIORITY = recordFactory
      .newRecordInstance(Priority.class);
  static {
    AM_CONTAINER_PRIORITY.setPriority(0);
  }

  private ResourceMgrDelegate resMgrDelegate;
  private ClientCache clientCache;
  private Configuration conf;
  private final FileContext defaultFileContext;
  
  /**
   * 构造YARNRunner实例，初始化默认资源管理器代理
   * @param conf 客户端配置对象
   */
  public YARNRunner(Configuration conf) {
   this(conf, new ResourceMgrDelegate(new YarnConfiguration(conf)));
  }

  /**
   * 构造YARNRunner实例，允许注入自定义资源管理器代理，用于测试
   * @param conf 客户端配置对象
   * @param resMgrDelegate 资源管理器代理实例
   */
  public YARNRunner(Configuration conf, ResourceMgrDelegate resMgrDelegate) {
   this(conf, resMgrDelegate, new ClientCache(conf, resMgrDelegate));
  }

  /**
   * 构造YARNRunner实例，允许注入资源管理器代理和客户端缓存，用于测试
   * @param conf 客户端配置对象
   * @param resMgrDelegate 资源管理器代理实例
   * @param clientCache MR客户端缓存实例
   */
  public YARNRunner(Configuration conf, ResourceMgrDelegate resMgrDelegate,
      ClientCache clientCache) {
    this.conf = conf;
    try {
      this.resMgrDelegate = resMgrDelegate;
      this.clientCache = clientCache;
      this.defaultFileContext = FileContext.getFileContext(this.conf);
    } catch (UnsupportedFileSystemException ufe) {
      throw new RuntimeException("Error in instantiating YarnClient", ufe);
    }
  }
  
  @Private
  /**
   * 设置资源管理器代理，主要用于测试场景
   * @param resMgrDelegate 要设置的资源管理器代理
   */
  public void setResourceMgrDelegate(ResourceMgrDelegate resMgrDelegate) {
    this.resMgrDelegate = resMgrDelegate;
  }
  
  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> arg0)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException("Use Token.renew instead");
  }

  @Override
  public TaskTrackerInfo[] getActiveTrackers() throws IOException,
      InterruptedException {
    return resMgrDelegate.getActiveTrackers();
  }

  @Override
  public JobStatus[] getAllJobs() throws IOException, InterruptedException {
    return resMgrDelegate.getAllJobs();
  }

  @Override
  public TaskTrackerInfo[] getBlacklistedTrackers() throws IOException,
      InterruptedException {
    return resMgrDelegate.getBlacklistedTrackers();
  }

  @Override
  public ClusterMetrics getClusterMetrics() throws IOException,
      InterruptedException {
    return resMgrDelegate.getClusterMetrics();
  }

  @VisibleForTesting
  /**
   * 向凭证中添加JobHistoryServer的委托令牌，保障Oozie等组件能正常访问历史服务器
   * @param ts 作业凭证存储对象
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  void addHistoryToken(Credentials ts) throws IOException, InterruptedException {
    /* check if we have a hsproxy, if not, no need */
    MRClientProtocol hsProxy = clientCache.getInitializedHSProxy();
    if (UserGroupInformation.isSecurityEnabled() && (hsProxy != null)) {
      /*
       * note that get delegation token was called. Again this is hack for oozie
       * to make sure we add history server delegation tokens to the credentials
       */
      RMDelegationTokenSelector tokenSelector = new RMDelegationTokenSelector();
      Text service = resMgrDelegate.getRMDelegationTokenService();
      if (tokenSelector.selectToken(service, ts.getAllTokens()) != null) {
        Text hsService = SecurityUtil.buildTokenService(hsProxy
            .getConnectAddress());
        if (ts.getToken(hsService) == null) {
          ts.addToken(hsService, getDelegationTokenFromHS(hsProxy));
        }
      }
    }
  }
  
  @VisibleForTesting
  /**
   * 从JobHistoryServer获取委托令牌
   * @param hsProxy HistoryServer客户端代理
   * @return 历史服务器委托令牌
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  Token<?> getDelegationTokenFromHS(MRClientProtocol hsProxy)
      throws IOException, InterruptedException {
    GetDelegationTokenRequest request = recordFactory
      .newRecordInstance(GetDelegationTokenRequest.class);
    request.setRenewer(Master.getMasterPrincipal(conf));
    org.apache.hadoop.yarn.api.records.Token mrDelegationToken;
    mrDelegationToken = hsProxy.getDelegationToken(request)
        .getDelegationToken();
    return ConverterUtils.convertFromYarn(mrDelegationToken,
        hsProxy.getConnectAddress());
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException, InterruptedException {
    // The token is only used for serialization. So the type information
    // mismatch should be fine.
    return resMgrDelegate.getDelegationToken(renewer);
  }

  @Override
  public String getFilesystemName() throws IOException, InterruptedException {
    return resMgrDelegate.getFilesystemName();
  }

  @Override
  public JobID getNewJobID() throws IOException, InterruptedException {
    return resMgrDelegate.getNewJobID();
  }

  @Override
  public QueueInfo getQueue(String queueName) throws IOException,
      InterruptedException {
    return resMgrDelegate.getQueue(queueName);
  }

  @Override
  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException,
      InterruptedException {
    return resMgrDelegate.getQueueAclsForCurrentUser();
  }

  @Override
  public QueueInfo[] getQueues() throws IOException, InterruptedException {
    return resMgrDelegate.getQueues();
  }

  @Override
  public QueueInfo[] getRootQueues() throws IOException, InterruptedException {
    return resMgrDelegate.getRootQueues();
  }

  @Override
  public QueueInfo[] getChildQueues(String parent) throws IOException,
      InterruptedException {
    return resMgrDelegate.getChildQueues(parent);
  }

  @Override
  public String getStagingAreaDir() throws IOException, InterruptedException {
    return resMgrDelegate.getStagingAreaDir();
  }

  @Override
  public String getSystemDir() throws IOException, InterruptedException {
    return resMgrDelegate.getSystemDir();
  }

  @Override
  public long getTaskTrackerExpiryInterval() throws IOException,
      InterruptedException {
    return resMgrDelegate.getTaskTrackerExpiryInterval();
  }

  @Override
  /**
   * 向YARN ResourceManager提交MapReduce作业，完成提交流程并返回作业状态
   * @param jobId 作业ID
   * @param jobSubmitDir 作业提交目录
   * @param ts 作业安全凭证
   * @return 提交后的作业状态
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public JobStatus submitJob(JobID jobId, String jobSubmitDir, Credentials ts)
  throws IOException, InterruptedException {
    
    addHistoryToken(ts);

    ApplicationSubmissionContext appContext =
      createApplicationSubmissionContext(conf, jobSubmitDir, ts);

    // Submit to ResourceManager
    try {
      ApplicationId applicationId =
          resMgrDelegate.submitApplication(appContext);

      ApplicationReport appMaster = resMgrDelegate
          .getApplicationReport(applicationId);
      String diagnostics =
          (appMaster == null ?
              "application report is null" : appMaster.getDiagnostics());
      if (appMaster == null
          || appMaster.getYarnApplicationState() == YarnApplicationState.FAILED
          || appMaster.getYarnApplicationState() == YarnApplicationState.KILLED) {
        throw new IOException("Failed to run job : " +
            diagnostics);
      }
      return clientCache.getClient(jobId).getJobStatus(jobId);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  private LocalResource createApplicationResource(FileContext fs, Path p,
      LocalResourceType type) throws IOException {
    return createApplicationResource(fs, p, null, type,
        LocalResourceVisibility.APPLICATION, false);
  }

  /**
   * 创建YARN本地资源对象，用于容器本地化时下载资源
   * @param fs 文件上下文
   * @param p 资源路径
   * @param fileSymlink 符号链接名称
   * @param type 资源类型（文件/归档/模式）
   * @param viz 资源可见性
   * @param uploadToSharedCache 是否上传到共享缓存
   * @return YARN LocalResource对象
   * @throws IOException IO异常
   */
  private LocalResource createApplicationResource(FileContext fs, Path p,
      String fileSymlink, LocalResourceType type, LocalResourceVisibility viz,
      Boolean uploadToSharedCache) throws IOException {
    LocalResource rsrc = recordFactory.newRecordInstance(LocalResource.class);
    FileStatus rsrcStat = fs.getFileStatus(p);
    // We need to be careful when converting from path to URL to add a fragment
    // so that the symlink name when localized will be correct.
    Path qualifiedPath =
        fs.getDefaultFileSystem().resolvePath(rsrcStat.getPath());
    URI uriWithFragment = null;
    boolean useFragment = fileSymlink != null && !fileSymlink.equals("");
    try {
      if (useFragment) {
        uriWithFragment = new URI(qualifiedPath.toUri() + "#" + fileSymlink);
      } else {
        uriWithFragment = qualifiedPath.toUri();
      }
    } catch (URISyntaxException e) {
      throw new IOException(
          "Error parsing local resource path."
              + " Path was not able to be converted to a URI: " + qualifiedPath,
          e);
    }
    rsrc.setResource(URL.fromURI(uriWithFragment));
    rsrc.setSize(rsrcStat.getLen());
    rsrc.setTimestamp(rsrcStat.getModificationTime());
    rsrc.setType(type);
    rsrc.setVisibility(viz);
    rsrc.setShouldBeUploadedToSharedCache(uploadToSharedCache);
    return rsrc;
  }

  /**
   * 准备AM容器需要的所有本地资源，包括作业配置、作业Jar、输入分片信息
   * @param jobConf 作业配置
   * @param jobSubmitDir 作业提交目录
   * @return 本地资源映射表（资源名->LocalResource对象）
   * @throws IOException IO异常
   */
  private Map<String, LocalResource> setupLocalResources(Configuration jobConf,
      String jobSubmitDir) throws IOException {
    Map<String, LocalResource> localResources = new HashMap<>();

    Path jobConfPath = new Path(jobSubmitDir, MRJobConfig.JOB_CONF_FILE);

    URL yarnUrlForJobSubmitDir = URL.fromPath(defaultFileContext
        .getDefaultFileSystem().resolvePath(
            defaultFileContext.makeQualified(new Path(jobSubmitDir))));
    LOG.debug("Creating setup context, jobSubmitDir url is "
        + yarnUrlForJobSubmitDir);

    localResources.put(MRJobConfig.JOB_CONF_FILE,
        createApplicationResource(defaultFileContext,
            jobConfPath, LocalResourceType.FILE));
    if (