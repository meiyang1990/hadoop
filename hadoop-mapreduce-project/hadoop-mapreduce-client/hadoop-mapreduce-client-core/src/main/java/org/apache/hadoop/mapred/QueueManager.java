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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.QueueState;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.net.URL;

/**
 * MapReduce队列管理器，负责管理系统中所有作业队列的配置、层级结构和访问权限。
 * <p>
 * 支持单级队列（兼容旧版配置）和层级队列，队列名称使用冒号分隔层级（如q1:q2:q3），
 * 仅允许向叶子队列提交作业，支持访问控制列表(ACL)进行权限管理，支持队列配置热刷新。
 * 队列配置默认从mapred-queues.xml读取，兼容旧版mapred-site.xml的单级队列配置。
 * </p>
 */
@InterfaceAudience.Private
public class QueueManager {

  private static final Logger LOG = LoggerFactory.getLogger(QueueManager.class);

  // 存储所有叶子队列的映射，key为队列全名称，value为Queue对象
  private Map<String, Queue> leafQueues = new HashMap<String,Queue>();
  // 存储所有队列（包括内部非叶子队列和叶子队列）的映射，key为队列全名称，value为Queue对象
  private Map<String, Queue> allQueues = new HashMap<String, Queue>();
  public static final String QUEUE_CONF_FILE_NAME = "mapred-queues.xml";
  static final String QUEUE_CONF_DEFAULT_FILE_NAME = "mapred-queues-default.xml";

  // 配置中队列相关属性的前缀
  static final String QUEUE_CONF_PROPERTY_NAME_PREFIX = "mapred.queue.";

  // 层级队列的根节点
  private Queue root = null;
  
  // 标记MapReduce集群是否开启作业和队列ACL权限检查
  private boolean areAclsEnabled = false;

  /**
   * 根据配置创建对应的队列配置解析器实例。
   * <p>
   * 优先解析mapred-site.xml中的旧版单级队列配置，若不存在则解析mapred-queues.xml中的层级队列配置
   * </p>
   * @param conf 集群配置对象，用于判断使用哪种解析器
   * @param reloadConf 是否需要重新加载配置
   * @param areAclsEnabled 是否开启ACL权限检查
   * @return 队列配置解析器实例
   */
  static QueueConfigurationParser getQueueConfigurationParser(
    Configuration conf, boolean reloadConf, boolean areAclsEnabled) {
    if (conf != null && conf.get(
      DeprecatedQueueConfigurationParser.MAPRED_QUEUE_NAMES_KEY) != null) {
      if (reloadConf) {
        conf.reloadConfiguration();
      }
      return new DeprecatedQueueConfigurationParser(conf);
    } else {
      URL xmlInUrl =
        Thread.currentThread().getContextClassLoader()
          .getResource(QUEUE_CONF_FILE_NAME);
      if (xmlInUrl == null) {
        xmlInUrl = Thread.currentThread().getContextClassLoader()
          .getResource(QUEUE_CONF_DEFAULT_FILE_NAME);
        assert xmlInUrl != null; // this should be in our jar
      }
      InputStream stream = null;
      try {
        stream = xmlInUrl.openStream();
        return new QueueConfigurationParser(new BufferedInputStream(stream),
            areAclsEnabled);
      } catch (IOException ioe) {
        throw new RuntimeException("Couldn't open queue configuration at " +
                                   xmlInUrl, ioe);
      } finally {
        IOUtils.closeStream(stream);
      }
    }
  }

  QueueManager() {// acls are disabled
    this(false);
  }

  QueueManager(boolean areAclsEnabled) {
    this.areAclsEnabled = areAclsEnabled;
    initialize(getQueueConfigurationParser(null, false, areAclsEnabled));
  }

  /**
   * 使用传入的集群配置构造QueueManager实例。
   * 优先读取mapred-site.xml中的单级队列配置，不存在则读取mapred-queues.xml中的层级队列配置
   * @param clusterConf MapReduce集群配置对象
   */
  public QueueManager(Configuration clusterConf) {
    areAclsEnabled = clusterConf.getBoolean(MRConfig.MR_ACLS_ENABLED, false);
    initialize(getQueueConfigurationParser(clusterConf, false, areAclsEnabled));
  }

  /**
   * 使用指定配置文件构造支持层级队列的QueueManager实例，仅用于测试。
   * @param confFile 队列配置文件路径
   * @param areAclsEnabled 是否开启ACL权限检查
   */
  QueueManager(String confFile, boolean areAclsEnabled) {
    this.areAclsEnabled = areAclsEnabled;
    QueueConfigurationParser cp =
        new QueueConfigurationParser(confFile, areAclsEnabled);
    initialize(cp);
  }

  /**
   * 使用解析后的队列层次结构初始化QueueManager，填充内部队列缓存。
   * @param cp 已完成解析的队列配置解析器
   */
  private void initialize(QueueConfigurationParser cp) {
    this.root = cp.getRoot();
    leafQueues.clear();
    allQueues.clear();
    // 获取根节点下所有叶子队列，更新缓存
    leafQueues = getRoot().getLeafQueues();
    // 将所有内部队列和叶子队列加入全量队列缓存
    allQueues.putAll(getRoot().getInnerQueues());
    allQueues.putAll(leafQueues);

    LOG.info("AllQueues : " + allQueues + "; LeafQueues : " + leafQueues);
  }

  /**
   * 获取系统中所有可提交作业的叶子队列名称集合。
   * @return 叶子队列名称集合
   */
  public synchronized Set<String> getLeafQueueNames() {
    return leafQueues.keySet();
  }

  /**
   * 检查指定用户对指定队列的指定操作是否有权限。
   * @param queueName 目标队列名称
   * @param qACL 需要检查的队列操作权限类型
   * @param ugi 操作用户的用户组信息
   * @return 有权限返回true，否则返回false
   */
  public synchronized boolean hasAccess(
    String queueName, QueueACL qACL, UserGroupInformation ugi) {

    Queue q = leafQueues.get(queueName);

    if (q == null) {
      LOG.info("Queue " + queueName + " is not present");
      return false;
    }

    if(q.getChildren() != null && !q.getChildren().isEmpty()) {
      LOG.info("Cannot submit job to parent queue " + q.getName());
      return false;
    }

    if (!areAclsEnabled()) {
      return true;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking access for the acl " + toFullPropertyName(queueName,
        qACL.getAclName()) + " for user " + ugi.getShortUserName());
    }

    AccessControlList acl = q.getAcls().get(
        toFullPropertyName(queueName, qACL.getAclName()));
    if (acl == null) {
      return false;
    }

    // 检查用户是否在ACL允许列表中
    return acl.isUserAllowed(ugi);
  }

  /**
   * 检查指定叶子队列是否处于RUNNING运行状态。
   * @param queueName 队列名称
   * @return 队列存在且处于RUNNING状态返回true，否则返回false
   */
  synchronized boolean isRunning(String queueName) {
    Queue q = leafQueues.get(queueName);
    if (q != null) {
      return q.getState().equals(QueueState.RUNNING);
    }
    return false;
  }

  /**
   * 为指定队列设置调度器相关信息。
   * @param queueName 目标队列名称
   * @param queueInfo 调度器相关的调度信息对象
   */
  public synchronized void setSchedulerInfo(
    String queueName,
    Object queueInfo) {
    if (allQueues.get(queueName) != null) {
      allQueues.get(queueName).setSchedulingInfo(queueInfo);
    }
  }

  /**
   * 获取指定队列的调度器相关信息。
   * @param queueName 目标队列名称
   * @return 调度信息对象，队列不存在则返回null
   */
  public synchronized Object getSchedulerInfo(String queueName) {
    if (allQueues.get(queueName) != null) {
      return allQueues.get(queueName).getSchedulingInfo();
    }
    return null;
  }

  static final String MSG_REFRESH_FAILURE_WITH_CHANGE_OF_HIERARCHY =
      "Unable to refresh queues because queue-hierarchy changed. "
          + "Retaining existing configuration. ";

  static final String MSG_REFRESH_FAILURE_WITH_SCHEDULER_FAILURE =
      "Scheduler couldn't refresh it's queues with the new"
          + " configuration properties. "
          + "Retaining existing configuration throughout the system.";

  /**
   * 刷新队列的ACL、状态和调度属性，不支持修改队列层级结构。
   * 刷新失败时保证原有配置保持不变和一致。
   * @param conf 最新集群配置对象
   * @param schedulerRefresher 调度器刷新回调，用于通知调度器刷新自身配置
   * @throws IOException 队列层级变更或调度器刷新失败时抛出异常
   */
  synchronized void refreshQueues(Configuration conf,
      QueueRefresher schedulerRefresher)
      throws IOException {

    // 使用新配置创建解析器
    QueueConfigurationParser cp =
        getQueueConfigurationParser(conf, true, areAclsEnabled);

    // 检查队列层级结构是否和原来一致，不允许刷新时修改层级
    if (!root.isHierarchySameAs(cp.getRoot())) {
      LOG.warn(MSG_REFRESH_FAILURE_WITH_CHANGE_OF_HIERARCHY);
      throw new IOException(MSG_REFRESH_FAILURE_WITH_CHANGE_OF_HIERARCHY);
    }

    // 调用调度器回调刷新自身配置
    if (schedulerRefresher != null) {
      try {
        schedulerRefresher.refreshQueues(cp.getRoot().getJobQueueInfo().getChildren());
      } catch (Throwable e) {
        StringBuilder msg =
            new StringBuilder(
                "Scheduler's refresh-queues failed with the exception : "
                    + StringUtils.stringifyException(e));
        msg.append("\n");
        msg.append(MSG_REFRESH_FAILURE_WITH_SCHEDULER_FAILURE);
        LOG.error(msg.toString());
        throw new IOException(msg.toString());
      }
    }

    // 复制原有调度信息到新的队列层次结构
    cp.getRoot().copySchedulingInfo(this.root);

    // 切换到新配置，更新内部缓存
    initialize(cp);

    LOG.info("Queue configuration is refreshed successfully.");
  }

  /**
   * 拼接队列属性的完整配置键名。
   * @param queue 队列名称
   * @param property 属性名称
   * @return 完整配置键名
   */
  public static final String toFullPropertyName(
    String queue,
    String property) {
    return QUEUE_CONF_PROPERTY_NAME_PREFIX + queue + "." + property;
  }

  /**
   * 获取系统中所有队列的JobQueueInfo信息数组。
   * @return 所有队列的JobQueueInfo数组
   */
  synchronized JobQueueInfo[] getJobQueueInfos() {
    ArrayList<JobQueueInfo> queueInfoList = new ArrayList<JobQueueInfo>();
    for (String queue : allQueues.keySet()) {
      JobQueueInfo queueInfo = getJobQueueInfo(queue);
      if (queueInfo != null) {
        queueInfoList.add(queueInfo);
      }
    }
    return queueInfoList.toArray(
      new JobQueueInfo[queueInfoList.size()]);
  }

  /**
   * 获取指定队列的JobQueueInfo信息。
   * @param queue 队列名称
   * @return 队列的JobQueueInfo，队列不存在返回null
   */
  synchronized JobQueueInfo getJobQueueInfo(String queue) {
    if (allQueues.containsKey(queue)) {
      return allQueues.get(queue).getJobQueueInfo();
    }

    return null;
  }

  /**
   * 获取所有队列名称到JobQueueInfo的映射，方便遍历和导航。
   * @return 队列名称到JobQueueInfo的映射
   */
  synchronized Map<String, JobQueueInfo> getJobQueueInfoMapping() {
    Map<String, JobQueueInfo> m = new HashMap<String, JobQueueInfo>();

    for (Map.Entry<String,Queue> entry : allQueues.entrySet()) {
      m.put(entry.getKey(), entry.getValue().getJobQueueInfo());
    }

    return m;
  }

  /**
   * 获取当前用户拥有操作权限的所有队列的ACL信息数组。
   * @param ugi 当前用户的用户组信息
   * @return 用户有权限的队列ACL信息数组
   * @throws IOException
   */
  synchronized QueueAclsInfo[] getQueueAcls(UserGroupInformation ugi)
    throws IOException {
    // 存储所有用户有权限的队列ACL信息
    ArrayList<QueueAclsInfo> queueAclsInfolist =
      new ArrayList<QueueAclsInfo>();
    QueueACL[] qAcls = QueueACL.values();
    for (String queueName : leafQueues.keySet()) {
      QueueAclsInfo queueAclsInfo = null;
      ArrayList<String> operationsAllowed = null;
      for (QueueACL qAcl : qAcls) {
        if (hasAccess(queueName, qAcl, ugi)) {
          if (operationsAllowed == null) {
            operationsAllowed = new ArrayList<String>();
          }
          operationsAllowed.add(qAcl.getAclName());
        }
      }
      if (operationsAllowed != null) {
        // 当前用户至少有一个操作权限，添加到结果列表
        queueAclsInfo = new QueueAclsInfo(
          queueName, operationsAllowed.toArray
            (new String[operationsAllowed.size()]));
        queueAclsInfolist.add(queueAclsInfo);
      }
    }
    return queueAclsInfolist.toArray(
      new QueueAclsInfo[queueAclsInfolist.size()]);
  }

  /**
   * 获取当前集群是否开启ACL权限检查。
   * @return 开启返回true，否则返回false
   */
  boolean areAclsEnabled() {
    return areAclsEnabled;
  }

  /**
   * 获取队列层级结构的根节点，仅用于测试。
   * @return 根队列对象
   */
  Queue getRoot() {
    return root;
  }

  /**
   * 将队列层次结构配置导出为JSON格式写入指定输出流。
   * @param out 输出Writer
   * @param conf 集群配置
   * @throws IOException
   */
  static void dumpConfiguration(Writer out,Configuration conf) throws IOException {
    dumpConfiguration(out, null,conf);
  }
  
  /**
   * 将指定配置文件中的队列层次结构导出为JSON格式，仅用于测试。
   * @param out 输出Writer
   * @param configFile 队列配置文件路径
   * @param conf 集群配置
   * @throws IOException
   */
  static void dumpConfiguration(Writer out, String configFile,
      Configuration conf) throws IOException {
    if (conf != null && conf.get(DeprecatedQueueConfigurationParser.
        MAPRED_QUEUE_NAMES_KEY) != null) {
      return;
    }
    
    JsonFactory dumpFactory = new JsonFactory();
    JsonGenerator dumpGenerator = dumpFactory.createGenerator(out);
    QueueConfigurationParser parser;
    boolean aclsEnabled = false;
    if (conf != null) {
      aclsEnabled = conf.getBoolean(MRConfig.MR_ACLS_ENABLED, false);
    }
    if (configFile != null && !"".equals(configFile)) {
      parser = new QueueConfigurationParser(configFile, aclsEnabled);
    }
    else {
      parser = getQueueConfigurationParser(null, false, aclsEnabled);
    }
    dumpGenerator.writeStartObject();
    dumpGenerator.writeFieldName("queues");
    dumpGenerator.writeStartArray();
    dumpConfiguration(dumpGenerator,parser.getRoot().getChildren());
    dumpGenerator.writeEnd