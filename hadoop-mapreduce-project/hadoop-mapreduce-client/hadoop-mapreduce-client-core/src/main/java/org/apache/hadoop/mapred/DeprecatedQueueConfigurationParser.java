// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.QueueState;
import org.apache.hadoop.security.authorize.AccessControlList;
import static org.apache.hadoop.mapred.QueueManager.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

/**
 * 文件级注释：解析已废弃的mapred-site.xml格式调度队列配置，生成单层队列层次结构
 * 用于兼容旧版本Hadoop MapReduce的队列配置方式，是向后兼容性组件
 */

/**
 * 基于已废弃配置格式构建调度队列层次结构的解析器，仅生成单层队列结构
 * 用于兼容旧版本Hadoop中在mapred-site.xml中配置队列的方式
 */
class DeprecatedQueueConfigurationParser extends QueueConfigurationParser {
  private static final Logger LOG =
      LoggerFactory.getLogger(DeprecatedQueueConfigurationParser.class);
  // 已废弃配置中，队列名称列表的配置键
  static final String MAPRED_QUEUE_NAMES_KEY = "mapred.queue.names";

  /**
   * 构造函数，从已废弃配置中解析并构建队列层次结构
   * @param conf 配置对象
   */
  DeprecatedQueueConfigurationParser(Configuration conf) {
    //如果不存在已废弃配置，直接返回
    if(!deprecatedConf(conf)) {
      return;
    }
    // 创建所有队列对象
    List<Queue> listq = createQueues(conf);
    // 设置ACL开关状态
    this.setAclsEnabled(conf.getBoolean(MRConfig.MR_ACLS_ENABLED, false));
    // 创建根队列
    root = new Queue();
    root.setName("");
    // 将所有解析出的队列作为根队列的子节点，构建单层结构
    for (Queue q : listq) {
      root.addChild(q);
    }
  }

  /**
   * 从配置中解析并创建所有队列对象
   * @param conf 配置对象
   * @return 创建完成的队列列表
   */
  private List<Queue> createQueues(Configuration conf) {
    // 从配置中获取队列名称数组
    String[] queueNameValues = conf.getStrings(
      MAPRED_QUEUE_NAMES_KEY);
    List<Queue> list = new ArrayList<Queue>();
    // 遍历每个队列名称，创建队列对象
    for (String name : queueNameValues) {
      try {
        // 解析队列ACL权限
        Map<String, AccessControlList> acls = getQueueAcls(
          name, conf);
        // 解析队列状态
        QueueState state = getQueueState(name, conf);
        // 创建队列对象并加入列表
        Queue q = new Queue(name, acls, state);
        list.add(q);
      } catch (Throwable t) {
        // 初始化失败记录警告日志
        LOG.warn("Not able to initialize queue " + name);
      }
    }
    return list;
  }

  /**
   * 从配置中解析指定队列的运行状态，仅适用于叶子队列
   * @param name 队列名称
   * @param conf 配置对象
   * @return 队列运行状态
   */
  private QueueState getQueueState(String name, Configuration conf) {
    String stateVal = conf.get(
        toFullPropertyName(name, "state"),
        QueueState.RUNNING.getStateName());
    return QueueState.getState(stateVal);
  }

  /**
   * 检查配置中是否存在已废弃的队列配置，如果存在则打印 deprecation 警告
   * @param conf 配置对象
   * @return true表示存在已废弃配置，false表示不存在
   */
  private boolean deprecatedConf(Configuration conf) {
    String[] queues = null;
    // 获取配置中的队列名称
    String queueNameValues = getQueueNames(conf);
    if (queueNameValues == null) {
      // 没有队列名称配置，不存在已废弃配置
      return false;
    } else {
      // 打印已废弃配置警告，提示用户迁移到新配置文件
      LOG.warn(
          "Configuring \"" + MAPRED_QUEUE_NAMES_KEY
          + "\" in mapred-site.xml or "
          + "hadoop-site.xml is deprecated and will overshadow "
          + QUEUE_CONF_FILE_NAME + ". Remove this property and configure "
          + "queue hierarchy in " + QUEUE_CONF_FILE_NAME);
      // 保存队列名称列表，后续检查ACL配置
      queues = conf.getStrings(MAPRED_QUEUE_NAMES_KEY);
    }

    // 检查是否存在ACL配置在已废弃位置
    if (queues != null) {
      for (String queue : queues) {
        // 遍历所有ACL类型
        for (QueueACL qAcl : QueueACL.values()) {
          String key = toFullPropertyName(queue, qAcl.getAclName());
          String aclString = conf.get(key);
          if (aclString != null) {
            // 存在ACL配置，打印警告
            LOG.warn(
              "Configuring queue ACLs in mapred-site.xml or " +
                "hadoop-site.xml is deprecated. Configure queue ACLs in " +
                QUEUE_CONF_FILE_NAME);
            // 只要存在一个配置就可以返回，无需继续检查
            return true;
          }
        }
      }
    }
    return true;
  }

  /**
   * 从配置中获取队列名称列表字符串
   * @param conf 配置对象
   * @return 队列名称列表配置值
   */
  private String getQueueNames(Configuration conf) {
    String queueNameValues = conf.get(MAPRED_QUEUE_NAMES_KEY);
    return queueNameValues;
  }

  /**
   * 从配置中解析指定队列的所有ACL权限配置
   * @param name 队列名称
   * @param conf 配置对象
   * @return ACL权限映射表，键为ACL属性名，值为对应的ACL对象
   */
  private Map<String, AccessControlList> getQueueAcls(
    String name,
    Configuration conf) {
    HashMap<String, AccessControlList> map =
      new HashMap<String, AccessControlList>();
    // 遍历所有ACL类型
    for (QueueACL qAcl : QueueACL.values()) {
      // 拼接完整配置键
      String aclKey = toFullPropertyName(name, qAcl.getAclName());
      // 创建ACL对象，默认允许所有用户访问
      map.put(
        aclKey, new AccessControlList(
          conf.get(
            aclKey, "*")));
    }
    return map;
  }
}