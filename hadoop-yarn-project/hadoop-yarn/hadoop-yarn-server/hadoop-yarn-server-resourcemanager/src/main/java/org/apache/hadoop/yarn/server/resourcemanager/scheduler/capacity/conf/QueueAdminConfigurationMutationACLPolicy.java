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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ConfigurationMutationACLPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
 org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 文件所属模块: YARN 资源调度器容量调度器
 * 核心职责: 实现队列配置修改访问控制策略，验证用户对所有待修改队列是否具备管理员权限
 */
public class QueueAdminConfigurationMutationACLPolicy implements
    ConfigurationMutationACLPolicy {

  private Configuration conf;
  private RMContext rmContext;
  private YarnAuthorizationProvider authorizer;

  @Override
  public void init(Configuration config, RMContext context) {
    this.conf = config;
    this.rmContext = context;
    // 初始化YARN权限验证器实例
    this.authorizer = YarnAuthorizationProvider.getInstance(conf);
  }

  @Override
  public boolean isMutationAllowed(UserGroupInformation user,
      SchedConfUpdateInfo confUpdate) {
    // 如果包含全局配置修改，先检查用户是否是集群管理员
    Map<String, String> globalParams = confUpdate.getGlobalParams();
    if (globalParams != null && globalParams.size() != 0) {
      if (!authorizer.isAdmin(user)) {
        return false;
      }
    }

    // 收集所有待修改的队列（新增、删除、更新）
    Set<String> queues = new HashSet<>();
    for (QueueConfigInfo addQueueInfo : confUpdate.getAddQueueInfo()) {
      queues.add(addQueueInfo.getQueue());
    }
    for (String removeQueue : confUpdate.getRemoveQueueInfo()) {
      queues.add(removeQueue);
    }
    for (QueueConfigInfo updateQueueInfo : confUpdate.getUpdateQueueInfo()) {
      queues.add(updateQueueInfo.getQueue());
    }

    // 遍历检查所有待修改队列的权限
    for (String queuePath : queues) {
      QueueInfo queueInfo = null;
      String parentPath = queuePath;

      // 从最深层节点开始向上遍历队列层次，直到找到存在的队列
      String queueName;
      while (queueInfo == null) {
        // 确定当前检查的队列名称：如果存在子节点则检查最后一个子节点
        queueName = queueHasAChild(parentPath) ?
            getLastChildForQueue(parentPath) : parentPath;
        try {
          // 从RM调度器获取队列信息
          queueInfo = rmContext.getScheduler()
              .getQueueInfo(queueName, false, false);
        } catch (IOException e) {
          // 队列不存在，继续向上查找
        }

        // 向上移动一层，继续查找存在的队列
        parentPath = queueHasAChild(parentPath) ?
            getQueueBeforeLastChild(parentPath) : parentPath;
      }

      // 获取队列对象，检查用户是否具备该队列的管理权限
      Queue queue = ((MutableConfScheduler) rmContext.getScheduler())
          .getQueue(queueInfo.getQueueName());
      if (queue != null && !queue.hasAccess(QueueACL.ADMINISTER_QUEUE, user)) {
        return false;
      }
    }

    return true;
  }

  /**
   * 检查队列路径是否包含子队列（包含'.'说明存在层级）
   * @param queue 待检查的队列路径
   * @return True如果存在子队列，否则False
   */
  private boolean queueHasAChild(String queue) {
    return queue.lastIndexOf('.') != -1;
  }

  /**
   * 从队列路径中提取最后一级子队列名称
   * @param queue 完整队列路径
   * @return 最后一级子队列名称
   */
  private String getLastChildForQueue(String queue) {
    return queue.substring(queue.lastIndexOf('.') + 1);
  }

  /**
   * 从队列路径中移除最后一级子队列，得到父队列路径
   * @param queue 完整队列路径
   * @return 移除最后一级后的父队列路径
   */
  private String getQueueBeforeLastChild(String queue) {
    return queue.substring(0, queue.lastIndexOf('.'));
  }

}