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
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 容量调度器配置更新组装器，将队列增删改操作转换为配置键值对更新集合.
 * 负责处理Web UI提交的调度配置变更请求，生成最终需要持久化的配置更新
 */
public final class ConfigurationUpdateAssembler {

  private ConfigurationUpdateAssembler() {
  }

  /**
   * 根据调度配置变更信息，组装生成最终的配置键值对更新集合.
   * @param proposedConf 拟修改的容量调度器配置对象
   * @param mutationInfo 调度配置变更信息（包含增/删/改队列和全局参数变更）
   * @return 组装完成的配置键值对更新集合，键为配置项名称，值为目标值（null表示删除该配置）
   * @throws IOException 当变更非法（删除根队列、队列不存在等）时抛出异常
   */
  public static Map<String, String> constructKeyValueConfUpdate(
          CapacitySchedulerConfiguration proposedConf,
          SchedConfUpdateInfo mutationInfo) throws IOException {

    Map<String, String> confUpdate = new HashMap<>();
    // 处理所有待删除队列
    for (String queueToRemove : mutationInfo.getRemoveQueueInfo()) {
      removeQueue(queueToRemove, proposedConf, confUpdate);
    }
    // 处理所有待新增队列
    for (QueueConfigInfo addQueueInfo : mutationInfo.getAddQueueInfo()) {
      addQueue(addQueueInfo, proposedConf, confUpdate);
    }
    // 处理所有待更新队列配置
    for (QueueConfigInfo updateQueueInfo : mutationInfo.getUpdateQueueInfo()) {
      updateQueue(updateQueueInfo, proposedConf, confUpdate);
    }
    // 处理全局参数变更
    for (Map.Entry<String, String> global : mutationInfo.getGlobalParams()
            .entrySet()) {
      confUpdate.put(global.getKey(), global.getValue());
    }
    return confUpdate;
  }

  /**
   * 处理删除队列操作，更新拟配置和配置更新集合.
   * @param queueToRemove 待删除队列路径
   * @param proposedConf 拟修改的容量调度器配置
   * @param confUpdate 配置更新集合
   * @throws IOException 当删除非法队列或队列不存在时抛出异常
   */
  private static void removeQueue(
          String queueToRemove, CapacitySchedulerConfiguration proposedConf,
          Map<String, String> confUpdate) throws IOException {
    if (queueToRemove == null) {
      return;
    }
    // 解析队列路径
    QueuePath queuePath = new QueuePath(queueToRemove);
    // 不允许删除根队列或非法路径队列
    if (queuePath.isRoot() || queuePath.isInvalid()) {
      throw new IOException("Can't remove queue " + queuePath.getFullPath());
    }
    String queueName = queuePath.getLeafName();
    // 获取父队列下所有兄弟队列
    List<String> siblingQueues = getSiblingQueues(queuePath,
            proposedConf);
    // 队列不存在则抛出异常
    if (!siblingQueues.contains(queueName)) {
      throw new IOException("Queue " + queuePath.getFullPath() + " not found");
    }
    // 从兄弟队列列表移除待删除队列
    siblingQueues.remove(queueName);

    // 更新拟配置中父队列的子队列列表
    QueuePath parentPath = queuePath.getParentObject();
    proposedConf.setQueues(parentPath, siblingQueues.toArray(
            new String[0]));
    String queuesConfig = getQueuesConfig(parentPath);
    // 如果删除后父队列没有子队列，清除子队列配置，并清除排序策略配置（原父队列转为叶子队列）
    if (siblingQueues.isEmpty()) {
      confUpdate.put(queuesConfig, null);
      // 清除原父队列排序策略配置（删除后转为叶子队列，不需要排序策略）
      String queueOrderingPolicy = getOrderingPolicyConfig(parentPath);
      proposedConf.unset(queueOrderingPolicy);
      confUpdate.put(queueOrderingPolicy, null);
    } else {
      // 更新父队列子队列配置
      confUpdate.put(queuesConfig, Joiner.on(',').join(siblingQueues));
    }
    // 清除当前队列所有相关配置，添加到更新集合
    for (Map.Entry<String, String> confRemove : proposedConf.getValByRegex(
                    ".*" + queuePath.getFullPath() + "\\..*")
            .entrySet()) {
      proposedConf.unset(confRemove.getKey());
      confUpdate.put(confRemove.getKey(), null);
    }
  }

  /**
   * 处理新增队列操作，更新拟配置和配置更新集合.
   * @param addInfo 新增队列配置信息
   * @param proposedConf 拟修改的容量调度器配置
   * @param confUpdate 配置更新集合
   * @throws IOException 当新增非法队列或队列已存在时抛出异常
   */
  private static void addQueue(
          QueueConfigInfo addInfo, CapacitySchedulerConfiguration proposedConf,
          Map<String, String> confUpdate) throws IOException {
    if (addInfo == null) {
      return;
    }
    // 解析队列路径
    QueuePath queuePath = new QueuePath(addInfo.getQueue());
    String queueName = queuePath.getLeafName();
    // 不允许新增根队列或非法路径队列
    if (queuePath.isRoot() || queuePath.isInvalid()) {
      throw new IOException("Can't add invalid queue " + queuePath);
    } else if (getSiblingQueues(queuePath, proposedConf).contains(
            queueName)) {
      // 队列已存在则抛出异常
      throw new IOException("Can't add existing queue " + queuePath);
    }

    // 更新父队列子队列列表
    QueuePath parentPath = queuePath.getParentObject();
    List<String> siblingQueues = proposedConf.getQueues(parentPath);
    siblingQueues.add(queueName);
    proposedConf.setQueues(parentPath,
            siblingQueues.toArray(new String[0]));
    // 更新父队列子队列配置
    confUpdate.put(getQueuesConfig(parentPath),
            Joiner.on(',').join(siblingQueues));
    // 生成当前队列配置前缀
    String keyPrefix = QueuePrefixes.getQueuePrefix(queuePath);
    // 处理新增队列的所有自定义参数
    for (Map.Entry<String, String> kv : addInfo.getParams().entrySet()) {
      String keyValue = kv.getValue();
      if (keyValue == null || keyValue.isEmpty()) {
        // 空值表示清除该配置
        proposedConf.unset(keyPrefix + kv.getKey());
        confUpdate.put(keyPrefix + kv.getKey(), null);
      } else {
        // 设置配置值
        proposedConf.set(keyPrefix + kv.getKey(), keyValue);
        confUpdate.put(keyPrefix + kv.getKey(), keyValue);
      }
    }
    // 如果新增后父队列第一个子队列，清除父队列排序策略配置（原叶子队列转为父队列不需要，但这里正好相反：原父队列本来是叶子，新增第一个子节点后成为父队列？不，注释说新增后如果是第一个子节点，父队列原来为叶子，不需要排序策略？不对，原注释写的是：新增队列后，如果父队列从叶子队列转为父队列？不，原注释明确：新增队列后，如果父队列原来为叶子队列，现在转为父队列，原来的排序策略需要清除？不对，原注释写：新增队列后，如果兄弟队列数为1（即原来父队列没有子队列，是叶子队列），需要清除父队列原来的排序策略配置
    String queueOrderingPolicy = getOrderingPolicyConfig(parentPath);
    if (siblingQueues.size() == 1) {
      proposedConf.unset(queueOrderingPolicy);
      confUpdate.put(queueOrderingPolicy, null);
    }
  }

  /**
   * 处理更新已有队列配置操作，更新拟配置和配置更新集合.
   * @param updateInfo 更新队列配置信息
   * @param proposedConf 拟修改的容量调度器配置
   * @param confUpdate 配置更新集合
   */
  private static void updateQueue(QueueConfigInfo updateInfo,
                                  CapacitySchedulerConfiguration proposedConf,
                                  Map<String, String> confUpdate) {
    if (updateInfo == null) {
      return;
    }
    // 解析队列路径，生成配置前缀
    QueuePath queuePath = new QueuePath(updateInfo.getQueue());
    String keyPrefix = QueuePrefixes.getQueuePrefix(queuePath);
    // 遍历更新所有参数
    for (Map.Entry<String, String> kv : updateInfo.getParams().entrySet()) {
      String keyValue = kv.getValue();
      if (keyValue == null || keyValue.isEmpty()) {
        // 空值清除配置
        proposedConf.unset(keyPrefix + kv.getKey());
        confUpdate.put(keyPrefix + kv.getKey(), null);
      } else {
        // 设置配置值
        proposedConf.set(keyPrefix + kv.getKey(), keyValue);
        confUpdate.put(keyPrefix + kv.getKey(), keyValue);
      }
    }
  }

  /**
   * 获取指定队列父队列下所有兄弟队列列表.
   * @param queuePath 当前队列路径
   * @param conf 配置对象
   * @return 兄弟队列名称列表
   */
  private static List<String> getSiblingQueues(QueuePath queuePath, Configuration conf) {
    String childQueuesKey = getQueuesConfig(queuePath.getParentObject());
    return new ArrayList<>(conf.getTrimmedStringCollection(childQueuesKey));
  }

  /**
   * 生成指定队列的子队列配置项键名.
   * @param queuePath 队列路径
   * @return 配置项键名
   */
  private static String getQueuesConfig(QueuePath queuePath) {
    return QueuePrefixes.getQueuePrefix(queuePath) + CapacitySchedulerConfiguration.QUEUES;
  }

  /**
   * 生成指定队列的排序策略配置项键名.
   * @param queuePath 队列路径
   * @return 配置项键名
   */
  private static String getOrderingPolicyConfig(QueuePath queuePath) {
    return QueuePrefixes.getQueuePrefix(queuePath) + CapacitySchedulerConfiguration.ORDERING_POLICY;
  }
}