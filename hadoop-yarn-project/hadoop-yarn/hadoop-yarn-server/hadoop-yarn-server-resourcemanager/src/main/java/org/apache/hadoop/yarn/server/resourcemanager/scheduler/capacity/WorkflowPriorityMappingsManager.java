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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 容量调度器工作流优先级映射管理器，负责管理队列与工作流ID到优先级的映射配置，
 * 并在应用提交时根据配置重写应用优先级，实现基于工作流的优先级调度。
 */
@Private
@VisibleForTesting
public class WorkflowPriorityMappingsManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(WorkflowPriorityMappingsManager.class);

  // 工作流映射配置各部分分隔符
  private static final String WORKFLOW_PART_SEPARATOR = ":";

  // 多个工作流映射之间的分隔符
  private static final String WORKFLOW_SEPARATOR = ",";

  private CapacityScheduler scheduler;

  private CapacitySchedulerConfiguration conf;

  // 是否启用优先级映射覆盖，启用后会重写应用提交的优先级
  private boolean overrideWithPriorityMappings = false;
  // 存储映射关系：队列路径 -> (工作流ID -> 优先级)
  // Map of queue to a map of workflow ID to priority
  private Map<String, Map<String, Priority>> priorityMappings =
      new HashMap<>();

  /**
   * 工作流优先级映射实体类，存储单个工作流ID、队列、优先级的关联关系。
   */
  public static class WorkflowPriorityMapping {
    String workflowID;
    String queue;
    Priority priority;

    public WorkflowPriorityMapping(String workflowID, String queue,
        Priority priority) {
      this.workflowID = workflowID;
      this.queue = queue;
      this.priority = priority;
    }

    public Priority getPriority() {
      return this.priority;
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof WorkflowPriorityMapping) {
        WorkflowPriorityMapping other = (WorkflowPriorityMapping) obj;
        return (other.workflowID.equals(workflowID) &&
            other.queue.equals(queue) &&
            other.priority.equals(priority));
      } else {
        return false;
      }
    }

    public String toString() {
      return workflowID + WORKFLOW_PART_SEPARATOR + queue
          + WORKFLOW_PART_SEPARATOR + priority.getPriority();
    }
  }

  /**
   * 初始化工作流优先级映射管理器，从调度器配置加载映射规则。
   * @param scheduler 容量调度器实例
   * @throws IOException 初始化过程中IO异常
   */
  @VisibleForTesting
  public void initialize(CapacityScheduler scheduler) throws IOException {
    this.scheduler = scheduler;
    this.conf = scheduler.getConfiguration();
    // 从配置读取是否覆盖优先级标志
    boolean overrideWithWorkflowPriorityMappings =
        conf.getOverrideWithWorkflowPriorityMappings();
    LOG.info("Initialized workflow priority mappings, override: "
        + overrideWithWorkflowPriorityMappings);
    this.overrideWithPriorityMappings = overrideWithWorkflowPriorityMappings;
    // 加载所有工作流优先级映射规则
    this.priorityMappings = getWorkflowPriorityMappings();
  }

  /**
   * 从配置解析并获取所有工作流优先级映射关系。
   *
   * @return 完整映射关系：队列路径 -> (工作流ID -> 优先级)
   */
  public Map<String, Map<String, Priority>>
      getWorkflowPriorityMappings() {
    Map<String, Map<String, Priority>> mappings = new HashMap<>();

    // 获取配置中所有工作流优先级映射字符串
    Collection<String> workflowMappings = conf.getWorkflowPriorityMappings();
    for (String workflowMapping : workflowMappings) {
      // 解析单条映射规则
      WorkflowPriorityMapping mapping =
          getWorkflowMappingFromString(workflowMapping);
      if (mapping != null) {
        // 队列不存在则初始化队列映射表
        if (!mappings.containsKey(mapping.queue)) {
          mappings.put(mapping.queue,
              new HashMap<String, Priority>());
        }
        // 将工作流ID与优先级存入对应队列的映射表
        mappings.get(mapping.queue).put(mapping.workflowID, mapping.priority);
      }
    }
    return mappings;
  }

  /**
   * 从配置字符串解析单个工作流优先级映射规则。
   * @param mappingString 配置字符串，格式为：工作流ID:队列:优先级
   * @return 解析后的映射实体，输入为null时返回null
   * @throws IllegalArgumentException 格式或优先级数值非法时抛出
   */
  private WorkflowPriorityMapping getWorkflowMappingFromString(
      String mappingString) {
    if (mappingString == null) {
      return null;
    }
    // 按分隔符切割并裁剪每个部分的空白字符
    String[] mappingArray = StringUtils
        .getTrimmedStringCollection(mappingString, WORKFLOW_PART_SEPARATOR)
            .toArray(new String[] {});
    // 校验格式：必须有且仅有三个非空部分
    if (mappingArray.length != 3 || mappingArray[0].length() == 0
        || mappingArray[1].length() == 0 || mappingArray[2].length() == 0) {
      throw new IllegalArgumentException(
          "Illegal workflow priority mapping " + mappingString);
    }
    WorkflowPriorityMapping mapping;
    try {
      // 将工作流ID转为小写，和YARN应用标签处理逻辑保持一致
      //Converting workflow id to lowercase as yarn converts application tags also to lowercase
      mapping = new WorkflowPriorityMapping(StringUtils.toLowerCase(mappingArray[0]),
          mappingArray[1], Priority.newInstance(Integer.parseInt(mappingArray[2])));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Illegal workflow priority for mapping " + mappingString);
    }
    return mapping;
  }

  /**
   * 根据工作流ID和队列获取映射后的优先级，沿队列层级向上查找直到根队列，返回第一个匹配结果。
   * @param workflowID 工作流ID
   * @param queue 当前队列
   * @return 匹配到的优先级，未找到返回null
   */
  public Priority getMappedPriority(String workflowID, CSQueue queue) {
    // 递归沿队列层级向上查找，直到找到第一个匹配的映射
    // Recursively fetch the priority mapping for the given workflow tracing
    // up the queue hierarchy until the first match.
    if (queue.equals(scheduler.getRootQueue())) {
      return null;
    }
    String queuePath = queue.getQueuePath();
    // 当前队列存在映射规则且包含该工作流ID，直接返回优先级
    if (priorityMappings.containsKey(queuePath)
        && priorityMappings.get(queuePath).containsKey(workflowID)) {
      return priorityMappings.get(queuePath).get(workflowID);
    } else {
      // 向上查找父队列
      queue = queue.getParent();
      return getMappedPriority(workflowID, queue);
    }
  }

  /**
   * 对应用执行工作流优先级映射，若启用覆盖且匹配到规则，则修改应用优先级。
   * @param applicationId 应用ID
   * @param queue 应用所属队列
   * @param user 提交应用的用户
   * @param priority 应用原始优先级
   * @return 映射后的最终优先级
   * @throws YarnException 优先级校验失败时抛出
   */
  public Priority mapWorkflowPriorityForApp(ApplicationId applicationId,
      CSQueue queue, String user, Priority priority) throws YarnException {
    if (overrideWithPriorityMappings) {
      // 从RM上下文获取应用实例
      RMApp rmApp = scheduler.getRMContext().getRMApps().get(applicationId);
      if (rmApp != null && rmApp.getApplicationTags() != null
          && rmApp.getApplicationSubmissionContext() != null) {
        // 从配置读取工作流ID标签前缀
        String workflowTagPrefix = scheduler.getConf().get(
            YarnConfiguration.YARN_WORKFLOW_ID_TAG_PREFIX,
            YarnConfiguration.DEFAULT_YARN_WORKFLOW_ID_TAG_PREFIX);
        String workflowID = null;
        // 遍历应用标签，提取工作流ID
        for(String tag : rmApp.getApplicationTags()) {
          if (tag.trim().startsWith(workflowTagPrefix)) {
            workflowID = tag.trim().substring(workflowTagPrefix.length());
          }
        }
        // 成功提取到工作流ID且存在映射规则
        if (workflowID != null && !workflowID.isEmpty()
            && priorityMappings != null && priorityMappings.size() > 0) {
          // 查找映射后的优先级
          Priority mappedPriority = getMappedPriority(workflowID, queue);
          if (mappedPriority != null) {
            LOG.info("Application " + applicationId + " user " + user
                + " workflow " + workflowID + " queue " + queue.getQueuePath()
                + " mapping [" + priority + "] to [" + mappedPriority
                + "] override " + overrideWithPriorityMappings);

            // 如果匹配到映射规则，使用映射后的优先级覆盖原有优先级
            // If workflow ID exists in workflow mapping, change this
            // application's priority to mapped value. Else, use queue
            // default priority.
            priority = mappedPriority;
            // 校验优先级合法性，获取最终生效的优先级
            priority = scheduler.checkAndGetApplicationPriority(
                priority, UserGroupInformation.createRemoteUser(user),
                queue.getQueuePath(), applicationId);
            // 更新应用提交上下文和应用实例中的优先级
            rmApp.getApplicationSubmissionContext().setPriority(priority);
            ((RMAppImpl)rmApp).setApplicationPriority(priority);
          }
        }
      }
    }
    return priority;
  }

  /**
   * 将工作流优先级映射列表转换为配置字符串。
   * @param workflowPriorityMappings 映射列表
   * @return 拼接后的配置字符串
   */
  public static String getWorkflowPriorityMappingStr(
      List<WorkflowPriorityMapping> workflowPriorityMappings) {
    if (workflowPriorityMappings == null) {
      return "";
    }
    List<String> workflowPriorityMappingStrs = new ArrayList<>();
    for (WorkflowPriorityMapping mapping : workflowPriorityMappings) {
      workflowPriorityMappingStrs.add(mapping.toString());
    }
    return StringUtils.join(WORKFLOW_SEPARATOR, workflowPriorityMappingStrs);
  }
}