// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

/**
 * 容量调度器队列配置更新过程中产生的警告信息实体，用于记录更新阶段发现的配置异常问题
 */
public class QueueUpdateWarning {
  private final String queue;
  private final QueueUpdateWarningType warningType;
  private String info = "";

  /**
   * 构造队列更新警告实例
   * @param queueUpdateWarningType 警告类型
   * @param queue 产生警告的队列路径
   */
  public QueueUpdateWarning(QueueUpdateWarningType queueUpdateWarningType, String queue) {
    this.warningType = queueUpdateWarningType;
    this.queue = queue;
  }

  /**
   * 队列更新警告类型枚举，定义所有可能的配置警告场景
   */
  public enum QueueUpdateWarningType {
    /** 父队列下仍有未分配的剩余资源 */
    BRANCH_UNDERUTILIZED("Remaining resource found in branch under parent queue '%s'. %s"),
    /** 队列配置的资源总量超出父队列可用资源 */
    QUEUE_OVERUTILIZED("Queue '%s' is configured to use more resources than what is available " +
        "under its parent. %s"),
    /** 队列被分配了0资源，无法调度任何应用 */
    QUEUE_ZERO_RESOURCE("Queue '%s' is assigned zero resource. %s"),
    /** 集群资源不足导致子队列绝对容量被按比例缩小 */
    BRANCH_DOWNSCALED("Child queues with absolute configured capacity under parent queue '%s' are" +
        " downscaled due to insufficient cluster resource. %s"),
    /** 队列已用资源超出其最大可用资源限制 */
    QUEUE_EXCEEDS_MAX_RESOURCE("Queue '%s' exceeds its maximum available resources. %s"),
    /** 队列配置的最大资源大于父队列的最大资源 */
    QUEUE_MAX_RESOURCE_EXCEEDS_PARENT("Maximum resources of queue '%s' are greater than its " +
        "parent's. %s");

    private final String template;

    QueueUpdateWarningType(String template) {
      this.template = template;
    }

    /**
     * 创建指定队列的警告实例
     * @param queue 目标队列路径
     * @return 构建完成的警告实例
     */
    public QueueUpdateWarning ofQueue(String queue) {
      return new QueueUpdateWarning(this, queue);
    }

    /**
     * 获取警告信息格式化模板
     * @return 警告信息模板字符串
     */
    public String getTemplate() {
      return template;
    }
  }

  /**
   * 为警告添加额外补充信息
   * @param info 补充说明信息
   * @return 当前警告实例（支持链式调用）
   */
  public QueueUpdateWarning withInfo(String info) {
    this.info = info;

    return this;
  }

  public String getQueue() {
    return queue;
  }

  public QueueUpdateWarningType getWarningType() {
    return warningType;
  }

  @Override
  public String toString() {
    // 按照模板格式化生成完整警告文本
    return String.format(warningType.getTemplate(), queue, info);
  }
}