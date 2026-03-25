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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.QueueState;

/**
 * 封装动态调整队列容量所需的队列权限和状态变更信息
 *
 */
@Private
@Unstable
public abstract class QueueManagementChange {

  private final CSQueue queue;

  /**
   * 队列管理操作类型，当前仅支持更新队列，未来可扩展添加/删除队列功能
   */
  public enum QueueAction {
    /** 更新队列配置 */
    UPDATE_QUEUE
  }

  /** 自动创建叶子队列的模板配置更新 */
  private AutoCreatedLeafQueueConfig
      queueTemplateUpdate;

  private final QueueAction queueAction;
  /** 变更后队列需要切换到的目标状态 */
  private QueueState transitionToQueueState;

  /**
   * 构造队列管理变更对象
   * @param queue 目标队列
   * @param queueAction 要执行的操作
   */
  public QueueManagementChange(final CSQueue queue,
      final QueueAction queueAction) {
    this.queue = queue;
    this.queueAction = queueAction;
  }

  /**
   * 构造包含状态变更和模板更新的队列管理变更对象
   * @param queue 目标队列
   * @param queueAction 要执行的操作
   * @param targetQueueState 目标队列状态
   * @param queueTemplateUpdates 队列模板更新配置
   */
  public QueueManagementChange(final CSQueue queue,
      final QueueAction queueAction, QueueState targetQueueState,
      final AutoCreatedLeafQueueConfig
          queueTemplateUpdates) {
    this(queue, queueAction, queueTemplateUpdates);
    this.transitionToQueueState = targetQueueState;
  }

  /**
   * 构造包含模板更新的队列管理变更对象
   * @param queue 目标队列
   * @param queueAction 要执行的操作
   * @param queueTemplateUpdates 队列模板更新配置
   */
  public QueueManagementChange(final CSQueue queue,
      final QueueAction queueAction,
      final AutoCreatedLeafQueueConfig
      queueTemplateUpdates) {
    this(queue, queueAction);
    this.queueTemplateUpdate = queueTemplateUpdates;
  }

  /**
   * 获取队列需要切换到的目标状态
   * @return 目标队列状态
   */
  public QueueState getTransitionToQueueState() {
    return transitionToQueueState;
  }

  /**
   * 获取需要变更的目标队列
   * @return 目标队列对象
   */
  public CSQueue getQueue() {
    return queue;
  }

  /**
   * 获取更新后的队列模板配置
   * @return 更新后的队列模板
   */
  public AutoCreatedLeafQueueConfig getUpdatedQueueTemplate() {
    return queueTemplateUpdate;
  }

  /**
   * 获取本次要执行的队列操作类型
   * @return 操作类型枚举
   */
  public QueueAction getQueueAction() {
    return queueAction;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof QueueManagementChange))
      return false;

    QueueManagementChange that = (QueueManagementChange) o;

    if (queue != null ? !queue.equals(that.queue) : that.queue != null)
      return false;
    if (queueTemplateUpdate != null ? !queueTemplateUpdate.equals(
        that.queueTemplateUpdate) : that.queueTemplateUpdate != null)
      return false;
    if (queueAction != that.queueAction)
      return false;
    return transitionToQueueState == that.transitionToQueueState;
  }

  @Override
  public int hashCode() {
    int result = queue != null ? queue.hashCode() : 0;
    result = 31 * result + (queueTemplateUpdate != null ?
        queueTemplateUpdate.hashCode() :
        0);
    result = 31 * result + (queueAction != null ? queueAction.hashCode() : 0);
    result = 31 * result + (transitionToQueueState != null ?
        transitionToQueueState.hashCode() :
        0);
    return result;
  }

  @Override
  public String toString() {
    return "QueueManagementChange{" + "queue=" + queue.getQueuePath()
        + ", updatedEntitlementsByPartition=" + queueTemplateUpdate
        + ", queueAction=" + queueAction + ", transitionToQueueState="
        + transitionToQueueState + '}';
  }

  /**
   * 更新队列操作的具体实现类
   */
  public static class UpdateQueue extends QueueManagementChange {

    /**
     * 构造包含状态变更和模板更新的更新队列操作
     * @param queue 目标队列
     * @param targetQueueState 目标队列状态
     * @param queueTemplateUpdate 队列模板更新配置
     */
    public UpdateQueue(final CSQueue queue, QueueState targetQueueState,
        final AutoCreatedLeafQueueConfig
            queueTemplateUpdate) {
      super(queue, QueueAction.UPDATE_QUEUE, targetQueueState,
          queueTemplateUpdate);
    }

    /**
     * 构造仅包含模板更新的更新队列操作
     * @param queue 目标队列
     * @param queueTemplateUpdate 队列模板更新配置
     */
    public UpdateQueue(final CSQueue queue,
        final AutoCreatedLeafQueueConfig
            queueTemplateUpdate) {
      super(queue, QueueAction.UPDATE_QUEUE, queueTemplateUpdate);
    }
  }
}