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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;

/**
 * YARN 队列放置规则映射类，存储应用程序到队列的映射关系。
 * 用于容量调度器的队列放置功能，根据用户/用户组/应用名匹配将应用放置到指定队列。
 */
@Private
public class QueueMapping {

  /**
   * QueueMapping 的 Builder 构造类，支持链式创建 QueueMapping 对象。
   */
  public static class QueueMappingBuilder {

    private MappingType type;
    private String source;
    private String queue;
    private String parentQueue;

    public QueueMappingBuilder() {
    }

    /**
     * 创建新的 Builder 实例。
     * @return Builder 实例
     */
    public static QueueMappingBuilder create() {
      return new QueueMappingBuilder();
    }

    /**
     * 设置映射类型。
     * @param mappingType 映射类型
     * @return 当前 Builder 实例
     */
    public QueueMappingBuilder type(MappingType mappingType) {
      this.type = mappingType;
      return this;
    }

    /**
     * 设置匹配源（对应用户名/用户组名/应用名）。
     * @param mappingSource 匹配源名称
     * @return 当前 Builder 实例
     */
    public QueueMappingBuilder source(String mappingSource) {
      this.source = mappingSource;
      return this;
    }

    /**
     * 设置目标叶子队列名称。
     * @param mappingQueue 叶子队列名称
     * @return 当前 Builder 实例
     */
    public QueueMappingBuilder queue(String mappingQueue) {
      this.queue = mappingQueue;
      return this;
    }

    /**
     * 设置目标父队列名称。
     * @param mappingParentQueue 父队列名称
     * @return 当前 Builder 实例
     */
    public QueueMappingBuilder parentQueue(String mappingParentQueue) {
      this.parentQueue = mappingParentQueue;
      return this;
    }

    /**
     * 从完整队列路径字符串解析父队列和叶子队列。
     * @param queuePath 完整队列路径（点分隔）
     * @return 当前 Builder 实例
     */
    public QueueMappingBuilder parsePathString(String queuePath) {
      // 查找最后一个点分隔符，分割父队列和叶子队列
      int parentQueueNameEndIndex = queuePath.lastIndexOf(DOT);

      if (parentQueueNameEndIndex > -1) {
        // 提取父队列名称并去除空格
        final String parentQueue =
            queuePath.substring(0, parentQueueNameEndIndex).trim();
        // 提取叶子队列名称并去除空格
        final String leafQueue =
            queuePath.substring(parentQueueNameEndIndex + 1).trim();
        // 设置解析后的队列信息
        return this
            .parentQueue(parentQueue)
            .queue(leafQueue);
      }

      // 没有父队列，整个路径就是叶子队列
      return this.queue(queuePath);
    }

    /**
     * 构造 QueueMapping 对象。
     * @return 构造完成的 QueueMapping 实例
     */
    public QueueMapping build() {
      return new QueueMapping(this);
    }
  }

  /**
   * 私有构造方法，通过 Builder 构造 QueueMapping。
   * @param builder Builder 实例
   */
  private QueueMapping(QueueMappingBuilder builder) {
    this.type = builder.type;
    this.source = builder.source;
    this.queue = builder.queue;
    this.parentQueue = builder.parentQueue;
    // 拼接完整队列路径
    this.fullPath = (parentQueue != null) ? (parentQueue + DOT + queue) : queue;
  }

  /**
   * 队列映射的类型枚举，定义支持的匹配维度。
   */
  public enum MappingType {
    /** 按用户名匹配 */
    USER("u"),
    /** 按用户组匹配 */
    GROUP("g"),
    /** 按应用名称匹配 */
    APPLICATION("a");

    private final String type;

    MappingType(String type) {
      this.type = type;
    }

    @Override
    public String toString() {
      return type;
    }

  };

  private MappingType type;
  private String source;
  private String queue;
  private String parentQueue;
  private String fullPath;

  private final static String DELIMITER = ":";

  /**
   * 获取目标叶子队列名称。
   * @return 叶子队列名称
   */
  public String getQueue() {
    return queue;
  }

  /**
   * 获取目标父队列名称。
   * @return 父队列名称
   */
  public String getParentQueue() {
    return parentQueue;
  }

  /**
   * 检查是否存在父队列。
   * @return true 如果存在父队列，否则 false
   */
  public boolean hasParentQueue() {
    return parentQueue != null;
  }

  /**
   * 获取映射类型。
   * @return 映射类型
   */
  public MappingType getType() {
    return type;
  }

  /**
   * 获取匹配源名称。
   * @return 匹配源（用户名/用户组名/应用名）
   */
  public String getSource() {
    return source;
  }

  /**
   * 获取完整的目标队列路径（点分隔）。
   * @return 完整队列路径
   */
  public String getFullPath() {
    return fullPath;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result =
        prime * result + ((parentQueue == null) ? 0 : parentQueue.hashCode());
    result = prime * result + ((queue == null) ? 0 : queue.hashCode());
    result = prime * result + ((source == null) ? 0 : source.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    QueueMapping other = (QueueMapping) obj;
    if (parentQueue == null) {
      if (other.parentQueue != null) {
        return false;
      }
    } else if (!parentQueue.equals(other.parentQueue)) {
      return false;
    }
    if (queue == null) {
      if (other.queue != null) {
        return false;
      }
    } else if (!queue.equals(other.queue)) {
      return false;
    }
    if (source == null) {
      if (other.source != null) {
        return false;
      }
    } else if (!source.equals(other.source)) {
      return false;
    }
    if (type != other.type) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return type.toString() + DELIMITER + source + DELIMITER
        + (parentQueue != null ? parentQueue + "." + queue : queue);
  }

  /**
   * 转换为不带类型的字符串表示。
   * @return 不带类型的映射字符串
   */
  public String toTypelessString() {
    return source + DELIMITER
        + (parentQueue != null ? parentQueue + "." + queue : queue);
  }

}