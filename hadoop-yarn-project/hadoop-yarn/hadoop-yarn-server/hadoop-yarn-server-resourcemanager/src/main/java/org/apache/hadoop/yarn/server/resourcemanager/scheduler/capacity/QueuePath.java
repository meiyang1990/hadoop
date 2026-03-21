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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueTemplate.WILDCARD_QUEUE;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ROOT;

/**
 * 容量调度器队列路径工具类，封装队列路径并提供便捷方法获取父路径、叶子节点等信息
 */
public class QueuePath implements Iterable<String> {
  // 队列路径分隔符正则表达式（点分隔）
  private static final String QUEUE_REGEX_DELIMITER = "\\.";
  /**
   * 队列路径的父路径部分
   */
  private String parent;

  /**
   * 队列路径的叶子节点名称
   */
  private String leaf;

  /**
   * 通过父路径和叶子节点名称构造QueuePath
   * @param parent 队列父路径
   * @param leaf 叶子队列名称
   */
  public QueuePath(String parent, String leaf) {
    this.parent = parent;
    this.leaf = leaf;
  }

  /**
   * 通过完整路径字符串构造QueuePath
   * @param fullPath 队列完整路径
   */
  public QueuePath(String fullPath) {
    setFromFullPath(fullPath);
  }

  /**
   * 通过队列名称分段列表拼接构造QueuePath
   * @param parts 队列路径分段数组
   * @return 构造完成的QueuePath对象
   */
  public static QueuePath createFromQueues(String... parts) {
    return new QueuePath(String.join(DOT, parts));
  }

  /**
   * 将完整队列路径拆分为父路径和叶子节点名称
   * @param fullPath 需要处理的队列完整路径
   */
  private void setFromFullPath(String fullPath) {
    parent = null;
    leaf = fullPath;

    if (leaf == null) {
      leaf = "";
      return;
    }

    int lastDotIdx = fullPath.lastIndexOf(DOT);
    if (lastDotIdx > -1) {
      parent = fullPath.substring(0, lastDotIdx).trim();
      leaf = fullPath.substring(lastDotIdx + 1).trim();
    }
  }

  /**
   * 检查路径是否包含空分段
   * @return true 如果路径中至少有一个空分段
   */
  public boolean hasEmptyPart() {
    // 迭代器不会包含空叶子队列，直接检查
    if (leaf.isEmpty()) {
      return true;
    }

    for (String part : this) {
      if (part.isEmpty()) {
        return true;
      }
    }

    return false;
  }

  /**
   * 检查队列路径是否非法
   * @return true 如果队列路径非法
   */
  public boolean isInvalid() {
    return getPathComponents().length <= 1 && !isRoot();
  }

  /**
   * 获取路径的父路径部分
   * @return 队列父路径，无父路径时返回null
   */
  public String getParent() {
    return parent;
  }

  /**
   * 获取父路径对应的QueuePath对象
   * @return 父路径QueuePath对象，无父路径时返回null
   */
  public QueuePath getParentObject() {
    return hasParent() ? new QueuePath(parent) : null;
  }

  /**
   * 获取路径的叶子节点名称
   * @return 叶子队列名称
   */
  public String getLeafName() {
    return leaf;
  }

  /**
   * 获取队列完整路径
   * @return 队列完整路径字符串
   */
  public String getFullPath() {
    return hasParent() ? (parent + DOT + leaf) : leaf;
  }

  /**
   * 检查队列是否存在父路径
   * @return true 如果定义了父路径
   */
  public boolean hasParent() {
    return parent != null;
  }

  /**
   * 检查当前队列是否为根队列
   * @return true 如果当前路径是根队列
   */
  public boolean isRoot() {
    return !hasParent() && leaf.equals(ROOT);
  }

  /**
   * 基于当前路径作为父路径，追加子队列创建新QueuePath
   * @param childQueue 子队列叶子路径
   * @return 拼接后新的队列路径
   */
  public QueuePath createNewLeaf(String childQueue) {
    return new QueuePath(getFullPath(), childQueue);
  }

  /**
   * 获取队列分段迭代器，从最高层级（通常是root）开始遍历
   * @return 队列分段迭代器
   */
  @Override
  public Iterator<String> iterator() {
    return Arrays.asList(getPathComponents()).iterator();
  }

  /**
   * 获取反向迭代器，从当前队列向上遍历到根节点
   * @return 反向队列路径迭代器
   */
  public Iterator<String> reverseIterator() {

    return new Iterator<String>() {
      private String current = getFullPath();

      @Override
      public boolean hasNext() {
        return current != null;
      }

      @Override
      public String next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        int parentQueueNameEndIndex = current.lastIndexOf(".");
        String old = current;
        if (parentQueueNameEndIndex > -1) {
          current = current.substring(0, parentQueueNameEndIndex).trim();
        } else {
          current = null;
        }

        return old;
      }
    };
  }

  /**
   * 根据自动创建队列深度配置，生成所有通配符形式的队列路径（用于模板配置查找）
   * 例如root.a的模板优先级从高到低为：root.a.*、root.*
   * @param maxAutoCreatedQueueDepth 配置中允许的自动创建队列最大深度
   * @return 通配符QueuePath列表
   */
  public List<QueuePath> getWildcardedQueuePaths(int maxAutoCreatedQueueDepth) {
    List<QueuePath> wildcardedPaths = new ArrayList<>();
    // 首先添加最明确的原路径（不带通配符）
    wildcardedPaths.add(this);

    String[] pathComponents = getPathComponents();
    int supportedWildcardLevel = getSupportedWildcardLevel(maxAutoCreatedQueueDepth);

    // 收集所有通配符模板路径
    for (int wildcardLevel = 1; wildcardLevel <= supportedWildcardLevel; wildcardLevel++) {
      int wildcardedComponentIndex = pathComponents.length - wildcardLevel;
      pathComponents[wildcardedComponentIndex] = WILDCARD_QUEUE;
      QueuePath wildcardedPath = createFromQueues(pathComponents);
      wildcardedPaths.add(wildcardedPath);
    }

    return wildcardedPaths;
  }

  /**
   * 计算当前队列路径支持的通配符层级数
   * @param maxAutoCreatedQueueDepth 配置中允许的自动创建队列最大深度
   * @return 支持的通配符层级数值
   */
  private int getSupportedWildcardLevel(int maxAutoCreatedQueueDepth) {
    int queuePathMaxIndex = getPathComponents().length - 1;
    // 根队列允许使用模板配置
    return isRoot() ? 0 : Math.min(queuePathMaxIndex, maxAutoCreatedQueueDepth);
  }

  /**
   * 拆分获取队列路径所有分段
   * @return 包含各级队列名称的字符串数组
   */
  public String[] getPathComponents() {
    return getFullPath().split(QUEUE_REGEX_DELIMITER);
  }

  @Override
  public String toString() {
    return getFullPath();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueuePath other = (QueuePath) o;
    return Objects.equals(parent, other.parent) &&
        Objects.equals(leaf, other.leaf);
  }

  @Override
  public int hashCode() {
    return Objects.hash(parent, leaf);
  }
}