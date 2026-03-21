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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.QueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.io.IOException;

/**
 * 公平调度器所有应用队列放置规则的抽象基类，提供所有放置规则共享的基础能力
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class FSPlacementRule extends PlacementRule {
  private static final Logger LOG =
      LoggerFactory.getLogger(FSPlacementRule.class);

  // 标记是否允许规则创建新队列
  @VisibleForTesting
  protected boolean createQueue = true;
  private QueueManager queueManager;
  private PlacementRule parentRule;

  /**
   * 获取规则初始化时从调度器加载的队列管理器
   * @return 调度器提供的队列管理器，初始化完成后不可能为null
   */
  QueueManager getQueueManager() {
    return queueManager;
  }

  /**
   * 设置用于动态生成父队列的规则
   * @param parent 父队列生成规则
   */
  public void setParentRule(PlacementRule parent) {
    this.parentRule = parent;
  }

  /**
   * 获取动态生成父队列的规则
   * @return 父队列生成规则，未设置则返回null
   */
  @VisibleForTesting
  public PlacementRule getParentRule() {
    return parentRule;
  }

  /**
   * 根据传入配置对象的类型设置规则配置
   * @param initArg 配置对象
   */
  @Override
  public void setConfig(Object initArg) {
    if (null == initArg) {
      LOG.debug("Null object passed in: no config set");
      return;
    }
    if (initArg instanceof Element) {
      LOG.debug("Setting config from XML");
      setConfig((Element) initArg);
    } else if (initArg instanceof Boolean) {
      LOG.debug("Setting config from Boolean");
      setConfig((Boolean) initArg);
    } else {
      LOG.info("Unknown object type passed in as config for rule {}: {}",
          getName(), initArg.getClass());
    }
  }

  /**
   * 从XML配置元素中设置规则配置
   * @param conf 公平调度器配置中的XML元素
   */
  protected void setConfig(Element conf) {
    // 从配置获取创建标志，未设置默认值为true
    createQueue = getCreateFlag(conf);
  }

  /**
   * 仅通过布尔值设置规则配置
   * @param create 是否允许本规则创建队列
   */
  protected void setConfig(Boolean create) {
    createQueue = create;
  }

  /**
   * 公平调度器放置规则的标准初始化逻辑，所有具体规则共享
   * 继承该类并重写此方法的规则必须调用super.initialize()完成基础初始化
   * @param scheduler 使用该规则的调度器
   * @return 始终返回true表示初始化成功
   * @throws IOException 初始化错误时抛出
   */
  @Override
  public boolean initialize(ResourceScheduler scheduler) throws IOException {
    if (!(scheduler instanceof FairScheduler)) {
      throw new IOException(getName() +
          " rule can only be configured for the FairScheduler");
    }
    if (getParentRule() != null &&
        getParentRule().getName().equals(getName())) {
      throw new IOException("Parent rule may not be the same type as the " +
          "child rule: " + getName());
    }

    FairScheduler fs = (FairScheduler) scheduler;
    queueManager = fs.getQueueManager();

    return true;
  }

  /**
   * 检查队列是否为静态配置队列（非动态创建队列）且存在
   * @param queueName 待检查队列名
   * @return 队列存在且是静态配置队列返回true，否则返回false
   */
  boolean configuredQueue(String queueName) {
    FSQueue queue = queueManager.getQueue(queueName);
    return (queue != null && !queue.isDynamic());
  }

  /**
   * 获取配置设置的队列创建允许标志
   * @return 队列创建标志值
   */
  public boolean getCreateFlag() {
    return createQueue;
  }

  /**
   * 从XML配置元素解析队列创建标志
   * @param conf 调度器配置中的队列配置XML元素
   * @return 只有配置显式设置为非true值返回false，其他情况都返回true
   */
  boolean getCreateFlag(Element conf) {
    if (conf != null) {
      String create = conf.getAttribute("create");
      return create.isEmpty() || Boolean.parseBoolean(create);
    }
    return true;
  }
}