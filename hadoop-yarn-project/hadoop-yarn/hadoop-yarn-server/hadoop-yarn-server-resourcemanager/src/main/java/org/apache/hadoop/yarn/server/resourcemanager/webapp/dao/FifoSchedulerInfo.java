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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;

/**
 * FIFO调度器信息数据访问对象，封装FIFO调度器状态信息，供Web UI展示使用
 */
@XmlRootElement(name = "fifoScheduler")
@XmlType(name = "fifoScheduler")
@XmlAccessorType(XmlAccessType.FIELD)
public class FifoSchedulerInfo extends SchedulerInfo {

  // 队列总容量
  protected float capacity;
  // 队列已使用容量
  protected float usedCapacity;
  // 队列状态
  protected QueueState qstate;
  // 队列最小内存分配额度
  protected long minQueueMemoryCapacity;
  // 队列最大内存分配额度
  protected long maxQueueMemoryCapacity;
  // 集群节点数量
  protected int numNodes;
  // 节点已使用内存总容量
  protected int usedNodeCapacity;
  // 节点可用内存总容量
  protected int availNodeCapacity;
  // 节点总内存容量
  protected int totalNodeCapacity;
  // 已分配容器总数
  protected int numContainers;

  @XmlTransient
  protected String qstateFormatted;

  @XmlTransient
  protected String qName;

  /**
   * JAXB默认构造函数，用于序列化反序列化
   */
  public FifoSchedulerInfo() {
  } // JAXB needs this

  /**
   * 从ResourceManager构建FIFO调度器信息对象
   * @param rm ResourceManager实例
   */
  public FifoSchedulerInfo(final ResourceManager rm) {

    // 获取RM上下文
    RMContext rmContext = rm.getRMContext();

    // 获取FIFO调度器实例
    FifoScheduler fs = (FifoScheduler) rm.getResourceScheduler();
    qName = fs.getQueueInfo("", false, false).getQueueName();
    QueueInfo qInfo = fs.getQueueInfo(qName, true, true);

    // 填充队列基础容量信息
    this.usedCapacity = qInfo.getCurrentCapacity();
    this.capacity = qInfo.getCapacity();
    this.minQueueMemoryCapacity = fs.getMinimumResourceCapability().getMemorySize();
    this.maxQueueMemoryCapacity = fs.getMaximumResourceCapability().getMemorySize();
    this.qstate = qInfo.getQueueState();

    // 初始化集群节点统计信息
    this.numNodes = rmContext.getRMNodes().size();
    this.usedNodeCapacity = 0;
    this.availNodeCapacity = 0;
    this.totalNodeCapacity = 0;
    this.numContainers = 0;

    // 遍历所有节点累加统计容量和容器信息
    for (RMNode ni : rmContext.getRMNodes().values()) {
      SchedulerNodeReport report = fs.getNodeReport(ni.getNodeID());
      this.usedNodeCapacity += report.getUsedResource().getMemorySize();
      this.availNodeCapacity += report.getAvailableResource().getMemorySize();
      this.totalNodeCapacity += ni.getTotalCapability().getMemorySize();
      this.numContainers += fs.getNodeReport(ni.getNodeID()).getNumContainers();
    }

    // 设置调度器名称
    this.schedulerName = "Fifo Scheduler";
  }

  /**
   * 获取集群活跃节点数量
   * @return 节点数量
   */
  public int getNumNodes() {
    return this.numNodes;
  }

  /**
   * 获取所有节点已使用内存总容量
   * @return 已使用内存容量
   */
  public int getUsedNodeCapacity() {
    return this.usedNodeCapacity;
  }

  /**
   * 获取所有节点可用内存总容量
   * @return 可用内存容量
   */
  public int getAvailNodeCapacity() {
    return this.availNodeCapacity;
  }

  /**
   * 获取所有节点总内存容量
   * @return 总内存容量
   */
  public int getTotalNodeCapacity() {
    return this.totalNodeCapacity;
  }

  /**
   * 获取已分配容器总数
   * @return 容器数量
   */
  public int getNumContainers() {
    return this.numContainers;
  }

  /**
   * 获取队列状态字符串
   * @return 队列状态字符串
   */
  public String getState() {
    return this.qstate.toString();
  }

  /**
   * 获取队列名称
   * @return 队列名称
   */
  public String getQueueName() {
    return this.qName;
  }

  /**
   * 获取队列允许的最小内存分配额度
   * @return 最小内存容量
   */
  public long getMinQueueMemoryCapacity() {
    return this.minQueueMemoryCapacity;
  }

  /**
   * 获取队列允许的最大内存分配额度
   * @return 最大内存容量
   */
  public long getMaxQueueMemoryCapacity() {
    return this.maxQueueMemoryCapacity;
  }

  /**
   * 获取队列总容量
   * @return 队列总容量
   */
  public float getCapacity() {
    return this.capacity;
  }

  /**
   * 获取队列已使用容量
   * @return 已使用容量
   */
  public float getUsedCapacity() {
    return this.usedCapacity;
  }

}