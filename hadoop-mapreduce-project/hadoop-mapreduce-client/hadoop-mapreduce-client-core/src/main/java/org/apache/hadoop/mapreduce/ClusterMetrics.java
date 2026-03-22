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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/**
 * 存储MapReduce集群当前状态的指标信息
 * 
 * <p><code>ClusterMetrics</code> 向客户端提供以下集群信息:
 * <ol>
 *   <li>
 *   集群规模（节点总数）
 *   </li>
 *   <li>
 *   黑名单和已退役节点的数量
 *   </li>
 *   <li>
 *   集群总slot容量
 *   </li>
 *   <li>
 *   当前已被占用/预留的Map和Reduce slot数量
 *   </li>
 *   <li>
 *   当前正在运行的Map和Reduce任务数量
 *   </li>
 *   <li>
 *   累计作业提交数量
 *   </li>
 * </ol>
 * 
 * <p>客户端可以通过 {@link Cluster#getClusterStatus()} 获取最新的集群指标信息</p>
 * 
 * @see Cluster
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
/**
 * 集群指标信息类，存储MapReduce集群当前运行状态的各类统计指标，
 * 实现Writable接口支持序列化，可在客户端与服务端之间传输
 */
public class ClusterMetrics implements Writable {
  private int runningMaps;
  private int runningReduces;
  private int occupiedMapSlots;
  private int occupiedReduceSlots;
  private int reservedMapSlots;
  private int reservedReduceSlots;
  private int totalMapSlots;
  private int totalReduceSlots;
  private int totalJobSubmissions;
  private int numTrackers;
  private int numBlacklistedTrackers;
  private int numGraylistedTrackers;
  private int numDecommissionedTrackers;

  /**
   * 空构造方法，用于反序列化
   */
  public ClusterMetrics() {
  }
  
  /**
   * 构造ClusterMetrics对象，graylisted节点默认设为0
   * @param runningMaps 当前运行Map任务数
   * @param runningReduces 当前运行Reduce任务数
   * @param occupiedMapSlots 已占用Map slot数
   * @param occupiedReduceSlots 已占用Reduce slot数
   * @param reservedMapSlots 预留Map slot数
   * @param reservedReduceSlots 预留Reduce slot数
   * @param mapSlots 总Map slot容量
   * @param reduceSlots 总Reduce slot容量
   * @param totalJobSubmissions 累计作业提交总数
   * @param numTrackers 活跃TaskTracker节点数
   * @param numBlacklistedTrackers 黑名单TaskTracker节点数
   * @param numDecommissionedNodes 已退役节点数
   */
  public ClusterMetrics(int runningMaps, int runningReduces,
      int occupiedMapSlots, int occupiedReduceSlots, int reservedMapSlots,
      int reservedReduceSlots, int mapSlots, int reduceSlots,
      int totalJobSubmissions, int numTrackers, int numBlacklistedTrackers,
      int numDecommissionedNodes) {
    this(runningMaps, runningReduces, occupiedMapSlots, occupiedReduceSlots,
      reservedMapSlots, reservedReduceSlots, mapSlots, reduceSlots,
      totalJobSubmissions, numTrackers, numBlacklistedTrackers, 0,
      numDecommissionedNodes);
  }

  /**
   * 构造完整ClusterMetrics对象，包含所有指标参数
   * @param runningMaps 当前运行Map任务数
   * @param runningReduces 当前运行Reduce任务数
   * @param occupiedMapSlots 已占用Map slot数
   * @param occupiedReduceSlots 已占用Reduce slot数
   * @param reservedMapSlots 预留Map slot数
   * @param reservedReduceSlots 预留Reduce slot数
   * @param mapSlots 总Map slot容量
   * @param reduceSlots 总Reduce slot容量
   * @param totalJobSubmissions 累计作业提交总数
   * @param numTrackers 活跃TaskTracker节点数
   * @param numBlacklistedTrackers 黑名单TaskTracker节点数
   * @param numGraylistedTrackers 灰名单TaskTracker节点数
   * @param numDecommissionedNodes 已退役节点数
   */
  public ClusterMetrics(int runningMaps, int runningReduces,
      int occupiedMapSlots, int occupiedReduceSlots, int reservedMapSlots,
      int reservedReduceSlots, int mapSlots, int reduceSlots,
      int totalJobSubmissions, int numTrackers, int numBlacklistedTrackers,
      int numGraylistedTrackers, int numDecommissionedNodes) {
    this.runningMaps = runningMaps;
    this.runningReduces = runningReduces;
    this.occupiedMapSlots = occupiedMapSlots;
    this.occupiedReduceSlots = occupiedReduceSlots;
    this.reservedMapSlots = reservedMapSlots;
    this.reservedReduceSlots = reservedReduceSlots;
    this.totalMapSlots = mapSlots;
    this.totalReduceSlots = reduceSlots;
    this.totalJobSubmissions = totalJobSubmissions;
    this.numTrackers = numTrackers;
    this.numBlacklistedTrackers = numBlacklistedTrackers;
    this.numGraylistedTrackers = numGraylistedTrackers;
    this.numDecommissionedTrackers = numDecommissionedNodes;
  }

  /**
   * 获取集群中当前正在运行的Map任务数量
   * 
   * @return 正在运行的Map任务数
   */
  public int getRunningMaps() {
    return runningMaps;
  }
  
  /**
   * 获取集群中当前正在运行的Reduce任务数量
   * 
   * @return 正在运行的Reduce任务数
   */
  public int getRunningReduces() {
    return runningReduces;
  }
  
  /**
   * 获取集群中已被占用的Map slot数量
   * 
   * @return 已占用Map slot数量
   */
  public int getOccupiedMapSlots() { 
    return occupiedMapSlots;
  }
  
  /**
   * 获取集群中已被占用的Reduce slot数量
   * 
   * @return 已占用Reduce slot数量
   */
  public int getOccupiedReduceSlots() { 
    return occupiedReduceSlots; 
  }

  /**
   * 获取集群中已预留的Map slot数量
   * 
   * @return 预留Map slot数量
   */
  public int getReservedMapSlots() { 
    return reservedMapSlots;
  }
  
  /**
   * 获取集群中已预留的Reduce slot数量
   * 
   * @return 预留Reduce slot数量
   */
  public int getReservedReduceSlots() { 
    return reservedReduceSlots; 
  }

  /**
   * 获取集群总Map slot容量
   * 
   * @return 集群总Map slot容量
   */
  public int getMapSlotCapacity() {
    return totalMapSlots;
  }
  
  /**
   * 获取集群总Reduce slot容量
   * 
   * @return 集群总Reduce slot容量
   */
  public int getReduceSlotCapacity() {
    return totalReduceSlots;
  }
  
  /**
   * 获取集群累计作业提交总数
   * 
   * @return 累计作业提交总数
   */
  public int getTotalJobSubmissions() {
    return totalJobSubmissions;
  }
  
  /**
   * 获取集群中活跃TaskTracker节点数量
   * 
   * @return 活跃TaskTracker节点数量
   */
  public int getTaskTrackerCount() {
    return numTrackers;
  }
  
  /**
   * 获取集群中黑名单TaskTracker节点数量
   * 黑名单节点不会被分配新任务
   * 
   * @return 黑名单TaskTracker节点数量
   */
  public int getBlackListedTaskTrackerCount() {
    return numBlacklistedTrackers;
  }
  
  /**
   * 获取集群中灰名单TaskTracker节点数量
   * 灰名单节点会降低分配优先级，仍可分配任务
   * 
   * @return 灰名单TaskTracker节点数量
   */
  public int getGrayListedTaskTrackerCount() {
    return numGraylistedTrackers;
  }
  
  /**
   * 获取集群中已退役TaskTracker节点数量
   * 已退役节点已下线不再提供服务
   * 
   * @return 已退役TaskTracker节点数量
   */
  public int getDecommissionedTaskTrackerCount() {
    return numDecommissionedTrackers;
  }

  /**
   * 从输入流反序列化读取所有集群指标字段
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    runningMaps = in.readInt();
    runningReduces = in.readInt();
    occupiedMapSlots = in.readInt();
    occupiedReduceSlots = in.readInt();
    reservedMapSlots = in.readInt();
    reservedReduceSlots = in.readInt();
    totalMapSlots = in.readInt();
    totalReduceSlots = in.readInt();
    totalJobSubmissions = in.readInt();
    numTrackers = in.readInt();
    numBlacklistedTrackers = in.readInt();
    numGraylistedTrackers = in.readInt();
    numDecommissionedTrackers = in.readInt();
  }

  /**
   * 将所有集群指标字段序列化写入输出流
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(runningMaps);
    out.writeInt(runningReduces);
    out.writeInt(occupiedMapSlots);
    out.writeInt(occupiedReduceSlots);
    out.writeInt(reservedMapSlots);
    out.writeInt(reservedReduceSlots);
    out.writeInt(totalMapSlots);
    out.writeInt(totalReduceSlots);
    out.writeInt(totalJobSubmissions);
    out.writeInt(numTrackers);
    out.writeInt(numBlacklistedTrackers);
    out.writeInt(numGraylistedTrackers);
    out.writeInt(numDecommissionedTrackers);
  }

}