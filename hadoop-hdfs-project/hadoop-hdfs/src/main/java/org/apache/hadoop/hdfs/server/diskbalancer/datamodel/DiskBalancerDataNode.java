// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.diskbalancer.datamodel;

import org.apache.hadoop.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * HDFS磁盘均衡器的数据模型：代表集群中一个DataNode节点，保存节点磁盘信息并计算节点数据密度，用于判断节点是否需要均衡。
 */
public class DiskBalancerDataNode implements Comparable<DiskBalancerDataNode> {
  private double nodeDataDensity;
  private Map<String, DiskBalancerVolumeSet> volumeSets;
  private String dataNodeUUID;
  private String dataNodeIP;
  private int dataNodePort;
  private String dataNodeName;
  private int volumeCount;

  /**
   * 构造空的DataNode对象。
   */
  public DiskBalancerDataNode() {
  }

  /**
   * 根据节点ID构造DataNode对象。
   *
   * @param dataNodeID DataNode的唯一ID
   */
  public DiskBalancerDataNode(String dataNodeID) {
    this.dataNodeUUID = dataNodeID;
    volumeSets = new HashMap<>();
  }

  /**
   * 获取当前DataNode的IP地址。
   *
   * @return IP地址字符串
   */
  public String getDataNodeIP() {
    return dataNodeIP;
  }

  /**
   * 设置当前DataNode的IP地址。
   *
   * @param ipaddress IP地址字符串
   */
  public void setDataNodeIP(String ipaddress) {
    this.dataNodeIP = ipaddress;
  }

  /**
   * 获取当前DataNode的服务端口。
   *
   * @return 端口号
   */
  public int getDataNodePort() {
    return dataNodePort;
  }

  /**
   * 设置当前DataNode的服务端口。
   *
   * @param port 端口号
   */
  public void setDataNodePort(int port) {
    this.dataNodePort = port;
  }

  /**
   * 获取当前DataNode的主机名。
   *
   * @return 节点主机名
   */
  public String getDataNodeName() {
    return dataNodeName;
  }

  /**
   * 设置当前DataNode的主机名。
   *
   * @param name 节点主机名
   */
  public void setDataNodeName(String name) {
    this.dataNodeName = name;
  }

  /**
   * 获取当前节点所有存储类型对应的卷集合。
   *
   * @return 存储类型 -> 卷集合 的映射
   */
  public Map<String, DiskBalancerVolumeSet> getVolumeSets() {
    return volumeSets;
  }

  /**
   * 获取当前DataNode的唯一UUID。
   **/
  public String getDataNodeUUID() {
    return dataNodeUUID;
  }

  /**
   * 设置当前DataNode的UUID。
   *
   * @param nodeID DataNode唯一ID
   */
  public void setDataNodeUUID(String nodeID) {
    this.dataNodeUUID = nodeID;
  }

  /**
   * 判断两个DataNode是否相等，基于UUID判断。
   */
  @Override
  public boolean equals(Object obj) {
    if ((obj == null) || (obj.getClass() != getClass())) {
      return false;
    }
    DiskBalancerDataNode that = (DiskBalancerDataNode) obj;
    return dataNodeUUID.equals(that.getDataNodeUUID());
  }

  /**
   * 基于节点数据密度比较两个DataNode，用于节点排序。
   *
   * @param that 待比较的DataNode对象
   * @return 负数表示当前对象更小，0表示相等，正数表示当前对象更大
   */
  @Override
  public int compareTo(DiskBalancerDataNode that) {
    Preconditions.checkNotNull(that);

    if (Double.compare(this.nodeDataDensity - that.getNodeDataDensity(), 0)
        < 0) {
      return -1;
    }

    if (Double.compare(this.nodeDataDensity - that.getNodeDataDensity(), 0)
        == 0) {
      return 0;
    }

    if (Double.compare(this.nodeDataDensity - that.getNodeDataDensity(), 0)
        > 0) {
      return 1;
    }
    return 0;
  }

  /**
   * 计算对象哈希码。
   */
  @Override
  public int hashCode() {
    return super.hashCode();
  }

  /**
   * 获取节点数据密度指标值。
   *
   * @return 节点数据密度值
   */
  public double getNodeDataDensity() {
    return nodeDataDensity;
  }

  /**
   * 计算当前节点整体数据密度，该指标反映节点内各磁盘数据分布的不均衡程度。
   * 统计节点所有磁盘的密度偏差总和，用于不同节点之间的均衡优先级排序。
   */
  public void computeNodeDensity() {
    double sum = 0;
    int volcount = 0;
    // 遍历所有存储类型的卷集合，累加各磁盘密度绝对值
    for (DiskBalancerVolumeSet vset : volumeSets.values()) {
      for (DiskBalancerVolume vol : vset.getVolumes()) {
        sum += Math.abs(vol.getVolumeDataDensity());
        volcount++;
      }
    }
    nodeDataDensity = sum;
    this.volumeCount = volcount;

  }

  /**
   * 判断当前节点是否需要进行磁盘均衡。
   * 只要任意一个卷集合达到均衡阈值，就需要执行均衡。
   *
   * @param threshold 不均衡阈值百分比
   * @return true表示需要均衡，false表示当前节点已经均衡
   */
  public boolean isBalancingNeeded(double threshold) {
    for (DiskBalancerVolumeSet vSet : getVolumeSets().values()) {
      if (vSet.isBalancingNeeded(threshold)) {
        return true;
      }
    }
    return false;
  }

  /**
   * 向当前DataNode添加一个磁盘卷，按存储类型分组管理。
   * 该方法非线程安全，设计上每个节点由单线程处理，无需同步。
   *
   * @param volume 待添加的磁盘卷对象
   */
  public void addVolume(DiskBalancerVolume volume) throws Exception {
    Preconditions.checkNotNull(volume, "volume cannot be null");
    Preconditions.checkNotNull(volumeSets, "volume sets cannot be null");
    Preconditions
        .checkNotNull(volume.getStorageType(), "storage type cannot be null");

    // 按存储类型作为卷集合的键
    String volumeSetKey = volume.getStorageType();
    DiskBalancerVolumeSet vSet;
    // 如果对应存储类型已存在卷集合，直接添加到现有集合
    if (volumeSets.containsKey(volumeSetKey)) {
      vSet = volumeSets.get(volumeSetKey);
    } else {
      // 否则创建新的卷集合
      vSet = new DiskBalancerVolumeSet(volume.isTransient());
      vSet.setStorageType(volumeSetKey);
      volumeSets.put(volumeSetKey, vSet);
    }

    vSet.addVolume(volume);
    // 添加完成后重新计算节点数据密度
    computeNodeDensity();
  }

  /**
   * 获取当前DataNode总的磁盘卷数量。
   *
   * @return 磁盘卷总数
   */
  public int getVolumeCount() {
    return volumeCount;
  }


}