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

package org.apache.hadoop.hdfs.server.diskbalancer.datamodel;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * 文件：磁盘均衡器卷集合模型
 * 该类是数据节点上相同存储类型磁盘的集合，是磁盘均衡计划生成的基本单位，负责计算各磁盘数据密度并判断是否需要均衡
 */
@JsonIgnoreProperties({"sortedQueue", "volumeCount", "idealUsed"})
public class DiskBalancerVolumeSet {
  private static final Logger LOG =
      LoggerFactory.getLogger(DiskBalancerVolumeSet.class);
  // 最大磁盘数量限制
  private final int maxDisks = 256;

  @JsonProperty("transient")
  private boolean isTransient;
  // 当前集合包含的磁盘卷集合
  private Set<DiskBalancerVolume> volumes;

  @JsonIgnore
  // 按数据密度排序的优先队列，存储需要移出数据的磁盘
  private TreeSet<DiskBalancerVolume> sortedQueue;
  // 当前集合的存储类型（如SSD、HDD）
  private String storageType;
  // 当前卷集合唯一ID
  private String setID;

  // 当前卷集合的理想利用率
  private double idealUsed;


  /**
   * 空构造函数，供Jackson反序列化使用
   */
  public DiskBalancerVolumeSet() {
    setID = UUID.randomUUID().toString();
  }

  /**
   * 构造指定 transient 属性的磁盘卷集合
   * @param isTransient 是否为瞬时卷
   */
  public DiskBalancerVolumeSet(boolean isTransient) {
    this.isTransient = isTransient;
    volumes = new HashSet<>(maxDisks);
    sortedQueue = new TreeSet<>(new MinHeap());
    this.storageType = null;
    setID = UUID.randomUUID().toString();
  }

  /**
   * 拷贝构造函数，基于已有卷集合创建新实例
   * @param volumeSet 要拷贝的源卷集合
   */
  public DiskBalancerVolumeSet(DiskBalancerVolumeSet volumeSet) {
    this.isTransient = volumeSet.isTransient();
    this.storageType = volumeSet.storageType;
    this.volumes = new HashSet<>(volumeSet.volumes);
    sortedQueue = new TreeSet<>(new MinHeap());
    setID = UUID.randomUUID().toString();
  }

  /**
   * 获取当前卷集合是否为瞬时卷
   * @return true 是瞬时卷，false 不是
   */
  @JsonProperty("transient")
  public boolean isTransient() {
    return isTransient;
  }

  /**
   * 设置当前卷集合的 transient 属性
   * @param transientValue 瞬时属性值
   */
  @JsonProperty("transient")
  public void setTransient(boolean transientValue) {
    this.isTransient = transientValue;
  }

  /**
   * 计算当前卷集合中所有正常磁盘的数据密度
   * 数据密度 = 理想利用率 - 当前磁盘实际利用率，反映该磁盘需要移出多少数据
   * 排除故障卷和跳过卷后重新计算所有磁盘的数据密度并重建排序队列
   */
  public void computeVolumeDataDensity() {
    long totalCapacity = 0;
    long totalUsed = 0;
    sortedQueue.clear();

    // 遍历所有卷，累加总容量和总已用空间，跳过故障和已标记跳过的卷
    for (DiskBalancerVolume volume : volumes) {
      if (!volume.isFailed() && !volume.isSkip()) {

        // 有效容量为负，标记该卷为配置错误跳过处理
        if (volume.computeEffectiveCapacity() < 0) {
          skipMisConfiguredVolume(volume);
          continue;
        }

        totalCapacity += volume.computeEffectiveCapacity();
        totalUsed += volume.getUsed();
      }
    }

    // 计算理想利用率：总已用空间 / 总有效容量，截断小数精度
    if (totalCapacity != 0) {
      this.idealUsed = truncateDecimals(totalUsed /
          (double) totalCapacity);
    }

    // 计算每个正常卷的数据密度并加入排序队列
    for (DiskBalancerVolume volume : volumes) {
      if (!volume.isFailed() && !volume.isSkip()) {
        double dfsUsedRatio =
            truncateDecimals(volume.getUsed() /
                (double) volume.computeEffectiveCapacity());

        volume.setVolumeDataDensity(this.idealUsed - dfsUsedRatio);
        sortedQueue.add(volume);
      }
    }
  }

  /**
   * 将double值截断为保留4位小数，避免精度过高影响用户理解
   * @param value 原始double值
   * @return 截断后保留4位小数的值
   */
  private double truncateDecimals(double value) {
    final int multiplier = 10000;
    return (double) ((long) (value * multiplier)) / multiplier;
  }

  /**
   * 处理配置错误的卷：记录错误日志并标记该卷跳过处理
   * @param volume 配置错误的卷
   */
  private void skipMisConfiguredVolume(DiskBalancerVolume volume) {
    String errMessage = String.format("Real capacity is negative." +
                                          "This usually points to some " +
                                          "kind of mis-configuration.%n" +
                                          "Capacity : %d Reserved : %d " +
                                          "realCap = capacity - " +
                                          "reserved = %d.%n" +
                                          "Skipping this volume from " +
                                          "all processing. type : %s id" +
                                          " :%s",
                                      volume.getCapacity(),
                                      volume.getReserved(),
                                      volume.computeEffectiveCapacity(),
                                      volume.getStorageType(),
                                      volume.getUuid());

    LOG.error(errMessage);
    volume.setSkip(true);
  }

  /**
   * 获取当前卷集合包含的卷总数
   * @return 卷数量
   */
  @JsonIgnore
  public int getVolumeCount() {
    return volumes.size();
  }

  /**
   * 获取当前卷集合的存储类型
   * @return 存储类型字符串
   */
  public String getStorageType() {
    return storageType;
  }

  /**
   * 设置当前卷集合的存储类型
   * @param typeOfStorage 存储类型字符串
   */
  public void setStorageType(String typeOfStorage) {
    this.storageType = typeOfStorage;
  }

  /**
   * 向当前卷集合添加新磁盘卷，并重新计算数据密度
   * @param volume 要添加的磁盘卷
   * @throws Exception 参数校验失败时抛出异常
   */
  public void addVolume(DiskBalancerVolume volume) throws Exception {
    Preconditions.checkNotNull(volume, "volume cannot be null");
    Preconditions.checkState(isTransient() == volume.isTransient(),
                             "Mismatch in volumeSet and volume's transient " +
                                 "properties.");

    // 第一个卷确定集合的存储类型，后续添加的卷必须和集合存储类型一致
    if (this.storageType == null) {
      Preconditions.checkState(volumes.size() == 0L, "Storage Type is Null but"
          + " volume size is " + volumes.size());
      this.storageType = volume.getStorageType();
    } else {
      Preconditions.checkState(this.storageType.equals(volume.getStorageType()),
                               "Adding wrong type of disk to this volume set");
    }
    volumes.add(volume);
    computeVolumeDataDensity();

  }

  /**
   * 获取当前卷集合所有磁盘卷列表
   * @return 磁盘卷列表
   */
  public List<DiskBalancerVolume> getVolumes() {
    return new ArrayList<>(volumes);
  }


  @JsonIgnore
  /**
   * 获取按数据密度排序的优先队列
   * @return 排序后的TreeSet
   */
  public TreeSet<DiskBalancerVolume> getSortedQueue() {
    return sortedQueue;
  }

  /**
   * 判断当前卷集合是否需要执行磁盘均衡
   * 检查是否存在任意正常磁盘的数据密度绝对值超过阈值，超过则需要均衡
   * @param thresholdPercentage 均衡阈值百分比
   * @return true 需要均衡，false 不需要均衡
   */
  public boolean isBalancingNeeded(double thresholdPercentage) {
    double threshold = thresholdPercentage / 100.0d;

    // 少于等于1个磁盘无需均衡
    if(volumes == null || volumes.size() <= 1) {
      return false;
    }

    // 遍历所有卷，只要有一个正常卷超过阈值就需要均衡
    for (DiskBalancerVolume vol : volumes) {
      boolean notSkip = !vol.isFailed() && !vol.isTransient() && !vol.isSkip();
      Double absDensity =
          truncateDecimals(Math.abs(vol.getVolumeDataDensity()));

      if ((absDensity > threshold) && notSkip) {
        return true;
      }
    }
    return false;
  }

  /**
   * 从当前集合移除指定磁盘卷
   * 调用该方法后需要手动重新计算数据密度
   * @param volume 要移除的磁盘卷
   */
  public void removeVolume(DiskBalancerVolume volume) {
    volumes.remove(volume);
    sortedQueue.remove(volume);
  }

  /**
   * 获取当前卷集合唯一ID
   * @return 卷集合ID字符串
   */
  public String getSetID() {
    return setID;
  }

  /**
   * 设置当前卷集合唯一ID
   * @param volID 卷集合ID字符串
   */
  public void setSetID(String volID) {
    this.setID = volID;
  }

  /**
   * 获取当前卷集合的理想利用率
   * @return 理想利用率
   */
  @JsonIgnore
  public double getIdealUsed() {
    return this.idealUsed;
  }

  /**
   * 最小堆比较器，按磁盘数据密度降序排序
   * 数据密度越大，说明该磁盘已用占比越低，需要移入更多数据；反之需要移出数据
   */
  static class MinHeap implements Comparator<DiskBalancerVolume>, Serializable {

    /**
     * 比较两个磁盘卷的数据密度，按降序排序
     * @param first 第一个磁盘卷
     * @param second 第二个磁盘卷
     * @return 比较结果
     */
    @Override
    public int compare(DiskBalancerVolume first, DiskBalancerVolume second) {
      return Double.compare(second.getVolumeDataDensity(),
          first.getVolumeDataDensity());
    }
  }
}