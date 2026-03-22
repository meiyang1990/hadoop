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

package org.apache.hadoop.hdfs.server.diskbalancer.planner;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolume;
import org.apache.hadoop.util.StringUtils;

/**
 * 磁盘均衡器单次数据搬移任务描述，记录从源磁盘到目标磁盘的数据迁移信息
 * 忽略默认值字段序列化，减少JSON输出体积
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
/**
 * 表示磁盘均衡规划器生成的一次数据搬移步骤，将数据从一个磁盘卷移动到另一个磁盘卷，实现磁盘空间均衡
 */
public class MoveStep implements Step {
  private DiskBalancerVolume sourceVolume;
  private DiskBalancerVolume destinationVolume;
  private double idealStorage;
  private long bytesToMove;
  private String volumeSetID;

  private long maxDiskErrors;
  private long tolerancePercent;
  private long bandwidth;

  /**
   * 构造数据搬移步骤
   *
   * @param sourceVolume      源磁盘卷，数据从该卷迁出
   * @param idealStorage      当前卷组的理想存储容量值，用于均衡目标
   * @param destinationVolume 目标磁盘卷，数据迁入该卷
   * @param bytesToMove       需要移动的字节数
   * @param volumeSetID       卷组ID，标识当前步骤所属的磁盘卷组
   */
  public MoveStep(DiskBalancerVolume sourceVolume, double idealStorage,
                  DiskBalancerVolume destinationVolume, long bytesToMove,
                  String volumeSetID) {
    this.destinationVolume = destinationVolume;
    this.idealStorage = idealStorage;
    this.sourceVolume = sourceVolume;
    this.bytesToMove = bytesToMove;
    this.volumeSetID = volumeSetID;

  }

  /**
   * 空构造函数，供JSON序列化/反序列化使用
   */
  public MoveStep() {
  }

  /**
   * 获取本次需要移动的字节数
   *
   * @return 需要移动的字节数
   */
  @Override
  public long getBytesToMove() {
    return bytesToMove;
  }

  /**
   * 获取本次数据搬移的目标磁盘卷
   *
   * @return 目标磁盘卷
   */
  @Override
  public DiskBalancerVolume getDestinationVolume() {
    return destinationVolume;
  }

  /**
   * 获取均衡后卷组的理想存储容量值
   *
   * @return 理想存储容量值
   */
  @Override
  public double getIdealStorage() {
    return idealStorage;
  }

  /**
   * 获取本次数据搬移的源磁盘卷
   *
   * @return 源磁盘卷
   */

  @Override
  public DiskBalancerVolume getSourceVolume() {
    return sourceVolume;
  }

  /**
   * 获取当前步骤所属卷组的ID
   *
   * @return 卷组ID
   */
  @Override
  public String getVolumeSetID() {
    return volumeSetID;
  }

  /**
   * 设置源磁盘卷
   *
   * @param sourceVolume 源磁盘卷对象
   */
  public void setSourceVolume(DiskBalancerVolume sourceVolume) {
    this.sourceVolume = sourceVolume;
  }

  /**
   * 设置目标磁盘卷
   *
   * @param destinationVolume 目标磁盘卷对象
   */
  public void setDestinationVolume(DiskBalancerVolume destinationVolume) {
    this.destinationVolume = destinationVolume;
  }

  /**
   * 设置理想存储容量值
   *
   * @param idealStorage 理想存储容量值
   */
  public void setIdealStorage(double idealStorage) {
    this.idealStorage = idealStorage;
  }

  /**
   * 设置需要移动的字节数
   *
   * @param bytesToMove 需要移动的字节数
   */
  public void setBytesToMove(long bytesToMove) {
    this.bytesToMove = bytesToMove;
  }

  /**
   * 设置卷组ID
   *
   * @param volumeSetID 卷组ID
   */
  public void setVolumeSetID(String volumeSetID) {
    this.volumeSetID = volumeSetID;
  }

  /**
   * 格式化输出当前搬移步骤的信息
   *
   * @return 格式化后的步骤信息字符串
   */
  @Override
  public String toString() {
    return String.format("%s\t %s\t %s\t %s%n",
        this.getSourceVolume().getPath(),
        this.getDestinationVolume().getPath(),
        getSizeString(this.getBytesToMove()),
        this.getDestinationVolume().getStorageType());

  }

  /**
   * 将字节大小转换为人类可读的格式
   *
   * @param size 需要转换的字节大小
   * @return 人类可读的容量字符串
   */
  @Override
  public String getSizeString(long size) {
    return StringUtils.TraditionalBinaryPrefix.long2String(size, "", 1);
  }

  /**
   * 获取本次搬移允许容忍的最大磁盘错误数，超过该值则终止本次搬移
   * @return  容忍的最大磁盘错误数
   */
  @Override
  public long getMaxDiskErrors() {
    return maxDiskErrors;
  }

  /**
   * 设置本次搬移允许容忍的最大磁盘错误数
   * @param maxDiskErrors 容忍的最大磁盘错误数
   */
  @Override
  public void setMaxDiskErrors(long maxDiskErrors) {
    this.maxDiskErrors = maxDiskErrors;
  }

  /**
   * 获取均衡容差百分比，当实际存储与理想存储的偏差在容差范围内时，可认为均衡完成，无需继续搬移
   *
   * @return 容差百分比
   */
  @Override
  public long getTolerancePercent() {
    return tolerancePercent;
  }

  /**
   * 设置均衡容差百分比
   * @param tolerancePercent 容差百分比
   */
  @Override
  public void setTolerancePercent(long tolerancePercent) {
    this.tolerancePercent = tolerancePercent;
  }

  /**
   * 获取本次搬移允许使用的最大磁盘带宽，单位MB/s，用于限制均衡操作对DataNode正常服务的影响
   * @return  最大磁盘带宽，单位MB/s
   */
  @Override
  public long getBandwidth() {
    return bandwidth;
  }

  /**
   * 设置本次搬移允许使用的最大磁盘带宽
   * @param bandwidth  最大磁盘带宽，单位MB/s
   */
  @Override
  public void setBandwidth(long bandwidth) {
    this.bandwidth = bandwidth;
  }
}