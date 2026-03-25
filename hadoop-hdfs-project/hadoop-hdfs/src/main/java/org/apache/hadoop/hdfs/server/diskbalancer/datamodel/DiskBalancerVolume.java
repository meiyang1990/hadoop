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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import org.apache.hadoop.hdfs.web.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 文件级注释：磁盘平衡器数据模型，描述DataNode上单个磁盘卷的元数据与使用信息
 * 表示DataNode节点中的一个磁盘卷，存储磁盘的容量、使用量、存储类型等信息供磁盘平衡计算使用。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DiskBalancerVolume {
  // Jackson JSON解析对象读取器，复用提高性能
  private static final ObjectReader READER =
      new ObjectMapper().readerFor(DiskBalancerVolume.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(DiskBalancerVolume.class);

  private String path;
  private long capacity;
  private String storageType;
  private long used;
  private long reserved;
  private String uuid;
  private boolean failed;
  private boolean isTransient;
  private double volumeDataDensity;
  private boolean skip = false;
  private boolean isReadOnly;

  /**
   * 构造空的磁盘卷对象，供JSON反序列化使用。
   */
  public DiskBalancerVolume() {
  }

  /**
   * 从JSON字符串解析出DiskBalancerVolume对象。
   *
   * @param json 待解析的JSON字符串
   * @return 解析完成的DiskBalancerVolume对象
   * @throws IOException 解析过程中发生IO错误抛出
   */
  public static DiskBalancerVolume parseJson(String json) throws IOException {
    return READER.readValue(json);
  }

  /**
   * 获取当前磁盘卷的数据密度值。
   * 数据密度计算方式参见DiskBalancerVolumeSet#computeVolumeDataDensity。
   *
   * @return 当前卷的数据密度
   */
  public double getVolumeDataDensity() {
    return volumeDataDensity;
  }

  /**
   * 设置当前磁盘卷的数据密度值。
   *
   * @param volDataDensity 数据密度值
   */
  public void setVolumeDataDensity(double volDataDensity) {
    this.volumeDataDensity = volDataDensity;
  }

  /**
   * 获取当前卷是否为瞬时存储。
   *
   * @return true表示是瞬时存储，false表示持久存储
   */
  public boolean isTransient() {
    return isTransient;
  }

  /**
   * 设置当前卷是否为瞬时存储。
   *
   * @param aTransient true表示瞬时存储，false表示持久存储
   */
  public void setTransient(boolean aTransient) {
    this.isTransient = aTransient;
  }

  /**
   * 判断两个磁盘卷是否为同一个卷，基于UUID判断。
   *
   * @param o 待比较的对象
   * @return 是同一个卷返回true，否则返回false
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DiskBalancerVolume that = (DiskBalancerVolume) o;
    return uuid.equals(that.uuid);
  }

  /**
   * 基于UUID计算当前磁盘卷的哈希码。
   *
   * @return 当前卷的哈希码
   */
  @Override
  public int hashCode() {
    return uuid.hashCode();
  }

  /**
   * 获取当前磁盘卷的总容量。
   *
   * @return 总容量，单位字节
   */
  public long getCapacity() {
    return capacity;
  }

  /**
   * 计算并获取当前磁盘卷的可用空间。
   *
   * @return 可用空间，单位字节
   */
  @JsonIgnore
  public long getFreeSpace() {
    return getCapacity() - getUsed();
  }

  /**
   * 计算当前磁盘卷已用空间占总容量的比例。
   *
   * @return 已用空间比例，范围[0, 1]
   */
  @JsonIgnore
  public double getUsedRatio() {
    return (1.0 * getUsed()) / getCapacity();
  }

  /**
   * 计算当前磁盘卷可用空间占总容量的比例。
   *
   * @return 可用空间比例，范围[0, 1]
   */
  @JsonIgnore
  public double getFreeRatio() {
    return (1.0 * getFreeSpace()) / getCapacity();
  }

  /**
   * 设置当前磁盘卷的总容量。
   *
   * @param totalCapacity 总容量，单位字节
   */
  public void setCapacity(long totalCapacity) {
    this.capacity = totalCapacity;
  }

  /**
   * 获取当前磁盘卷是否故障。
   *
   * @return true表示卷故障，false表示卷正常
   */
  public boolean isFailed() {
    return failed;
  }

  /**
   * 设置当前磁盘卷的故障状态。
   *
   * @param fail true表示故障，false表示正常
   */
  public void setFailed(boolean fail) {
    this.failed = fail;
  }

  /**
   * 获取当前磁盘卷的挂载路径。
   *
   * @return 磁盘卷挂载路径字符串
   */
  public String getPath() {
    return path;
  }

  /**
   * 设置当前磁盘卷的挂载路径。
   *
   * @param volPath 挂载路径字符串
   */
  public void setPath(String volPath) {
    this.path = volPath;
  }

  /**
   * 获取当前磁盘卷的预留空间大小。
   *
   * @return 预留空间大小，单位字节
   */
  public long getReserved() {
    return reserved;
  }

  /**
   * 设置当前磁盘卷的预留空间大小。
   *
   * @param reservedSize 预留空间大小，单位字节
   */
  public void setReserved(long reservedSize) {
    this.reserved = reservedSize;
  }

  /**
   * 获取当前磁盘卷的存储类型。
   *
   * @return 存储类型字符串
   */
  public String getStorageType() {
    return storageType;
  }

  /**
   * 设置当前磁盘卷的存储类型。
   *
   * @param typeOfStorage 存储类型字符串
   */
  public void setStorageType(String typeOfStorage) {
    this.storageType = typeOfStorage;
  }

  /**
   * 获取当前磁盘卷已用空间大小。
   *
   * @return 已用空间大小，单位字节
   */
  public long getUsed() {
    return used;
  }

  /**
   * 设置当前磁盘卷已用空间大小，对异常值做校验修正。
   *
   * @param dfsUsedSpace 已用空间大小，单位字节
   */
  public void setUsed(long dfsUsedSpace) {
    if (dfsUsedSpace > this.getCapacity()) {
      // 已用空间超过总容量时，记录警告并修正为总容量
      LOG.warn("Volume usage ("+dfsUsedSpace+") is greater than capacity ("+
        this.getCapacity()+"). Setting volume usage to the capacity");
      this.used = this.getCapacity();
    } else {
      this.used = dfsUsedSpace;
    }
  }

  /**
   * 获取当前磁盘卷的唯一标识UUID。
   *
   * @return 当前卷的UUID字符串
   */
  public String getUuid() {
    return uuid;
  }

  /**
   * 设置当前磁盘卷的唯一标识UUID。
   *
   * @param id UUID字符串
   */
  public void setUuid(String id) {
    this.uuid = id;
  }

  /**
   * 计算当前磁盘卷的有效可用容量（总容量减去预留空间）。
   *
   * @return 有效容量，单位字节
   */
  @JsonIgnore
  public long computeEffectiveCapacity() {
    return getCapacity() - getReserved();
  }

  /**
   * 将当前DiskBalancerVolume对象序列化为JSON字符串。
   *
   * @return 序列化后的JSON字符串
   * @throws IOException 序列化过程中发生IO错误抛出
   */
  public String toJson() throws IOException {
    return JsonUtil.toJsonString(this);
  }

  /**
   * 获取当前卷是否需要被磁盘平衡跳过。
   * @return true表示跳过该卷，false表示参与平衡
   */
  public boolean isSkip() {
    return skip;
  }

  /**
   * 设置当前卷是否需要被磁盘平衡跳过。
   * @param skipValue true表示跳过，false表示参与平衡
   */
  public void setSkip(boolean skipValue) {
    this.skip = skipValue;
  }

  /**
   * 计算当前磁盘卷已用空间占总容量的百分比。
   * @return 已用百分比，范围[0, 1]
   */
  public float computeUsedPercentage() {
    return (float) (getUsed()) / (float) (getCapacity());
  }

  /**
   * 设置当前卷是否为瞬时存储。
   * @param transientValue true表示瞬时存储，false表示持久存储
   */
  public void setIsTransient(boolean transientValue) {
    this.isTransient = transientValue;
  }

  /**
   * 获取当前卷是否为只读。
   * @return true表示只读，false表示可写
   */
  public boolean isReadOnly() {
    return isReadOnly;
  }

  /**
   * 设置当前卷是否为只读。
   * @param readOnly true表示只读，false表示可写
   */
  public void setReadOnly(boolean readOnly) {
    isReadOnly = readOnly;
  }

}