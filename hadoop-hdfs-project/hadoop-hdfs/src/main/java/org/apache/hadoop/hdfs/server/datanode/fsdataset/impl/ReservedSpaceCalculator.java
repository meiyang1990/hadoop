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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.util.StringUtils;

import java.lang.reflect.Constructor;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DU_RESERVED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DU_RESERVED_PERCENTAGE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DU_RESERVED_CALCULATOR_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DU_RESERVED_CALCULATOR_KEY;

/**
 * 文件系统预留空间计算器抽象基类，用于计算DataNode磁盘上为非HDFS数据预留的空间大小，
 * 确保操作系统和其他应用有足够磁盘空间可用，避免HDFS占满整个磁盘。
 */
public abstract class ReservedSpaceCalculator {

  /**
   * ReservedSpaceCalculator的构造器，用于根据配置反射创建具体计算器实例，
   * 支持用户自定义预留空间计算策略。
   */
  public static class Builder {

    private final Configuration conf;

    private DF usage;
    private StorageType storageType;

    private String dir;

    public Builder(Configuration conf) {
      this.conf = conf;
    }

    public Builder setUsage(DF newUsage) {
      this.usage = newUsage;
      return this;
    }

    public Builder setStorageType(
        StorageType newStorageType) {
      this.storageType = newStorageType;
      return this;
    }

    public Builder setDir(String newDir) {
      this.dir = newDir;
      return this;
    }

    ReservedSpaceCalculator build() {
      try {
        // 从配置中获取计算器实现类，若无配置则使用默认实现
        Class<? extends ReservedSpaceCalculator> clazz = conf.getClass(
            DFS_DATANODE_DU_RESERVED_CALCULATOR_KEY,
            DFS_DATANODE_DU_RESERVED_CALCULATOR_DEFAULT,
            ReservedSpaceCalculator.class);
        // 获取对应构造方法
        Constructor constructor = clazz.getConstructor(
            Configuration.class, DF.class, StorageType.class, String.class);
        // 通过反射创建实例并返回
        return (ReservedSpaceCalculator) constructor.newInstance(
            conf, usage, storageType, dir);
      } catch (Exception e) {
        throw new IllegalStateException(
            "Error instantiating ReservedSpaceCalculator", e);
      }
    }
  }

  private final DF usage;
  private final Configuration conf;
  private final StorageType storageType;

  private final String dir;

  ReservedSpaceCalculator(Configuration conf, DF usage,
      StorageType storageType, String dir) {
    this.usage = usage;
    this.conf = conf;
    this.storageType = storageType;
    this.dir = dir;
  }

  DF getUsage() {
    return usage;
  }

  String getDir() {
    return dir;
  }

  /**
   * 从配置中按优先级读取预留空间数值，优先级：目录+存储类型 > 目录 > 存储类型 > 全局默认。
   * @param key 配置项键
   * @param defaultValue 默认值
   * @return 解析后的配置数值
   */
  long getReservedFromConf(String key, long defaultValue) {
    return conf.getLong(
        key + "." + getDir() + "." + StringUtils.toLowerCase(storageType.toString()),
        conf.getLong(key + "." + getDir(),
            conf.getLong(key + "." + StringUtils.toLowerCase(storageType.toString()),
                conf.getLongBytes(key, defaultValue))));
  }

  /**
   * 计算并返回为非HDFS数据预留的空间字节数。
   *
   * @return 预留空间字节数
   */
  abstract long getReserved();


  /**
   * 基于固定绝对字节数的预留空间计算器实现，预留空间大小为配置的固定值。
   */
  public static class ReservedSpaceCalculatorAbsolute extends
      ReservedSpaceCalculator {

    private final long reservedBytes;

    public ReservedSpaceCalculatorAbsolute(Configuration conf, DF usage,
        StorageType storageType, String dir) {
      super(conf, usage, storageType, dir);
      this.reservedBytes = getReservedFromConf(DFS_DATANODE_DU_RESERVED_KEY,
          DFS_DATANODE_DU_RESERVED_DEFAULT);
    }

    @Override
    long getReserved() {
      return reservedBytes;
    }
  }

  /**
   * 基于磁盘总容量百分比的预留空间计算器实现，预留空间按总容量的百分比计算。
   */
  public static class ReservedSpaceCalculatorPercentage extends
      ReservedSpaceCalculator {

    private final long reservedPct;

    public ReservedSpaceCalculatorPercentage(Configuration conf, DF usage,
        StorageType storageType, String dir) {
      super(conf, usage, storageType, dir);
      this.reservedPct = getReservedFromConf(
          DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY,
          DFS_DATANODE_DU_RESERVED_PERCENTAGE_DEFAULT);
    }

    @Override
    long getReserved() {
      return getPercentage(getUsage().getCapacity(), reservedPct);
    }
  }

  /**
   * 保守策略预留空间计算器，同时计算绝对数值和百分比两种方式，取结果更大的值作为预留空间。
   * 该策略会预留更多空间给非HDFS使用，HDFS可用空间更少，更加安全。
   */
  public static class ReservedSpaceCalculatorConservative extends
      ReservedSpaceCalculator {

    private final long reservedBytes;
    private final long reservedPct;

    public ReservedSpaceCalculatorConservative(Configuration conf, DF usage,
        StorageType storageType, String dir) {
      super(conf, usage, storageType, dir);
      this.reservedBytes = getReservedFromConf(DFS_DATANODE_DU_RESERVED_KEY,
          DFS_DATANODE_DU_RESERVED_DEFAULT);
      this.reservedPct = getReservedFromConf(
          DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY,
          DFS_DATANODE_DU_RESERVED_PERCENTAGE_DEFAULT);
    }

    long getReservedBytes() {
      return reservedBytes;
    }

    long getReservedPct() {
      return reservedPct;
    }

    @Override
    long getReserved() {
      return Math.max(getReservedBytes(),
          getPercentage(getUsage().getCapacity(), getReservedPct()));
    }
  }

  /**
   * 激进策略预留空间计算器，同时计算绝对数值和百分比两种方式，取结果更小的值作为预留空间。
   * 该策略会预留更少空间给非HDFS使用，HDFS可用空间更多，适合对空间利用率要求高的场景。
   */
  public static class ReservedSpaceCalculatorAggressive extends
      ReservedSpaceCalculator {

    private final long reservedBytes;
    private final long reservedPct;

    public ReservedSpaceCalculatorAggressive(Configuration conf, DF usage,
        StorageType storageType, String dir) {
      super(conf, usage, storageType, dir);
      this.reservedBytes = getReservedFromConf(DFS_DATANODE_DU_RESERVED_KEY,
          DFS_DATANODE_DU_RESERVED_DEFAULT);
      this.reservedPct = getReservedFromConf(
          DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY,
          DFS_DATANODE_DU_RESERVED_PERCENTAGE_DEFAULT);
    }

    long getReservedBytes() {
      return reservedBytes;
    }

    long getReservedPct() {
      return reservedPct;
    }

    @Override
    long getReserved() {
      return Math.min(getReservedBytes(),
          getPercentage(getUsage().getCapacity(), getReservedPct()));
    }
  }

  /**
   * 计算总容量对应百分比的字节数。
   * @param total 总容量字节数
   * @param percentage 百分比数值（如10代表10%）
   * @return 对应百分比的字节数
   */
  private static long getPercentage(long total, long percentage) {
    return (total * percentage) / 100;
  }
}