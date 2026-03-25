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

import java.io.IOException;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.datanode.FileIoProvider;

/**
 * HDFS数据节点存储卷构建器，用于构建{@link FsVolumeImpl}及其子类对象，
 * 根据存储类型自动选择构建普通存储卷或外部提供存储卷，遵循Builder设计模式。
 */
public class FsVolumeImplBuilder {

  private FsDatasetImpl dataset;
  private String storageID;
  private StorageDirectory sd;
  private Configuration conf;
  private FileIoProvider fileIoProvider;
  private DF usage;

  /**
   * 构造函数，初始化所有构建参数为空。
   */
  public FsVolumeImplBuilder() {
    dataset = null;
    storageID = null;
    sd = null;
    conf = null;
    usage = null;
  }

  /**
   * 设置所属的FsDatasetImpl实例。
   * @param dataset 数据节点数据集实现
   * @return 当前构建器实例
   */
  FsVolumeImplBuilder setDataset(FsDatasetImpl dataset) {
    this.dataset = dataset;
    return this;
  }

  /**
   * 设置存储卷ID。
   * @param id 存储ID字符串
   * @return 当前构建器实例
   */
  FsVolumeImplBuilder setStorageID(String id) {
    this.storageID = id;
    return this;
  }

  /**
   * 设置存储目录对象。
   * @param sd 存储目录实例
   * @return 当前构建器实例
   */
  FsVolumeImplBuilder setStorageDirectory(StorageDirectory sd) {
    this.sd = sd;
    return this;
  }

  /**
   * 设置Hadoop配置对象。
   * @param conf 配置对象
   * @return 当前构建器实例
   */
  FsVolumeImplBuilder setConf(Configuration conf) {
    this.conf = conf;
    return this;
  }

  /**
   * 设置文件IO提供者，用于文件操作的切面扩展。
   * @param fileIoProvider 文件IO提供者实例
   * @return 当前构建器实例
   */
  FsVolumeImplBuilder setFileIoProvider(FileIoProvider fileIoProvider) {
    this.fileIoProvider = fileIoProvider;
    return this;
  }

  /**
   * 设置磁盘容量统计工具，仅用于单元测试注入mock对象。
   * @param newUsage 磁盘容量统计工具
   * @return 当前构建器实例
   */
  @VisibleForTesting
  FsVolumeImplBuilder setUsage(DF newUsage) {
    this.usage = newUsage;
    return this;
  }

  /**
   * 根据配置参数构建FsVolumeImpl实例，根据存储类型自动选择实现类。
   * @return 构建完成的存储卷实例
   * @throws IOException 创建存储卷过程中发生IO异常
   */
  FsVolumeImpl build() throws IOException {
    // 当前存储目录是外部提供存储类型，构建ProvidedVolumeImpl
    if (sd.getStorageLocation().getStorageType() == StorageType.PROVIDED) {
      return new ProvidedVolumeImpl(dataset, storageID, sd,
          fileIoProvider != null ? fileIoProvider :
            new FileIoProvider(null, null), conf);
    }
    // 未注入磁盘统计工具，自动创建实例（单元测试会提前注入）
    if (null == usage) {
      // set usage unless overridden by unit tests
      usage = new DF(sd.getCurrentDir().getParentFile(), conf);
    }
    // 构建普通存储卷FsVolumeImpl
    return new FsVolumeImpl(
        dataset, storageID, sd,
        fileIoProvider != null ? fileIoProvider :
            new FileIoProvider(null, null), conf, usage);
  }
}