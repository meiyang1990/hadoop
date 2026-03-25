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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 文件级注释：底层文件系统挂载点与DataNode卷的映射关系管理类，用于支持同一磁盘挂载点下多存储类型卷的分层存储配置。
 *
 * 类级注释：维护底层文件系统挂载点与DataNode卷之间的关系，支持同一磁盘挂载点上配置多个不同存储类型的卷，实现块分层存储。
 * 当前设计不支持同一挂载点上配置多个相同存储类型的卷。
 */
@InterfaceAudience.Private
public class MountVolumeMap {
  private final ConcurrentMap<String, MountVolumeInfo>
      mountVolumeMapping;
  private final Configuration conf;

  /**
   * 构造函数：初始化挂载点卷映射表
   * @param conf Hadoop配置对象
   */
  MountVolumeMap(Configuration conf) {
    mountVolumeMapping = new ConcurrentHashMap<>();
    this.conf = conf;
  }

  /**
   * 根据挂载点和存储类型获取对应卷的引用
   * @param mount 挂载点路径
   * @param storageType 存储类型
   * @return 对应卷的引用，不存在则返回null
   */
  FsVolumeReference getVolumeRefByMountAndStorageType(String mount,
      StorageType storageType) {
    if (mountVolumeMapping.containsKey(mount)) {
      return mountVolumeMapping
          .get(mount).getVolumeRef(storageType);
    }
    return null;
  }

  /**
   * Return capacity ratio.
   * If not exists, return 1 to use full capacity.
   * 根据挂载点和存储类型获取容量占比，如果不存在则返回1表示使用全部容量
   * @param mount 挂载点路径
   * @param storageType 存储类型
   * @return 容量占比，不存在则返回1
   */
  double getCapacityRatioByMountAndStorageType(String mount,
      StorageType storageType) {
    if (mountVolumeMapping.containsKey(mount)) {
      return mountVolumeMapping.get(mount).getCapacityRatio(storageType);
    }
    return 1;
  }

  /**
   * 向映射表中添加一个卷
   * @param volume 要添加的DataNode卷对象
   */
  void addVolume(FsVolumeImpl volume) {
    String mount = volume.getMount();
    if (!mount.isEmpty()) {
      MountVolumeInfo info;
      if (mountVolumeMapping.containsKey(mount)) {
        info = mountVolumeMapping.get(mount);
      } else {
        info = new MountVolumeInfo(conf);
        mountVolumeMapping.put(mount, info);
      }
      info.addVolume(volume);
    }
  }

  /**
   * 从映射表中移除指定卷
   * @param target 要移除的目标卷
   */
  void removeVolume(FsVolumeImpl target) {
    String mount = target.getMount();
    if (!mount.isEmpty()) {
      MountVolumeInfo info = mountVolumeMapping.get(mount);
      info.removeVolume(target);
      if (info.size() == 0) {
        // 挂载点下没有卷时，移除该挂载点
        mountVolumeMapping.remove(mount);
      }
    }
  }

  /**
   * 设置指定卷的容量占比，同一挂载点下所有卷的容量占比总和不能超过1
   * @param target 目标卷
   * @param capacityRatio 要设置的容量占比
   * @throws IOException 容量占比总和超过1时抛出异常
   */
  void setCapacityRatio(FsVolumeImpl target, double capacityRatio)
      throws IOException {
    String mount = target.getMount();
    if (!mount.isEmpty()) {
      MountVolumeInfo info = mountVolumeMapping.get(mount);
      if (!info.setCapacityRatio(
          target.getStorageType(), capacityRatio)) {
        throw new IOException(
            "Not enough capacity ratio left on mount: "
                + mount + ", for " + target + ": capacity ratio: "
                + capacityRatio + ". Sum of the capacity"
                + " ratio of on same disk mount should be <= 1");
      }
    }
  }

  /**
   * 检查映射表中是否存在指定挂载点
   * @param mount 挂载点路径
   * @return 存在返回true，否则返回false
   */
  public boolean hasMount(String mount) {
    return mountVolumeMapping.containsKey(mount);
  }
}