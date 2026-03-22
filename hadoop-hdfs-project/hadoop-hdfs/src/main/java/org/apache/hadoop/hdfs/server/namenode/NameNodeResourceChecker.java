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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.Util;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件概述：NameNode磁盘资源检查器，负责检查NameNode所有需检查卷的可用磁盘空间是否满足要求
 * NameNodeResourceChecker提供了<code>hasAvailableDiskSpace</code>方法，
 * 当且仅当所有必填卷和配置要求数量的冗余卷都有可用磁盘空间时才返回true。
 * 默认会把包含edits目录的卷加入检查列表，同时也支持配置额外的任意卷进行检查。
 */
@InterfaceAudience.Private
public class NameNodeResourceChecker {
  private static final Logger LOG =
      LoggerFactory.getLogger(NameNodeResourceChecker.class.getName());

  // 每个卷预留的空间大小（字节）
  private final long duReserved;

  private final Configuration conf;
  private Map<String, CheckedVolume> volumes;
  private int minimumRedundantVolumes;
  
  /**
   * 表示一个需要检查的磁盘卷，实现资源可用性检查接口
   */
  @VisibleForTesting
  class CheckedVolume implements CheckableNameNodeResource {
    private DF df;
    private boolean required;
    private String volume;
    
    /**
     * 构造待检查磁盘卷对象，初始化磁盘空间查询器
     * @param dirToCheck 待检查的目录
     * @param required 是否为必填卷
     * @throws IOException 初始化失败时抛出异常
     */
    public CheckedVolume(File dirToCheck, boolean required)
        throws IOException {
      df = new DF(dirToCheck, conf);
      this.required = required;
      volume = df.getFilesystem();
    }
    
    /**
     * 获取当前卷的文件系统路径标识
     * @return 文件系统路径字符串
     */
    public String getVolume() {
      return volume;
    }
    
    @Override
    public boolean isRequired() {
      return required;
    }

    @Override
    public boolean isResourceAvailable() {
      // 获取当前卷可用空间
      long availableSpace = df.getAvailable();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Space available on volume '" + volume + "' is "
            + availableSpace);
      }
      // 可用空间低于预留阈值，资源不可用
      if (availableSpace < duReserved) {
        LOG.warn("Space available on volume '" + volume + "' is "
            + availableSpace +
            ", which is below the configured reserved amount " + duReserved);
        return false;
      } else {
        return true;
      }
    }
    
    @Override
    public String toString() {
      return "volume: " + volume + " required: " + required +
          " resource available: " + isResourceAvailable();
    }
  }

  /**
   * 构造NameNode资源检查器，从配置中加载所有需要检查的目录
   * @param conf Hadoop配置对象
   * @throws IOException 加载目录失败时抛出异常
   */
  public NameNodeResourceChecker(Configuration conf) throws IOException {
    this.conf = conf;
    volumes = new HashMap<String, CheckedVolume>();

    // 从配置中读取每个卷预留的空间大小
    duReserved = conf.getLongBytes(DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY,
        DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_DEFAULT);
    
    // 获取额外配置需要检查的卷
    Collection<URI> extraCheckedVolumes = Util.stringCollectionAsURIs(conf
        .getTrimmedStringCollection(DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_KEY));

    // 过滤出所有本地edits目录
    Collection<URI> localEditDirs =
        FSNamesystem.getNamespaceEditsDirs(conf).stream().filter(
            input -> {
              if (input.getScheme().equals(NNStorage.LOCAL_URI_SCHEME)) {
                return true;
              }
              return false;
            }).collect(Collectors.toList());

    // 将所有本地edits目录加入检查列表，根据配置标记是否为必填
    for (URI editsDirToCheck : localEditDirs) {
      addDirToCheck(editsDirToCheck,
          FSNamesystem.getRequiredNamespaceEditsDirs(conf).contains(
              editsDirToCheck));
    }

    // 所有额外配置的检查卷都标记为必填
    for (URI extraDirToCheck : extraCheckedVolumes) {
      addDirToCheck(extraDirToCheck, true);
    }
    
    // 从配置读取最小要求可用冗余卷数量
    minimumRedundantVolumes = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY,
        DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_DEFAULT);
  }

  /**
   * 将指定目录所在的卷添加到检查列表，处理卷的必填/冗余属性变更
   * @param directoryToCheck 待检查目录URI
   * @param required 是否为必填卷
   * @throws IOException 目录不存在时抛出异常
   */
  private void addDirToCheck(URI directoryToCheck, boolean required)
      throws IOException {
    File dir = new File(directoryToCheck.getPath());
    // 目录不存在直接抛出异常终止检查
    if (!dir.exists()) {
      throw new IOException("Missing directory "+dir.getAbsolutePath());
    }
    
    CheckedVolume newVolume = new CheckedVolume(dir, required);
    CheckedVolume volume = volumes.get(newVolume.getVolume());
    // 如果卷不存在，或原先是非必填现在改为必填，则更新列表
    if (volume == null || !volume.isRequired()) {
      volumes.put(newVolume.getVolume(), newVolume);
    }
  }

  /**
   * 检查所有卷的资源可用性：所有必填卷可用，且冗余可用卷数量不低于配置最小值
   * @return 满足要求返回true，否则返回false
   */
  public boolean hasAvailableDiskSpace() {
    return NameNodeResourcePolicy.areResourcesAvailable(volumes.values(),
        minimumRedundantVolumes);
  }

  /**
   * 获取空间不足的卷列表，仅用于测试
   * @return 空间不足的卷路径集合
   * @throws IOException 检查过程IO异常
   */
  @VisibleForTesting
  Collection<String> getVolumesLowOnSpace() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Going to check the following volumes disk space: " + volumes);
    }
    Collection<String> lowVolumes = new ArrayList<String>();
    for (CheckedVolume volume : volumes.values()) {
      lowVolumes.add(volume.getVolume());
    }
    return lowVolumes;
  }
  
  @VisibleForTesting
  void setVolumes(Map<String, CheckedVolume> volumes) {
    this.volumes = volumes;
  }
  
  @VisibleForTesting
  void setMinimumReduntdantVolumes(int minimumRedundantVolumes) {
    this.minimumRedundantVolumes = minimumRedundantVolumes;
  }
}