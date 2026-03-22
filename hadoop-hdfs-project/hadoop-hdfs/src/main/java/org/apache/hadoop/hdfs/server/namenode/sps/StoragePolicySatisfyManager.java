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
package org.apache.hadoop.hdfs.server.namenode.sps;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.sps.ExternalStoragePolicySatisfier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件级注释：存储策略满足管理器，负责管理等待满足存储策略的目录路径，根据配置的模式（内置/外置/禁用）处理路径移动任务
 * 本类由BlockManager实例化，核心职责是维护待处理路径队列，根据管理员配置的SPS模式执行对应逻辑：
 * <ul>
 * <li>EXTERNAL模式：仅维护待处理路径队列，不执行实际块移动，由外部独立的SPS服务主动拉取路径并处理</li>
 * <li>NONE模式：完全禁用SPS功能，清空所有待处理路径</li>
 * </ul>
 */
public class StoragePolicySatisfyManager {
  private static final Logger LOG = LoggerFactory
      .getLogger(StoragePolicySatisfyManager.class);
  // 内置SPS服务实例
  private final StoragePolicySatisfier spsService;
  // 存储策略功能是否全局启用
  private final boolean storagePolicyEnabled;
  // 当前SPS运行模式，支持动态修改
  private volatile StoragePolicySatisfierMode mode;
  // 待遍历处理的路径ID队列，存储需要满足存储策略的目录ID
  private final Queue<Long> pathsToBeTraversed;
  // 队列中允许的最大待处理路径数量
  private final int outstandingPathsLimit;
  // NameSystem引用，用于操作路径扩展属性
  private final Namesystem namesystem;

  /**
   * 构造方法，初始化存储策略满足管理器，加载配置参数创建SPS服务实例
   * @param conf Hadoop配置对象
   * @param namesystem NameSystem实例
   */
  public StoragePolicySatisfyManager(Configuration conf,
      Namesystem namesystem) {
    // 读取存储策略全局启用配置
    storagePolicyEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY,
        DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_DEFAULT);
    // 读取SPS运行模式配置
    String modeVal = conf.get(
        DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_DEFAULT);
    // 读取最大待处理路径数量限制
    outstandingPathsLimit = conf.getInt(
        DFSConfigKeys.DFS_SPS_MAX_OUTSTANDING_PATHS_KEY,
        DFSConfigKeys.DFS_SPS_MAX_OUTSTANDING_PATHS_DEFAULT);
    // 解析运行模式
    mode = StoragePolicySatisfierMode.fromString(modeVal);
    // 初始化待处理路径队列
    pathsToBeTraversed = new LinkedList<Long>();
    this.namesystem = namesystem;
    // 仅初始化SPS服务，不启动服务线程
    spsService = new StoragePolicySatisfier(conf);
  }

  /**
   * 根据当前配置的SPS模式启动管理器，不同模式执行对应初始化逻辑
   */
  public void start() {
    if (!storagePolicyEnabled) {
      LOG.info("Disabling StoragePolicySatisfier service as {} set to {}.",
          DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY, storagePolicyEnabled);
      return;
    }

    switch (mode) {
    case EXTERNAL:
      LOG.info("Storage policy satisfier is configured as external, "
          + "please start external sps service explicitly to satisfy policy");
      break;
    case NONE:
      LOG.info("Storage policy satisfier is disabled");
      break;
    default:
      LOG.info("Given mode: {} is invalid", mode);
      break;
    }
  }

  /**
   * 根据当前配置的SPS模式停止管理器，清理待处理队列资源
   */
  public void stop() {
    if (!storagePolicyEnabled) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Storage policy is not enabled, ignoring");
      }
      return;
    }

    switch (mode) {
    case EXTERNAL:
      removeAllPathIds();
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Storage policy satisfier service is running outside namenode"
            + ", ignoring");
      }
      break;
    case NONE:
      if (LOG.isDebugEnabled()) {
        LOG.debug("Storage policy satisfier is not enabled, ignoring");
      }
      break;
    default:
      if (LOG.isDebugEnabled()) {
        LOG.debug("Invalid mode:{}, ignoring", mode);
      }
      break;
    }
  }

  /**
   * 处理SPS运行模式变更事件，切换模式并清理原有状态，若切换为禁用模式则清空所有待处理路径
   * @param newMode 新的运行模式
   */
  public void changeModeEvent(StoragePolicySatisfierMode newMode) {
    if (!storagePolicyEnabled) {
      LOG.info("Failed to change storage policy satisfier as {} set to {}.",
          DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY, storagePolicyEnabled);
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Updating SPS service status, current mode:{}, new mode:{}",
          mode, newMode);
    }

    switch (newMode) {
    case EXTERNAL:
      if (mode == newMode) {
        LOG.info("Storage policy satisfier is already in mode:{},"
            + " so ignoring change mode event.", newMode);
        return;
      }
      // 停止内置SPS服务，切换为外置模式
      spsService.stopGracefully();
      break;
    case NONE:
      if (mode == newMode) {
        LOG.info("Storage policy satisfier is already disabled, mode:{}"
            + " so ignoring change mode event.", newMode);
        return;
      }
      LOG.info("Disabling StoragePolicySatisfier, mode:{}", newMode);
      spsService.stop(true);
      clearPathIds();
      break;
    default:
      if (LOG.isDebugEnabled()) {
        LOG.debug("Given mode: {} is invalid", newMode);
      }
      break;
    }

    // 更新运行模式
    mode = newMode;
  }

  /**
   * 检查内置SPS守护进程是否正在运行，仅用于测试
   * @return 内置SPS正在运行返回true，否则返回false
   */
  @VisibleForTesting
  public boolean isSatisfierRunning() {
    return spsService.isRunning();
  }

  /**
   * 从待处理队列弹出下一个需要满足存储策略的路径ID，供外部SPS服务拉取任务
   * @return 下一个路径ID，队列为空时返回null
   */
  public Long getNextPathId() {
    synchronized (pathsToBeTraversed) {
      return pathsToBeTraversed.poll();
    }
  }

  /**
   * 检查待处理队列是否超过最大长度限制，超过则抛出异常拒绝新任务
   * @throws IOException 队列超出限制时抛出IO异常
   */
  public void verifyOutstandingPathQLimit() throws IOException {
    long size = pathsToBeTraversed.size();
    // 检查队列剩余容量是否小于等于0
    if (outstandingPathsLimit - size <= 0) {
      LOG.debug("Satisifer Q - outstanding limit:{}, current size:{}",
          outstandingPathsLimit, size);
      throw new IOException("Outstanding satisfier queue limit: "
          + outstandingPathsLimit + " exceeded, try later!");
    }
  }

  /**
   * 清空所有待处理路径ID，并移除对应路径上的SPS扩展属性
   */
  private void clearPathIds(){
    synchronized (pathsToBeTraversed) {
      Iterator<Long> iterator = pathsToBeTraversed.iterator();
      while (iterator.hasNext()) {
        Long trackId = iterator.next();
        try {
          // 移除路径上的SPS标记扩展属性
          namesystem.removeXattr(trackId,
              HdfsServerConstants.XATTR_SATISFY_STORAGE_POLICY);
        } catch (IOException e) {
          LOG.debug("Failed to remove sps xattr!", e);
        }
        iterator.remove();
      }
    }
  }

  /**
   * 清空所有待处理路径ID队列，不移除扩展属性
   */
  public void removeAllPathIds() {
    synchronized (pathsToBeTraversed) {
      pathsToBeTraversed.clear();
    }
  }

  /**
   * 添加新的需要处理的路径ID到队列
   * @param id 待处理路径的文件ID
   */
  public void addPathId(long id) {
    synchronized (pathsToBeTraversed) {
      pathsToBeTraversed.add(id);
    }
  }

  /**
   * 检查当前是否配置为外部SPS服务模式，即SPS功能是否启用
   * @return 外部模式返回true，其他模式返回false
   */
  public boolean isEnabled() {
    return mode == StoragePolicySatisfierMode.EXTERNAL;
  }

  /**
   * 获取当前SPS运行模式
   * @return 当前SPS模式枚举值
   */
  public StoragePolicySatisfierMode getMode() {
    return mode;
  }

  /**
   * 获取当前待处理的路径数量
   * @return 待处理路径总数
   */
  public int getPendingSPSPaths() {
    return pathsToBeTraversed.size();
  }
}