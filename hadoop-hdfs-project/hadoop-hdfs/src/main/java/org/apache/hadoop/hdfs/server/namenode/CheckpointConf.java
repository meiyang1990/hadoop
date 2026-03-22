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

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;

import java.util.concurrent.TimeUnit;

/**
 * HDFS NameNode检查点（Checkpoint）配置容器类，封装SecondaryNameNode/StandbyNameNode执行检查点操作的所有配置参数
 * 负责从Hadoop配置中加载检查点相关参数，并提供参数查询接口，供检查点流程使用
 */
@InterfaceAudience.Private
public class CheckpointConf {
  private static final Logger LOG =
      LoggerFactory.getLogger(CheckpointConf.class);
  
  // 强制触发检查点的时间周期，无论事务数量多少，单位：秒
  private final long checkpointPeriod;    // in seconds
  
  // 检查是否满足检查点条件的轮询间隔，单位：秒
  private final long checkpointCheckPeriod; // in seconds
  
  // 达到该事务数量后触发检查点，无论时间间隔，单位：事务数
  private final long checkpointTxnCount;

  // 合并镜像发生错误时的最大重试次数
  private final int maxRetriesOnMergeError;

  // 离线镜像查看器（OIV）旧版镜像输出目录
  private final String legacyOivImageDir;

  /**
  * 非主检查点节点的检查点周期乘数，用于错开不同节点的检查点时间，避免多节点同时执行检查点
  */
  private double quietMultiplier;

  /**
   * 在包含Observer NameNode的集群中，是否允许Standby NameNode并行向多个NameNode上传FsImage
   */
  private final boolean parallelUploadEnabled;

  /**
   * 从Configuration加载所有检查点相关配置，构造CheckpointConf配置对象
   * @param conf Hadoop配置对象，用于提取检查点参数
   */
  public CheckpointConf(Configuration conf) {
    checkpointCheckPeriod = conf.getTimeDuration(
        DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY,
        DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_DEFAULT, TimeUnit.SECONDS);
        
    checkpointPeriod = conf.getTimeDuration(DFS_NAMENODE_CHECKPOINT_PERIOD_KEY,
        DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT, TimeUnit.SECONDS);
    checkpointTxnCount = conf.getLong(DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 
                                  DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT);
    maxRetriesOnMergeError = conf.getInt(DFS_NAMENODE_CHECKPOINT_MAX_RETRIES_KEY,
                                  DFS_NAMENODE_CHECKPOINT_MAX_RETRIES_DEFAULT);
    legacyOivImageDir = conf.get(DFS_NAMENODE_LEGACY_OIV_IMAGE_DIR_KEY);
    quietMultiplier = conf.getDouble(DFS_NAMENODE_CHECKPOINT_QUIET_MULTIPLIER_KEY,
      DFS_NAMENODE_CHECKPOINT_QUIET_MULTIPLIER_DEFAULT);
    parallelUploadEnabled = conf.getBoolean(
        DFS_NAMENODE_CHECKPOINT_PARALLEL_UPLOAD_ENABLED_KEY,
        DFS_NAMENODE_CHECKPOINT_PARALLEL_UPLOAD_ENABLED_DEFAULT);
    // 检查并警告已废弃的配置项
    warnForDeprecatedConfigs(conf);
  }
  
  /**
   * 检查配置中是否存在已废弃的检查点配置项，输出警告日志提示用户替换
   * @param conf Hadoop配置对象
   */
  private static void warnForDeprecatedConfigs(Configuration conf) {
    // 遍历已废弃的按大小触发检查点的配置键
    for (String key : ImmutableList.of(
          "fs.checkpoint.size",
          "dfs.namenode.checkpoint.size")) {
      if (conf.get(key) != null) {
        // 存在废弃配置时输出警告，提示用户替换为事务数量配置
        LOG.warn("Configuration key " + key + " is deprecated! Ignoring..." +
            " Instead please specify a value for " +
            DFS_NAMENODE_CHECKPOINT_TXNS_KEY);
      }
    }
  }

  /**
   * 获取强制检查点时间周期
   * @return 检查点周期，单位：秒
   */
  public long getPeriod() {
    return checkpointPeriod;
  }

  /**
   * 获取检查条件轮询间隔，保证轮询间隔不超过检查点周期本身
   * @return 实际使用的轮询间隔，单位：秒
   */
  public long getCheckPeriod() {
    return Math.min(checkpointCheckPeriod, checkpointPeriod);
  }

  /**
   * 获取触发检查点的事务数量阈值
   * @return 事务数量阈值
   */
  public long getTxnCount() {
    return checkpointTxnCount;
  }

  /**
   * 获取合并镜像出错后的最大重试次数
   * @return 最大重试次数
   */
  public int getMaxRetriesOnMergeError() {
    return maxRetriesOnMergeError;
  }

  /**
   * 获取旧版OIV镜像输出目录
   * @return 目录路径字符串
   */
  public String getLegacyOivImageDir() {
    return legacyOivImageDir;
  }

  /**
   * 获取非主检查点节点的安静检查点周期，错开多节点检查点执行时间
   * @return 计算后的实际检查点周期
   */
  public double getQuietPeriod() {
    return this.checkpointPeriod * this.quietMultiplier;
  }

  /**
   * 查询是否启用并行上传FsImage到多个NameNode
   * @return true表示启用，false表示禁用
   */
  public boolean isParallelUploadEnabled() {
    return parallelUploadEnabled;
  }
}