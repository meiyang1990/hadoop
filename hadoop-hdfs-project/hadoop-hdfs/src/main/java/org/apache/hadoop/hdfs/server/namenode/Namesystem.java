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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.util.RwLock;

/**
 * 文件系统命名空间顶层接口，定义了NameNode核心命名系统需要实现的基础操作。
 * 整合了读写锁和安全模式能力，为HDFS命名空间管理提供统一抽象。
 */
@InterfaceAudience.Private
public interface Namesystem extends RwLock, SafeMode {
  /**
   * 检查当前命名系统是否处于运行状态
   * @return true表示正在运行，false表示未运行
   */
  boolean isRunning();

  /**
   * 根据ID获取块集合对象（对应文件或目录的块信息）
   * @param id 块集合ID
   * @return 对应的块集合对象
   */
  BlockCollection getBlockCollection(long id);

  /**
   * 获取文件系统目录管理对象
   * @return FSDirectory实例，负责目录树和inode管理
   */
  FSDirectory getFSDirectory();

  /**
   * 如果需要则启动密钥管理器（用于加密功能）
   */
  void startSecretManagerIfNecessary();

  /**
   * 检查指定块集合是否处于快照中
   * @param blockCollectionID 块集合ID
   * @return true表示处于快照中，false否则
   */
  boolean isInSnapshot(long blockCollectionID);

  /**
   * 获取缓存管理器
   * @return 缓存管理器实例，负责HDFS缓存管理
   */
  CacheManager getCacheManager();

  /**
   * 获取高可用上下文对象
   * @return HA上下文，包含HA相关状态和配置
   */
  HAContext getHAContext();

  /**
   * 检查NameNode是否正在切换到激活状态，且处于启动激活服务过程中
   * @return true表示正在切换激活状态，false否则
   */
  boolean inTransitionToActive();

  /**
   * 从inode中移除指定扩展属性
   * @param id inode ID
   * @param xattrName 扩展属性名称
   * @throws IOException IO异常
   */
  void removeXattr(long id, String xattrName) throws IOException;

  /**
   * 检查并为所有已开启快照的目录创建快照回收站根目录，如果不存在则创建
   */
  void checkAndProvisionSnapshotTrashRoots();
}