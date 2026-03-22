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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.FormatConfirmable;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

/**
 * 文件级注释：HDFS NameNode编辑日志存储管理器接口，定义了单个编辑日志存储位置的核心操作契约
 * 
 * JournalManager负责管理单个编辑日志存储位置，该位置可以对应多个文件、备份节点等。
 * 即使底层存储发生滚动、故障恢复，每个逻辑存储位置始终对应一个该类的实例，实例在编辑日志首次打开时创建。
 * 该接口是HDFS高可用和EditLog持久化机制的核心抽象，支持多种存储实现（本地磁盘、共享存储、QJM等）。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface JournalManager extends Closeable, FormatConfirmable,
    LogsPurgeable {

  /**
   * 格式化底层存储，清除所有先前存储的数据，用于初始化新的NameNode命名空间
   * @param ns 命名空间信息，包含存储版本、集群ID等元数据
   * @param force 是否强制格式化，即使存储已有数据
   * @throws IOException 格式化过程中发生IO异常
   */
  void format(NamespaceInfo ns, boolean force) throws IOException;

  /**
   * 开始写入一个新的日志分段，该分段从指定事务ID开始
   * @param txId 新分段起始事务ID
   * @param layoutVersion HDFS存储布局版本
   * @return 新日志分段的输出流
   * @throws IOException 打开输出流过程中发生IO异常
   */
  EditLogOutputStream startLogSegment(long txId, int layoutVersion)
      throws IOException;

  /**
   * 将从firstTxId到lastTxId的日志分段标记为已完成（最终化）
   * @param firstTxId 分段起始事务ID
   * @param lastTxId 分段结束事务ID
   * @throws IOException 最终化过程中发生IO异常
   */
  void finalizeLogSegment(long firstTxId, long lastTxId) throws IOException;

  /**
   * 设置该日志流用于缓存编辑的内存缓冲区容量
   * @param size 缓冲区容量大小（字节）
   */
  void setOutputBufferCapacity(int size);

  /**
   * 恢复所有未完成最终化的日志分段，用于NameNode启动时的故障恢复
   * @throws IOException 恢复过程中发生IO异常
   */
  void recoverUnfinalizedSegments() throws IOException;
  
  /**
   * 在升级正式开始前，执行所有JournalManager必须成功完成的前置检查步骤
   * 如果任意一个JournalManager的前置升级操作失败，则不会执行任何JournalManager的正式升级
   * @throws IOException 前置升级检查失败
   */
  void doPreUpgrade() throws IOException;
  
  /**
   * 执行JournalManager的实际升级操作，完成后NameNode可以开始使用升级后的元数据
   * 升级完成后，后续可以选择最终化升级或回滚到升级前状态
   * @param storage 新升级版本的存储信息
   * @throws IOException 升级过程中发生IO异常
   */
  void doUpgrade(Storage storage) throws IOException;
  
  /**
   * 最终化升级操作，清理升级过程中保留的旧状态数据
   * 完成最终化后，不再允许回滚到升级前状态
   * @throws IOException 最终化过程中发生IO异常
   */
  void doFinalize() throws IOException;
  
  /**
   * 检查当前JournalManager是否支持回滚到升级前的存储状态
   * 至少有一个JournalManager或fsimage存储目录支持回滚，NameNode才会允许执行回滚操作
   * @param storage 当前状态的存储信息
   * @param prevStorage 升级前状态的存储信息
   * @param targetLayoutVersion 目标回滚的布局版本
   * @return 如果支持回滚返回true，否则返回false
   * @throws IOException 检查过程中发生IO异常
   */
  boolean canRollBack(StorageInfo storage, StorageInfo prevStorage,
      int targetLayoutVersion) throws IOException;
  
  /**
   * 执行回滚操作，恢复到升级前的文件系统状态
   * 不需要回滚自身状态的JournalManager直接返回成功即可
   * @throws IOException 回滚过程中发生IO异常
   */
  void doRollback() throws IOException;

  /**
   * 丢弃所有第一个事务ID大于等于指定txId的日志分段
   * 该方法用于回滚事务时清理超出NameNode当前状态的日志分段
   * @param startTxId 起始事务ID，必须是某个分段的第一个事务ID
   * @throws IOException 丢弃分段过程中发生IO异常
   */
  void discardSegments(long startTxId) throws IOException;

  /**
   * 获取当前JournalManager的创建时间（CTime）
   * @return JournalManager的创建时间戳
   * @throws IOException 获取过程中发生IO异常
   */
  long getJournalCTime() throws IOException;

  /**
   * 关闭JournalManager，释放占用的所有资源
   * @throws IOException 关闭过程中发生IO异常
   */
  @Override
  void close() throws IOException;
  
  /** 
   * 日志损坏异常，当Journal无法加载指定范围的编辑日志时抛出
   * 通常由事务断裂（存在缺口）或编辑文件损坏导致
   */
  public static class CorruptionException extends IOException {
    static final long serialVersionUID = -4687802717006172702L;
    
    public CorruptionException(String reason) {
      super(reason);
    }
  }

}