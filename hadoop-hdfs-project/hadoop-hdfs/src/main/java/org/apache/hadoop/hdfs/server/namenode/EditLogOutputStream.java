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
import java.io.Closeable;

import static org.apache.hadoop.util.Time.monotonicNow;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

/**
 * @file org/apache/hadoop/hdfs/server/namenode/EditLogOutputStream.java
 * @brief HDFS NameNode编辑日志输出抽象基类，支持将编辑日志持久化到存储介质
 * 
 * 该抽象类定义了编辑日志输出的统一接口，为不同的持久化存储实现提供公共模板，
 * 统计同步操作耗时和次数等监控指标，是NameNode编辑日志写入流程的核心抽象。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class EditLogOutputStream implements Closeable {
  // 同步操作统计计数器
  private long numSync;        // 磁盘同步操作次数
  private long totalTimeSync;  // 同步操作总耗时
  // 当前编辑日志的版本号
  private int currentLogVersion;

  /**
   * 构造方法，初始化同步统计计数器
   * @throws IOException 构造过程可能抛出IO异常
   */
  public EditLogOutputStream() throws IOException {
    numSync = totalTimeSync = 0;
  }

  /**
   * 获取当前流中已经写入的最后一个事务ID
   * JournalSet会统一管理所有底层流的事务ID，默认实现返回无效事务ID
   * @return 最后写入的事务ID，默认返回INVALID_TXID
   */
  public long getLastJournalledTxId() {
    return HdfsServerConstants.INVALID_TXID;
  };

  /**
   * 将一条编辑日志操作写入输出流
   * @param op 要写入的编辑日志操作对象
   * @throws IOException 写入过程IO异常
   */
  abstract public void write(FSEditLogOp op) throws IOException;

  /**
   * 将已经格式化好的原始字节数据直接写入编辑日志
   * 用于BackupNode从NameNode复制编辑日志场景，数据已经包含事务ID、校验和等信息
   * @param bytes 要写入的字节数组
   * @param offset 起始偏移量
   * @param length 要写入的字节长度
   * @throws IOException 写入过程IO异常
   */
  abstract public void writeRaw(byte[] bytes, int offset, int length)
      throws IOException;

  /**
   * 创建并初始化底层持久化编辑日志存储
   * @param layoutVersion 日志存储的布局版本号
   * @throws IOException 初始化过程IO异常
   */
  abstract public void create(int layoutVersion) throws IOException;

  /**
   * 关闭编辑日志输出流，会刷新未持久化的数据
   * @throws IOException 关闭过程IO异常，或存在未刷新数据时抛出
   */
  @Override
  abstract public void close() throws IOException;

  /**
   * 中止输出流，不保证刷新未持久化的数据，常用于出现异常后的清理
   * @throws IOException 中止过程IO异常
   */
  abstract public void abort() throws IOException;
  
  /**
   * 将当前已写入的数据标记为可刷新，允许后续刷新操作持久化数据，刷新过程仍可继续写入新数据
   * @throws IOException 标记过程IO异常
   */
  abstract public void setReadyToFlush() throws IOException;

  /**
   * 将已标记为就绪的数据刷新并同步到底层持久化存储
   * @param durable 如果为true，则需要确保数据真正持久化到磁盘后再返回
   * @throws IOException 刷新同步过程IO异常
   */
  abstract protected void flushAndSync(boolean durable) throws IOException;

  /**
   * 刷新数据到持久化存储，默认持久化并收集同步监控指标
   * @throws IOException 刷新过程IO异常
   */
  public void flush() throws IOException {
    flush(true);
  }
  
  /**
   * 刷新数据到持久化存储，统计同步操作次数和耗时
   * @param durable 是否需要确保数据持久化到磁盘
   * @throws IOException 刷新过程IO异常
   */
  public void flush(boolean durable) throws IOException {
    // 同步次数累加
    numSync++;
    // 记录同步开始时间
    long start = monotonicNow();
    // 执行实际刷新同步
    flushAndSync(durable);
    // 计算同步耗时并累加总耗时
    long end = monotonicNow();
    totalTimeSync += (end - start);
  }

  /**
   * 判断是否需要强制同步缓冲的编辑日志，实现自动同步策略
   * 当缓冲已满或超过指定时间间隔时，触发自动同步
   * @return true表示缓冲数据需要自动同步到磁盘
   */
  public boolean shouldForceSync() {
    return false;
  }
  
  /**
   * 获取所有同步操作的总耗时
   * @return 同步总耗时，单位毫秒
   */
  long getTotalSyncTime() {
    return totalTimeSync;
  }

  /**
   * 获取同步操作的总次数
   * @return 同步操作次数
   */
  protected long getNumSync() {
    return numSync;
  }

  /**
   * 生成当前输出流状态的简要描述报告，用于监控和调试
   * @return 状态描述文本
   */
  public String generateReport() {
    return toString();
  }

  /**
   * 获取当前编辑日志的版本号
   * @return 当前日志版本号
   */
  public int getCurrentLogVersion() {
    return currentLogVersion;
  }

  /**
   * 设置当前编辑日志的版本号
   * @param logVersion 要设置的日志版本号
   */
  public void setCurrentLogVersion(int logVersion) {
    this.currentLogVersion = logVersion;
  }
}