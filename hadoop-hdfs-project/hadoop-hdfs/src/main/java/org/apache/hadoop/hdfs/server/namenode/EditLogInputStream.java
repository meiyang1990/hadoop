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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

import java.io.Closeable;
import java.io.IOException;

/**
 * @file EditLogInputStream.java
 * @brief HDFS命名空间编辑日志的抽象输入流基类，支持从持久化存储读取编辑日志数据
 * 
 * 该抽象类定义了读取编辑日志的统一接口，子类实现不同存储介质（本地文件、远程存储等）
 * 的具体读取逻辑，保证读取到的字节流和EditLogOutputStream写入的完全一致。
 * 核心作用是为NameNode的编辑日志回放提供统一的输入抽象，支持错误恢复和跳转定位。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class EditLogInputStream implements Closeable {
  // 缓存预读取的编辑日志操作，用于重新同步和跳过操作时暂存
  private FSEditLogOp cachedOp = null; 

  /**
   * 获取当前活跃的底层流名称，默认实现直接返回getName，子类可覆盖
   * @return 当前底层流的名称
   */
  public String getCurrentStreamName() {
    return getName();
  }

  /** 
   * 获取该编辑日志输入流的名称
   * @return 输入流名称
   */
  public abstract String getName();
  
  /** 
   * 获取该流中第一个事务的事务ID
   * @return 首事务ID
   */
  public abstract long getFirstTxId();
  
  /** 
   * 获取该流中最后一个事务的事务ID
   * @return 尾事务ID
   */
  public abstract long getLastTxId();


  /**
   * 关闭输入流，释放资源
   * @throws IOException 关闭过程中发生IO异常
   */
  @Override
  public abstract void close() throws IOException;

  /** 
   * 从流中读取一个编辑日志操作，优先返回缓存的操作
   * @return 读取到的操作，到达流末尾则返回null
   * @throws IOException 读取过程中发生IO异常
   */
  public FSEditLogOp readOp() throws IOException {
    FSEditLogOp ret;
    if (cachedOp != null) {
      // 有缓存操作，取出缓存并清空缓存后返回
      ret = cachedOp;
      cachedOp = null;
      return ret;
    }
    // 无缓存，从流读取下一个操作
    return nextOp();
  }
  
  /** 
   * 重新同步流位置，确保下一次readOp能读取到有效操作
   * 可用于跳过编辑日志中损坏的区块
   */
  public void resync() {
    // 已有缓存操作，直接返回，无需重新同步
    if (cachedOp != null) {
      return;
    }
    // 查找下一个有效操作并缓存
    cachedOp = nextValidOp();
  }
  
  /** 
   * 从存储中读取下一个编辑日志操作，由子类实现
   * @return 读取到的操作，到达流末尾则返回null
   * @throws IOException 读取过程中发生IO异常
   */
  protected abstract FSEditLogOp nextOp() throws IOException;

  /**
   * 扫描流获取下一个操作的事务ID，不保留操作内容
   * @return 下一个操作的事务ID，到达末尾返回无效事务ID
   * @throws IOException 扫描过程中发生IO异常
   */
  protected long scanNextOp() throws IOException {
    FSEditLogOp next = readOp();
    return next != null ? next.txid : HdfsServerConstants.INVALID_TXID;
  }
  
  /** 
   * 从存储中获取下一个有效操作，会尝试跳过日志损坏部分
   * 默认实现简单处理：遇到错误直接返回null，支持错误恢复的子类需要覆盖此方法
   * 
   * @return 有效操作，到达末尾或遇到不可恢复错误则返回null
   */
  protected FSEditLogOp nextValidOp() {
    try {
      return nextOp();
    } catch (Throwable e) {
      // 默认实现遇到异常直接终止，返回null
      return null;
    }
  }
  
  /** 
   * 跳过操作直到找到指定事务ID，或到达编辑日志末尾
   * 方法返回后，下一次readOp调用会返回null（末尾）或txid大于等于目标的事务
   *
   * @param txid 目标事务ID，需要跳到该ID或之后
   * @return 如果找到txid大于等于目标的事务返回true，否则返回false
   */
  public boolean skipUntil(long txid) throws IOException {
    while (true) {
      FSEditLogOp op = readOp();
      if (op == null) {
        // 已到流末尾，未找到目标事务
        return false;
      }
      if (op.getTransactionId() >= txid) {
        // 找到目标，缓存操作后返回
        cachedOp = op;
        return true;
      }
    }
  }

  /**
   * 获取并清空缓存的操作
   * @return 之前缓存的操作
   */
  FSEditLogOp getCachedOp() {
    FSEditLogOp op = this.cachedOp;
    cachedOp = null;
    return op;
  }
  
  /** 
   * 获取当前流中编辑日志的布局版本号
   * @param verifyVersion 是否需要验证版本合法性
   * @return 当前流的布局版本号
   * @throws IOException 读取版本过程中发生IO错误
   */
  public abstract int getVersion(boolean verifyVersion) throws IOException;

  /**
   * 获取当前流的读取位置，用于调试和运维
   * 不同流类型位置含义不同，文件流中表示从文件开头开始的字节偏移量
   *
   * @return 当前读取位置
   */
  public abstract long getPosition();

  /**
   * 获取当前编辑日志的总大小，未知则返回-1
   * 
   * @return 当前编辑日志大小，单位字节，未知返回-1
   */
  public abstract long length() throws IOException;
  
  /**
   * 判断当前编辑日志是否处于正在写入的进行中状态
   * @return true表示日志还在写入，false表示日志已经封闭完成
   */
  public abstract boolean isInProgress();
  
  /**
   * 设置单个操作允许的最大字节大小，防止OOM
   * @param maxOpSize 最大操作大小，单位字节
   */
  public abstract void setMaxOpSize(int maxOpSize);

  /**
   * 判断当前日志是否来自本地磁盘或更快的数据源（如内存缓冲区）
   * @return true表示本地/高速数据源，false表示远程数据源
   */
  public abstract boolean isLocalLog();

  @Override
  public String toString() {
    return getName();
  }
}