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
package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.ipc.StandbyException;

/**
 * HDFS高可用(HA)场景下NameNode状态上下文接口，为HAState提供状态管理、服务启停和操作校验能力
 * 定义了NameNode在Active/Standby状态切换过程中需要执行的核心操作契约，供具体NameNode实现
 */
@InterfaceAudience.Private
public interface HAContext {
  /**
   * 将上下文当前状态设置为指定HA状态
   * @param state 目标HA状态
   */
  public void setState(HAState state);
  
  /**
   * 获取上下文当前的HA状态
   * @return 当前HA状态
   */
  public HAState getState();
  
  /**
   * 启动Active状态NameNode所需的核心服务
   * @throws IOException 启动服务失败时抛出异常
   */
  public void startActiveServices() throws IOException;
  
  /**
   * 退出Active状态时停止对应核心服务
   * @throws IOException 停止服务失败时抛出异常
   */
  public void stopActiveServices() throws IOException;
  
  /**
   * 启动Standby状态NameNode所需的核心服务
   * @throws IOException 启动服务失败时抛出异常
   */
  public void startStandbyServices() throws IOException;

  /**
   * 退出Standby状态前的准备工作
   * @throws ServiceFailedException 准备失败时抛出异常
   */
  public void prepareToStopStandbyServices() throws ServiceFailedException;

  /**
   * 退出Standby状态时停止对应核心服务
   * @throws IOException 停止服务失败时抛出异常
   */
  public void stopStandbyServices() throws IOException;

  /**
   * 对底层命名系统加写锁，防止并发状态转换和编辑操作，保证状态切换的线程安全
   */
  void writeLock();

  /**
   * 释放writeLock()获取的写锁
   */
  void writeUnlock();

  /**
   * 校验当前HA状态是否允许执行指定类别的操作
   * 允许不同NameNode实现（如BackupNode）自定义节点专属的校验逻辑
   * 
   * 对于需要获取FSNS锁的操作，建议在获取锁前后各校验一次。这是因为客户端依赖此方法抛出的
   * StandbyException触发故障转移，如果客户端先连接到StandbyNameNode，若Standby正在执行
   * Checkpoint长时间持有锁，会导致客户端长时间阻塞。更多细节见HDFS-4591
   * 
   * @param op 待校验的操作类别（读/写）
   * @throws StandbyException 当前状态不允许该操作时抛出异常
   */
  void checkOperation(OperationCategory op) throws StandbyException;

  /**
   * 判断节点是否允许过时读（即在命名空间数据不是最新时允许读操作）
   * @return true表示允许过时读，false表示不允许
   */
  boolean allowStaleReads();
}