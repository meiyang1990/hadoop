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
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;

/**
 * 文件: org.apache.hadoop.hdfs.server.namenode.ha.ActiveState.java
 * 所属模块: HDFS NameNode高可用模块
 * 核心职责: 实现NameNode高可用中的Active状态逻辑，定义NameNode作为活跃节点时的状态行为
 *
 * Active状态的NameNode，提供完整的命名空间服务，允许处理读和写所有类型操作。
 */
/**
 * Active状态实现类，定义NameNode处于活跃状态时的行为逻辑
 * 在该状态下，NameNode提供完整的NameNode服务，可处理读写所有类型操作
 */
@InterfaceAudience.Private
public class ActiveState extends HAState {
  /**
   * 构造Active状态对象，设置状态类型为ACTIVE
   */
  public ActiveState() {
    super(HAServiceState.ACTIVE);
  }

  /**
   * 检查当前状态是否允许执行指定类型操作
   * Active状态允许所有操作，直接返回不做检查
   * @param context HA状态上下文
   * @param op 操作类型
   */
  @Override
  public void checkOperation(HAContext context, OperationCategory op) {
    return; // All operations are allowed in active state
  }
  
  /**
   * 判断是否需要填充复制队列（用于数据块复制恢复）
   * @return Active状态下返回true，允许块复制处理
   */
  @Override
  public boolean shouldPopulateReplQueues() {
    return true;
  }
  
  /**
   * 切换到目标状态，处理Active到其他状态的转换逻辑
   * @param context HA状态上下文
   * @param s 目标状态
   * @throws ServiceFailedException 状态切换失败时抛出异常
   */
  @Override
  public void setState(HAContext context, HAState s) throws ServiceFailedException {
    if (s == NameNode.STANDBY_STATE) {
      // 切换到Standby状态，执行内部状态转换逻辑
      setStateInternal(context, s);
      return;
    }
    // 其他状态转换调用父类默认处理逻辑
    super.setState(context, s);
  }

  /**
   * 进入Active状态执行的初始化逻辑
   * @param context HA状态上下文
   * @throws ServiceFailedException 启动活跃服务失败时抛出异常
   */
  @Override
  public void enterState(HAContext context) throws ServiceFailedException {
    try {
      // 启动所有活跃模式需要的服务
      context.startActiveServices();
    } catch (IOException e) {
      throw new ServiceFailedException("Failed to start active services", e);
    }
  }

  /**
   * 退出Active状态执行的清理逻辑
   * @param context HA状态上下文
   * @throws ServiceFailedException 停止活跃服务失败时抛出异常
   */
  @Override
  public void exitState(HAContext context) throws ServiceFailedException {
    try {
      // 停止所有活跃模式提供的服务
      context.stopActiveServices();
    } catch (IOException e) {
      throw new ServiceFailedException("Failed to stop active services", e);
    }
  }

}