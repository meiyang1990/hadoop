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
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.ipc.ObserverRetryOnActiveException;
import org.apache.hadoop.ipc.StandbyException;

/**
 * HDFS高可用环境下NameNode的备节点状态类。
 * 该状态下NameNode作为热备节点运行，持续同步以下元数据保持最新：
 * <ul>
 * <li>通过获取edit日志同步命名空间信息</li>
 * <li>通过接收DataNode的块报告和块信息，维护数据块位置信息</li>
 * </ul>
 * 
 * 该状态下不处理客户端读写和检查点操作。同时支持Observer只读状态的变体实现。
 */
@InterfaceAudience.Private
public class StandbyState extends HAState {
  // TODO: 考虑单独实现ObserverState类而不是通过该标志位实现
  /** 标识是否为Observer只读状态，true表示是Observer，false表示普通Standby */
  private final boolean isObserver;

  /**
   * 构造普通Standby状态实例
   */
  public StandbyState() {
    this(false);
  }

  /**
   * 构造Standby/Observer状态实例
   * @param isObserver true表示构造Observer状态，false表示构造普通Standby状态
   */
  public StandbyState(boolean isObserver) {
    super(isObserver ? HAServiceState.OBSERVER : HAServiceState.STANDBY);
    this.isObserver = isObserver;
  }

  /**
   * 从当前状态切换到目标状态，检查允许的状态转换
   * @param context HA状态上下文
   * @param s 目标状态
   * @throws ServiceFailedException 状态切换失败时抛出
   */
  @Override
  public void setState(HAContext context, HAState s) throws ServiceFailedException {
    if (s == NameNode.ACTIVE_STATE ||
        (!isObserver && s == NameNode.OBSERVER_STATE) ||
        (isObserver && s == NameNode.STANDBY_STATE)) {
      // 允许的状态转换，执行内部状态切换逻辑
      setStateInternal(context, s);
      return;
    }
    // 不允许的转换，调用父类处理
    super.setState(context, s);
  }

  /**
   * 进入Standby状态，启动备节点所需服务
   * @param context HA状态上下文
   * @throws ServiceFailedException 启动服务失败时抛出
   */
  @Override
  public void enterState(HAContext context) throws ServiceFailedException {
    try {
      context.startStandbyServices();
    } catch (IOException e) {
      throw new ServiceFailedException("Failed to start standby services", e);
    }
  }

  /**
   * 准备退出Standby状态，执行停止服务前的准备工作
   * @param context HA状态上下文
   * @throws ServiceFailedException 准备过程失败时抛出
   */
  @Override
  public void prepareToExitState(HAContext context) throws ServiceFailedException {
    context.prepareToStopStandbyServices();
  }

  /**
   * 退出Standby状态，停止备节点服务
   * @param context HA状态上下文
   * @throws ServiceFailedException 停止服务失败时抛出
   */
  @Override
  public void exitState(HAContext context) throws ServiceFailedException {
    try {
      context.stopStandbyServices();
    } catch (IOException e) {
      throw new ServiceFailedException("Failed to stop standby services", e);
    }
  }

  /**
   * 检查当前状态是否允许执行指定类型的操作
   * @param context HA状态上下文
   * @param op 操作类别（读/写/不检查）
   * @throws StandbyException 操作不被允许时抛出
   */
  @Override
  public void checkOperation(HAContext context, OperationCategory op)
      throws StandbyException {
    // 放开不检查的操作，或者允许 stale 读时放开读操作
    if (op == OperationCategory.UNCHECKED ||
        (op == OperationCategory.READ && context.allowStaleReads())) {
      return;
    }
    String faq = ". Visit https://s.apache.org/sbnn-error";
    String msg = "Operation category " + op + " is not supported in state "
        + context.getState() + faq;
    if (op == OperationCategory.WRITE && isObserver) {
      // 如果Observer收到写请求，返回让客户端去Active节点重试的异常
      // 写操作本不应该出现在Observer上，但访问时间开启时，open操作会转为写操作
      // 这种情况需要通知客户端到Active节点重试该open操作
      throw new ObserverRetryOnActiveException(msg);
    } else {
      throw new StandbyException(msg);
    }
  }

  /**
   * 当前状态是否需要填充复制队列
   * @return 始终返回false，Standby状态不需要处理块复制任务
   */
  @Override
  public boolean shouldPopulateReplQueues() {
    return false;
  }

  @Override
  public String toString() {
    return isObserver ? "observer" : "standby";
  }
}