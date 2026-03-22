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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.util.Time;

/**
 * HDFS高可用环境下NameNode状态基类，实现状态机模式，定义状态公共接口和核心转换逻辑。
 * 不同高可用状态（活跃/待命/正在切换等）继承此类实现各自状态行为。
 */
@InterfaceAudience.Private
abstract public class HAState {
  protected final HAServiceState state;
  private long lastHATransitionTime;

  /**
   * 构造方法，基于通用HA服务状态创建状态对象
   * @param state HA服务通用状态枚举
   */
  public HAState(HAServiceState state) {
    this.state = state;
  }

  /**
   * 获取当前状态对应的通用HA服务状态枚举
   * @return 通用HA服务状态
   */
  public HAServiceState getServiceState() {
    return state;
  }

  /**
   * 状态转换核心内部方法，按固定流程完成从旧状态到新状态的切换
   * @param context HA状态上下文，持有当前NameNode的HA状态信息
   * @param s 目标新状态
   * @throws ServiceFailedException 状态转换失败时抛出异常
   */
  protected final void setStateInternal(final HAContext context, final HAState s)
      throws ServiceFailedException {
    // 旧状态准备退出，做前置检查和非破坏性准备工作
    prepareToExitState(context);
    // 新状态准备进入，做前置检查和准备工作
    s.prepareToEnterState(context);
    // 获取上下文写锁，保证状态转换原子性
    context.writeLock();
    try {
      // 旧状态执行退出清理工作
      exitState(context);
      // 更新上下文状态为新状态
      context.setState(s);
      // 新状态执行进入初始化工作
      s.enterState(context);
      // 更新最近一次状态转换时间
      s.updateLastHATransitionTime();
    } finally {
      // 释放写锁
      context.writeUnlock();
    }
  }

  /**
   * 获取最近一次HA状态转换的时间戳（毫秒，从纪元开始）
   * @return 最近一次状态转换时间戳
   */
  public long getLastHATransitionTime() {
    return lastHATransitionTime;
  }

  /**
   * 更新最近一次HA状态转换时间为当前时间
   */
  private void updateLastHATransitionTime() {
    lastHATransitionTime = Time.now();
  }

  /**
   * 准备进入当前状态的前置工作，在加锁前调用，可被子类重写实现自定义逻辑
   * @param context HA状态上下文
   * @throws ServiceFailedException 前置条件不满足时抛出异常
   */
  public void prepareToEnterState(final HAContext context)
      throws ServiceFailedException {}

  /**
   * 进入当前状态的初始化工作，加锁后调用，子类必须实现该方法
   * @param context HA状态上下文
   * @throws ServiceFailedException 进入状态失败时抛出异常
   */
  public abstract void enterState(final HAContext context)
      throws ServiceFailedException;

  /**
   * 准备退出当前状态的前置工作，在加锁前调用，可被子类重写实现自定义逻辑
   * 不做破坏性变更，仅做前置检查和取消正在进行的操作，避免状态转换失败后留下错误状态
   * @param context HA状态上下文
   * @throws ServiceFailedException 前置条件不满足时抛出异常
   */
  public void prepareToExitState(final HAContext context)
      throws ServiceFailedException {}

  /**
   * 退出当前状态的清理工作，加锁后调用，子类必须实现该方法
   * @param context HA状态上下文
   * @throws ServiceFailedException 退出状态失败时抛出异常
   */
  public abstract void exitState(final HAContext context)
      throws ServiceFailedException;

  /**
   * 从当前状态转换到目标状态，默认仅允许同状态转换，不同状态转换需要子类重写允许合法转换
   * @param context HA状态上下文
   * @param s 目标新状态
   * @throws ServiceFailedException 不允许该转换时抛出异常
   */
  public void setState(HAContext context, HAState s) throws ServiceFailedException {
    if (this == s) { // 已经处于目标状态，直接返回不需要转换
      return;
    }
    throw new ServiceFailedException("Transition from state " + this + " to "
        + s + " is not allowed.");
  }
  
  /**
   * 检查当前状态是否支持指定类型的操作，不支持则抛出异常
   * @param context HA状态上下文
   * @param op 操作类型（读/写等）
   * @throws StandbyException 待命状态不支持写操作时抛出异常
   */
  public abstract void checkOperation(final HAContext context, final OperationCategory op)
      throws StandbyException;

  /**
   * 检查当前状态是否需要填充复制队列，用于块复制任务调度
   * @return true表示需要填充，false不需要
   */
  public abstract boolean shouldPopulateReplQueues();

  /**
   * 返回当前状态的字符串表示
   * @return 状态字符串
   */
  @Override
  public String toString() {
    return state.toString();
  }
}