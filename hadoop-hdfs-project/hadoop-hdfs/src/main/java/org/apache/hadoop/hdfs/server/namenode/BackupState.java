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
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.server.namenode.ha.HAState;
import org.apache.hadoop.ipc.StandbyException;

/**
 * HDFS高可用场景下，Backup Namenode的状态实现类。
 * 负责定义Backup节点状态下的操作权限检查、状态进入/退出处理逻辑，
 * Backup节点作为热备，会保持与Active节点的状态同步，可以快速切换为Active。
 */
@InterfaceAudience.Private
public class BackupState extends HAState {

  /**
   * 构造Backup状态对象，基础状态为Standby。
   */
  public BackupState() {
    super(HAServiceState.STANDBY);
  }

  /**
   * 检查当前Backup状态下是否允许执行指定类型的操作。
   * @param context HA上下文环境
   * @param op 操作类别（读/写）
   * @throws StandbyException 如果操作不允许则抛出异常
   */
  @Override // HAState
  public void checkOperation(HAContext context, OperationCategory op)
      throws StandbyException {
    context.checkOperation(op);
  }

  /**
   * 判断当前状态是否需要填充复制队列。
   * @return false，Backup状态不需要填充复制队列
   */
  @Override // HAState
  public boolean shouldPopulateReplQueues() {
    return false;
  }

  /**
   * 进入Backup状态，启动Backup节点所需的服务。
   * @param context HA上下文环境
   * @throws ServiceFailedException 启动服务失败时抛出异常
   */
  @Override // HAState
  public void enterState(HAContext context) throws ServiceFailedException {
    try {
      context.startActiveServices();
    } catch (IOException e) {
      throw new ServiceFailedException("Failed to start backup services", e);
    }
  }

  /**
   * 退出Backup状态，停止Backup节点运行的服务。
   * @param context HA上下文环境
   * @throws ServiceFailedException 停止服务失败时抛出异常
   */
  @Override // HAState
  public void exitState(HAContext context) throws ServiceFailedException {
    try {
      context.stopActiveServices();
    } catch (IOException e) {
      throw new ServiceFailedException("Failed to stop backup services", e);
    }
  }

  /**
   * 准备退出Backup状态，执行预停止操作。
   * @param context HA上下文环境
   * @throws ServiceFailedException 准备操作失败时抛出异常
   */
  @Override // HAState
  public void prepareToExitState(HAContext context) throws ServiceFailedException {
    context.prepareToStopStandbyServices();
  }
}