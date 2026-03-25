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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;

/**
 * 块管理器故障注入器，用于HDFS单元测试场景中注入特定故障，验证异常处理逻辑。
 * 提供多个可重写的注入点，在块报告处理等关键流程触发预设故障。
 */
public class BlockManagerFaultInjector {
  @VisibleForTesting
  public static BlockManagerFaultInjector instance =
      new BlockManagerFaultInjector();

  /**
   * 获取故障注入器单例实例，供测试代码获取注入点。
   * @return 故障注入器单例
   */
  @VisibleForTesting
  public static BlockManagerFaultInjector getInstance() {
    return instance;
  }

  /**
   * 在接收到数据节点块报告RPC时注入故障，可模拟RPC处理异常场景。
   * @param nodeID 发送块报告的数据节点ID
   * @param context 块报告上下文信息
   * @throws IOException 可抛出预设的IO异常用于测试
   */
  @VisibleForTesting
  public void incomingBlockReportRpc(DatanodeID nodeID,
          BlockReportContext context) throws IOException {

  }

  /**
   * 在请求块报告租约时注入故障，用于测试租约申请异常流程。
   * @param node 申请租约的数据节点描述符
   * @param leaseId 申请的租约ID
   */
  @VisibleForTesting
  public void requestBlockReportLease(DatanodeDescriptor node, long leaseId) {
  }

  /**
   * 在移除块报告租约时注入故障，用于测试租约移除异常流程。
   * @param node 移除租约的数据节点描述符
   * @param leaseId 要移除的租约ID
   */
  @VisibleForTesting
  public void removeBlockReportLease(DatanodeDescriptor node, long leaseId) {
  }

  /**
   * 模拟抛出异常，用于通用异常场景测试。
   */
  @VisibleForTesting
  public void mockAnException() {
  }
}