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

package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 文件描述：数据节点块报告上下文信息容器，存储块报告RPC调用的拆分元数据
 * 
 * 当数据节点需要上报大量块信息时，会将整个块报告拆分为多个RPC请求发送给NameNode，
 * 此类用于携带整个块报告的全局标识和当前RPC请求的分片信息，帮助NameNode组装完整块报告。
 */
@InterfaceAudience.Private
public class BlockReportContext {
  /**
   * 整个块报告拆分后的总RPC请求数量
   */
  private final int totalRpcs;

  /**
   * 当前RPC请求在整个块报告中的分片索引
   */
  private final int curRpc;

  /**
   * 标识整个块报告的全局唯一64位ID
   */
  private final long reportId;

  /**
   * 当前块报告使用的限流租约ID，若为0表示本次块报告绕过速率限制
   */
  private final long leaseId;

  /**
   * 构造块报告上下文对象
   * @param totalRpcs 整个块报告拆分的总RPC数量
   * @param curRpc 当前RPC请求的分片索引
   * @param reportId 块报告全局唯一ID
   * @param leaseId 限流租约ID，0表示不限制
   */
  public BlockReportContext(int totalRpcs, int curRpc,
                            long reportId, long leaseId) {
    this.totalRpcs = totalRpcs;
    this.curRpc = curRpc;
    this.reportId = reportId;
    this.leaseId = leaseId;
  }

  /**
   * 获取整个块报告拆分的总RPC数量
   * @return 总RPC数量
   */
  public int getTotalRpcs() {
    return totalRpcs;
  }

  /**
   * 获取当前RPC请求的分片索引
   * @return 当前RPC索引
   */
  public int getCurRpc() {
    return curRpc;
  }

  /**
   * 获取块报告全局唯一ID
   * @return 块报告ID
   */
  public long getReportId() {
    return reportId;
  }

  /**
   * 获取限流租约ID
   * @return 租约ID，0表示不限流
   */
  public long getLeaseId() {
    return leaseId;
  }
}