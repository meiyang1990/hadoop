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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 数据节点发送全量块报告到名称节点时，若租约无效（已过期或其他问题）被拒绝，抛出此异常。
 * 用于标识块报告携带的租约验证失败，需要数据节点重新发起块报告流程。
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class InvalidBlockReportLeaseException extends IOException {
  /** 用于Java序列化机制的版本标识 */
  private static final long serialVersionUID = 1L;

  /**
   * 构造包含块报告ID和无效租约ID的异常实例
   * @param blockReportID 被拒绝的块报告ID
   * @param leaseID 无效的租约ID
   */
  public InvalidBlockReportLeaseException(long blockReportID, long leaseID) {
    super("Block report 0x" + Long.toHexString(blockReportID) + " was rejected as lease 0x"
        + Long.toHexString(leaseID) +  " is invalid");
  }
}