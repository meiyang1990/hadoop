// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.diskbalancer;

import java.io.IOException;

/**
 * 磁盘均衡器异常类，用于封装磁盘均衡过程中出现的各类错误
 */
public class DiskBalancerException extends IOException {
  /**
   * 磁盘均衡RPC层返回的错误结果枚举，定义了所有可能的错误类型
   */
  public enum Result {
    /** 磁盘均衡功能未启用 */
    DISK_BALANCER_NOT_ENABLED,
    /** 均衡计划版本无效 */
    INVALID_PLAN_VERSION,
    /** 均衡计划无效 */
    INVALID_PLAN,
    /** 均衡计划哈希校验不通过 */
    INVALID_PLAN_HASH,
    /** 提交了已过期的旧计划 */
    OLD_PLAN_SUBMITTED,
    /** 数据节点ID不匹配 */
    DATANODE_ID_MISMATCH,
    /** 均衡计划格式错误 */
    MALFORMED_PLAN,
    /** 已有均衡计划正在执行 */
    PLAN_ALREADY_IN_PROGRESS,
    /** 卷信息无效 */
    INVALID_VOLUME,
    /** 数据移动操作无效 */
    INVALID_MOVE,
    /** 内部错误 */
    INTERNAL_ERROR,
    /** 指定的计划不存在 */
    NO_SUCH_PLAN,
    /** 未知密钥 */
    UNKNOWN_KEY,
    /** 节点信息无效 */
    INVALID_NODE,
    /** 数据节点状态不正常 */
    DATANODE_STATUS_NOT_REGULAR,
    /** 主机文件路径无效 */
    INVALID_HOST_FILE_PATH,
  }

  private final Result result;

  /**
   * 构造带错误信息和错误结果的DiskBalancerException对象
   *
   * @param message 异常详细信息
   * @param result 错误结果枚举
   */
  public DiskBalancerException(String message, Result result) {
    super(message);
    this.result = result;
  }

  /**
   * 构造带错误信息、根异常和错误结果的DiskBalancerException对象
   *
   * @param message 异常详细信息
   * @param cause   根异常对象
   * @param result  错误结果枚举
   */
  public DiskBalancerException(String message, Throwable cause, Result result) {
    super(message, cause);
    this.result = result;
  }

  /**
   * 构造带根异常和错误结果的DiskBalancerException对象
   *
   * @param cause  根异常对象
   * @param result 错误结果枚举
   */
  public DiskBalancerException(Throwable cause, Result result) {
    super(cause);
    this.result = result;
  }

  /**
   * 获取当前异常对应的错误结果枚举
   * @return 错误结果枚举
   */
  public Result getResult() {
    return result;
  }
}