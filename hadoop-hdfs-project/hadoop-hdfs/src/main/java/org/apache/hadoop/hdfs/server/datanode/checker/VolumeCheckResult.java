// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode.checker;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * DataNode磁盘健康检查结果枚举，定义了存储卷磁盘检查完成后的不同健康状态结果。
 * 用于表示数据节点上单个存储卷的健康状况，供DataNode故障检测和自愈流程使用。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum VolumeCheckResult {
  /** 磁盘健康，读写正常 */
  HEALTHY(1),
  /** 磁盘降级，存在异常但仍可提供服务 */
  DEGRADED(2),
  /** 磁盘完全故障，无法继续使用 */
  FAILED(3);

  private final int value;

  /**
   * 构造检查结果枚举，绑定对应的数值标识
   * @param value 结果对应的数值编码
   */
  VolumeCheckResult(int value) {
    this.value = value;
  }

  /**
   * 获取当前检查结果的数值编码
   * @return 结果对应的数值编码
   */
  int getValue() {
    return value;
  }
}