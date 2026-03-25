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

package org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions;

import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * YARN资源预留规划异常，当预留准入控制子系统无法为用户的预留提交请求找到可用资源分配时抛出该异常。
 * @see ReservationSubmissionRequest
 */

@Public
@Unstable
public class PlanningException extends Exception {

  private static final long serialVersionUID = -684069387367879218L;

  /**
   * 使用指定错误信息构造规划异常。
   * @param message 错误描述信息
   */
  public PlanningException(String message) {
    super(message);
  }

  /**
   * 使用指定根异常构造规划异常。
   * @param cause 根异常原因
   */
  public PlanningException(Throwable cause) {
    super(cause);
  }

  /**
   * 使用指定错误信息和根异常构造规划异常。
   * @param message 错误描述信息
   * @param cause 根异常原因
   */
  public PlanningException(String message, Throwable cause) {
    super(message, cause);
  }

}